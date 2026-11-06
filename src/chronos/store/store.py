"""Versioned single-writer store with compare-and-swap updates (ADR-0007).

Every successful write bumps one global revision and stamps it on the object as
`metadata.resource_version`. Updates are accepted only when the caller's
`resource_version` matches the stored one, so a writer working from a stale copy
gets `Conflict` instead of silently erasing someone else's write (S5). Failed
writes change nothing, revision included.

This is the synchronous core: no `await`, so a single caller is a single writer.
The server wraps it in an async façade that serializes calls. Objects are
deep-copied on the way in and out, so callers can never mutate stored state.

Durability (ADR-0015): with a WAL, every change becomes one record (`put` of the whole
object, or `delete`) that is written and synced *before* memory changes, so an
acknowledged write survives a crash (S8). `Store.open()` rebuilds the state from the
newest snapshot plus the records after it, through the same `_apply_*` functions the
live path uses.
"""

from __future__ import annotations

import builtins
import logging
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path

from chronos.api import Deployment, Node, NodeCondition, Pod, PodPhase
from chronos.api.kinds import Obj
from chronos.api.resources import add, insufficient, sub
from chronos.common.clock import Clock
from chronos.store.codec import Record
from chronos.store.errors import (
    AlreadyExists,
    Conflict,
    InsufficientCapacity,
    Invalid,
    NotFound,
    Unavailable,
)
from chronos.store.validation import (
    TERMINAL,
    validate_create,
    validate_spec_update,
    validate_status_update,
)
from chronos.store.wal import DEFAULT_SEGMENT_SIZE, Snapshot, Wal, recover

log = logging.getLogger(__name__)

KINDS: tuple[type[Obj], ...] = (Node, Pod, Deployment)
MODELS: dict[str, type[Obj]] = {kind.__name__: kind for kind in KINDS}
DEFAULT_SNAPSHOT_EVERY = 5_000

UNBOUND = ""


def _new_uid() -> str:
    return uuid.uuid4().hex


class Store:
    def __init__(
        self,
        clock: Clock,
        uid_factory: Callable[[], str] = _new_uid,
        *,
        wal: Wal | None = None,
        snapshot_every: int = DEFAULT_SNAPSHOT_EVERY,
    ) -> None:
        self._clock = clock
        self._new_uid = uid_factory
        self._revision = 0
        self._objects: dict[str, dict[str, Obj]] = {kind.__name__: {} for kind in KINDS}
        # Secondary index: node name -> names of pods bound to it ("" = unbound).
        self._pods_by_node: dict[str, set[str]] = {}
        self._wal = wal
        self._snapshot_every = snapshot_every
        self._since_snapshot = 0

    @classmethod
    def open(
        cls,
        data_dir: Path,
        clock: Clock,
        *,
        sync: bool = True,
        segment_size: int = DEFAULT_SEGMENT_SIZE,
        snapshot_every: int = DEFAULT_SNAPSHOT_EVERY,
        uid_factory: Callable[[], str] = _new_uid,
    ) -> Store:
        """Recover the state in `data_dir` (created if missing) and keep logging there.

        Raises WalCorrupt if the log is damaged anywhere but a torn final record.
        """
        data_dir.mkdir(parents=True, exist_ok=True)
        found = recover(data_dir)
        store = cls(clock, uid_factory, snapshot_every=snapshot_every)
        if found.snapshot is not None:
            for kind, obj in found.snapshot.objects:
                store._apply_put(MODELS[kind].model_validate(obj))
            store._revision = found.snapshot.revision
        for record in found.records:
            store._apply(record)
        store._wal = Wal(data_dir, store._revision + 1, sync=sync, segment_size=segment_size)
        log.info(
            "recovered revision %d (snapshot %s + %d records%s)",
            store._revision,
            found.snapshot.revision if found.snapshot else "none",
            len(found.records),
            f", torn tail cut at {found.truncated[0]}:{found.truncated[1]}"
            if found.truncated
            else "",
        )
        return store

    def close(self) -> None:
        if self._wal is not None:
            self._wal.close()

    @property
    def revision(self) -> int:
        return self._revision

    # ---- reads ---------------------------------------------------------------

    def get[T: (Node, Pod, Deployment)](self, kind: type[T], name: str) -> T:
        return self._present(self._get(kind, name))

    def list[T: (Node, Pod, Deployment)](
        self,
        kind: type[T],
        *,
        labels: Mapping[str, str] | None = None,
        node_name: str | None = None,
    ) -> tuple[builtins.list[T], int]:
        """Objects sorted by name, plus the revision they were read at.

        `node_name` filters pods through the index ("" selects unbound pods).
        """
        if node_name is not None and kind is not Pod:
            raise Invalid("node_name filter only applies to Pod")
        if node_name is None:
            names = sorted(self._objects[kind.__name__])
        else:
            names = sorted(self._pods_by_node.get(node_name, ()))
        items = [self._get(kind, name) for name in names]
        if labels:
            items = [o for o in items if labels.items() <= o.metadata.labels.items()]
        return [self._present(o) for o in items], self._revision

    def pods_on(self, node_name: str) -> builtins.list[Pod]:
        return self.list(Pod, node_name=node_name)[0]

    def allocated(self, node_name: str) -> dict[str, int]:
        """Sum of requests of the non-terminal pods bound to the node (derived, never stored)."""
        total: dict[str, int] = {}
        for name in self._pods_by_node.get(node_name, ()):
            pod = self._get(Pod, name)
            if pod.status.phase not in TERMINAL:
                total = add(total, pod.requests)
        return total

    # ---- writes --------------------------------------------------------------

    def create[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        name = obj.metadata.name
        if name in self._objects[obj.kind]:
            raise AlreadyExists(obj.kind, name)
        validate_create(obj)
        new = obj.model_copy(deep=True)
        new.metadata.uid = self._new_uid()
        new.metadata.generation = 1
        new.metadata.created_at = self._clock.wall()
        new.metadata.deletion_timestamp = None
        return self._commit(new)

    def update[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        """CAS write of spec and labels; status in `obj` is ignored."""
        current = self._get_for_write(obj)
        validate_spec_update(current, obj)
        new = current.model_copy(deep=True)
        new.metadata.labels = dict(obj.metadata.labels)
        new.spec = obj.spec.model_copy(deep=True)
        if isinstance(new, Pod) and isinstance(current, Pod):
            new.spec.node_name = current.spec.node_name
        if isinstance(new, Node) and (
            over := insufficient(self.allocated(new.metadata.name), new.spec.capacity)
        ):
            raise Invalid(
                f"Node {new.metadata.name!r}: capacity below allocated for {', '.join(over)}"
            )
        if new.spec != current.spec:
            new.metadata.generation += 1
        return self._commit(new)

    def update_status[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        """CAS write of status only; spec and metadata in `obj` are ignored."""
        current = self._get_for_write(obj)
        validate_status_update(current, obj)
        new = current.model_copy(deep=True)
        new.status = obj.status.model_copy(deep=True)
        if isinstance(new, Node):
            new.status.allocated = {}  # derived on read; never stored
        return self._commit(new)

    def delete[T: (Node, Pod, Deployment)](
        self, kind: type[T], name: str, expected_rv: int | None = None, *, force: bool = False
    ) -> T:
        """Delete an object (P4.7: two-phase for pods that may have a running process).

        A bound, unfinished pod is only marked: `deletion_timestamp` is set and the pod
        stays until its agent has stopped the process and deletes it with `force`.
        Deleting a pod that's already terminating changes nothing. Anything else
        (unbound or finished pods, other kinds, `force`) is removed at once.
        """
        current = self._get(kind, name)
        if expected_rv is not None:
            self._check_rv(current, expected_rv)
        if (
            isinstance(current, Pod)
            and not force
            and current.spec.node_name
            and current.status.phase not in TERMINAL
        ):
            if current.metadata.deletion_timestamp is not None:
                return self._present(current)  # idempotent: already terminating
            marked = current.model_copy(deep=True)
            marked.metadata.deletion_timestamp = self._clock.wall()
            return self._commit(marked)
        gone = current.model_copy(deep=True)
        gone.metadata.resource_version = self._log(
            Record(self._revision + 1, "delete", kind.__name__, name)
        )
        self._apply_delete(kind.__name__, name)
        return gone

    def bind(self, pod_name: str, node_name: str, expected_rv: int) -> Pod:
        """Atomically assign an unbound pod to a node that has room for it (S1, S2, S11).

        Capacity is re-checked here for every requested resource name, whatever the
        scheduler decided: a node that doesn't advertise a resource has 0 of it.
        """
        pod = self._get(Pod, pod_name)
        self._check_rv(pod, expected_rv)
        if pod.spec.node_name:
            raise Invalid(f"Pod {pod_name!r} is already bound to {pod.spec.node_name!r}")
        if pod.status.phase in TERMINAL or pod.metadata.deletion_timestamp is not None:
            raise Invalid(f"Pod {pod_name!r} is {pod.status.phase}; can't bind")
        node = self._get(Node, node_name)
        if node.status.condition != NodeCondition.READY:
            raise Invalid(f"Node {node_name!r} is {node.status.condition}, not Ready")
        if node.spec.unschedulable:
            raise Invalid(f"Node {node_name!r} is unschedulable")
        free = sub(node.spec.capacity, self.allocated(node_name))
        if missing := insufficient(pod.requests, free):
            raise InsufficientCapacity(node_name, missing)

        new = pod.model_copy(deep=True)
        # Binding is a system write, not a user spec change, so generation stays.
        new.spec.node_name = node_name
        new.status.phase = PodPhase.BOUND
        new.status.reason, new.status.message = "", ""  # e.g. a stale "Unschedulable"
        return self._commit(new)

    # ---- internals -----------------------------------------------------------

    def _get[T: (Node, Pod, Deployment)](self, kind: type[T], name: str) -> T:
        obj = self._objects[kind.__name__].get(name)
        if not isinstance(obj, kind):
            raise NotFound(kind.__name__, name)
        return obj

    def _get_for_write[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        current = self._get(type(obj), obj.metadata.name)
        self._check_rv(current, obj.metadata.resource_version)
        return current

    def _check_rv(self, current: Obj, expected: int) -> None:
        if expected != current.metadata.resource_version:
            raise Conflict(
                current.kind, current.metadata.name, expected, current.metadata.resource_version
            )

    def _commit[T: (Node, Pod, Deployment)](self, new: T) -> T:
        """The only place a stored object is replaced. All checks happen before it."""
        new.metadata.resource_version = self._revision + 1
        self._log(
            Record(
                new.metadata.resource_version,
                "put",
                new.kind,
                new.metadata.name,
                new.model_dump(mode="json"),
            )
        )
        self._apply_put(new)
        return self._present(new)

    def _log(self, record: Record) -> int:
        """Make the change durable (if there's a WAL) before memory changes."""
        if self._wal is not None:
            try:
                self._wal.append(record)
            except OSError as exc:  # e.g. disk full: nothing was applied
                raise Unavailable(f"WAL write failed: {exc}") from exc
        return record.rev

    def _apply(self, record: Record) -> None:
        if record.op == "delete":
            self._apply_delete(record.kind, record.name)
        else:
            self._apply_put(MODELS[record.kind].model_validate(record.obj))
        self._revision = record.rev

    def _apply_put(self, obj: Obj) -> None:
        name = obj.metadata.name
        old = self._objects[obj.kind].get(name)
        if isinstance(old, Pod):
            self._pods_by_node[old.spec.node_name].discard(name)
        if isinstance(obj, Pod):
            self._pods_by_node.setdefault(obj.spec.node_name, set()).add(name)
        self._objects[obj.kind][name] = obj
        self._revision = obj.metadata.resource_version
        self._after_write()

    def _apply_delete(self, kind: str, name: str) -> None:
        old = self._objects[kind].pop(name)
        if isinstance(old, Pod):
            self._pods_by_node[old.spec.node_name].discard(name)
        self._revision += 1
        self._after_write()

    def _after_write(self) -> None:
        if self._wal is None:
            return
        self._since_snapshot += 1
        if self._since_snapshot >= self._snapshot_every:
            self._since_snapshot = 0
            self._wal.snapshot(self.snapshot())

    def snapshot(self) -> Snapshot:
        objects = [
            (kind, obj.model_dump(mode="json"))
            for kind, by_name in self._objects.items()
            for _, obj in sorted(by_name.items())
        ]
        return Snapshot(self._revision, objects)

    def _present[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        out = obj.model_copy(deep=True)
        if isinstance(out, Node):
            # Same keys as capacity (zeros included), plus anything bound beyond it.
            used = self.allocated(out.metadata.name)
            out.status.allocated = {n: used.get(n, 0) for n in out.spec.capacity.keys() | used}
        return out
