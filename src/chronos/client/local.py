"""In-process client: the async façade over the synchronous store (ADR-0004, ADR-0007).

Every method runs the store call without awaiting in between, so on one event
loop the calls are serialized: the store keeps a single writer without a lock.

Group commit (ADR-0015, P6.2): with the WAL in "group" mode, a write is in memory
before it is on disk. So every call, read or write, waits until the store's durable
revision covers the revision it saw or wrote; the first waiter starts a short timer,
and one sync then releases everyone who arrived meanwhile. No client and no
controller ever acts on state that a crash could take back. In "full" mode the
durable revision always equals the revision, so nothing waits.
"""

from __future__ import annotations

import asyncio
import builtins
import logging
import os
from collections.abc import Callable, Mapping

from chronos.api import Deployment, Node, NodeSpec, NodeStatus, ObjectMeta, Pod
from chronos.api.resources import format_resources
from chronos.api.wire import HeartbeatReply, RegisterReply
from chronos.common.clock import Clock, RealClock
from chronos.common.config import FencingConfig
from chronos.core.fencing import inequality_violation
from chronos.store import Invalid, NotFound, Store, StoreError
from chronos.store.errors import EpochMismatch, Unavailable
from chronos.store.leases import Leases

log = logging.getLogger(__name__)

FSYNC_FAILED_EXIT = 70  # a failed group sync: restart and let the log decide (no retry)


class LocalStoreClient:
    def __init__(
        self,
        store: Store,
        leases: Leases,
        eviction_timeout: float = 10.0,
        *,
        clock: Clock | None = None,
        group_window: float = 0.002,
    ) -> None:
        self.store = store
        self.leases = leases
        self.eviction_timeout = eviction_timeout
        self._clock = clock or RealClock()
        self._window = group_window
        self._waiters: list[tuple[int, asyncio.Future[None]]] = []
        self._flusher: asyncio.Task[None] | None = None

    async def _durable(self) -> None:
        """Return once everything this call saw or wrote is on disk."""
        rev = self.store.revision
        if self.store.durable_revision >= rev:
            return
        future = asyncio.get_running_loop().create_future()
        self._waiters.append((rev, future))
        if self._flusher is None or self._flusher.done():
            self._flusher = asyncio.create_task(self._flush_soon())
        await future

    async def _run[R](self, call: Callable[[], R]) -> R:
        """Run a store call; answer (result or error) only once its state is durable."""
        try:
            result = call()
        except StoreError:
            await self._durable()  # an error can reveal a pending write too
            raise
        await self._durable()
        return result

    async def _flush_soon(self) -> None:
        await self._clock.sleep(self._window)  # let concurrent writes join the batch
        try:
            durable = self.store.flush()
        except Unavailable:
            # Memory already holds writes that may not be on disk. Retrying could
            # report success for data the kernel dropped; showing them would break S8.
            # So stop, and let recovery rebuild exactly what the log has.
            log.critical("WAL sync failed with writes pending: exiting to recover from the log")
            os._exit(FSYNC_FAILED_EXIT)
        waiting, self._waiters = self._waiters, []
        for rev, future in waiting:
            if rev <= durable and not future.done():
                future.set_result(None)
            elif not future.done():
                self._waiters.append((rev, future))
        if self._waiters:
            self._flusher = asyncio.create_task(self._flush_soon())

    async def get[T: (Node, Pod, Deployment)](self, kind: type[T], name: str) -> T:
        return await self._run(lambda: self.store.get(kind, name))

    async def list[T: (Node, Pod, Deployment)](
        self,
        kind: type[T],
        *,
        labels: Mapping[str, str] | None = None,
        node_name: str | None = None,
    ) -> tuple[builtins.list[T], int]:
        return await self._run(lambda: self.store.list(kind, labels=labels, node_name=node_name))

    async def create[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        return await self._run(lambda: self.store.create(obj))

    async def update[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        return await self._run(lambda: self.store.update(obj))

    async def update_status[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        return await self._run(lambda: self.store.update_status(obj))

    async def delete[T: (Node, Pod, Deployment)](
        self, kind: type[T], name: str, expected_rv: int | None = None, *, force: bool = False
    ) -> T:
        gone = await self._run(lambda: self.store.delete(kind, name, expected_rv, force=force))
        if kind is Node:
            self.leases.forget(name)
        return gone

    async def bind(self, pod: str, node: str, expected_rv: int) -> Pod:
        return await self._run(lambda: self.store.bind(pod, node, expected_rv))

    async def register(
        self,
        node: str,
        agent_epoch: str,
        capacity: Mapping[str, int],
        fencing: FencingConfig | None = None,
    ) -> RegisterReply:
        """Create the node, or adopt it for a new agent incarnation (ADR-0011).

        The new epoch makes heartbeats from any older incarnation fail with
        EpochMismatch. The node turns Ready on the lifecycle controller's next tick.
        """
        return await self._run(lambda: self._register(node, agent_epoch, capacity, fencing))

    def _register(
        self,
        node: str,
        agent_epoch: str,
        capacity: Mapping[str, int],
        fencing: FencingConfig | None,
    ) -> RegisterReply:
        if not agent_epoch:
            raise EpochMismatch("agent_epoch must be non-empty")
        f = fencing or FencingConfig()
        if problem := inequality_violation(
            f.fence_timeout, f.termination_grace, f.drift_margin, self.eviction_timeout
        ):
            raise Invalid(f"Node {node!r}: agent timings rejected: {problem}")
        try:
            current = self.store.get(Node, node)
        except NotFound:
            current = self.store.create(
                Node(
                    metadata=ObjectMeta(name=node),
                    spec=NodeSpec.model_validate({"capacity": format_resources(capacity)}),
                    status=NodeStatus(agent_epoch=agent_epoch),
                )
            )
        else:
            current.spec.capacity = dict(capacity)
            current = self.store.update(current)
            current.status.agent_epoch = agent_epoch
            current = self.store.update_status(current)
        self.leases.renew(node)
        return RegisterReply(node=current, eviction_timeout=self.eviction_timeout)

    async def heartbeat(self, node: str, agent_epoch: str) -> HeartbeatReply:
        return await self._run(lambda: self._heartbeat(node, agent_epoch))

    def _heartbeat(self, node: str, agent_epoch: str) -> HeartbeatReply:
        current = self.store.get(Node, node)
        if not agent_epoch or agent_epoch != current.status.agent_epoch:
            raise EpochMismatch(f"Node {node!r}: agent_epoch {agent_epoch!r} is not current")
        self.leases.renew(node)
        return HeartbeatReply(condition=current.status.condition)
