"""Durability harness writer (ADR-0015, S8): `python -m chronos.testing.durability DIR SEED`.

Runs a seeded sequence of store writes against a WAL in DIR and prints one line per
finished operation (`ack <i>` or `rej <i>`) *after* the store returned. Combined with
`CHRONOS_CRASHPOINT`, the process dies somewhere inside an operation; the test then
recovers DIR and compares it with an in-memory replay of the same sequence.
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

from chronos.api import Node, NodeCondition, NodeSpec, ObjectMeta, Pod, PodSpec
from chronos.common.clock import FakeClock
from chronos.store import Store, StoreError
from chronos.store.wal import SyncMode

OPS = 60
SNAPSHOT_EVERY = 7
SEGMENT_SIZE = 1200  # small, so segments rotate during a run


def uids() -> object:
    counter = iter(range(10**9))
    return lambda: f"uid-{next(counter)}"


GROUP_BATCH = 4  # group mode: sync (and only then ack) every 4 operations


def open_store(data_dir: Path | None, mode: SyncMode = "full") -> Store:
    clock = FakeClock()
    factory = uids()
    assert callable(factory)
    if data_dir is None:
        return Store(clock, factory)
    return Store.open(
        data_dir,
        clock,
        sync=mode,
        snapshot_every=SNAPSHOT_EVERY,
        segment_size=SEGMENT_SIZE,
        uid_factory=factory,
    )


def step(store: Store, rng: random.Random, i: int) -> bool:
    """One seeded operation; True if the store accepted it."""
    # Each operation is at most one store write, so "lost or durable" is per write.
    if i == 0:
        spec = NodeSpec(capacity={"cpu": 10**6})
        store.create(Node(metadata=ObjectMeta(name="n1"), spec=spec))
        return True
    if i == 1:
        n = store.get(Node, "n1")
        n.status.condition = NodeCondition.READY
        store.update_status(n)
        return True
    name = f"p{rng.randrange(6)}"
    op = rng.choice(["create", "create", "status", "bind", "delete", "force"])
    try:
        match op:
            case "create":
                store.create(Pod(metadata=ObjectMeta(name=name), spec=PodSpec(command=["x"])))
            case "status":
                p = store.get(Pod, name)
                p.status.message = f"m{i}"
                store.update_status(p)
            case "bind":
                store.bind(name, "n1", store.get(Pod, name).metadata.resource_version)
            case _:
                store.delete(Pod, name, force=op == "force")
    except StoreError:
        return False
    return True


def states(seed: int) -> list[tuple[int, list[tuple[str, dict[str, object]]]]]:
    """Reference: the state before any op ([0]) and after each op i ([i + 1])."""
    store, rng = open_store(None), random.Random(seed)
    out = [(store.snapshot().revision, store.snapshot().objects)]
    for i in range(OPS):
        step(store, rng, i)
        snap = store.snapshot()
        out.append((snap.revision, snap.objects))
    return out


def run(data_dir: Path, seed: int, report: bool = False, mode: SyncMode = "full") -> None:
    """In "group" mode, results are reported only after a flush, like the server's acks."""
    store, rng = open_store(data_dir, mode), random.Random(seed)
    pending: list[str] = []
    for i in range(OPS):
        ok = step(store, rng, i)
        pending.append(f"{'ack' if ok else 'rej'} {i}")
        if mode != "group" or len(pending) == GROUP_BATCH or i == OPS - 1:
            if mode == "group":
                store.flush()
            if report:
                print("\n".join(pending), flush=True)
            pending = []
    store.close()


def main(argv: list[str]) -> int:
    mode: SyncMode = "group" if argv[2:] == ["group"] else "full"
    run(Path(argv[0]), int(argv[1]), report=True, mode=mode)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
