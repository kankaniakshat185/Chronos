"""Group commit (ADR-0015, P6.2): batched syncs, and nothing visible before it's durable."""

import asyncio
from pathlib import Path

import pytest

from chronos.api import ObjectMeta, Pod, PodSpec
from chronos.client import LocalStoreClient
from chronos.common.clock import FakeClock, RealClock
from chronos.store import AlreadyExists, Store
from chronos.store import wal as wal_module
from chronos.store.leases import Leases


def pod(name: str) -> Pod:
    return Pod(metadata=ObjectMeta(name=name), spec=PodSpec(command=["x"]))


@pytest.fixture
def syncs(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    calls: list[int] = []
    real = wal_module.durable_sync

    def counting(fd: int) -> None:
        calls.append(fd)
        real(fd)

    monkeypatch.setattr(wal_module, "durable_sync", counting)
    return calls


def client(tmp_path: Path, mode: str) -> LocalStoreClient:
    store = Store.open(tmp_path, FakeClock(), sync=mode)  # type: ignore[arg-type]
    return LocalStoreClient(store, Leases(FakeClock()), clock=RealClock(), group_window=0.005)


async def test_concurrent_writes_share_a_few_syncs(tmp_path: Path, syncs: list[int]) -> None:
    c = client(tmp_path, "group")
    syncs.clear()
    created = await asyncio.gather(*(c.create(pod(f"p{i}")) for i in range(20)))
    assert len(created) == 20
    assert len(syncs) <= 2, f"{len(syncs)} syncs for 20 concurrent writes"
    assert c.store.durable_revision == c.store.revision == 20


async def test_full_mode_syncs_every_write(tmp_path: Path, syncs: list[int]) -> None:
    c = client(tmp_path, "full")
    syncs.clear()
    await asyncio.gather(*(c.create(pod(f"p{i}")) for i in range(5)))
    assert len(syncs) == 5


async def test_nothing_is_answered_before_its_state_is_durable(tmp_path: Path) -> None:
    c = client(tmp_path, "group")
    events: list[str] = []
    real_flush = c.store.flush

    def flush() -> int:
        events.append("sync")
        return real_flush()

    c.store.flush = flush  # type: ignore[method-assign]

    async def write() -> None:
        await c.create(pod("a"))
        events.append("write acked")

    async def read() -> None:
        await asyncio.sleep(0)  # noqa: TID251 (start after the write is in memory)
        await c.get(Pod, "a")  # sees the pending write, so must wait for its sync
        events.append("read answered")

    async def conflict() -> None:
        await asyncio.sleep(0)  # noqa: TID251
        with pytest.raises(AlreadyExists):
            await c.create(pod("a"))  # the error itself reveals the pending write
        events.append("error answered")

    async def watch() -> None:
        batch = await c.watch(Pod, 0, wait=5.0)  # already waiting when the write lands
        assert [e.object.metadata.name for e in batch.events] == ["a"]
        events.append("watch answered")

    await asyncio.gather(watch(), write(), read(), conflict())
    assert events[0] == "sync"
    assert sorted(events[1:]) == [
        "error answered",
        "read answered",
        "watch answered",
        "write acked",
    ]


async def test_reopen_after_group_commit_has_everything_acked(tmp_path: Path) -> None:
    c = client(tmp_path, "group")
    await asyncio.gather(*(c.create(pod(f"p{i}")) for i in range(10)))
    c.store.close()
    again = Store.open(tmp_path, FakeClock(), sync="group")
    assert len(again.list(Pod)[0]) == 10


@pytest.mark.parametrize("mode", ["full", "group"])
async def test_every_ack_follows_a_sync_that_covers_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    """Sync accounting. A process crash keeps the kernel's page cache, so crash tests
    can't tell "written" from "synced"; this checks the order directly: when a write is
    acknowledged, an fsync has already covered the bytes of its record."""
    import os  # noqa: PLC0415

    synced_upto = 0
    written_upto: dict[int, int] = {}  # revision -> log size right after its record
    real_sync, real_append = wal_module.durable_sync, wal_module.Wal.append

    def tracking_sync(fd: int) -> None:
        nonlocal synced_upto
        real_sync(fd)
        synced_upto = os.fstat(fd).st_size

    def tracking_append(self: wal_module.Wal, record: object) -> None:
        real_append(self, record)  # type: ignore[arg-type]
        written_upto[record.rev] = os.fstat(self._fd).st_size  # type: ignore[attr-defined]

    monkeypatch.setattr(wal_module, "durable_sync", tracking_sync)
    monkeypatch.setattr(wal_module.Wal, "append", tracking_append)
    c = client(tmp_path, mode)

    async def write(i: int) -> None:
        created = await c.create(pod(f"p{i}"))
        rev = created.metadata.resource_version
        assert synced_upto >= written_upto[rev], f"rev {rev} acked before it was synced"

    await asyncio.gather(*(write(i) for i in range(12)))
