"""Controller runtime (ADR-0009): work queue, expectations, informer, on a FakeClock."""

import asyncio
from pathlib import Path

import pytest

from chronos.api import ObjectMeta, Pod, PodSpec
from chronos.client import LocalStoreClient
from chronos.common.clock import FakeClock
from chronos.controllers.runtime import Expectations, Informer, Signal, WorkQueue
from chronos.store import Store
from chronos.store.leases import Leases


async def settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)  # noqa: TID251 (let tasks waiting on the FakeClock run)


def pod(name: str) -> Pod:
    return Pod(metadata=ObjectMeta(name=name), spec=PodSpec(command=["x"]))


# ---- work queue ------------------------------------------------------------------------


async def test_queue_dedups_and_keeps_order() -> None:
    q = WorkQueue(FakeClock())
    for key in ("a", "b", "a", "c", "b"):
        q.add(key)
    assert len(q) == 3
    assert [await q.get(), await q.get(), await q.get()] == ["a", "b", "c"]


async def test_a_key_is_handed_out_once_and_requeued_if_added_meanwhile() -> None:
    q = WorkQueue(FakeClock())
    q.add("a")
    assert await q.get() == "a"
    q.add("a")  # changed while being processed
    assert len(q) == 0  # not handed to a second worker
    q.done("a")
    assert len(q) == 1 and await q.get() == "a"  # processed again, once
    q.done("a")
    assert len(q) == 0


async def test_get_waits_for_an_add() -> None:
    q = WorkQueue(FakeClock())
    getting = asyncio.create_task(q.get())
    await settle()
    assert not getting.done()
    q.add("a")
    assert await asyncio.wait_for(getting, 1) == "a"


async def test_failures_back_off_per_key_and_forget_resets() -> None:
    clock = FakeClock()
    q = WorkQueue(clock, base_delay=0.125, max_delay=0.5)  # exact in binary
    delays = []
    for _ in range(4):
        q.add_rate_limited("a")
        await settle()
        waited = 0.0
        while not len(q):
            clock.advance(0.0625)
            waited += 0.0625
            await settle()
        delays.append(waited)
        await q.get()
        q.done("a")
    assert delays == [0.125, 0.25, 0.5, 0.5]  # doubling, capped
    q.forget("a")
    assert q.failures("a") == 0


async def test_add_after() -> None:
    clock = FakeClock()
    q = WorkQueue(clock)
    q.add_after("a", 1.0)
    await settle()
    clock.advance(0.9)
    await settle()
    assert len(q) == 0
    clock.advance(0.1)
    await settle()
    assert len(q) == 1


async def test_signal_is_not_lost_when_nobody_waits_yet() -> None:
    s = Signal()
    s.notify()
    await asyncio.wait_for(s.wait(), 1)  # returns at once


async def test_signal_wait_gives_up_after_at_most() -> None:
    clock = FakeClock()
    s = Signal()
    waiting = asyncio.create_task(s.wait(clock, 1.0))
    await settle()
    assert not waiting.done()
    clock.advance(1.0)
    await settle()
    assert waiting.done()


# ---- expectations ------------------------------------------------------------------------


def test_expectations_are_met_by_observations() -> None:
    e = Expectations(FakeClock())
    assert e.satisfied("d")
    e.expect("d", creates=2, deletes=["p1"])
    assert not e.satisfied("d")
    e.created("d")
    e.deleted("d", "p1")
    e.deleted("d", "p1")  # observed twice (e.g. terminating, then gone): counted once
    assert not e.satisfied("d")
    e.created("d")
    assert e.satisfied("d")
    assert e.pending("d") == (0, 0)


def test_expectations_expire_after_the_ttl() -> None:
    clock = FakeClock()
    e = Expectations(clock, ttl=30.0)
    e.expect("d", creates=1)
    clock.advance(30.0)
    assert not e.satisfied("d")
    clock.advance(0.1)
    assert e.satisfied("d")  # the event must have been lost: let resync put it right
    assert e.expired == 1


# ---- informer ------------------------------------------------------------------------------


@pytest.fixture
def client() -> LocalStoreClient:
    clock = FakeClock()
    return LocalStoreClient(Store(clock, watch_history=3), Leases(clock))


async def test_informer_follows_the_watch(client: LocalStoreClient) -> None:
    informer = Informer(client, Pod, FakeClock(), watch_wait=0)
    seen: list[tuple[str | None, str | None]] = []
    informer.add_handler(
        lambda old, new: seen.append(
            (old and old.metadata.name, new and new.metadata.name)  # type: ignore[arg-type]
        )
    )
    await informer.relist()
    await client.create(pod("a"))
    await client.create(pod("b"))
    await client.delete(Pod, "a")
    await informer.watch_once()
    assert seen == [(None, "a"), (None, "b"), ("a", None)]
    assert [p.metadata.name for p in informer.list()] == ["b"]
    assert informer.revision == client.store.revision


async def test_informer_relists_on_compaction_and_reports_the_difference(
    client: LocalStoreClient,
) -> None:
    informer = Informer(client, Pod, FakeClock(), watch_wait=0)
    await client.create(pod("a"))
    await informer.step()  # the first list
    assert (informer.relists, informer.needs_relist) == (1, False)
    changes: list[tuple[str | None, str | None]] = []
    informer.add_handler(
        lambda old, new: changes.append(
            (old and old.metadata.name, new and new.metadata.name)  # type: ignore[arg-type]
        )
    )
    await client.delete(Pod, "a")
    for name in ("b", "c", "d", "e"):  # more than the 3 events the server keeps
        await client.create(pod(name))
    await informer.step()  # the watch gets 410: nothing is applied, a relist is due
    assert informer.needs_relist and changes == []
    assert [p.metadata.name for p in informer.list()] == ["a"]
    await informer.step()
    assert informer.relists == 2
    assert [p.metadata.name for p in informer.list()] == ["b", "c", "d", "e"]
    assert changes == [("a", None), (None, "b"), (None, "c"), (None, "d"), (None, "e")]
    await informer.step()  # and from the relist's revision, watching works again
    assert not informer.needs_relist


async def test_informer_returns_copies(client: LocalStoreClient) -> None:
    informer = Informer(client, Pod, FakeClock())
    await client.create(pod("a"))
    await informer.relist()
    mine = informer.get("a")
    assert mine is not None
    mine.metadata.labels["mutated"] = "yes"
    cached = informer.get("a")
    assert cached is not None and cached.metadata.labels == {}


async def test_watch_does_not_miss_a_write_that_races_its_wait() -> None:
    """Lost-wakeup regression: the write lands after the watch found nothing, before it
    waits. The waiter must already be registered by then."""
    store = Store(FakeClock())
    client = LocalStoreClient(store, Leases(FakeClock()))  # real clock: wait up to 5 s
    watching = asyncio.create_task(client.watch(Pod, 0, wait=5.0))
    await asyncio.sleep(0)  # noqa: TID251 (exactly one step: the watch checks, finds nothing)
    store.create(pod("a"))  # no await: lands before the watch's next step
    batch = await asyncio.wait_for(watching, 1.0)
    assert [e.object.metadata.name for e in batch.events] == ["a"]


async def test_after_a_restart_informers_relist(tmp_path: Path) -> None:
    """ADR-0008 4: the history is gone after a restart, so every watcher relists."""
    store = Store.open(tmp_path, FakeClock())
    before = LocalStoreClient(store, Leases(FakeClock()))
    informer = Informer(before, Pod, FakeClock(), watch_wait=0)
    await before.create(pod("a"))
    await informer.step()
    store.close()

    again = Store.open(tmp_path, FakeClock())  # the restart: revision 1, no history
    await LocalStoreClient(again, Leases(FakeClock())).create(pod("b"))  # revision 2
    informer._client = LocalStoreClient(again, Leases(FakeClock()))
    assert again.watch.floor == 1
    await informer.step()  # from revision 1: still answerable, it's the floor
    assert [p.metadata.name for p in informer.list()] == ["a", "b"]

    again.close()
    third = Store.open(tmp_path, FakeClock())  # another restart, at revision 2...
    third.create(pod("c"))
    third.close()
    fourth = Store.open(tmp_path, FakeClock())  # ...and one more: floor is now 3
    informer._client = LocalStoreClient(fourth, Leases(FakeClock()))
    await informer.step()  # watching from 2 < 3: Compacted
    assert informer.needs_relist
    await informer.step()
    assert [p.metadata.name for p in informer.list()] == ["a", "b", "c"]
