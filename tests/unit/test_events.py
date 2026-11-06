"""Events (P7.3): one per (object, reason), counted; best effort; garbage-collected."""

import pytest

from chronos.api import Event, ObjectMeta, Pod, PodSpec
from chronos.api.meta import NAME_PATTERN
from chronos.client import LocalStoreClient
from chronos.client.events import EventRecorder, event_name
from chronos.common.clock import FakeClock
from chronos.controllers.event_gc import EventGarbageCollector
from chronos.store import Store
from chronos.store.errors import Unavailable
from chronos.store.leases import Leases


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def client(clock: FakeClock) -> LocalStoreClient:
    return LocalStoreClient(Store(clock), Leases(clock), clock=clock)


def pod(name: str = "web-1") -> Pod:
    return Pod(metadata=ObjectMeta(name=name), spec=PodSpec(command=["x"]))


async def test_repeats_are_counted_on_one_event(client: LocalStoreClient, clock: FakeClock) -> None:
    rec = EventRecorder(client, clock, "agent/n1")
    await rec.record(pod(), "BackOff", "back-off 1s", "Warning")
    clock.advance(5)
    await rec.record(pod(), "BackOff", "back-off 2s", "Warning")
    [event] = (await client.list(Event))[0]
    assert (event.spec.reason, event.spec.type, event.spec.source) == (
        "BackOff",
        "Warning",
        "agent/n1",
    )
    assert (event.spec.involved_kind, event.spec.involved_name) == ("Pod", "web-1")
    assert (event.status.count, event.status.message) == (2, "back-off 2s")
    assert event.status.first_seen is not None and event.status.last_seen is not None
    assert (event.status.last_seen - event.status.first_seen).total_seconds() == 5


def test_names_are_valid_and_bounded() -> None:
    import re  # noqa: PLC0415

    for name in (event_name("Pod", "web-1", "Scheduled"), event_name("Pod", "x" * 63, "BackOff")):
        assert len(name) <= 63 and re.match(NAME_PATTERN, name)
    assert event_name("Pod", "x" * 63, "A") != event_name("Pod", "x" * 63, "B")


async def test_recording_never_raises(clock: FakeClock) -> None:
    class Down:
        async def get(self, *_: object) -> None:
            raise Unavailable("down")

    await EventRecorder(Down(), clock, "x").record(pod(), "Started", "")  # type: ignore[arg-type]


async def test_old_events_are_collected(client: LocalStoreClient, clock: FakeClock) -> None:
    rec = EventRecorder(client, clock, "scheduler")
    await rec.record(pod("old"), "Scheduled", "")
    await rec.record(pod("recurring"), "BackOff", "")
    clock.advance(3000)
    await rec.record(pod("new"), "Scheduled", "")
    await rec.record(pod("recurring"), "BackOff", "")  # first seen long ago, but still going
    clock.advance(700)  # "old" last seen 3 700 s ago; "new" and "recurring" 700 s ago
    await EventGarbageCollector(client, clock, ttl=3600).run_once()
    assert [e.spec.involved_name for e in (await client.list(Event))[0]] == ["new", "recurring"]
