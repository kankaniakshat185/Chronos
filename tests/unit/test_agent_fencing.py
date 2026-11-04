"""Agent fencing logic on a FakeClock (ADR-0012): send-time rule, Fenced mode in and out."""

import asyncio
from collections.abc import Mapping

import pytest

from chronos.agent.agent import Agent
from chronos.api import Node, ObjectMeta, Pod, PodSpec
from chronos.api.wire import HeartbeatReply, RegisterReply
from chronos.common.clock import FakeClock
from chronos.common.config import FencingConfig
from chronos.core.agent_sync import LocalProc
from chronos.store.errors import Unavailable

FENCING = FencingConfig(heartbeat_interval=1.0, fence_timeout=6.0, termination_grace=2.0)


class FakeClient:
    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.reply_delay = 0.0
        self.down = False
        self.pods: list[Pod] = []

    async def register(
        self, node: str, epoch: str, capacity: Mapping[str, int], fencing: FencingConfig
    ) -> RegisterReply:
        return RegisterReply(node=Node(metadata=ObjectMeta(name=node)), eviction_timeout=10.0)

    async def heartbeat(self, node: str, epoch: str) -> HeartbeatReply:
        await self.clock.sleep(self.reply_delay)
        if self.down:
            raise Unavailable("down")
        return HeartbeatReply(condition="Ready")

    async def list(self, kind: type, **_: object) -> tuple[list[Pod], int]:
        return self.pods, 1


class FakeRuntime:
    def __init__(self) -> None:
        self.eviction_timeout: float | None = None
        self.procs: dict[str, LocalProc] = {}
        self.leases: list[float] = []
        self.stopped: list[tuple[str, bool]] = []
        self.started: list[str] = []

    def write_lease(self, t: float) -> None:
        self.leases.append(t)

    def snapshot(self) -> dict[str, LocalProc]:
        return dict(self.procs)

    def stop(self, uid: str, *, fenced: bool = False) -> None:
        self.stopped.append((uid, fenced))

    async def start(self, pod: Pod, **_: int) -> None:
        self.started.append(pod.metadata.uid)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(start=1000.0)


@pytest.fixture
def parts(clock: FakeClock) -> tuple[Agent, FakeClient, FakeRuntime]:
    client, runtime = FakeClient(clock), FakeRuntime()
    agent = Agent(
        name="n1", capacity={}, client=client, runtime=runtime,  # type: ignore[arg-type]
        clock=clock, fencing=FENCING, poll_interval=0.5,
    )  # fmt: skip
    return agent, client, runtime


async def settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)  # noqa: TID251 (let tasks waiting on the FakeClock run)


async def test_lease_records_the_send_time_not_the_ack_time(
    parts: tuple[Agent, FakeClient, FakeRuntime], clock: FakeClock
) -> None:
    agent, client, runtime = parts
    await agent.register()
    client.reply_delay = 3.0  # a slow response
    sent_at = clock.monotonic()
    beat = asyncio.create_task(agent.heartbeat_once())
    await settle()
    clock.advance(3.0)
    await beat
    assert agent.last_ok_send == sent_at
    assert runtime.leases[-1] == sent_at  # what the shims will measure from


async def test_fences_after_the_timeout_and_stops_running_workloads(
    parts: tuple[Agent, FakeClient, FakeRuntime], clock: FakeClock
) -> None:
    agent, _client, runtime = parts
    await agent.register()
    runtime.procs = {"u1": LocalProc("u1", "p1", 100), "u2": LocalProc("u2", "p2", 101, 0)}
    clock.advance(6.0)
    await agent.fence_check()
    assert not agent.fenced  # exactly F: not yet
    clock.advance(0.001)
    await agent.fence_check()
    assert agent.fenced
    assert runtime.stopped == [("u1", True)]  # running ones only, marked as fenced


async def test_fenced_agent_starts_and_reports_nothing(
    parts: tuple[Agent, FakeClient, FakeRuntime], clock: FakeClock
) -> None:
    agent, client, runtime = parts
    await agent.register()
    client.pods = [
        Pod(metadata=ObjectMeta(name="p", uid="u9"), spec=PodSpec(command=["x"], node_name="n1"))
    ]
    clock.advance(7.0)
    await agent.sync_once()  # checks the fence before acting
    assert agent.fenced and runtime.started == []


async def test_failed_heartbeats_dont_renew_and_a_success_unfences(
    parts: tuple[Agent, FakeClient, FakeRuntime], clock: FakeClock
) -> None:
    agent, client, _runtime = parts
    await agent.register()
    client.down = True
    for _ in range(7):
        clock.advance(1.0)
        await agent.heartbeat_once()
        await agent.fence_check()
    assert agent.fenced
    client.down = False
    await agent.heartbeat_once()
    await agent.fence_check()
    assert not agent.fenced
