"""Agent fencing logic on a FakeClock (ADR-0012): send-time rule, Fenced mode in and out."""

import asyncio
from collections.abc import Mapping

import pytest

from chronos.agent.agent import Agent
from chronos.api import Node, ObjectMeta, Pod, PodPhase, PodSpec
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
        self.revision = 1

    async def register(
        self, node: str, epoch: str, capacity: Mapping[str, int], fencing: FencingConfig
    ) -> RegisterReply:
        meta = ObjectMeta(name=node, resource_version=self.revision)
        return RegisterReply(node=Node(metadata=meta), eviction_timeout=10.0)

    async def heartbeat(self, node: str, epoch: str) -> HeartbeatReply:
        await self.clock.sleep(self.reply_delay)
        if self.down:
            raise Unavailable("down")
        return HeartbeatReply(condition="Ready")

    async def list(self, kind: type, **_: object) -> tuple[list[Pod], int]:
        return self.pods, self.revision


class FakeRuntime:
    def __init__(self) -> None:
        self.eviction_timeout: float | None = None
        self.procs: dict[str, LocalProc] = {}
        self.leases: list[float] = []
        self.stopped: list[tuple[str, bool]] = []
        self.started: list[str] = []
        self.killed: list[str] = []
        self.events: list[str] = []

    def write_lease(self, t: float) -> None:
        self.leases.append(t)
        self.events.append("lease")

    def snapshot(self) -> dict[str, LocalProc]:
        return dict(self.procs)

    def stop(self, uid: str, *, fenced: bool = False) -> None:
        self.stopped.append((uid, fenced))

    def kill_now(self, uid: str) -> None:
        self.killed.append(uid)
        self.events.append(f"kill {uid}")

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
        clock=clock, fencing=FENCING,
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
    await agent.pods.relist()
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


async def test_coming_back_past_eviction_kills_before_the_lease_looks_fresh(
    parts: tuple[Agent, FakeClient, FakeRuntime], clock: FakeClock
) -> None:
    """Simulator finding (seed 9): after a whole-node freeze, the agent's re-register
    renewed the lease before the shim's next poll, so the shim let a replaced workload
    run on. Past E the agent must SIGKILL first, and only then renew."""
    agent, _client, runtime = parts
    await agent.register()  # eviction timeout 10 s
    runtime.procs = {"u1": LocalProc("u1", "p1", 100)}
    runtime.events.clear()
    clock.advance(10.5)  # frozen past E
    await agent.register()  # what the resumed agent does after EpochMismatch
    assert runtime.events == ["kill u1", "lease"]


async def test_within_eviction_nothing_is_killed_on_renewal(
    parts: tuple[Agent, FakeClient, FakeRuntime], clock: FakeClock
) -> None:
    agent, _client, runtime = parts
    await agent.register()
    runtime.procs = {"u1": LocalProc("u1", "p1", 100)}
    clock.advance(9.0)
    await agent.heartbeat_once()
    assert runtime.killed == []


async def test_after_registering_nothing_starts_before_the_cache_reaches_it(
    parts: tuple[Agent, FakeClient, FakeRuntime], clock: FakeClock
) -> None:
    """Simulator finding (seed 25): a resumed agent re-registered and restarted pods
    from its stale cache that the server had already evicted and replaced. The agent
    now acts only once its pod cache has reached the registration's revision."""
    agent, client, runtime = parts
    pod = Pod(
        metadata=ObjectMeta(name="p", uid="u9", resource_version=1),
        spec=PodSpec(command=["x"], node_name="n1"),
    )
    client.pods = [pod]
    await agent.register()
    await agent.pods.relist()  # the cache: "p" is desired, at revision 1
    evicted = pod.model_copy(deep=True)
    evicted.status.phase = PodPhase.FAILED
    evicted.metadata.resource_version = 4
    client.pods, client.revision = [evicted], 5  # the server evicted it meanwhile
    await agent.register()  # new epoch at revision 5; the cache is still at 1
    assert agent.registered_at == 5
    await agent.sync_once()
    assert runtime.started == []  # it listed first, and "p" is Failed now
    assert agent.pods.revision == 5
