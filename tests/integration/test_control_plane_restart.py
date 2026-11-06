"""Control-plane restarts with a durable store (PM-2: U2.1, U2.2, P6.6; S8, L4).

The fence timeout here is longer than a chronosd restart takes (a fresh Python process
needs about a second), so "back within F" can actually happen. F + K + drift =
3.0 + 0.5 + 0.2 = 3.7 < E = 5.0.
"""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from chronos.api import Deployment, Node, NodeCondition, Pod
from chronos.client import HttpStoreClient
from chronos.common.clock import RealClock
from chronos.testing.cluster import Cluster, Timings, eventually
from tests.integration.helpers import (
    deployment,
    live_processes,
    owned_pods,
    scale,
    wait_nodes_ready,
    wait_running,
)

pytestmark = pytest.mark.integration
clock = RealClock()
SLOW_FENCE = Timings(fence_timeout=3.0, eviction_timeout=5.0)


@pytest.fixture
async def slow(tmp_path: Path) -> AsyncIterator[Cluster]:
    c = Cluster(tmp_path, SLOW_FENCE)
    await c.start()
    try:
        yield c
    finally:
        leaked = await c.close()
        print(c.logs())
        assert leaked == [], f"pod process groups leaked: {leaked}"


async def test_restart_within_fence_timeout_keeps_everything(slow: Cluster) -> None:
    """U2.2 (a): kill -9 chronosd, back within F: same objects, same PIDs, no fencing."""
    async with HttpStoreClient(slow.url) as api:
        for name in ("a", "b"):
            slow.start_agent(name)
        await wait_nodes_ready(api, "a", "b")
        await api.create(deployment("web", 3))
        before = {p.metadata.name: p.status.pid for p in await wait_running(api, "web", 3)}
        revision = (await api.list(Pod))[1]

        restart_began = clock.monotonic()
        await slow.restart_server()
        assert clock.monotonic() - restart_began < SLOW_FENCE.fence_timeout

        after = {p.metadata.name: p.status.pid for p in await owned_pods(api, "web")}
        assert after == before  # S8: nothing lost; nothing restarted
        assert (await api.list(Pod))[1] >= revision  # revisions continue (U2.3)
        await clock.sleep(SLOW_FENCE.eviction_timeout + 1.0)  # past any eviction point
        assert live_processes("web") == set(before.values())
        assert all(n.status.condition == NodeCondition.READY for n in (await api.list(Node))[0])


async def test_long_outage_fences_everything_then_reconverges(slow: Cluster) -> None:
    """U2.2 (b), fencing Option A: down longer than F, every agent fences (zero workload
    processes); after the restart, startup grace prevents evictions and the still-bound
    pods restart on their nodes with new PIDs (L4)."""
    t = SLOW_FENCE
    async with HttpStoreClient(slow.url) as api:
        for name in ("a", "b"):
            slow.start_agent(name)
        await wait_nodes_ready(api, "a", "b")
        await api.create(deployment("web", 3))
        before = {p.metadata.name: (p.spec.node_name, p.status.pid)
                  for p in await wait_running(api, "web", 3)}  # fmt: skip

        slow.kill_server()

        async def all_fenced() -> bool:
            return not live_processes("web")

        await eventually(all_fenced, within=t.fence_timeout + t.termination_grace + 2.0)
        await clock.sleep(1.0)  # well past F: an outage, not a blip
        await slow.start()

        pods = await wait_running(api, "web", 3, within=t.eviction_timeout + 5.0)
        after = {p.metadata.name: (p.spec.node_name, p.status.pid) for p in pods}
        assert after.keys() == before.keys(), "same pods: none evicted or replaced"
        assert all(after[n][0] == before[n][0] for n in after), "restarted on their own nodes"
        assert all(after[n][1] != before[n][1] for n in after), "with new processes"
        assert not [p for p in await owned_pods(api, "web") if p.status.reason == "NodeLost"]


async def test_crash_during_scale_up_converges_without_duplicates(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    """P6.6: kill -9 chronosd in the middle of a 0 -> 20 scale-up; after restart the
    deployment still exists and ends with exactly 20 pods, all running."""
    for name in ("a", "b"):
        cluster.start_agent(name)
    await wait_nodes_ready(api, "a", "b")
    await api.create(deployment("web", 0))
    await scale(api, "web", 20)

    async def some_created() -> bool:
        return len(await owned_pods(api, "web")) >= 5

    await eventually(some_created, interval=0.01)
    await cluster.restart_server()

    assert (await api.get(Deployment, "web")).spec.replicas == 20  # the acked scale survived
    pods = await wait_running(api, "web", 20, within=30.0)
    assert len({p.metadata.name for p in pods}) == 20
    assert len(await owned_pods(api, "web")) == 20  # no extra, no duplicate pods
