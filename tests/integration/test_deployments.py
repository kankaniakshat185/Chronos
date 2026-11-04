"""Deployments on a real cluster (P4.10 subset): apply, scale, delete, node loss (S3, L2, L5)."""

import asyncio
import itertools

import pytest

from chronos.api import Deployment, Node, Pod, PodPhase, RestartPolicy
from chronos.client import HttpStoreClient
from chronos.common.clock import RealClock
from chronos.testing.cluster import Cluster, eventually
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


async def test_apply_scale_delete(cluster: Cluster, api: HttpStoreClient) -> None:
    for name in ("a", "b", "c"):
        cluster.start_agent(name)
    await wait_nodes_ready(api, "a", "b", "c")

    await api.create(deployment("web", 3))
    pods = await wait_running(api, "web", 3)
    assert sorted(p.spec.node_name for p in pods) == ["a", "b", "c"]  # spread

    for replicas in (5, 1):
        await scale(api, "web", replicas)
        await wait_running(api, "web", replicas)

    async def status_caught_up() -> bool:
        s = (await api.get(Deployment, "web")).status
        return (s.replicas, s.ready_replicas) == (1, 1)

    await eventually(status_caught_up)

    await api.delete(Deployment, "web")  # L5: owner GC removes pods, agents stop processes

    async def all_gone() -> bool:
        return not await owned_pods(api, "web") and not live_processes("web")

    await eventually(all_gone)


async def test_crash_loop_restarts_in_place_with_growing_backoff(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    """P2.6: Always restarts the same pod; consecutive crashes wait 0.2, 0.4, 0.8 s, ..."""
    cluster.start_agent("a")
    await wait_nodes_ready(api, "a")
    dep = deployment("crashy", 1)
    dep.spec.template.command += ["--crash-after", "0.1", "--exit-code", "3"]
    await api.create(dep)

    seen: dict[int, float] = {}  # restart_count -> when first observed
    reasons = set()

    async def four_restarts() -> bool:
        pods = await owned_pods(api, "crashy")
        assert len(pods) <= 1, "restarted in place: the pod is never replaced"
        if not pods:
            return False
        [pod] = pods
        seen.setdefault(pod.status.restart_count, clock.monotonic())
        reasons.add(pod.status.reason)
        return pod.status.restart_count >= 4

    await eventually(four_restarts, within=15.0, interval=0.02)
    gaps = [seen[n + 1] - seen[n] for n in range(1, 4) if n in seen and n + 1 in seen]
    assert len(gaps) >= 2
    assert all(b > a for a, b in itertools.pairwise(gaps)), f"gaps {gaps}"
    assert "CrashLoopBackOff" in reasons
    [pod] = await owned_pods(api, "crashy")
    assert pod.status.exit_code == 3 or pod.status.phase == PodPhase.RUNNING


async def test_never_policy_fails_and_is_replaced(cluster: Cluster, api: HttpStoreClient) -> None:
    cluster.start_agent("a")
    await wait_nodes_ready(api, "a")
    dep = deployment("once", 1)
    dep.spec.template.command += ["--crash-after", "0.3", "--exit-code", "3"]
    dep.spec.template.restart_policy = RestartPolicy.NEVER
    await api.create(dep)

    async def replaced() -> list[Pod] | None:
        pods = await owned_pods(api, "once")
        failed = [p for p in pods if p.status.phase == PodPhase.FAILED]
        return failed if failed and len(pods) > len(failed) else None

    failed = await eventually(replaced)
    assert (failed[0].status.reason, failed[0].status.exit_code) == ("Exited", 3)


async def test_node_loss_replaces_pods_without_exceeding_replicas(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    """S3 (crash-stop variant): live processes never exceed R while replacements start."""
    t = cluster.timings
    for name in ("a", "b", "c"):
        cluster.start_agent(name)
    await wait_nodes_ready(api, "a", "b", "c")
    await api.create(deployment("web", 3))
    await wait_running(api, "web", 3)

    peak = 0
    stop = asyncio.Event()

    async def watch_processes() -> None:
        nonlocal peak
        while not stop.is_set():
            peak = max(peak, len(live_processes("web")))
            await clock.sleep(0.02)

    watcher = asyncio.create_task(watch_processes())
    cluster.kill_node("a")
    killed_at = clock.monotonic()
    pods = await wait_running(api, "web", 3, within=t.eviction_timeout + 5.0)
    recovered_after = clock.monotonic() - killed_at
    stop.set()
    await watcher

    assert {p.spec.node_name for p in pods} <= {"b", "c"}
    assert peak <= 3, f"saw {peak} live replicas of a 3-replica deployment"
    assert recovered_after <= t.eviction_timeout + 3.0
    lost = [p for p in await owned_pods(api, "web") if p.status.reason == "NodeLost"]
    assert [p.spec.node_name for p in lost] == ["a"]


async def test_control_plane_restart_loses_state_and_agents_stop_workloads(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    """Pins down README limitation #1 (no durability) until PM-2 changes it on purpose:
    after a chronosd restart the agent re-registers, finds no pods, and stops its work."""
    cluster.start_agent("a")
    await wait_nodes_ready(api, "a")
    await api.create(deployment("web", 2))
    await wait_running(api, "web", 2)

    await cluster.restart_server()

    async def stopped_and_back() -> bool:
        nodes = (await api.list(Node))[0]
        return [n.metadata.name for n in nodes] == ["a"] and not live_processes("web")

    await eventually(stopped_and_back)
    assert (await api.list(Deployment))[0] == []
