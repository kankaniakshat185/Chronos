"""Deployments on a real cluster (P4.10 subset): apply, scale, delete, node loss (S3, L2, L5)."""

import asyncio

import pytest

from chronos.api import Deployment, Pod, PodPhase
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


async def test_crashed_workload_is_replaced(cluster: Cluster, api: HttpStoreClient) -> None:
    cluster.start_agent("a")
    await wait_nodes_ready(api, "a")
    dep = deployment("crashy", 1)
    dep.spec.template.command += ["--crash-after", "0.5", "--exit-code", "3"]
    await api.create(dep)

    async def replaced() -> list[Pod] | None:
        pods = await owned_pods(api, "crashy")
        failed = [p for p in pods if p.status.phase == PodPhase.FAILED]
        return failed if failed and len(pods) > len(failed) else None

    failed = await eventually(replaced)
    assert (failed[0].status.reason, failed[0].status.exit_code) == ("Exited", 3)  # M6.4


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
