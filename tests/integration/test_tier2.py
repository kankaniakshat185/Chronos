"""Tier 2 on real processes: probes (P10.1), rolling updates (P10.2, S7), memory limits (P10.3)."""

import sys

import pytest

from chronos.api import Deployment, ObjectMeta, Pod, PodPhase, PodSpec, RestartPolicy
from chronos.client import HttpStoreClient
from chronos.common.clock import RealClock
from chronos.store import Conflict
from chronos.testing.cluster import Cluster, eventually, free_port
from tests.integration.helpers import owned_pods, wait_nodes_ready

pytestmark = pytest.mark.integration
clock = RealClock()
WORKLOAD = [sys.executable, "-m", "chronos.testing.workload"]


async def test_readiness_probe_gates_ready(cluster: Cluster, api: HttpStoreClient) -> None:
    cluster.start_agent("a")
    await wait_nodes_ready(api, "a")
    port = free_port()
    spec = PodSpec.model_validate({
        "command": [*WORKLOAD, "--serve", str(port), "--ready-after", "1.5"],
        "readiness_probe": {"http_get": {"port": port, "path": "/ready"}, "period_seconds": 0.2},
    })  # fmt: skip
    await api.create(Pod(metadata=ObjectMeta(name="slow"), spec=spec))

    async def running() -> Pod | None:
        p = await api.get(Pod, "slow")
        return p if p.status.phase == PodPhase.RUNNING else None

    started = await eventually(running)
    assert not started.status.ready  # running, but not ready yet
    t0 = clock.monotonic()

    async def ready() -> bool:
        return (await api.get(Pod, "slow")).status.ready

    await eventually(ready, within=8.0)
    assert clock.monotonic() - t0 >= 0.8  # the workload said 1.5 s after it started
    await api.delete(Pod, "slow")


async def test_failing_liveness_probe_restarts(cluster: Cluster, api: HttpStoreClient) -> None:
    cluster.start_agent("a")
    await wait_nodes_ready(api, "a")
    port = free_port()
    spec = PodSpec.model_validate({
        "command": [*WORKLOAD, "--serve", str(port), "--unhealthy-after", "1"],
        "liveness_probe": {"http_get": {"port": port, "path": "/healthz"}, "period_seconds": 0.2,
                           "failure_threshold": 2},
    })  # fmt: skip
    await api.create(Pod(metadata=ObjectMeta(name="sick"), spec=spec))

    async def restarted() -> bool:
        return (await api.get(Pod, "sick")).status.restart_count >= 1

    await eventually(restarted, within=15.0)
    await api.delete(Pod, "sick")


async def test_memory_limit_kills_with_oomkilled(cluster: Cluster, api: HttpStoreClient) -> None:
    cluster.start_agent("a")
    await wait_nodes_ready(api, "a")
    spec = PodSpec.model_validate({
        "command": [*WORKLOAD, "--leak-mb-per-s", "60"],
        "restart_policy": RestartPolicy.NEVER,
        "resources": {"requests": {"memory": "64Mi"}, "limits": {"memory": "96Mi"}},
    })  # fmt: skip
    await api.create(Pod(metadata=ObjectMeta(name="leaky"), spec=spec))

    async def oom() -> Pod | None:
        p = await api.get(Pod, "leaky")
        return p if p.status.phase == PodPhase.FAILED else None

    pod = await eventually(oom, within=15.0)
    assert pod.status.reason == "OOMKilled", pod.status


async def test_rolling_update_keeps_ready_pods_and_respects_surge(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    """S7 on real processes, sampled: replicas 3, surge 1, unavailable 0."""
    for name in ("a", "b"):
        cluster.start_agent(name)
    await wait_nodes_ready(api, "a", "b")
    dep = Deployment.model_validate({
        "metadata": {"name": "web"},
        "spec": {"replicas": 3, "strategy": {"max_surge": 1, "max_unavailable": 0},
                 "template": {"command": [*WORKLOAD, "--label", "v1"],
                              "readiness_probe": {"exec": ["true"], "period_seconds": 0.2}}},
    })  # fmt: skip
    await api.create(dep)

    async def all_ready(version: str) -> bool:
        pods = [p for p in await owned_pods(api, "web") if p.status.phase == PodPhase.RUNNING]
        current = [p for p in pods if version in p.spec.command and p.status.ready]
        return len(current) == 3 and len(pods) == 3

    await eventually(lambda: all_ready("v1"), within=15.0)
    for _ in range(10):
        try:
            current = await api.get(Deployment, "web")
            current.spec.template.command = [*WORKLOAD, "--label", "v2"]
            await api.update(current)
            break
        except Conflict:
            continue
    worst_ready, most = 3, 3
    while not await all_ready("v2"):
        pods = await owned_pods(api, "web")
        active = [p for p in pods if p.status.phase not in (PodPhase.FAILED, PodPhase.SUCCEEDED)
                  and p.metadata.deletion_timestamp is None]  # fmt: skip
        most = max(most, len(active))
        worst_ready = min(worst_ready, sum(p.status.ready for p in active))
        await clock.sleep(0.05)
    assert most <= 4, "more than replicas + max_surge pods during the rollout"
    assert worst_ready >= 3, "fewer than replicas - max_unavailable ready during the rollout"


async def test_drain_moves_pods_off_and_cordons(cluster: Cluster, api: HttpStoreClient) -> None:
    import asyncio  # noqa: PLC0415

    for name in ("a", "b"):
        cluster.start_agent(name)
    await wait_nodes_ready(api, "a", "b")
    await api.create(Deployment.model_validate({
        "metadata": {"name": "web"},
        "spec": {"replicas": 2, "template": {
            "command": [*WORKLOAD, "--label", "drain"],
            "resources": {"requests": {"cpu": "1"}},  # so LeastAllocated spreads them
        }},
    }))  # fmt: skip

    async def spread() -> bool:
        pods = [p for p in await owned_pods(api, "web") if p.status.phase == PodPhase.RUNNING]
        return sorted(p.spec.node_name for p in pods) == ["a", "b"]

    await eventually(spread)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "chronos.cli.main", "--server", cluster.url, "drain", "a",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )  # fmt: skip
    out, _ = await asyncio.wait_for(proc.communicate(), 60)
    assert proc.returncode == 0, out.decode()
    assert "node/a drained (1 pods)" in out.decode()

    async def all_on_b() -> bool:
        pods = [p for p in await owned_pods(api, "web") if p.status.phase == PodPhase.RUNNING]
        return len(pods) == 2 and {p.spec.node_name for p in pods} == {"b"}

    await eventually(all_on_b, within=15.0)
    from chronos.api import Node  # noqa: PLC0415

    assert (await api.get(Node, "a")).spec.unschedulable
