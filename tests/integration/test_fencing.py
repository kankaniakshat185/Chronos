"""Self-fencing on a real cluster (ADR-0012): S3 and S4 beyond crash-stop.

Each test runs a 2-replica deployment over nodes a and b, breaks node a in one way,
and samples the live workload process groups every 20 ms until a replacement runs
on b. "At most R" must hold throughout: the old workload is dead before the new one
starts.
"""

import asyncio
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from chronos.api import Pod
from chronos.client import HttpStoreClient
from chronos.common.clock import RealClock
from chronos.testing.cluster import Cluster, alive, eventually
from tests.integration.helpers import (
    deployment,
    live_processes,
    owned_pods,
    wait_nodes_ready,
    wait_running,
)
from tests.integration.proxy import Partition

pytestmark = pytest.mark.integration
clock = RealClock()


class Sampler:
    """Live process groups of a deployment over time, from the OS process table."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.peak = 0
        self.samples: list[tuple[float, set[int]]] = []

    @asynccontextmanager
    async def running(self) -> AsyncIterator["Sampler"]:
        stop = asyncio.Event()

        async def loop() -> None:
            while not stop.is_set():
                groups = live_processes(self.name)
                self.samples.append((clock.monotonic(), groups))
                self.peak = max(self.peak, len(groups))
                await clock.sleep(0.02)

        task = asyncio.create_task(loop())
        try:
            yield self
        finally:
            stop.set()
            await task


async def two_replicas(cluster: Cluster, api: HttpStoreClient, *workload_args: str) -> int:
    """web x2, one pod per node; returns the process group of the pod on node a."""
    cluster.start_agent("a")
    cluster.start_agent("b")
    await wait_nodes_ready(api, "a", "b")
    dep = deployment("web", 2)
    dep.spec.template.command += list(workload_args)
    await api.create(dep)
    pods = await wait_running(api, "web", 2)
    assert sorted(p.spec.node_name for p in pods) == ["a", "b"]
    [on_a] = [p.status.pid for p in pods if p.spec.node_name == "a"]
    assert on_a is not None
    return on_a


async def replaced_on_b(api: HttpStoreClient, within: float) -> list[Pod]:
    pods = await wait_running(api, "web", 2, within=within)
    assert {p.spec.node_name for p in pods} == {"b"}
    return pods


async def test_partitioned_node_fences_before_replacements_start(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    """S3/S4 under network partition: the agent is alive but can't reach the control
    plane, so it fences itself; replacements start only after eviction."""
    t = cluster.timings
    link = Partition(cluster.port)
    await link.heal()
    cluster.start_agent("a", server=link.url)
    cluster.start_agent("b")
    await wait_nodes_ready(api, "a", "b")
    await api.create(deployment("web", 2))
    pods = await wait_running(api, "web", 2)
    assert sorted(p.spec.node_name for p in pods) == ["a", "b"]
    [old] = [p.status.pid for p in pods if p.spec.node_name == "a"]
    assert old is not None

    async with Sampler("web").running() as s:
        await link.cut()
        cut_at = clock.monotonic()

        async def old_dead() -> bool:
            return not alive(old)

        await eventually(old_dead, within=t.fence_timeout + t.termination_grace + 1.0)
        dead_after = clock.monotonic() - cut_at
        await replaced_on_b(api, within=t.eviction_timeout + 5.0)
    assert s.peak <= 2, f"saw {s.peak} live replicas of a 2-replica deployment"
    assert dead_after <= t.fence_timeout + t.termination_grace + 0.5  # S4

    # Healing the partition doesn't resurrect anything: the pod on a was evicted.
    await link.heal()
    await clock.sleep(t.eviction_timeout)
    assert len(live_processes("web")) == 2
    await link.cut()


async def test_paused_agent_fences_before_replacements_start(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    """S3/S4 under agent pause (e.g. a long GC): the shim fences while the agent is stopped."""
    t = cluster.timings
    old = await two_replicas(cluster, api)
    async with Sampler("web").running() as s:
        cluster.signal_agent("a", signal.SIGSTOP)
        paused_at = clock.monotonic()

        async def old_dead() -> bool:
            return not alive(old)

        await eventually(old_dead, within=t.fence_timeout + t.termination_grace + 1.0)
        dead_after = clock.monotonic() - paused_at
        await replaced_on_b(api, within=t.eviction_timeout + 5.0)
        cluster.signal_agent("a", signal.SIGCONT)
    assert s.peak <= 2, f"saw {s.peak} live replicas of a 2-replica deployment"
    # S4: gone within F + K (+ one shim poll and process overhead) of the pause.
    assert dead_after <= t.fence_timeout + t.termination_grace + 0.5


async def test_killed_agent_is_fenced_by_its_shims(cluster: Cluster, api: HttpStoreClient) -> None:
    """S3/S4 under agent-only crash: nothing is left to stop the workload but its shim."""
    t = cluster.timings
    old = await two_replicas(cluster, api)
    async with Sampler("web").running() as s:
        cluster.signal_agent("a", signal.SIGKILL)

        async def old_dead() -> bool:
            return not alive(old)

        await eventually(old_dead, within=t.fence_timeout + t.termination_grace + 1.0)
        await replaced_on_b(api, within=t.eviction_timeout + 5.0)
    assert s.peak <= 2, f"saw {s.peak} live replicas of a 2-replica deployment"


async def test_whole_node_freeze_window_is_one_shim_poll(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    """B.5 #2: if agent, shims and workloads all freeze past eviction, nothing can act
    until they resume; then each shim SIGKILLs within one poll (100 ms)."""
    t = cluster.timings
    # Ignoring SIGTERM, only the immediate-SIGKILL path can end it within one poll;
    # the graceful path would take at least the termination grace.
    old = await two_replicas(cluster, api, "--ignore-sigterm")
    async with Sampler("web").running() as s:
        cluster.signal_agent("a", signal.SIGSTOP)
        cluster.signal_pods("a", signal.SIGSTOP)
        try:

            async def replacement_running_on_b() -> bool:
                pods = await owned_pods(api, "web")
                on_b = [p for p in pods if p.spec.node_name == "b" and p.status.phase == "Running"]
                return len(on_b) == 2

            await eventually(replacement_running_on_b, within=t.eviction_timeout + 5.0)
            assert alive(old), "frozen, so it couldn't have fenced itself"
        finally:
            # Resume the shims and workloads only: the agent stays frozen, so the
            # measured window is the shim's own immediate-SIGKILL path.
            cluster.signal_pods("a", signal.SIGCONT)
            resumed_at = clock.monotonic()

        async def old_dead() -> bool:
            return not alive(old)

        try:
            await eventually(old_dead, within=2.0)
            window = clock.monotonic() - resumed_at
        finally:
            cluster.signal_agent("a", signal.SIGCONT)
    # Measured ~65 ms locally (one 100 ms poll at most, plus overhead). Without the
    # immediate-SIGKILL rule it takes >= K = 0.5 s, so 0.3 s tells the two apart.
    assert window < 0.3, f"stale workload outlived the resume by {window:.2f}s"
    over = [ts for ts, groups in s.samples if len(groups) > 2]
    assert not over or max(over) - min(over) <= 0.5


async def test_short_agent_pause_does_not_fence(cluster: Cluster, api: HttpStoreClient) -> None:
    """A stall shorter than the fence timeout must not cost the workloads anything."""
    old = await two_replicas(cluster, api)
    cluster.signal_agent("a", signal.SIGSTOP)
    await clock.sleep(cluster.timings.fence_timeout * 0.5)
    cluster.signal_agent("a", signal.SIGCONT)
    await clock.sleep(cluster.timings.fence_timeout)
    assert alive(old)
    pods = await owned_pods(api, "web")
    assert old in {p.status.pid for p in pods}
