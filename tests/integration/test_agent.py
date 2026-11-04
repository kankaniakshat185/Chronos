"""Agents and real processes (P2.10 subset): registration, health, start/stop, exits."""

import sys

import pytest

from chronos.api import Node, NodeCondition, ObjectMeta, Pod, PodPhase, PodSpec
from chronos.client import HttpStoreClient
from chronos.common.clock import RealClock
from chronos.testing.cluster import Cluster, alive, eventually

pytestmark = pytest.mark.integration
clock = RealClock()


def workload(*args: str) -> list[str]:
    return [sys.executable, "-m", "chronos.testing.workload", *args]


async def ready_nodes(api: HttpStoreClient, *names: str) -> None:
    async def all_ready() -> bool:
        nodes, _ = await api.list(Node)
        conditions = {n.metadata.name: n.status.condition for n in nodes}
        return all(conditions.get(n) == NodeCondition.READY for n in names)

    await eventually(all_ready)


async def run_pod(api: HttpStoreClient, name: str, node: str, command: list[str]) -> Pod:
    """Create a pod and wait until it's Running; the scheduler must have put it on `node`."""
    await api.create(Pod(metadata=ObjectMeta(name=name), spec=PodSpec(command=command)))

    async def running() -> Pod | None:
        p = await api.get(Pod, name)
        return p if p.status.phase == PodPhase.RUNNING else None

    pod = await eventually(running)
    assert pod.spec.node_name == node
    return pod


async def test_three_agents_register_and_become_ready(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    for name in ("a", "b", "c"):
        cluster.start_agent(name, resources={"chronos.dev/slot": 1} if name != "a" else None)
    await ready_nodes(api, "a", "b", "c")
    b = await api.get(Node, "b")
    assert b.spec.capacity == {"cpu": 4000, "memory": 8 * 2**30, "chronos.dev/slot": 1}


async def test_bound_pod_becomes_a_real_process(cluster: Cluster, api: HttpStoreClient) -> None:
    cluster.start_agent("a")
    await ready_nodes(api, "a")
    pod = await run_pod(api, "web", "a", workload("--label", "hello"))
    assert pod.status.pid is not None and alive(pod.status.pid)

    log = cluster.agent_dir("a") / "pods" / pod.metadata.uid / "stdout.log"

    async def logged() -> str:
        return log.read_text() if log.exists() else ""

    line = await eventually(logged)
    assert f"hello pod=web node=a pid={pod.status.pid}" in line
    assert (cluster.agent_dir("a") / "pids" / pod.metadata.uid).read_text() == str(pod.status.pid)


async def test_deleting_the_pod_stops_its_process(cluster: Cluster, api: HttpStoreClient) -> None:
    cluster.start_agent("a")
    await ready_nodes(api, "a")
    pod = await run_pod(api, "web", "a", workload())
    assert pod.status.pid is not None
    await api.delete(Pod, "web")

    async def gone() -> bool:
        return not alive(pod.status.pid or 0)

    await eventually(gone, within=3.0)
    pidfile = cluster.agent_dir("a") / "pids" / pod.metadata.uid

    async def cleaned() -> bool:
        return not pidfile.exists()

    await eventually(cleaned)


async def test_sigterm_ignoring_workload_is_killed_after_grace(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    grace = cluster.timings.termination_grace
    cluster.start_agent("a")
    await ready_nodes(api, "a")
    pod = await run_pod(api, "stubborn", "a", workload("--ignore-sigterm"))
    pid = pod.status.pid
    assert pid is not None
    await api.delete(Pod, "stubborn")
    start = clock.monotonic()
    await clock.sleep(grace / 2)
    assert alive(pid), "SIGTERM should have been ignored"

    async def gone() -> bool:
        return not alive(pid)

    await eventually(gone, within=grace + 2.0)
    assert clock.monotonic() - start >= grace * 0.9


async def test_workload_exit_is_reported_as_failed(cluster: Cluster, api: HttpStoreClient) -> None:
    cluster.start_agent("a")
    await ready_nodes(api, "a")
    await run_pod(api, "crashy", "a", workload("--crash-after", "0.3", "--exit-code", "3"))

    async def failed() -> Pod | None:
        p = await api.get(Pod, "crashy")
        return p if p.status.phase == PodPhase.FAILED else None

    pod = await eventually(failed)
    assert (pod.status.reason, pod.status.exit_code) == ("Exited", 3)


async def test_unstartable_command_fails_the_pod(cluster: Cluster, api: HttpStoreClient) -> None:
    cluster.start_agent("a")
    await ready_nodes(api, "a")
    await api.create(
        Pod(metadata=ObjectMeta(name="typo"), spec=PodSpec(command=["/no/such/binary"]))
    )

    async def failed() -> Pod | None:
        p = await api.get(Pod, "typo")
        return p if p.status.phase == PodPhase.FAILED else None

    assert (await eventually(failed)).status.exit_code == 127


async def test_kill_node_goes_unknown_then_not_ready(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    """L1: a dead node is detected within the configured windows."""
    t = cluster.timings
    cluster.start_agent("a")
    await ready_nodes(api, "a")
    pod = await run_pod(api, "web", "a", workload())
    cluster.kill_node("a")
    killed_at = clock.monotonic()
    pid = pod.status.pid
    assert pid is not None

    async def dead() -> bool:
        return not alive(pid)

    await eventually(dead, within=1.0)  # crash-stop: the workload died with its node

    async def condition(want: NodeCondition) -> bool:
        return (await api.get(Node, "a")).status.condition == want

    await eventually(lambda: condition(NodeCondition.UNKNOWN), within=t.node_grace + 1.0)
    unknown_after = clock.monotonic() - killed_at
    await eventually(lambda: condition(NodeCondition.NOT_READY), within=t.eviction_timeout + 1.0)
    not_ready_after = clock.monotonic() - killed_at
    # Silence counts from the last heartbeat, up to one interval before the kill.
    # Upper bounds: one 250 ms lifecycle tick plus slack for slow CI runners.
    assert t.node_grace - t.heartbeat_interval <= unknown_after <= t.node_grace + 1.0
    assert t.eviction_timeout - t.heartbeat_interval <= not_ready_after
    assert not_ready_after <= t.eviction_timeout + 1.0


async def test_restarted_agent_re_registers_with_a_new_epoch(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    cluster.start_agent("a")
    await ready_nodes(api, "a")
    first = (await api.get(Node, "a")).status.agent_epoch
    cluster.stop_agent("a")
    cluster.start_agent("a")

    async def new_epoch() -> bool:
        node = await api.get(Node, "a")
        return node.status.agent_epoch not in ("", first)

    await eventually(new_epoch)
    await ready_nodes(api, "a")


async def test_graceful_agent_stop_stops_its_workloads(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    cluster.start_agent("a")
    await ready_nodes(api, "a")
    pod = await run_pod(api, "web", "a", workload())
    cluster.stop_agent("a")
    assert pod.status.pid is not None
    assert not alive(pod.status.pid)
    assert not list((cluster.agent_dir("a") / "pids").iterdir())
