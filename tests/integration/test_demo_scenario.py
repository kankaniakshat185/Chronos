"""The demo script (PLAN.md A.7) as a test: the demo is a test before it's a demo.

Cluster: plain-1 (no slots), slot-a and slot-b (chronos.dev/slot=1 each).
Steps follow the demo's numbering. S11 is checked continuously: no slot-demo pod
is ever bound to plain-1, not even for one sample.
"""

import asyncio

import pytest

from chronos.api import Node, NodeCondition, Pod, PodPhase
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
SLOT = "chronos.dev/slot"
SLOT_REQUESTS = {"cpu": "500m", "memory": "128Mi", SLOT: 1}


async def pending_slot_pods(api: HttpStoreClient) -> list[Pod]:
    return [
        p
        for p in await owned_pods(api, "slot-demo")
        if p.status.phase == PodPhase.PENDING and p.status.reason == "Unschedulable"
    ]


async def test_demo_scenario(cluster: Cluster, api: HttpStoreClient) -> None:  # noqa: PLR0915 (one walkthrough)
    t = cluster.timings
    # 0-1: one control plane, three agents.
    cluster.start_agent("plain-1")
    cluster.start_agent("slot-a", resources={SLOT: 1})
    cluster.start_agent("slot-b", resources={SLOT: 1})
    await wait_nodes_ready(api, "plain-1", "slot-a", "slot-b")
    nodes = {n.metadata.name: n for n in (await api.list(Node))[0]}
    assert SLOT not in nodes["plain-1"].spec.capacity
    assert nodes["slot-a"].status.allocated[SLOT] == 0

    violations: list[str] = []
    stop = asyncio.Event()

    async def watch_s11() -> None:
        while not stop.is_set():
            for p in await owned_pods(api, "slot-demo"):
                if p.spec.node_name == "plain-1":
                    violations.append(p.metadata.name)
            await clock.sleep(0.05)

    watcher = asyncio.create_task(watch_s11())
    try:
        # 2: apply web (3 replicas): spread across the nodes, each a real process.
        await api.create(deployment("web", 3))
        web = await wait_running(api, "web", 3)
        assert sorted(p.spec.node_name for p in web) == ["plain-1", "slot-a", "slot-b"]

        # 4: scale web to 5.
        await scale(api, "web", 5)
        await wait_running(api, "web", 5)

        # 5: slot-demo wants 3 slots; only 2 exist. The third waits, and says why.
        await api.create(deployment("slot-demo", 3, SLOT_REQUESTS))

        async def two_running_one_waiting() -> list[Pod] | None:
            pods = await owned_pods(api, "slot-demo")
            running = [p for p in pods if p.status.phase == PodPhase.RUNNING]
            waiting = await pending_slot_pods(api)
            return running if len(running) == 2 and len(waiting) == 1 else None

        slot = await eventually(two_running_one_waiting)
        assert sorted(p.spec.node_name for p in slot) == ["slot-a", "slot-b"]
        [waiting] = await pending_slot_pods(api)
        assert waiting.status.message == (
            "0/3 nodes available: Insufficient chronos.dev/slot (3/3 nodes)"
        )

        # 6: scale slot-demo to 2: the Pending pod is the victim; running work is untouched.
        await scale(api, "slot-demo", 2)
        after = await wait_running(api, "slot-demo", 2)
        assert {p.metadata.name for p in after} == {p.metadata.name for p in slot}

        # 7: kill slot-a. Unknown, then NotReady; its pods become Failed/NodeLost.
        cluster.kill_node("slot-a")

        async def node_is(want: NodeCondition) -> bool:
            return (await api.get(Node, "slot-a")).status.condition == want

        await eventually(lambda: node_is(NodeCondition.UNKNOWN), within=t.node_grace + 1)
        await eventually(lambda: node_is(NodeCondition.NOT_READY), within=t.eviction_timeout + 1)
        web = await wait_running(api, "web", 5)
        assert "slot-a" not in {p.spec.node_name for p in web}

        async def slot_replacement_waits() -> list[Pod] | None:
            pods = await owned_pods(api, "slot-demo")
            running = [p for p in pods if p.status.phase == PodPhase.RUNNING]
            lost = [p for p in pods if p.status.reason == "NodeLost"]
            waiting = await pending_slot_pods(api)
            ok = len(running) == 1 and len(waiting) == 1 and len(lost) == 1
            return running if ok else None

        [survivor] = await eventually(slot_replacement_waits)
        assert survivor.spec.node_name == "slot-b"
        assert len(live_processes("slot-demo")) == 1

        # 8: restart slot-a: capacity comes back, the waiting pod takes it.
        cluster.start_agent("slot-a", resources={SLOT: 1})
        await wait_nodes_ready(api, "slot-a")
        slot = await wait_running(api, "slot-demo", 2)
        assert sorted(p.spec.node_name for p in slot) == ["slot-a", "slot-b"]

        # Clean up through the API: deleting deployments stops every process (L5).
        for name in ("web", "slot-demo"):
            await scale(api, name, 0)

        async def nothing_runs() -> bool:
            return not live_processes("web") and not live_processes("slot-demo")

        await eventually(nothing_runs)
    finally:
        stop.set()
        await watcher
    assert violations == [], f"slot-demo pods bound to plain-1: {sorted(set(violations))}"
