"""Scheduling on a real cluster: the extended-resource hard constraint end to end (S11)."""

import sys

import pytest

from chronos.api import Node, NodeCondition, ObjectMeta, Pod, PodPhase, PodSpec
from chronos.client import HttpStoreClient
from chronos.store import NotFound
from chronos.testing.cluster import Cluster, eventually

pytestmark = pytest.mark.integration
SLOT = "chronos.dev/slot"


def slot_pod(name: str) -> Pod:
    spec = PodSpec.model_validate(
        {
            "command": [sys.executable, "-m", "chronos.testing.workload", "--label", name],
            "resources": {"requests": {"cpu": "500m", "memory": "128Mi", SLOT: 1}},
        }
    )
    return Pod(metadata=ObjectMeta(name=name), spec=spec)


async def test_slot_pods_only_land_on_slot_nodes(cluster: Cluster, api: HttpStoreClient) -> None:
    cluster.start_agent("plain-1")
    cluster.start_agent("slot-a", resources={SLOT: 1})

    async def ready() -> bool:
        nodes = (await api.list(Node))[0]
        return len(nodes) == 2 and all(n.status.condition == NodeCondition.READY for n in nodes)

    await eventually(ready)
    for name in ("s1", "s2"):
        await api.create(slot_pod(name))

    async def settled() -> list[Pod] | None:
        pods = (await api.list(Pod))[0]
        running = [p for p in pods if p.status.phase == PodPhase.RUNNING]
        waiting = [p for p in pods if p.status.reason == "Unschedulable"]
        return pods if len(running) == 1 and len(waiting) == 1 else None

    pods = {p.status.phase: p for p in await eventually(settled)}
    assert pods[PodPhase.RUNNING].spec.node_name == "slot-a"
    assert pods[PodPhase.RUNNING].status.assigned == {SLOT: ["0"]}  # X-5 counting provider
    waiting = pods[PodPhase.PENDING]
    assert waiting.spec.node_name == ""
    assert waiting.status.message == (
        "0/2 nodes available: Insufficient chronos.dev/slot (2/2 nodes)"
    )
    slot_a = await api.get(Node, "slot-a")
    assert slot_a.status.allocated[SLOT] == 1
    plain = await api.get(Node, "plain-1")
    assert SLOT not in plain.spec.capacity and plain.status.allocated.get(SLOT, 0) == 0

    # Free the slot: the waiting pod gets it, and still never touches plain-1.
    await api.delete(Pod, pods[PodPhase.RUNNING].metadata.name)

    async def moved_in() -> Pod | None:
        p = await api.get(Pod, waiting.metadata.name)
        return p if p.status.phase == PodPhase.RUNNING else None

    now_running = await eventually(moved_in)
    assert now_running.spec.node_name == "slot-a"
    assert now_running.status.reason == ""


async def test_slot_units_are_exclusive_and_reused(cluster: Cluster, api: HttpStoreClient) -> None:
    """S12 on a real node: two pods on a 2-slot node hold different units; a unit freed
    by a deleted pod goes to the next one."""
    cluster.start_agent("slots", resources={SLOT: 2})

    async def ready() -> bool:
        nodes = (await api.list(Node))[0]
        return bool(nodes) and nodes[0].status.condition == NodeCondition.READY

    await eventually(ready)
    for name in ("s1", "s2"):
        await api.create(slot_pod(name))

    async def both_assigned() -> dict[str, list[str]] | None:
        pods = (await api.list(Pod))[0]
        units = {p.metadata.name: p.status.assigned.get(SLOT, []) for p in pods}
        return units if all(units.get(n) for n in ("s1", "s2")) else None

    units = await eventually(both_assigned)
    assert sorted(units["s1"] + units["s2"]) == ["0", "1"]

    await api.delete(Pod, "s1")
    await api.create(slot_pod("s3"))

    async def s3_assigned() -> list[str] | None:
        try:
            return (await api.get(Pod, "s3")).status.assigned.get(SLOT) or None
        except NotFound:
            return None

    assert await eventually(s3_assigned) == units["s1"]
