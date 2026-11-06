"""Scheduler on informer caches: the assume cache (ADR-0013, P3.3)."""

import pytest

from chronos.api import Node, NodeCondition, NodeSpec, ObjectMeta, Pod, PodSpec
from chronos.client import LocalStoreClient
from chronos.common.clock import FakeClock
from chronos.controllers.scheduler import SchedulerController
from chronos.core.schedule import SchedulerConfig, resolve
from chronos.store import Store
from chronos.store.leases import Leases


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def client(clock: FakeClock) -> LocalStoreClient:
    return LocalStoreClient(Store(clock), Leases(clock))


@pytest.fixture
def scheduler(client: LocalStoreClient, clock: FakeClock) -> SchedulerController:
    return SchedulerController(client, clock, resolve(SchedulerConfig()), assume_ttl=30.0)


async def setup(client: LocalStoreClient, clock: FakeClock) -> None:
    """Two 1-cpu nodes and two 1-cpu pods: each pod fits on exactly one node at a time."""
    for name in ("n1", "n2"):
        spec = NodeSpec.model_validate({"capacity": {"cpu": "1"}})
        node = await client.create(Node(metadata=ObjectMeta(name=name), spec=spec))
        node.status.condition = NodeCondition.READY
        await client.update_status(node)
    for name in ("p1", "p2"):
        clock.advance(1.0)  # p1 is older
        spec = PodSpec.model_validate({"command": ["x"], "resources": {"requests": {"cpu": "1"}}})
        await client.create(Pod(metadata=ObjectMeta(name=name), spec=spec))


async def test_assumed_binds_count_before_the_cache_sees_them(
    client: LocalStoreClient, clock: FakeClock, scheduler: SchedulerController
) -> None:
    await setup(client, clock)
    # One pass with no watch running: after p1's bind the cache still shows n1 empty.
    await scheduler.run_once()
    placed = {p.metadata.name: p.spec.node_name for p in (await client.list(Pod))[0]}
    assert placed == {"p1": "n1", "p2": "n2"}  # without the assumption p2 picks n1, and fails
    assert set(scheduler.assumed) == {"p1", "p2"}
    await scheduler.run_once()  # the cache sees both bindings: assumptions are dropped
    assert scheduler.assumed == {}


async def test_an_assumption_that_is_never_observed_expires(
    client: LocalStoreClient, clock: FakeClock, scheduler: SchedulerController
) -> None:
    await setup(client, clock)
    await scheduler.run_once()
    await client.delete(Pod, "p1", force=True)  # gone before the cache saw it bound
    clock.advance(30.1)
    infos = {n.name: n for n in scheduler.node_infos()}
    assert "p1" not in scheduler.assumed
    assert infos["n1"].allocated.get("cpu", 0) == 0


async def test_a_node_change_requeues_pending_pods(
    client: LocalStoreClient, clock: FakeClock, scheduler: SchedulerController
) -> None:
    spec = PodSpec.model_validate({"command": ["x"], "resources": {"requests": {"cpu": "1"}}})
    await client.create(Pod(metadata=ObjectMeta(name="p"), spec=spec))
    await scheduler.run_once()  # no nodes: Unschedulable
    assert (await client.get(Pod, "p")).status.reason == "Unschedulable"
    await scheduler._pods.relist()
    while len(scheduler.queue):
        await scheduler.process_one()  # still nowhere to go; nothing left queued
    node = await client.create(
        Node(
            metadata=ObjectMeta(name="n1"), spec=NodeSpec.model_validate({"capacity": {"cpu": "1"}})
        )
    )
    node.status.condition = NodeCondition.READY
    await client.update_status(node)
    await scheduler._nodes.relist()
    assert len(scheduler.queue) == 1  # the waiting pod gets another look
    await scheduler.process_one()
    assert (await client.get(Pod, "p")).spec.node_name == "n1"
