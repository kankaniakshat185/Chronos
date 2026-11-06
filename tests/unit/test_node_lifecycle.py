"""Node lifecycle controller on a FakeClock (ADR-0011)."""

import pytest

from chronos.api import Node, NodeCondition
from chronos.client import LocalStoreClient
from chronos.common.clock import FakeClock
from chronos.common.config import Timings
from chronos.controllers.node_lifecycle import NodeLifecycleController
from chronos.controllers.runtime import Informers
from chronos.store import Store
from chronos.store.errors import EpochMismatch
from chronos.store.leases import Leases

TIMINGS = Timings(
    heartbeat_interval=0.5, heartbeat_timeout=0.4, node_grace=2.0, eviction_timeout=5.0
)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def client(clock: FakeClock) -> LocalStoreClient:
    return LocalStoreClient(Store(clock), Leases(clock))


@pytest.fixture
def controller(client: LocalStoreClient, clock: FakeClock) -> NodeLifecycleController:
    return NodeLifecycleController(client, clock, TIMINGS)


async def state(client: LocalStoreClient) -> tuple[NodeCondition, str]:
    node = await client.get(Node, "n1")
    return node.status.condition, node.status.agent_epoch


async def test_full_lifecycle(
    client: LocalStoreClient, clock: FakeClock, controller: NodeLifecycleController
) -> None:
    await client.register("n1", "e1", {"cpu": 1000})
    await controller.run_once()
    assert await state(client) == (NodeCondition.READY, "e1")

    # Heartbeats keep it Ready past the grace period.
    for _ in range(10):
        clock.advance(0.5)
        await client.heartbeat("n1", "e1")
        await controller.run_once()
    assert await state(client) == (NodeCondition.READY, "e1")

    clock.advance(1.999)
    await controller.run_once()
    assert (await state(client))[0] == NodeCondition.READY
    clock.advance(0.002)
    await controller.run_once()
    assert (await state(client))[0] == NodeCondition.UNKNOWN

    # A heartbeat during Unknown brings it straight back.
    await client.heartbeat("n1", "e1")
    await controller.run_once()
    assert (await state(client))[0] == NodeCondition.READY

    clock.advance(5.0)
    await controller.run_once()
    assert await state(client) == (NodeCondition.NOT_READY, "")

    # The old incarnation is fenced out; only a re-registration revives the node.
    with pytest.raises(EpochMismatch):
        await client.heartbeat("n1", "e1")
    clock.advance(1.0)
    await controller.run_once()
    assert (await state(client))[0] == NodeCondition.NOT_READY

    await client.register("n1", "e2", {"cpu": 1000})
    await controller.run_once()
    assert await state(client) == (NodeCondition.READY, "e2")


async def test_transitions_are_single_writes(
    client: LocalStoreClient, clock: FakeClock, controller: NodeLifecycleController
) -> None:
    await client.register("n1", "e1", {})
    await controller.run_once()
    rev = client.store.revision
    for _ in range(5):
        await controller.run_once()  # no change -> no write
    assert client.store.revision == rev


async def test_node_created_without_a_lease_gets_a_full_grace_period(
    client: LocalStoreClient, clock: FakeClock, controller: NodeLifecycleController
) -> None:
    clock.advance(100.0)
    await client.create(Node.model_validate({"metadata": {"name": "n1"}}))
    await controller.run_once()
    assert (await state(client))[0] == NodeCondition.READY
    clock.advance(5.0)
    await controller.run_once()
    assert (await state(client))[0] == NodeCondition.NOT_READY


async def test_runs_on_informers_and_reacts_to_a_watched_change(
    client: LocalStoreClient, clock: FakeClock
) -> None:
    import asyncio  # noqa: PLC0415

    informers = Informers(client, clock)
    controller = NodeLifecycleController(client, clock, TIMINGS, informers)
    tasks = [asyncio.create_task(informers.run()), asyncio.create_task(controller.run())]
    for _ in range(10):
        await asyncio.sleep(0)  # noqa: TID251
    await client.register("n1", "e1", {})  # after start: only the watch can deliver it
    for _ in range(20):
        await asyncio.sleep(0)  # noqa: TID251
    assert (await state(client))[0] == NodeCondition.READY
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def test_not_ready_evicts_active_pods_as_node_lost(
    client: LocalStoreClient, clock: FakeClock, controller: NodeLifecycleController
) -> None:
    from chronos.api import ObjectMeta, Pod, PodPhase, PodSpec  # noqa: PLC0415

    await client.register("n1", "e1", {"cpu": 4000})
    await controller.run_once()
    for name in ("a", "b"):
        p = await client.create(Pod(metadata=ObjectMeta(name=name), spec=PodSpec(command=["x"])))
        await client.bind(name, "n1", p.metadata.resource_version)
    done = await client.get(Pod, "b")
    done.status.phase = PodPhase.SUCCEEDED
    await client.update_status(done)

    clock.advance(5.0)
    await controller.run_once()
    a, b = await client.get(Pod, "a"), await client.get(Pod, "b")
    assert (a.status.phase, a.status.reason) == (PodPhase.FAILED, "NodeLost")
    assert (b.status.phase, b.status.reason) == (PodPhase.SUCCEEDED, "")  # finished: untouched
    assert (await client.get(Node, "n1")).status.allocated["cpu"] == 0

    rev = client.store.revision
    await controller.run_once()
    assert client.store.revision == rev  # nothing left to evict


async def test_a_stale_cache_never_evicts_from_a_node_that_came_back(
    client: LocalStoreClient, clock: FakeClock, controller: NodeLifecycleController
) -> None:
    from chronos.api import ObjectMeta, Pod, PodPhase, PodSpec  # noqa: PLC0415

    await client.register("n1", "e1", {"cpu": 4000})
    await controller.run_once()
    clock.advance(5.0)
    await controller.run_once()  # NotReady, epoch revoked
    await controller._nodes.relist()
    # Meanwhile the agent comes back, the node is Ready again, and a pod is bound to it.
    await client.register("n1", "e2", {"cpu": 4000})
    node = await client.get(Node, "n1")
    node.status.condition = NodeCondition.READY
    await client.update_status(node)
    p = await client.create(Pod(metadata=ObjectMeta(name="q"), spec=PodSpec(command=["x"])))
    await client.bind("q", "n1", p.metadata.resource_version)
    await controller._pods.relist()

    controller.queue.add("n1")
    await controller.process_one()
    assert (await client.get(Pod, "q")).status.phase == PodPhase.BOUND  # the live read saved it
