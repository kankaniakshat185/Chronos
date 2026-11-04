"""Node lifecycle controller on a FakeClock (ADR-0011)."""

import pytest

from chronos.api import Node, NodeCondition
from chronos.client import LocalStoreClient
from chronos.common.clock import FakeClock
from chronos.common.config import Timings
from chronos.controllers.node_lifecycle import NodeLifecycleController
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
    await controller.tick()
    assert await state(client) == (NodeCondition.READY, "e1")

    # Heartbeats keep it Ready past the grace period.
    for _ in range(10):
        clock.advance(0.5)
        await client.heartbeat("n1", "e1")
        await controller.tick()
    assert await state(client) == (NodeCondition.READY, "e1")

    clock.advance(1.999)
    await controller.tick()
    assert (await state(client))[0] == NodeCondition.READY
    clock.advance(0.002)
    await controller.tick()
    assert (await state(client))[0] == NodeCondition.UNKNOWN

    # A heartbeat during Unknown brings it straight back.
    await client.heartbeat("n1", "e1")
    await controller.tick()
    assert (await state(client))[0] == NodeCondition.READY

    clock.advance(5.0)
    await controller.tick()
    assert await state(client) == (NodeCondition.NOT_READY, "")

    # The old incarnation is fenced out; only a re-registration revives the node.
    with pytest.raises(EpochMismatch):
        await client.heartbeat("n1", "e1")
    clock.advance(1.0)
    await controller.tick()
    assert (await state(client))[0] == NodeCondition.NOT_READY

    await client.register("n1", "e2", {"cpu": 1000})
    await controller.tick()
    assert await state(client) == (NodeCondition.READY, "e2")


async def test_transitions_are_single_writes(
    client: LocalStoreClient, clock: FakeClock, controller: NodeLifecycleController
) -> None:
    await client.register("n1", "e1", {})
    await controller.tick()
    rev = client.store.revision
    for _ in range(5):
        await controller.tick()  # no change -> no write
    assert client.store.revision == rev


async def test_node_created_without_a_lease_gets_a_full_grace_period(
    client: LocalStoreClient, clock: FakeClock, controller: NodeLifecycleController
) -> None:
    clock.advance(100.0)
    await client.create(Node.model_validate({"metadata": {"name": "n1"}}))
    await controller.tick()
    assert (await state(client))[0] == NodeCondition.READY
    clock.advance(5.0)
    await controller.tick()
    assert (await state(client))[0] == NodeCondition.NOT_READY


async def test_run_loop_ticks_on_the_clock(
    client: LocalStoreClient, clock: FakeClock, controller: NodeLifecycleController
) -> None:
    import asyncio  # noqa: PLC0415

    await client.register("n1", "e1", {})
    task = asyncio.create_task(controller.run())
    for _ in range(3):
        await asyncio.sleep(0)  # noqa: TID251
    assert (await state(client))[0] == NodeCondition.READY
    task.cancel()


async def test_not_ready_evicts_active_pods_as_node_lost(
    client: LocalStoreClient, clock: FakeClock, controller: NodeLifecycleController
) -> None:
    from chronos.api import ObjectMeta, Pod, PodPhase, PodSpec  # noqa: PLC0415

    await client.register("n1", "e1", {"cpu": 4000})
    await controller.tick()
    for name in ("a", "b"):
        p = await client.create(Pod(metadata=ObjectMeta(name=name), spec=PodSpec(command=["x"])))
        await client.bind(name, "n1", p.metadata.resource_version)
    done = await client.get(Pod, "b")
    done.status.phase = PodPhase.SUCCEEDED
    await client.update_status(done)

    clock.advance(5.0)
    await controller.tick()
    a, b = await client.get(Pod, "a"), await client.get(Pod, "b")
    assert (a.status.phase, a.status.reason) == (PodPhase.FAILED, "NodeLost")
    assert (b.status.phase, b.status.reason) == (PodPhase.SUCCEEDED, "")  # finished: untouched
    assert (await client.get(Node, "n1")).status.allocated["cpu"] == 0

    rev = client.store.revision
    await controller.tick()
    assert client.store.revision == rev  # nothing left to evict
