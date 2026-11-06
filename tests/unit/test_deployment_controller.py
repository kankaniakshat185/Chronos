"""Deployment controller on a FakeClock and a real store."""

import pytest

from chronos.api import Deployment, DeploymentSpec, ObjectMeta, Pod, PodPhase, PodTemplate
from chronos.client import LocalStoreClient
from chronos.common.clock import FakeClock
from chronos.controllers.deployment import DeploymentController, pod_name
from chronos.core.reconcile import DEPLOYMENT_LABEL
from chronos.store import Store
from chronos.store.leases import Leases


@pytest.fixture
def client() -> LocalStoreClient:
    clock = FakeClock()
    return LocalStoreClient(Store(clock), Leases(clock))


@pytest.fixture
def controller(client: LocalStoreClient) -> DeploymentController:
    return DeploymentController(client, FakeClock())


async def make(client: LocalStoreClient, replicas: int) -> Deployment:
    return await client.create(
        Deployment(
            metadata=ObjectMeta(name="web"),
            spec=DeploymentSpec(replicas=replicas, template=PodTemplate(command=["sleep", "9"])),
        )
    )


async def pods(client: LocalStoreClient) -> list[Pod]:
    return (await client.list(Pod))[0]


async def test_creates_owned_pods_and_records_status(
    client: LocalStoreClient, controller: DeploymentController
) -> None:
    d = await make(client, 3)
    await controller.run_once()
    owned = await pods(client)
    assert len(owned) == 3
    assert {p.metadata.owner for p in owned} == {d.metadata.uid}
    assert all(p.metadata.labels[DEPLOYMENT_LABEL] == "web" for p in owned)
    await controller.run_once()
    status = (await client.get(Deployment, "web")).status
    assert (status.replicas, status.observed_generation) == (3, 1)


async def test_scales_up_and_down(
    client: LocalStoreClient, controller: DeploymentController
) -> None:
    await make(client, 3)
    await controller.run_once()
    for replicas in (5, 1):
        d = await client.get(Deployment, "web")
        d.spec.replicas = replicas
        await client.update(d)
        await controller.run_once()
        assert len(await pods(client)) == replicas


async def test_failed_pod_is_replaced(
    client: LocalStoreClient, controller: DeploymentController
) -> None:
    await make(client, 1)
    await controller.run_once()
    [p] = await pods(client)
    p.status.phase = PodPhase.FAILED
    await client.update_status(p)
    await controller.run_once()
    phases = sorted(q.status.phase for q in await pods(client))
    assert phases == [PodPhase.FAILED, PodPhase.PENDING]


async def test_deleting_the_deployment_collects_its_pods(
    client: LocalStoreClient, controller: DeploymentController
) -> None:
    await make(client, 2)
    await controller.run_once()
    await client.delete(Deployment, "web")
    await controller.run_once()
    assert await pods(client) == []


async def test_recreated_deployment_does_not_adopt_old_pods(
    client: LocalStoreClient, controller: DeploymentController
) -> None:
    await make(client, 1)
    await controller.run_once()
    [old] = await pods(client)
    await client.delete(Deployment, "web")
    await make(client, 1)
    await controller.run_once()
    names = [p.metadata.name for p in await pods(client)]
    assert old.metadata.name not in names and len(names) == 1


def test_pod_names_are_valid_and_bounded() -> None:
    name = pod_name("x" * 63)
    assert len(name) == 63
    assert ObjectMeta(name=name).name == name


async def test_expectations_stop_duplicate_creates_from_a_stale_cache(
    client: LocalStoreClient, controller: DeploymentController
) -> None:
    """ADR-0009 3.C: creates conflict with nothing, so only expectations prevent this."""
    d = await make(client, 3)
    await controller.run_once()
    assert len(await pods(client)) == 3
    # The pod cache hasn't seen those creates (no watch is running here). Reconcile again:
    controller.queue.add(d.metadata.uid)
    await controller.process_one()
    assert len(await pods(client)) == 3  # without expectations: 6
    assert controller.expectations.pending(d.metadata.uid) == (3, 0)
    await controller.run_once()  # the cache catches up: met, and still nothing extra
    assert len(await pods(client)) == 3
    assert controller.expectations.pending(d.metadata.uid) == (0, 0)


async def test_orphans_are_only_collected_when_the_owner_is_really_gone(
    client: LocalStoreClient, controller: DeploymentController
) -> None:
    await make(client, 1)
    await controller.run_once()
    [p] = await pods(client)
    assert p.metadata.owner is not None
    # A controller whose deployment cache is behind (it has the pod, not its owner)
    # must not delete the pod: the live read finds the owner.
    behind = DeploymentController(client, FakeClock())
    await behind._pods.relist()
    behind.queue.add(p.metadata.owner)
    await behind.process_one()
    assert len(await pods(client)) == 1
