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
    return DeploymentController(client, FakeClock(), 0.25)


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
    await controller.tick()
    owned = await pods(client)
    assert len(owned) == 3
    assert {p.metadata.owner for p in owned} == {d.metadata.uid}
    assert all(p.metadata.labels[DEPLOYMENT_LABEL] == "web" for p in owned)
    await controller.tick()
    status = (await client.get(Deployment, "web")).status
    assert (status.replicas, status.observed_generation) == (3, 1)


async def test_scales_up_and_down(
    client: LocalStoreClient, controller: DeploymentController
) -> None:
    await make(client, 3)
    await controller.tick()
    for replicas in (5, 1):
        d = await client.get(Deployment, "web")
        d.spec.replicas = replicas
        await client.update(d)
        await controller.tick()
        assert len(await pods(client)) == replicas


async def test_failed_pod_is_replaced(
    client: LocalStoreClient, controller: DeploymentController
) -> None:
    await make(client, 1)
    await controller.tick()
    [p] = await pods(client)
    p.status.phase = PodPhase.FAILED
    await client.update_status(p)
    await controller.tick()
    phases = sorted(q.status.phase for q in await pods(client))
    assert phases == [PodPhase.FAILED, PodPhase.PENDING]


async def test_deleting_the_deployment_collects_its_pods(
    client: LocalStoreClient, controller: DeploymentController
) -> None:
    await make(client, 2)
    await controller.tick()
    await client.delete(Deployment, "web")
    await controller.tick()
    assert await pods(client) == []


async def test_recreated_deployment_does_not_adopt_old_pods(
    client: LocalStoreClient, controller: DeploymentController
) -> None:
    await make(client, 1)
    await controller.tick()
    [old] = await pods(client)
    await client.delete(Deployment, "web")
    await make(client, 1)
    await controller.tick()
    names = [p.metadata.name for p in await pods(client)]
    assert old.metadata.name not in names and len(names) == 1


def test_pod_names_are_valid_and_bounded() -> None:
    name = pod_name("x" * 63)
    assert len(name) == 63
    assert ObjectMeta(name=name).name == name
