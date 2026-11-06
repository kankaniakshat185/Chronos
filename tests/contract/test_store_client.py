"""One contract suite for both StoreClient implementations (ADR-0004).

The HTTP client talks to the real FastAPI app in-process through
`httpx.ASGITransport` (no sockets), so routing, body parsing and the error
mapping are all exercised.
"""

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest

from chronos.api import (
    Node,
    NodeCondition,
    NodeSpec,
    NodeStatus,
    ObjectMeta,
    Pod,
    PodSpec,
)
from chronos.client import HttpStoreClient, LocalStoreClient, StoreClient
from chronos.common.clock import FakeClock
from chronos.common.config import FencingConfig
from chronos.server.app import create_app
from chronos.store import (
    AlreadyExists,
    Compacted,
    Conflict,
    InsufficientCapacity,
    Invalid,
    NotFound,
    Store,
)
from chronos.store.errors import EpochMismatch
from chronos.store.leases import Leases


@pytest.fixture
def local() -> LocalStoreClient:
    clock = FakeClock()
    return LocalStoreClient(Store(clock), Leases(clock))


@pytest.fixture(params=["local", "http"])
async def client(
    request: pytest.FixtureRequest, local: LocalStoreClient
) -> AsyncIterator[StoreClient]:
    if request.param == "local":
        yield local
        return
    transport = httpx.ASGITransport(app=create_app(local))
    async with HttpStoreClient("http://chronosd", transport=transport) as http:
        yield http


def pod(name: str = "p1", **requests: int | str) -> Pod:
    spec = PodSpec(command=["sleep", "1"], resources={"requests": requests})
    return Pod(metadata=ObjectMeta(name=name), spec=spec)


async def ready_node(client: StoreClient, local: LocalStoreClient, name: str = "n1") -> Node:
    node = (await client.register(name, "epoch-1", {"cpu": 4000, "chronos.dev/slot": 1})).node
    node.status.condition = NodeCondition.READY  # what the lifecycle controller would do
    return await client.update_status(node)


async def test_create_get_round_trip(client: StoreClient) -> None:
    created = await client.create(pod(cpu="250m", memory="64Mi"))
    assert created.metadata.resource_version == 1
    got = await client.get(Pod, "p1")
    assert got == created
    assert got.requests == {"cpu": 250, "memory": 64 * 2**20}


async def test_list_filters_and_revision(client: StoreClient) -> None:
    a = pod("a")
    a.metadata.labels = {"app": "web"}
    await client.create(a)
    await client.create(pod("b"))
    items, rev = await client.list(Pod, labels={"app": "web"})
    assert [p.metadata.name for p in items] == ["a"]
    assert rev == 2
    unbound, _ = await client.list(Pod, node_name="")
    assert [p.metadata.name for p in unbound] == ["a", "b"]


async def test_not_found(client: StoreClient) -> None:
    with pytest.raises(NotFound):
        await client.get(Pod, "nope")


async def test_already_exists(client: StoreClient) -> None:
    await client.create(pod())
    with pytest.raises(AlreadyExists):
        await client.create(pod())


async def test_update_conflict_carries_versions(client: StoreClient) -> None:
    stale = await client.create(pod())
    fresh = await client.get(Pod, "p1")
    fresh.spec.env = {"A": "1"}
    await client.update(fresh)
    stale.spec.env = {"B": "2"}
    with pytest.raises(Conflict) as err:
        await client.update(stale)
    assert (err.value.expected, err.value.current) == (1, 2)


async def test_update_status(client: StoreClient) -> None:
    p = await client.create(pod())
    p.status.message = "hi"
    assert (await client.update_status(p)).status.message == "hi"


async def test_invalid_write(client: StoreClient) -> None:
    p = await client.create(pod())
    p.metadata.uid = "forged"
    with pytest.raises(Invalid, match="uid"):
        await client.update(p)


async def test_delete_with_stale_rv_then_current(client: StoreClient) -> None:
    await client.create(pod())
    with pytest.raises(Conflict):
        await client.delete(Pod, "p1", expected_rv=7)
    gone = await client.delete(Pod, "p1", expected_rv=1)
    assert gone.metadata.name == "p1"
    with pytest.raises(NotFound):
        await client.get(Pod, "p1")


async def test_bind_and_derived_allocation(client: StoreClient, local: LocalStoreClient) -> None:
    await ready_node(client, local)
    p = await client.create(pod(cpu=1, **{"chronos.dev/slot": 1}))
    bound = await client.bind("p1", "n1", p.metadata.resource_version)
    assert bound.spec.node_name == "n1"
    node = await client.get(Node, "n1")
    assert node.status.allocated == {"cpu": 1000, "chronos.dev/slot": 1}


async def test_bind_insufficient_names_the_resource(
    client: StoreClient, local: LocalStoreClient
) -> None:
    await ready_node(client, local)
    p = await client.create(pod(**{"example.com/widget": 1}))
    with pytest.raises(InsufficientCapacity) as err:
        await client.bind("p1", "n1", p.metadata.resource_version)
    assert err.value.resources == ["example.com/widget"]


async def test_register_creates_then_adopts(client: StoreClient) -> None:
    first = (await client.register("n1", "epoch-1", {"cpu": 2000})).node
    assert first.status.agent_epoch == "epoch-1"
    assert first.status.condition == NodeCondition.UNKNOWN
    assert first.spec.capacity == {"cpu": 2000}  # internal units in, internal units out
    again = (await client.register("n1", "epoch-2", {"cpu": 4000})).node
    assert again.status.agent_epoch == "epoch-2"
    assert again.spec.capacity == {"cpu": 4000}
    assert again.metadata.uid == first.metadata.uid


async def test_heartbeat_with_current_epoch(client: StoreClient) -> None:
    await client.register("n1", "epoch-1", {})
    reply = await client.heartbeat("n1", "epoch-1")
    assert reply.condition == NodeCondition.UNKNOWN


async def test_heartbeat_from_an_old_incarnation_is_refused(client: StoreClient) -> None:
    await client.register("n1", "epoch-1", {})
    await client.register("n1", "epoch-2", {})
    with pytest.raises(EpochMismatch):
        await client.heartbeat("n1", "epoch-1")


async def test_heartbeat_for_unknown_node(client: StoreClient) -> None:
    with pytest.raises(NotFound):
        await client.heartbeat("ghost", "epoch-1")


async def test_node_created_directly_can_heartbeat(client: StoreClient) -> None:
    node = Node(
        metadata=ObjectMeta(name="n1"),
        spec=NodeSpec(capacity={"cpu": 1}),
        status=NodeStatus(agent_epoch="e1"),
    )
    await client.create(node)
    assert (await client.heartbeat("n1", "e1")).condition == NodeCondition.UNKNOWN


async def test_register_returns_the_eviction_timeout(client: StoreClient) -> None:
    reply = await client.register("n1", "e1", {}, FencingConfig())
    assert reply.eviction_timeout == 10.0


async def test_register_refuses_unsafe_fencing_timings(client: StoreClient) -> None:
    unsafe = FencingConfig(fence_timeout=8.0, termination_grace=2.0, drift_margin=0.5)
    with pytest.raises(Invalid, match=r"must be < eviction_timeout = 10s"):
        await client.register("n1", "e1", {}, unsafe)
    with pytest.raises(NotFound):
        await client.get(Node, "n1")  # nothing was created


# ---- watch (ADR-0008, S6) ----------------------------------------------------------


async def test_watch_returns_every_change_after_since_in_order(client: StoreClient) -> None:
    start = (await client.list(Pod))[1]
    a = await client.create(pod("a"))
    a.metadata.labels = {"x": "1"}
    a = await client.update(a)
    await client.delete(Pod, "a")
    batch = await client.watch(Pod, start, wait=0)
    assert [(e.type, e.object.metadata.name) for e in batch.events] == [
        ("ADDED", "a"),
        ("MODIFIED", "a"),
        ("DELETED", "a"),
    ]
    revisions = [e.object.metadata.resource_version for e in batch.events]
    assert revisions == [start + 1, start + 2, start + 3]
    assert batch.events[1].object.metadata.labels == {"x": "1"}
    assert batch.revision == start + 3


async def test_watch_is_per_kind_and_still_advances(client: StoreClient) -> None:
    await client.create(pod("a"))
    batch = await client.watch(Node, 0, wait=0)
    assert batch.events == []
    assert batch.revision == 1  # nothing for nodes, but everything up to 1 was seen


async def test_watch_waits_for_the_next_change(client: StoreClient) -> None:
    watching = asyncio.create_task(client.watch(Pod, 0, wait=5.0))
    await asyncio.sleep(0.05)  # noqa: TID251 (let it start waiting)
    assert not watching.done()
    await client.create(pod("a"))
    batch = await asyncio.wait_for(watching, 2.0)
    assert [e.object.metadata.name for e in batch.events] == ["a"]


async def test_watch_times_out_empty(client: StoreClient) -> None:
    await client.create(pod("a"))
    batch = await client.watch(Pod, 1, wait=0.05)
    assert (batch.events, batch.revision) == ([], 1)


async def test_node_filter_sees_its_pods_arrive_and_leave(
    client: StoreClient, local: LocalStoreClient
) -> None:
    await ready_node(client, local, "n1")
    await ready_node(client, local, "n2")
    start = (await client.list(Pod))[1]
    for name, node in (("a", "n1"), ("b", "n2")):
        created = await client.create(pod(name))
        await client.bind(name, node, created.metadata.resource_version)
    await client.delete(Pod, "a", force=True)
    batch = await client.watch(Pod, start, wait=0, node_name="n1")
    assert [(e.type, e.object.metadata.name) for e in batch.events] == [
        ("MODIFIED", "a"),  # the bind: unbound before, on n1 after
        ("DELETED", "a"),
    ]


async def test_watch_from_lost_history_is_compacted(
    client: StoreClient, local: LocalStoreClient
) -> None:
    await client.create(pod("a"))
    local.store.watch.reset(local.store.revision)  # what a restart does to the history
    with pytest.raises(Compacted):
        await client.watch(Pod, 0, wait=0)
    assert (await client.watch(Pod, 1, wait=0)).events == []  # from the list's revision: fine
