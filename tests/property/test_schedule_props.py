"""Scheduler properties over random clusters and random resource names (S2, S11, L3; X-1/X-2).

Each example builds a real store, runs the real scheduler controller, and checks
the outcome. Every extended resource name is missing from at least one node, so
the "missing name = capacity 0" path is always exercised.
"""

import asyncio
from collections.abc import Mapping

from hypothesis import given
from hypothesis import strategies as st

from chronos.api import Node, NodeCondition, NodeSpec, ObjectMeta, Pod, PodSpec
from chronos.api.quantity import is_extended
from chronos.api.resources import format_resources
from chronos.client.local import LocalStoreClient
from chronos.common.clock import FakeClock
from chronos.controllers.scheduler import UNSCHEDULABLE, SchedulerController
from chronos.core.schedule import (
    NodeInfo,
    Profile,
    SchedulerConfig,
    Unschedulable,
    pick_node,
    resolve,
)
from chronos.store import Store
from chronos.store.leases import Leases
from tests.strategies import extended_names

PROFILE = resolve(SchedulerConfig())

type Cluster = tuple[list[tuple[dict[str, int], NodeCondition, bool]], list[dict[str, int]]]


@st.composite
def clusters(draw: st.DrawFn, max_pods: int = 10) -> Cluster:
    names = draw(st.lists(extended_names, max_size=3, unique=True))
    n_nodes = draw(st.integers(1, 4))
    nodes = []
    for i in range(n_nodes):
        cap = {"cpu": draw(st.integers(0, 4)) * 1000, "memory": draw(st.integers(0, 4)) * 2**30}
        for name in names:
            # Node 0 never has extended resources, so each name is missing somewhere.
            if i > 0 and draw(st.booleans()):
                cap[name] = draw(st.integers(0, 4))
        condition = draw(st.sampled_from([NodeCondition.READY] * 3 + [NodeCondition.UNKNOWN]))
        nodes.append((cap, condition, draw(st.booleans()) and draw(st.booleans())))
    req_names = st.sampled_from(["cpu", "memory", *names])
    pods = draw(
        st.lists(
            st.dictionaries(req_names, st.integers(0, 3), max_size=3).map(
                lambda r: {k: v * 1000 if k == "cpu" else v * 2**29 if k == "memory" else v
                           for k, v in r.items()}
            ),
            max_size=max_pods,
        )
    )  # fmt: skip
    return nodes, pods


def build(cluster: Cluster, profile: Profile = PROFILE) -> tuple[Store, SchedulerController]:
    nodes, pods = cluster
    clock = FakeClock()
    store = Store(clock, uid_factory=iter(f"uid-{i}" for i in range(10**6)).__next__)
    for i, (cap, condition, unschedulable) in enumerate(nodes):
        spec = NodeSpec.model_validate(
            {"capacity": format_resources(cap), "unschedulable": unschedulable}
        )
        created = store.create(Node(metadata=ObjectMeta(name=f"n{i}"), spec=spec))
        created.status.condition = condition
        store.update_status(created)
    for i, req in enumerate(pods):
        clock.advance(1.0)  # distinct creation times: oldest-first is well defined
        spec = PodSpec.model_validate(
            {"command": ["true"], "resources": {"requests": format_resources(req)}}
        )
        store.create(Pod(metadata=ObjectMeta(name=f"p{i:02d}"), spec=spec))
    controller = SchedulerController(LocalStoreClient(store, Leases(clock)), clock, profile, 0.25)
    return store, controller


def placement(store: Store) -> dict[str, str]:
    return {p.metadata.name: p.spec.node_name for p in store.list(Pod)[0]}


def within_capacity(used: Mapping[str, int], cap: Mapping[str, int]) -> bool:
    return all(n <= cap.get(name, 0) for name, n in used.items())


@given(clusters())
def test_no_node_is_ever_overcommitted(cluster: Cluster) -> None:  # S2, S11
    store, controller = build(cluster)
    asyncio.run(controller.tick())
    for node in store.list(Node)[0]:
        assert within_capacity(store.allocated(node.metadata.name), node.spec.capacity)


@given(clusters())
def test_extended_resources_are_a_hard_constraint(cluster: Cluster) -> None:  # S11
    store, controller = build(cluster)
    asyncio.run(controller.tick())
    nodes = {n.metadata.name: n for n in store.list(Node)[0]}
    for pod in store.list(Pod)[0]:
        wants = {r: n for r, n in pod.requests.items() if is_extended(r) and n > 0}
        if pod.spec.node_name:
            cap = nodes[pod.spec.node_name].spec.capacity
            assert all(cap.get(r, 0) >= n for r, n in wants.items()), "placed without the units"


@given(clusters())
def test_whatever_stays_pending_fits_nowhere(cluster: Cluster) -> None:
    """Work-conserving: after a tick, every pending pod is Unschedulable with a reason,
    and really has no feasible node (capacity only shrinks during a tick)."""
    store, controller = build(cluster)
    asyncio.run(controller.tick())
    infos = [NodeInfo.of(n) for n in store.list(Node)[0]]
    for pod in store.list(Pod, node_name="")[0]:
        assert isinstance(pick_node(pod, infos, PROFILE), Unschedulable)
        assert pod.status.reason == UNSCHEDULABLE
        assert pod.status.message.startswith("0/")


@given(clusters())
def test_placement_is_deterministic(cluster: Cluster) -> None:
    first, c1 = build(cluster)
    second, c2 = build(cluster)
    asyncio.run(c1.tick())
    asyncio.run(c2.tick())
    assert placement(first) == placement(second)


@given(clusters(max_pods=1))
def test_a_single_pod_that_fits_somewhere_is_placed(cluster: Cluster) -> None:  # L3
    store, controller = build(cluster)
    pods = store.list(Pod)[0]
    if not pods:
        return
    infos = [NodeInfo.of(n) for n in store.list(Node)[0]]
    fits_somewhere = not isinstance(pick_node(pods[0], infos, PROFILE), Unschedulable)
    asyncio.run(controller.tick())
    assert bool(store.get(Pod, pods[0].metadata.name).spec.node_name) == fits_somewhere


@given(clusters())
def test_ticks_reach_a_fixed_point(cluster: Cluster) -> None:
    """No write churn: a pending pod's message is rewritten only when its reasons change
    (e.g. because another pod was bound in the same tick), so ticks settle quickly."""
    store, controller = build(cluster)
    asyncio.run(controller.tick())
    placed = placement(store)
    asyncio.run(controller.tick())
    assert placement(store) == placed  # nothing new becomes placeable without a change
    rev = store.revision
    asyncio.run(controller.tick())
    assert store.revision == rev
