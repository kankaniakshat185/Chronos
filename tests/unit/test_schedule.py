"""Scheduling core: each plugin, FitsResources table, registry config, pick_node (ADR-0013)."""

import pytest

from chronos.api import NodeCondition, ObjectMeta, Pod, PodSpec
from chronos.core.schedule import (
    Bind,
    ConfigError,
    NodeInfo,
    SchedulerConfig,
    Unschedulable,
    fits_resources,
    least_allocated,
    node_ready,
    node_schedulable,
    pick_node,
    register_filter,
    register_score,
    resolve,
)

SLOT = "chronos.dev/slot"
PROFILE = resolve(SchedulerConfig())


def pod(**requests: int) -> Pod:
    """Requests in internal units (cpu millicores, memory bytes, extended whole units)."""
    p = Pod(metadata=ObjectMeta(name="p"), spec=PodSpec(command=["true"]))
    p.spec.resources.requests = dict(requests)
    return p


def node(
    name: str = "n1",
    condition: NodeCondition = NodeCondition.READY,
    unschedulable: bool = False,
    allocated: dict[str, int] | None = None,
    **capacity: int,
) -> NodeInfo:
    return NodeInfo(name, condition, unschedulable, capacity, allocated or {})


@pytest.mark.parametrize("condition", [NodeCondition.UNKNOWN, NodeCondition.NOT_READY])
def test_node_ready_rejects_unready_nodes(condition: NodeCondition) -> None:
    assert node_ready(pod(), node(condition=condition)) == f"node {condition}"
    assert node_ready(pod(), node()) is None


def test_node_schedulable() -> None:
    assert node_schedulable(pod(), node(unschedulable=True)) == "node unschedulable"
    assert node_schedulable(pod(), node()) is None


@pytest.mark.parametrize(
    ("requests", "capacity", "allocated", "reason"),
    [
        ({"cpu": 1000}, {"cpu": 1000}, {}, None),  # exactly full is fine
        ({"cpu": 1001}, {"cpu": 1000}, {}, "Insufficient cpu"),
        ({"cpu": 500}, {"cpu": 1000}, {"cpu": 600}, "Insufficient cpu"),
        ({}, {}, {}, None),  # requests nothing: fits anywhere
        ({SLOT: 1}, {"cpu": 1000}, {}, f"Insufficient {SLOT}"),  # node lacks the resource
        ({SLOT: 1}, {SLOT: 1}, {SLOT: 1}, f"Insufficient {SLOT}"),  # all units taken
        ({SLOT: 0}, {}, {}, None),  # zero of something the node lacks is fine
        ({"cpu": 5000, SLOT: 2}, {"cpu": 1000, SLOT: 1}, {}, f"Insufficient {SLOT}, cpu"),
    ],
)
def test_fits_resources(
    requests: dict[str, int],
    capacity: dict[str, int],
    allocated: dict[str, int],
    reason: str | None,
) -> None:
    assert fits_resources(pod(**requests), node(allocated=allocated, **capacity)) == reason


def test_least_allocated_prefers_emptier_nodes() -> None:
    p = pod(cpu=1000, memory=1024)
    empty = node(cpu=4000, memory=4096)
    busy = node(cpu=4000, memory=4096, allocated={"cpu": 2000, "memory": 2048})
    assert least_allocated(p, empty) == 75
    assert least_allocated(p, busy) == 25


def test_least_allocated_without_capacity_scores_zero() -> None:
    assert least_allocated(pod(), node()) == 0


def test_pick_node_spreads() -> None:
    nodes = [
        node("a", cpu=4000, memory=4096, allocated={"cpu": 3000}),
        node("b", cpu=4000, memory=4096),
    ]
    assert pick_node(pod(cpu=500), nodes, PROFILE) == Bind("b")


def test_ties_break_by_name_regardless_of_input_order() -> None:
    nodes = [node("c", cpu=1000), node("a", cpu=1000), node("b", cpu=1000)]
    assert pick_node(pod(), nodes, PROFILE) == Bind("a")
    assert pick_node(pod(), list(reversed(nodes)), PROFILE) == Bind("a")


def test_unschedulable_aggregates_reasons() -> None:
    nodes = [
        node("plain-1", cpu=4000),
        node("slot-a", cpu=4000, allocated={SLOT: 1}, **{SLOT: 1}),
        node("slot-b", condition=NodeCondition.NOT_READY, cpu=4000, **{SLOT: 1}),
    ]
    decision = pick_node(pod(**{SLOT: 1}), nodes, PROFILE)
    assert isinstance(decision, Unschedulable)
    assert decision.message == (
        "0/3 nodes available: Insufficient chronos.dev/slot (2/3 nodes), node NotReady (1/3 nodes)"
    )


def test_no_nodes() -> None:
    decision = pick_node(pod(), [], PROFILE)
    assert isinstance(decision, Unschedulable)
    assert "no nodes" in decision.message


def test_unknown_plugin_name_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="NoSuchFilter"):
        resolve(SchedulerConfig(filters=("NodeReady", "NoSuchFilter")))
    with pytest.raises(ConfigError, match="NoSuchScore"):
        resolve(SchedulerConfig(scores={"NoSuchScore": 1}))


def test_negative_weight_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="negative"):
        resolve(SchedulerConfig(scores={"LeastAllocated": -1}))


def test_duplicate_registration_is_an_error() -> None:
    with pytest.raises(ConfigError, match="already registered"):
        register_filter("NodeReady", node_ready)
    with pytest.raises(ConfigError, match="already registered"):
        register_score("LeastAllocated", least_allocated)


def test_disabling_a_filter_changes_feasibility() -> None:
    unready = [node("a", condition=NodeCondition.UNKNOWN, cpu=1000)]
    assert isinstance(pick_node(pod(), unready, PROFILE), Unschedulable)
    lenient = resolve(SchedulerConfig(filters=("FitsResources",)))
    assert pick_node(pod(), unready, lenient) == Bind("a")


def test_zero_weight_turns_a_score_off() -> None:
    nodes = [node("a", cpu=1000, allocated={"cpu": 900}), node("b", cpu=1000)]
    assert pick_node(pod(), nodes, PROFILE) == Bind("b")
    unweighted = resolve(SchedulerConfig(scores={"LeastAllocated": 0}))
    assert pick_node(pod(), nodes, unweighted) == Bind("a")  # name tie-break only
