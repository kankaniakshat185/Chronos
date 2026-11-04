"""Store unit tests: every operation's happy path and every error path (ADR-0007)."""

import pytest

from chronos.api import (
    Deployment,
    DeploymentSpec,
    Node,
    NodeCondition,
    NodeSpec,
    ObjectMeta,
    Pod,
    PodPhase,
    PodSpec,
    PodTemplate,
)
from chronos.common.clock import FakeClock
from chronos.store import AlreadyExists, Conflict, InsufficientCapacity, Invalid, NotFound, Store


def node(name: str = "n1", **capacity: int | str) -> Node:
    return Node(metadata=ObjectMeta(name=name), spec=NodeSpec(capacity=capacity))


def pod(name: str = "p1", **requests: int | str) -> Pod:
    spec = PodSpec(command=["sleep", "1"], resources={"requests": requests})
    return Pod(metadata=ObjectMeta(name=name), spec=spec)


def ready(store: Store, name: str = "n1") -> Node:
    n = store.get(Node, name)
    n.status.condition = NodeCondition.READY
    return store.update_status(n)


@pytest.fixture
def store() -> Store:
    uids = iter(range(1_000))
    return Store(FakeClock(), uid_factory=lambda: f"uid-{next(uids)}")


@pytest.fixture
def cluster(store: Store) -> Store:
    """One Ready node with 4 cpu, 8Gi and 1 slot, plus one unbound pod."""
    store.create(node("n1", cpu=4, memory="8Gi", **{"chronos.dev/slot": 1}))
    ready(store)
    store.create(pod("p1", cpu="500m", memory="1Gi"))
    return store


# ---- create / get / list ------------------------------------------------------


def test_create_assigns_identity_and_revision(store: Store) -> None:
    created = store.create(node())
    assert created.metadata.uid == "uid-0"
    assert created.metadata.resource_version == 1 == store.revision
    assert created.metadata.generation == 1
    assert created.metadata.created_at is not None


def test_create_existing_name_fails_and_changes_nothing(store: Store) -> None:
    store.create(node())
    with pytest.raises(AlreadyExists):
        store.create(node())
    assert store.revision == 1


def test_same_name_in_different_kinds_is_fine(store: Store) -> None:
    store.create(node("x"))
    store.create(pod("x"))
    assert store.revision == 2


def test_create_rejects_a_prebound_pod(store: Store) -> None:
    p = pod()
    p.spec.node_name = "n1"
    with pytest.raises(Invalid, match="use bind"):
        store.create(p)


def test_create_rejects_a_non_pending_pod(store: Store) -> None:
    p = pod()
    p.status.phase = PodPhase.RUNNING
    with pytest.raises(Invalid, match="Pending"):
        store.create(p)


def test_get_missing_raises_not_found(store: Store) -> None:
    with pytest.raises(NotFound):
        store.get(Pod, "nope")


def test_list_is_sorted_and_reports_revision(store: Store) -> None:
    for name in ("b", "a", "c"):
        store.create(pod(name))
    items, rev = store.list(Pod)
    assert [p.metadata.name for p in items] == ["a", "b", "c"]
    assert rev == 3


def test_list_filters_by_labels(store: Store) -> None:
    a = pod("a")
    a.metadata.labels = {"app": "web", "tier": "fe"}
    store.create(a)
    store.create(pod("b"))
    items, _ = store.list(Pod, labels={"app": "web"})
    assert [p.metadata.name for p in items] == ["a"]


def test_node_name_filter_only_applies_to_pods(store: Store) -> None:
    with pytest.raises(Invalid):
        store.list(Node, node_name="n1")


# ---- copies --------------------------------------------------------------------


def test_callers_cannot_mutate_stored_state(store: Store) -> None:
    original = pod()
    created = store.create(original)
    original.metadata.labels["x"] = "1"
    created.metadata.labels["y"] = "2"
    store.get(Pod, "p1").metadata.labels["z"] = "3"
    assert store.get(Pod, "p1").metadata.labels == {}


# ---- update (spec, CAS) -----------------------------------------------------------


def test_update_with_current_rv_succeeds_and_bumps_generation(store: Store) -> None:
    p = store.create(pod())
    p.spec.env = {"A": "1"}
    updated = store.update(p)
    assert updated.metadata.resource_version == 2
    assert updated.metadata.generation == 2
    assert updated.spec.env == {"A": "1"}


def test_update_with_stale_rv_conflicts(store: Store) -> None:
    stale = store.create(pod())
    fresh = store.get(Pod, "p1")
    fresh.spec.env = {"A": "1"}
    store.update(fresh)
    stale.spec.env = {"B": "2"}
    with pytest.raises(Conflict) as err:
        store.update(stale)
    assert (err.value.expected, err.value.current) == (1, 2)
    assert store.get(Pod, "p1").spec.env == {"A": "1"}
    assert store.revision == 2


def test_label_only_update_keeps_generation(store: Store) -> None:
    p = store.create(pod())
    p.metadata.labels = {"app": "web"}
    updated = store.update(p)
    assert updated.metadata.generation == 1
    assert updated.metadata.labels == {"app": "web"}


def test_update_ignores_status(store: Store) -> None:
    p = store.create(pod())
    p.status.message = "sneaky"
    assert store.update(p).status.message == ""


def test_update_missing_object_raises_not_found(store: Store) -> None:
    with pytest.raises(NotFound):
        store.update(pod())


def test_uid_is_immutable(store: Store) -> None:
    p = store.create(pod())
    p.metadata.uid = "other"
    with pytest.raises(Invalid, match="uid"):
        store.update(p)


def test_owner_is_immutable(store: Store) -> None:
    p = store.create(pod())
    p.metadata.owner = "someone"
    with pytest.raises(Invalid, match="owner"):
        store.update(p)


def test_update_cannot_bind(cluster: Store) -> None:
    p = cluster.get(Pod, "p1")
    p.spec.node_name = "n1"
    with pytest.raises(Invalid, match="bind"):
        cluster.update(p)


def test_update_keeps_node_name_when_caller_sends_empty(cluster: Store) -> None:
    bound = cluster.bind("p1", "n1", cluster.get(Pod, "p1").metadata.resource_version)
    bound.spec.node_name = ""
    bound.spec.env = {"A": "1"}
    assert cluster.update(bound).spec.node_name == "n1"


def test_node_capacity_cannot_shrink_below_allocated(cluster: Store) -> None:
    cluster.bind("p1", "n1", cluster.get(Pod, "p1").metadata.resource_version)
    n = cluster.get(Node, "n1")
    n.spec.capacity = {"cpu": 100, "memory": 1 << 33}
    with pytest.raises(Invalid, match="cpu"):
        cluster.update(n)


def test_deployment_update_bumps_generation(store: Store) -> None:
    d = store.create(
        Deployment(
            metadata=ObjectMeta(name="web"),
            spec=DeploymentSpec(replicas=1, template=PodTemplate(command=["sleep", "1"])),
        )
    )
    d.spec.replicas = 3
    assert store.update(d).metadata.generation == 2


# ---- update_status -------------------------------------------------------------------


def test_update_status_keeps_spec_and_generation(store: Store) -> None:
    p = store.create(pod())
    p.spec.env = {"ignored": "1"}
    p.status.message = "hello"
    updated = store.update_status(p)
    assert updated.status.message == "hello"
    assert updated.spec.env == {}
    assert updated.metadata.generation == 1


def test_update_status_with_stale_rv_conflicts(store: Store) -> None:
    stale = store.create(pod())
    store.update_status(store.get(Pod, "p1"))
    with pytest.raises(Conflict):
        store.update_status(stale)


def test_unbound_pod_cannot_be_marked_running(store: Store) -> None:
    p = store.create(pod())
    p.status.phase = PodPhase.RUNNING
    with pytest.raises(Invalid, match="bind"):
        store.update_status(p)


def test_bound_pod_cannot_go_back_to_pending(cluster: Store) -> None:
    p = cluster.bind("p1", "n1", cluster.get(Pod, "p1").metadata.resource_version)
    p.status.phase = PodPhase.PENDING
    with pytest.raises(Invalid, match="Pending"):
        cluster.update_status(p)


def test_terminal_phase_is_final(cluster: Store) -> None:
    p = cluster.bind("p1", "n1", cluster.get(Pod, "p1").metadata.resource_version)
    p.status.phase = PodPhase.FAILED
    p = cluster.update_status(p)
    p.status.phase = PodPhase.RUNNING
    with pytest.raises(Invalid, match="terminal"):
        cluster.update_status(p)


def test_node_allocated_is_derived_not_stored(cluster: Store) -> None:
    n = cluster.get(Node, "n1")
    n.status.allocated = {"cpu": 999_000}
    assert cluster.update_status(n).status.allocated == {
        "cpu": 0,
        "memory": 0,
        "chronos.dev/slot": 0,
    }


# ---- delete --------------------------------------------------------------------------


def test_delete_removes_and_bumps_revision(store: Store) -> None:
    store.create(pod())
    gone = store.delete(Pod, "p1")
    assert gone.metadata.resource_version == 2 == store.revision
    with pytest.raises(NotFound):
        store.get(Pod, "p1")


def test_delete_with_stale_rv_conflicts(store: Store) -> None:
    store.create(pod())
    with pytest.raises(Conflict):
        store.delete(Pod, "p1", expected_rv=99)
    assert store.get(Pod, "p1")


def test_delete_missing_raises_not_found(store: Store) -> None:
    with pytest.raises(NotFound):
        store.delete(Pod, "p1")


def test_recreated_object_gets_a_new_uid(store: Store) -> None:
    first = store.create(pod())
    store.delete(Pod, "p1")
    assert store.create(pod()).metadata.uid != first.metadata.uid


# ---- bind ------------------------------------------------------------------------------


def test_bind_sets_node_phase_and_allocation(cluster: Store) -> None:
    rv = cluster.get(Pod, "p1").metadata.resource_version
    bound = cluster.bind("p1", "n1", rv)
    assert bound.spec.node_name == "n1"
    assert bound.status.phase == PodPhase.BOUND
    assert bound.metadata.generation == 1
    assert [p.metadata.name for p in cluster.pods_on("n1")] == ["p1"]
    assert cluster.pods_on("") == []
    assert cluster.get(Node, "n1").status.allocated == {
        "cpu": 500,
        "memory": 1 << 30,
        "chronos.dev/slot": 0,
    }


def test_bind_with_stale_rv_conflicts(cluster: Store) -> None:
    with pytest.raises(Conflict):
        cluster.bind("p1", "n1", expected_rv=1)


def test_bind_is_once(cluster: Store) -> None:
    cluster.create(node("n2", cpu=4, memory="8Gi"))
    ready(cluster, "n2")
    bound = cluster.bind("p1", "n1", cluster.get(Pod, "p1").metadata.resource_version)
    with pytest.raises(Invalid, match="already bound"):
        cluster.bind("p1", "n2", bound.metadata.resource_version)


def test_bind_to_missing_node(cluster: Store) -> None:
    with pytest.raises(NotFound):
        cluster.bind("p1", "nope", cluster.get(Pod, "p1").metadata.resource_version)


def test_bind_missing_pod(cluster: Store) -> None:
    with pytest.raises(NotFound):
        cluster.bind("nope", "n1", 1)


@pytest.mark.parametrize("condition", [NodeCondition.UNKNOWN, NodeCondition.NOT_READY])
def test_bind_needs_a_ready_node(store: Store, condition: NodeCondition) -> None:
    n = store.create(node(cpu=4))
    n.status.condition = condition
    store.update_status(n)
    p = store.create(pod())
    with pytest.raises(Invalid, match="not Ready"):
        store.bind("p1", "n1", p.metadata.resource_version)


def test_bind_needs_a_schedulable_node(cluster: Store) -> None:
    n = cluster.get(Node, "n1")
    n.spec.unschedulable = True
    cluster.update(n)
    with pytest.raises(Invalid, match="unschedulable"):
        cluster.bind("p1", "n1", cluster.get(Pod, "p1").metadata.resource_version)


def test_bind_rejects_terminal_pod(cluster: Store) -> None:
    p = cluster.get(Pod, "p1")
    p.status.phase = PodPhase.FAILED
    p = cluster.update_status(p)
    with pytest.raises(Invalid, match="Failed"):
        cluster.bind("p1", "n1", p.metadata.resource_version)


def test_bind_names_every_insufficient_resource(cluster: Store) -> None:
    p = cluster.create(pod("big", cpu=5, memory="9Gi"))
    with pytest.raises(InsufficientCapacity) as err:
        cluster.bind("big", "n1", p.metadata.resource_version)
    assert err.value.resources == ["cpu", "memory"]
    assert "Insufficient cpu, memory" in str(err.value)
    assert cluster.get(Pod, "big").spec.node_name == ""


def test_missing_extended_resource_means_zero_capacity(store: Store) -> None:
    store.create(node(cpu=4))
    ready(store)
    p = store.create(pod(**{"example.com/widget": 1}))
    with pytest.raises(InsufficientCapacity, match=r"example\.com/widget"):
        store.bind("p1", "n1", p.metadata.resource_version)


def test_extended_resource_is_never_overcommitted(cluster: Store) -> None:
    a = cluster.create(pod("a", **{"chronos.dev/slot": 1}))
    b = cluster.create(pod("b", **{"chronos.dev/slot": 1}))
    cluster.bind("a", "n1", a.metadata.resource_version)
    with pytest.raises(InsufficientCapacity, match=r"chronos\.dev/slot"):
        cluster.bind("b", "n1", b.metadata.resource_version)


def test_terminal_pods_free_their_resources(cluster: Store) -> None:
    a = cluster.create(pod("a", **{"chronos.dev/slot": 1}))
    a = cluster.bind("a", "n1", a.metadata.resource_version)
    a.status.phase = PodPhase.SUCCEEDED
    cluster.update_status(a)
    b = cluster.create(pod("b", **{"chronos.dev/slot": 1}))
    assert cluster.bind("b", "n1", b.metadata.resource_version).spec.node_name == "n1"


def test_deleting_a_bound_pod_marks_it_terminating(cluster: Store) -> None:
    a = cluster.create(pod("a", **{"chronos.dev/slot": 1}))
    a = cluster.bind("a", "n1", a.metadata.resource_version)
    marked = cluster.delete(Pod, "a")
    assert marked.metadata.deletion_timestamp is not None
    assert marked.metadata.resource_version == a.metadata.resource_version + 1
    assert cluster.get(Node, "n1").status.allocated["chronos.dev/slot"] == 1  # still running
    rev = cluster.revision
    assert cluster.delete(Pod, "a").metadata.deletion_timestamp is not None  # idempotent
    assert cluster.revision == rev


def test_terminating_pod_spec_is_frozen_but_status_isnt(cluster: Store) -> None:
    a = cluster.create(pod("a"))
    cluster.bind("a", "n1", a.metadata.resource_version)
    p = cluster.delete(Pod, "a")
    p.spec.env = {"X": "1"}
    with pytest.raises(Invalid, match="terminating"):
        cluster.update(p)
    p = cluster.get(Pod, "a")
    p.status.message = "stopping"
    assert cluster.update_status(p).status.message == "stopping"


def test_force_delete_removes_and_frees_resources(cluster: Store) -> None:
    a = cluster.create(pod("a", **{"chronos.dev/slot": 1}))
    cluster.bind("a", "n1", a.metadata.resource_version)
    cluster.delete(Pod, "a")
    cluster.delete(Pod, "a", force=True)
    assert cluster.pods_on("n1") == []
    assert cluster.get(Node, "n1").status.allocated["chronos.dev/slot"] == 0


def test_unbound_and_finished_pods_are_removed_at_once(cluster: Store) -> None:
    cluster.delete(Pod, "p1")  # unbound
    with pytest.raises(NotFound):
        cluster.get(Pod, "p1")
    done = cluster.create(pod("done"))
    done = cluster.bind("done", "n1", done.metadata.resource_version)
    done.status.phase = PodPhase.SUCCEEDED
    cluster.update_status(done)
    cluster.delete(Pod, "done")
    with pytest.raises(NotFound):
        cluster.get(Pod, "done")


def test_terminating_pod_cannot_be_bound(store: Store) -> None:
    store.create(node(cpu=4))
    ready(store)
    p = store.create(pod())
    p = store.bind("p1", "n1", p.metadata.resource_version)
    p = store.delete(Pod, "p1")
    store.create(node("n2", cpu=4))
    with pytest.raises(Invalid):
        store.bind("p1", "n2", p.metadata.resource_version)
