"""core.reconcile: counting, victim order, finished-pod GC, status, orphans."""

from datetime import UTC, datetime, timedelta

from chronos.api import (
    Deployment,
    DeploymentSpec,
    DeploymentStatus,
    ObjectMeta,
    Pod,
    PodPhase,
    PodSpec,
    PodTemplate,
)
from chronos.core.reconcile import (
    DEPLOYMENT_LABEL,
    CreatePod,
    DeletePod,
    UpdateStatus,
    orphans,
    pick_victims,
    pod_for,
    reconcile,
)

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def dep(replicas: int = 3) -> Deployment:
    template = PodTemplate.model_validate(
        {"command": ["sleep", "1"], "resources": {"requests": {"cpu": "2", "memory": "1Gi"}}}
    )
    return Deployment(
        metadata=ObjectMeta(name="web", uid="dep-uid", generation=1),
        spec=DeploymentSpec(replicas=replicas, template=template),
    )


def pod(name: str, phase: PodPhase = PodPhase.RUNNING, age: int = 0, ready: bool = True) -> Pod:
    p = Pod(
        metadata=ObjectMeta(
            name=name, owner="dep-uid", resource_version=7, created_at=T0 + timedelta(seconds=age)
        ),
        spec=PodSpec(command=["sleep", "1"]),
    )
    p.status.phase, p.status.ready = phase, ready and phase == PodPhase.RUNNING
    return p


def pod_actions(actions: list[object]) -> list[object]:
    return [a for a in actions if not isinstance(a, UpdateStatus)]


def test_creates_the_missing_replicas() -> None:
    assert pod_actions(reconcile(dep(3), [pod("a")])) == [CreatePod(2)]


def test_does_nothing_at_the_desired_count() -> None:
    pods = [pod(n) for n in "abc"]
    d = dep(3)
    d.status = DeploymentStatus(observed_generation=1, replicas=3, ready_replicas=3)
    assert reconcile(d, pods) == []


def test_finished_and_terminating_pods_are_not_counted() -> None:
    gone = pod("b")
    gone.metadata.deletion_timestamp = T0
    pods = [pod("a"), gone, pod("c", PodPhase.FAILED), pod("d", PodPhase.SUCCEEDED)]
    assert pod_actions(reconcile(dep(3), pods)) == [CreatePod(2)]


def test_victim_order_pending_then_bound_then_running_newest_first() -> None:
    pods = [
        pod("run-old", PodPhase.RUNNING, age=1),
        pod("run-new", PodPhase.RUNNING, age=9),
        pod("bound", PodPhase.BOUND, age=2),
        pod("pend-old", PodPhase.PENDING, age=3),
        pod("pend-new", PodPhase.PENDING, age=4),
    ]
    order = [p.metadata.name for p in pick_victims(pods, 5)]
    assert order == ["pend-new", "pend-old", "bound", "run-new", "run-old"]


def test_scale_down_deletes_victims_with_their_versions() -> None:
    pods = [pod("a", age=1), pod("b", PodPhase.PENDING, age=2), pod("c", age=3)]
    assert pod_actions(reconcile(dep(1), pods)) == [DeletePod("b", 7), DeletePod("c", 7)]


def test_keeps_only_the_three_newest_finished_pods() -> None:
    finished = [pod(f"f{i}", PodPhase.FAILED, age=i) for i in range(5)]
    actions = pod_actions(reconcile(dep(0), finished))
    assert actions == [DeletePod("f1", 7), DeletePod("f0", 7)]


def test_status_counts_active_and_ready() -> None:
    pods = [pod("a"), pod("b", PodPhase.BOUND), pod("c", PodPhase.FAILED)]
    statuses = [a for a in reconcile(dep(2), pods) if isinstance(a, UpdateStatus)]
    assert statuses == [
        UpdateStatus(DeploymentStatus(observed_generation=1, replicas=2, ready_replicas=1))
    ]


def test_pod_for_keeps_units_and_records_ownership() -> None:
    p = pod_for(dep(), "web-x2k9p")
    assert p.metadata.owner == "dep-uid"
    assert p.metadata.labels == {DEPLOYMENT_LABEL: "web"}
    assert p.requests == {"cpu": 2000, "memory": 2**30}  # 2 cores, not 2000
    assert len(p.spec.template_hash) == 10


def test_template_change_changes_the_hash() -> None:
    a, b = dep(), dep()
    b.spec.template.env = {"V": "2"}
    assert pod_for(a, "x").spec.template_hash != pod_for(b, "x").spec.template_hash


def test_orphans() -> None:
    mine, theirs, bare = pod("a"), pod("b"), pod("c")
    theirs.metadata.owner = "deleted-uid"
    bare.metadata.owner = None
    assert orphans([mine, theirs, bare], {"dep-uid"}) == [theirs]
