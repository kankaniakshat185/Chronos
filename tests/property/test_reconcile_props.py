"""reconcile properties (TESTING.md section 5, L2): converges in one round, then idempotent."""

from datetime import UTC, datetime, timedelta

from hypothesis import given
from hypothesis import strategies as st

from chronos.api import Deployment, DeploymentSpec, ObjectMeta, Pod, PodPhase, PodSpec, PodTemplate
from chronos.api.pod import TERMINAL_PHASES
from chronos.core.reconcile import (
    KEEP_FINISHED,
    CreatePod,
    DeletePod,
    UpdateStatus,
    is_active,
    pod_for,
    reconcile,
)

T0 = datetime(2026, 1, 1, tzinfo=UTC)


@st.composite
def states(draw: st.DrawFn) -> tuple[Deployment, list[Pod]]:
    d = Deployment(
        metadata=ObjectMeta(name="web", uid="dep-uid", generation=draw(st.integers(1, 3))),
        spec=DeploymentSpec(
            replicas=draw(st.integers(0, 6)), template=PodTemplate(command=["sleep", "1"])
        ),
    )
    pods = []
    for i in range(draw(st.integers(0, 10))):
        p = Pod(
            metadata=ObjectMeta(
                name=f"p{i}",
                owner="dep-uid",
                created_at=T0 + timedelta(seconds=draw(st.integers(0, 5))),
            ),
            spec=PodSpec(command=["sleep", "1"]),
        )
        p.status.phase = draw(st.sampled_from(list(PodPhase)))
        p.status.ready = p.status.phase == PodPhase.RUNNING and draw(st.booleans())
        if draw(st.integers(0, 9)) == 0:
            p.metadata.deletion_timestamp = T0
        pods.append(p)
    return d, pods


def apply(d: Deployment, pods: list[Pod], actions: list[object], round_: int) -> list[Pod]:
    pods = list(pods)
    for a in actions:
        match a:
            case CreatePod(count=n):
                pods += [pod_for(d, f"new-{round_}-{i}") for i in range(n)]
            case DeletePod(name=name):
                pods = [p for p in pods if p.metadata.name != name]
            case UpdateStatus(status=status):
                d.status = status
    return pods


@given(states())
def test_one_round_reaches_the_desired_count(state: tuple[Deployment, list[Pod]]) -> None:
    d, pods = state
    pods = apply(d, pods, reconcile(d, pods), 1)
    assert sum(is_active(p) for p in pods) == d.spec.replicas
    assert sum(p.status.phase in TERMINAL_PHASES for p in pods) <= KEEP_FINISHED


@given(states())
def test_after_converging_reconcile_is_idempotent(state: tuple[Deployment, list[Pod]]) -> None:
    """Round 1 fixes pods, round 2 records the status it led to, then nothing more."""
    d, pods = state
    for round_ in (1, 2):
        pods = apply(d, pods, reconcile(d, pods), round_)
    assert reconcile(d, pods) == []


@given(states())
def test_never_deletes_more_active_pods_than_needed(state: tuple[Deployment, list[Pod]]) -> None:
    d, pods = state
    active = {p.metadata.name for p in pods if is_active(p)}
    deleted = {a.name for a in reconcile(d, pods) if isinstance(a, DeletePod)}
    assert len(deleted & active) == max(len(active) - d.spec.replicas, 0)
