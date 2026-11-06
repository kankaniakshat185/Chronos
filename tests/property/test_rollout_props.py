"""Rolling updates (P10.2, S7) on a model: reconcile's actions applied step by step.

Each round applies reconcile's creates and deletes, then lets a random subset of the new
pods become ready. Checked on every round:

* surge: never more than replicas + max_surge active pods;
* availability: deletions never take the ready count below
  min(what it was, replicas - max_unavailable); and old pods that aren't ready yet (still
  starting) aren't all thrown away at once: at least replicas - max_unavailable pods stay;
* progress: if every new pod eventually becomes ready, the rollout finishes (all pods
  current, exactly `replicas`) within a bounded number of rounds;
* a new template that never becomes ready stalls with the old pods still serving.
"""

from __future__ import annotations

import itertools

from hypothesis import given
from hypothesis import strategies as st

from chronos.api import Deployment, DeploymentSpec, ObjectMeta, Pod, PodPhase, PodTemplate
from chronos.api.deployment import RollingUpdate
from chronos.core.reconcile import CreatePod, DeletePod, pod_for, reconcile, template_hash
from chronos.core.rollout import is_outdated

strategies = st.tuples(st.integers(0, 3), st.integers(0, 3)).filter(lambda s: s != (0, 0))


def deployment(replicas: int, surge: int, unavailable: int, label: str) -> Deployment:
    return Deployment(
        metadata=ObjectMeta(name="web", uid="dep"),
        spec=DeploymentSpec(
            replicas=replicas,
            template=PodTemplate(command=["serve"], env={"v": label}),
            strategy=RollingUpdate(max_surge=surge, max_unavailable=unavailable),
        ),
    )


def running(pod: Pod, ready: bool) -> Pod:
    pod.status.phase = PodPhase.RUNNING
    pod.status.ready = ready
    return pod


def rounds(
    dep: Deployment, pods: list[Pod], ready_new: list[bool], counter: itertools.count[int]
) -> tuple[list[Pod], int, int]:
    """One round; returns the pods after it, the ready count before the deletions and after."""
    actions = reconcile(dep, pods)
    deleted = {a.name for a in actions if isinstance(a, DeletePod)}
    ready_before = sum(p.status.ready for p in pods)
    after = [p for p in pods if p.metadata.name not in deleted]
    ready_after_deletes = sum(p.status.ready for p in after)
    for a in actions:
        if isinstance(a, CreatePod):
            after += [running(pod_for(dep, f"p{next(counter)}"), False) for _ in range(a.count)]
    current = template_hash(dep)
    flags = iter(ready_new)
    for p in after:
        if not is_outdated(p, current) and not p.status.ready:
            p.status.ready = next(flags, True)
    return after, ready_before, ready_after_deletes


@given(st.integers(1, 6), strategies, st.lists(st.booleans(), max_size=60))
def test_surge_and_availability_hold_and_the_rollout_finishes(
    replicas: int, strategy: tuple[int, int], readiness: list[bool]
) -> None:
    surge, unavailable = strategy
    counter = itertools.count()
    old = deployment(replicas, surge, unavailable, "v1")
    pods = [running(pod_for(old, f"p{next(counter)}"), True) for _ in range(replicas)]
    dep = deployment(replicas, surge, unavailable, "v2")  # the template changed
    floor = replicas - unavailable
    flags = iter(readiness)
    for _ in range(12 * replicas + 10):
        batch = [next(flags, True) for _ in range(replicas + surge)]
        pods, before, after_deletes = rounds(dep, pods, batch, counter)
        assert len(pods) <= replicas + surge, "surge exceeded"
        assert after_deletes >= min(before, floor), "took ready pods below the floor"
        current = template_hash(dep)
        if len(pods) == replicas and not any(is_outdated(p, current) for p in pods):
            return
    raise AssertionError(f"rollout didn't finish: {[p.spec.template_hash for p in pods]}")


@given(st.integers(1, 6), strategies)
def test_a_template_that_never_becomes_ready_stalls_with_the_old_pods_serving(
    replicas: int, strategy: tuple[int, int]
) -> None:
    surge, unavailable = strategy
    counter = itertools.count()
    old = deployment(replicas, surge, unavailable, "v1")
    pods = [running(pod_for(old, f"p{next(counter)}"), True) for _ in range(replicas)]
    dep = deployment(replicas, surge, unavailable, "bad")
    for _ in range(20):
        pods, _, _ = rounds(dep, pods, [False] * (replicas + surge), counter)
    assert sum(p.status.ready for p in pods) >= replicas - unavailable
    assert len(pods) <= replicas + surge


@given(st.integers(1, 6), strategies, st.lists(st.booleans(), min_size=6, max_size=6))
def test_old_pods_still_starting_are_not_all_dropped_at_once(
    replicas: int, strategy: tuple[int, int], old_ready: list[bool]
) -> None:
    surge, unavailable = strategy
    old = deployment(replicas, surge, unavailable, "v1")
    pods = [running(pod_for(old, f"p{i}"), old_ready[i]) for i in range(replicas)]
    dep = deployment(replicas, surge, unavailable, "v2")
    actions = reconcile(dep, pods)
    deleted = sum(isinstance(a, DeletePod) for a in actions)
    created = sum(a.count for a in actions if isinstance(a, CreatePod))
    assert replicas - deleted + created >= replicas - unavailable


def test_no_rollout_when_every_pod_is_current() -> None:
    dep = deployment(3, 1, 0, "v1")
    pods = [running(pod_for(dep, f"p{i}"), True) for i in range(3)]
    actions = reconcile(dep, pods)
    assert not any(isinstance(a, CreatePod | DeletePod) for a in actions)
