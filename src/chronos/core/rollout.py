"""Rolling updates (P10.2, S7): which pods to create and delete while a template changes. Pure.

A rollout is in progress while some active pod carries a template hash that isn't the
deployment's current one ("old"). Each call computes one step from the observed state
(level-triggered, like `reconcile`):

* **Surge:** never more than `replicas + max_surge` active pods: new pods are created
  only into that room.
* **Availability:** never take a *ready* pod away if that would leave fewer than
  `replicas - max_unavailable` ready. Old pods that aren't ready can always go: removing
  them costs no availability.
* New pods beyond `replicas` (a scale-down mid-rollout) go first.

So with the defaults (surge 1, unavailable 0) a rollout is "start one new pod, wait until
it's ready, stop one old pod". A new template that never becomes ready stalls the rollout
with the old pods still serving, which is the point.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from chronos.api import Deployment, Pod

_UNREADY_FIRST = 0
_READY_LAST = 1


@dataclass(frozen=True)
class Step:
    create: int  # new pods to create
    delete: list[Pod]  # pods to remove (old ones, or new ones beyond replicas)


def is_outdated(pod: Pod, current_hash: str) -> bool:
    """Pods without a hash (made before hashing existed) count as current."""
    return bool(pod.spec.template_hash) and pod.spec.template_hash != current_hash


def step(dep: Deployment, active: Sequence[Pod], current_hash: str) -> Step:
    """`active`: the deployment's pods that aren't finished or terminating."""
    replicas = dep.spec.replicas
    surge, unavailable = dep.spec.strategy.max_surge, dep.spec.strategy.max_unavailable
    old = [p for p in active if is_outdated(p, current_hash)]
    new = [p for p in active if not is_outdated(p, current_hash)]

    create = max(0, min(replicas - len(new), replicas + surge - len(active)))

    ready = sum(p.status.ready for p in active)
    may_take = ready - (replicas - unavailable)  # ready pods we may remove right now
    delete: list[Pod] = []
    # New pods beyond replicas: the least progressed first.
    for p in sorted(new, key=_order)[: max(0, len(new) - replicas)]:
        if p.status.ready:
            if may_take <= 0:
                continue
            may_take -= 1
        delete.append(p)
    # Old pods: unready ones cost nothing; ready ones only within `may_take`. Overall, keep
    # at least `replicas - max_unavailable` pods: with no surge room (max_surge = 0) the
    # only way forward is to go below `replicas` first, which is what max_unavailable allows.
    budget = len(active) + create - (replicas - unavailable)
    budget -= len(delete)
    for p in sorted(old, key=_order):
        if budget <= 0:
            break
        if p.status.ready:
            if may_take <= 0:
                continue
            may_take -= 1
        delete.append(p)
        budget -= 1
    return Step(create, delete)


def _order(p: Pod) -> tuple[int, str]:
    return (_READY_LAST if p.status.ready else _UNREADY_FIRST, p.metadata.name)
