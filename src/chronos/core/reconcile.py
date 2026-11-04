"""Deployment reconciliation: (deployment, its pods) -> actions (ADR-0009). Pure.

Level-triggered: every call recomputes the whole diff from the observed state,
so it doesn't matter which changes the caller saw or missed.

* Active pods (not Succeeded/Failed, not terminating) are counted against
  `spec.replicas`; missing ones are created, extra ones deleted.
* Scale-down victims: Pending first, then Bound, then Running, newest first in
  each group, so running work is disturbed last.
* At most `KEEP_FINISHED` finished pods are kept (newest), for `get pods` and
  debugging; older ones are deleted.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from chronos.api import Deployment, DeploymentStatus, ObjectMeta, Pod, PodPhase, PodSpec
from chronos.api.pod import TERMINAL_PHASES

KEEP_FINISHED = 3
DEPLOYMENT_LABEL = "chronos.dev/deployment"
_VICTIM_RANK = {PodPhase.PENDING: 0, PodPhase.BOUND: 1, PodPhase.RUNNING: 2}
_EPOCH = datetime.min.replace(tzinfo=UTC)


@dataclass(frozen=True)
class CreatePod:
    count: int


@dataclass(frozen=True)
class DeletePod:
    name: str
    resource_version: int


@dataclass(frozen=True)
class UpdateStatus:
    status: DeploymentStatus


type Action = CreatePod | DeletePod | UpdateStatus


def is_active(pod: Pod) -> bool:
    return pod.status.phase not in TERMINAL_PHASES and pod.metadata.deletion_timestamp is None


def _created(pod: Pod) -> tuple[datetime, str]:
    return (pod.metadata.created_at or _EPOCH, pod.metadata.name)


def pick_victims(active: Iterable[Pod], n: int) -> list[Pod]:
    """The `n` pods to delete on scale-down: least progressed first, newest first."""
    newest_first = sorted(active, key=_created, reverse=True)
    return sorted(newest_first, key=lambda p: _VICTIM_RANK.get(p.status.phase, 3))[:n]


def template_hash(dep: Deployment) -> str:
    return hashlib.sha256(dep.spec.template.model_dump_json().encode()).hexdigest()[:10]


def pod_for(dep: Deployment, name: str) -> Pod:
    """A new pod of this deployment (the caller picks a unique name)."""
    return Pod(
        metadata=ObjectMeta(
            name=name, owner=dep.metadata.uid, labels={DEPLOYMENT_LABEL: dep.metadata.name}
        ),
        # Via JSON: a python dump has cpu in millicores, which would be re-read as cores.
        spec=PodSpec.model_validate(
            {**dep.spec.template.model_dump(mode="json"), "template_hash": template_hash(dep)}
        ),
    )


def reconcile(dep: Deployment, pods: list[Pod]) -> list[Action]:
    """`pods` must be exactly the pods owned by `dep` (owner == its uid)."""
    active = [p for p in pods if is_active(p)]
    actions: list[Action] = []
    diff = dep.spec.replicas - len(active)
    if diff > 0:
        actions.append(CreatePod(diff))
    elif diff < 0:
        actions += [
            DeletePod(p.metadata.name, p.metadata.resource_version)
            for p in pick_victims(active, -diff)
        ]

    finished = sorted((p for p in pods if not is_active(p)), key=_created, reverse=True)
    actions += [
        DeletePod(p.metadata.name, p.metadata.resource_version) for p in finished[KEEP_FINISHED:]
    ]

    status = DeploymentStatus(
        observed_generation=dep.metadata.generation,
        replicas=len(active),
        ready_replicas=sum(p.status.ready for p in active),
    )
    if status != dep.status:
        actions.append(UpdateStatus(status))
    return actions


def orphans(pods: Iterable[Pod], owner_uids: set[str]) -> list[Pod]:
    """Pods whose owner no longer exists (e.g. their deployment was deleted)."""
    return [p for p in pods if p.metadata.owner is not None and p.metadata.owner not in owner_uids]
