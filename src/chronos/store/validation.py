"""Per-kind write rules the store enforces inside its transaction (ADR-0007).

Each function either returns normally or raises `Invalid`; none mutates its inputs.
"""

from __future__ import annotations

from chronos.api import Deployment, Node, Pod, PodPhase
from chronos.store.errors import Invalid

type Obj = Node | Pod | Deployment


def validate_create(obj: Obj) -> None:
    if isinstance(obj, Pod) and obj.spec.node_name:
        # Pods only reach a node through bind(), which checks capacity (S2, S11).
        raise Invalid(f"Pod {obj.metadata.name!r}: spec.node_name must be empty; use bind")
    if isinstance(obj, Pod) and obj.status.phase != PodPhase.PENDING:
        raise Invalid(f"Pod {obj.metadata.name!r}: a new pod must be Pending")


def validate_spec_update(current: Obj, new: Obj) -> None:
    """`new` is the caller's object; `current` is what the store holds."""
    name = current.metadata.name
    if new.metadata.uid and new.metadata.uid != current.metadata.uid:
        raise Invalid(f"{current.kind} {name!r}: metadata.uid is immutable")
    if new.metadata.owner is not None and new.metadata.owner != current.metadata.owner:
        raise Invalid(f"{current.kind} {name!r}: metadata.owner is immutable")
    if current.metadata.deletion_timestamp is not None:
        raise Invalid(f"{current.kind} {name!r} is terminating; spec is frozen")
    # Bind-once (S1): an empty node_name in an update means "unchanged".
    if (
        isinstance(current, Pod)
        and isinstance(new, Pod)
        and new.spec.node_name
        and new.spec.node_name != current.spec.node_name
    ):
        raise Invalid(f"Pod {name!r}: spec.node_name can only be set by bind, once")


TERMINAL = frozenset({PodPhase.SUCCEEDED, PodPhase.FAILED})


def validate_status_update(current: Obj, new: Obj) -> None:
    if not (isinstance(current, Pod) and isinstance(new, Pod)):
        return
    name = current.metadata.name
    old, phase = current.status.phase, new.status.phase
    # Terminal is final: a terminal pod's resources are already free for other binds,
    # so letting it come back to life could overcommit its node (S2).
    if old in TERMINAL and phase != old:
        raise Invalid(f"Pod {name!r}: phase {old} is terminal (got {phase})")
    if not current.spec.node_name and phase in (PodPhase.BOUND, PodPhase.RUNNING):
        raise Invalid(f"Pod {name!r}: phase {phase} needs a bound pod; use bind")
    if current.spec.node_name and phase == PodPhase.PENDING:
        raise Invalid(f"Pod {name!r}: a bound pod can't go back to Pending")
