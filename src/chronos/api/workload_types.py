"""WorkloadType registry (ADR-0023 X-4).

`template.workload_type` names a registered handler that validates the template and
prepares what to run. The agent's runtime only calls the protocol, so a new type
(e.g. Stage 2's `cuda_task`, deferred) is one registration, not a framework change.
Stage 1 ships only `generic`: run `command` as given.

It lives in `chronos.api` because validation happens wherever a template is parsed:
the API server (422), `chronosctl apply` and the agent all reject an unknown type.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from chronos.api.pod import Pod, PodTemplate


class WorkloadType(Protocol):
    def validate(self, template: PodTemplate) -> None:
        """Raise ValueError if the template can't run as this type."""

    def prepare(self, pod: Pod) -> tuple[list[str], dict[str, str]]:
        """The argv to run and any extra environment variables."""


_TYPES: dict[str, WorkloadType] = {}


def register_workload_type(name: str, handler: WorkloadType) -> None:
    if name in _TYPES:
        raise ValueError(f"workload type {name!r} is already registered")
    _TYPES[name] = handler


def workload_type(name: str) -> WorkloadType:
    try:
        return _TYPES[name]
    except KeyError:
        known = ", ".join(sorted(_TYPES))
        raise ValueError(f"unknown workload_type {name!r} (known: {known})") from None


class Generic:
    def validate(self, template: PodTemplate) -> None:
        pass

    def prepare(self, pod: Pod) -> tuple[list[str], dict[str, str]]:
        return list(pod.spec.command), {}


register_workload_type("generic", Generic())
