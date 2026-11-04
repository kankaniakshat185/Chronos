"""The Pod kind: one workload process bound to at most one node (ADR-0005, ADR-0007)."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from chronos.api.meta import ObjectMeta
from chronos.api.resources import ResourceRequirements
from chronos.api.workload_types import workload_type


class RestartPolicy(StrEnum):
    ALWAYS = "Always"
    ON_FAILURE = "OnFailure"
    NEVER = "Never"


class PodPhase(StrEnum):
    PENDING = "Pending"
    BOUND = "Bound"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"


TERMINAL_PHASES = frozenset({PodPhase.SUCCEEDED, PodPhase.FAILED})


class PodTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command: list[str] = Field(min_length=1)
    env: dict[str, str] = {}
    resources: ResourceRequirements = ResourceRequirements()
    restart_policy: RestartPolicy = RestartPolicy.ALWAYS
    workload_type: str = "generic"  # a registered WorkloadType (X-4)

    @model_validator(mode="after")
    def _check_workload_type(self) -> Self:
        workload_type(self.workload_type).validate(self)  # ValueError -> 422
        return self


class PodSpec(PodTemplate):
    node_name: str = ""  # empty until bound; immutable once set (enforced by the store)
    template_hash: str = ""


class PodStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    phase: PodPhase = PodPhase.PENDING
    reason: str = ""
    message: str = ""
    pid: int | None = None
    exit_code: int | None = None
    restart_count: int = Field(default=0, ge=0)
    ready: bool = False
    assigned: dict[str, list[str]] = {}  # extended resource -> unit ids (X-5, S12)


class Pod(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["Pod"] = "Pod"
    metadata: ObjectMeta
    spec: PodSpec
    status: PodStatus = PodStatus()

    @property
    def requests(self) -> dict[str, int]:
        return self.spec.resources.requests
