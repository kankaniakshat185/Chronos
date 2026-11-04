"""The Pod kind: one workload process bound to at most one node (ADR-0005, ADR-0007)."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from chronos.api.meta import ObjectMeta
from chronos.api.resources import ResourceRequirements


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


class PodTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command: list[str] = Field(min_length=1)
    env: dict[str, str] = {}
    resources: ResourceRequirements = ResourceRequirements()
    restart_policy: RestartPolicy = RestartPolicy.ALWAYS


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


class Pod(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["Pod"] = "Pod"
    metadata: ObjectMeta
    spec: PodSpec
    status: PodStatus = PodStatus()

    @property
    def requests(self) -> dict[str, int]:
        return self.spec.resources.requests
