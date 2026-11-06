"""The Pod kind: one workload process bound to at most one node (ADR-0005, ADR-0007)."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from chronos.api.meta import ApiObject, ObjectMeta
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


class HttpGet(BaseModel):
    model_config = ConfigDict(extra="forbid")

    port: int = Field(ge=1, le=65535)
    path: str = "/"


class Probe(BaseModel):
    """Exactly one of exec / http_get / tcp_port (P10.1). Runs on the node, against the
    pod's process (workloads share the host's network)."""

    model_config = ConfigDict(extra="forbid")

    exec: list[str] | None = None  # success: exit code 0
    http_get: HttpGet | None = None  # success: 2xx or 3xx from 127.0.0.1
    tcp_port: int | None = Field(default=None, ge=1, le=65535)  # success: connects
    period_seconds: float = Field(default=1.0, gt=0)
    timeout_seconds: float = Field(default=1.0, gt=0)
    initial_delay_seconds: float = Field(default=0.0, ge=0)
    failure_threshold: int = Field(default=3, ge=1)
    success_threshold: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def _one_kind(self) -> Self:
        kinds = [k for k in (self.exec, self.http_get, self.tcp_port) if k is not None]
        if len(kinds) != 1:
            raise ValueError("a probe needs exactly one of exec, http_get, tcp_port")
        if self.exec is not None and not self.exec:
            raise ValueError("an exec probe needs a command")
        return self


class PodTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command: list[str] = Field(min_length=1)
    env: dict[str, str] = {}
    resources: ResourceRequirements = ResourceRequirements()
    restart_policy: RestartPolicy = RestartPolicy.ALWAYS
    workload_type: str = "generic"  # a registered WorkloadType (X-4)
    readiness_probe: Probe | None = None  # not Ready (no traffic, not "available") until it passes
    liveness_probe: Probe | None = None  # failing it restarts the process

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


class Pod(ApiObject):
    kind: Literal["Pod"] = "Pod"
    metadata: ObjectMeta
    spec: PodSpec
    status: PodStatus = PodStatus()

    @property
    def requests(self) -> dict[str, int]:
        return self.spec.resources.requests
