"""The Deployment kind: desired replica count of a pod template (ADR-0009, ADR-0017)."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from chronos.api.meta import ApiObject, ObjectMeta
from chronos.api.pod import PodTemplate


class RollingUpdate(BaseModel):
    """How a template change rolls out (P10.2), against *ready* pods: at most
    `replicas + max_surge` pods exist, and the controller never takes a ready pod away if
    that would leave fewer than `replicas - max_unavailable` ready (S7)."""

    model_config = ConfigDict(extra="forbid")

    max_surge: int = Field(default=1, ge=0)
    max_unavailable: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _can_progress(self) -> Self:
        if self.max_surge == 0 and self.max_unavailable == 0:
            raise ValueError(
                "max_surge and max_unavailable can't both be 0: no rollout could start"
            )
        return self


class DeploymentSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    replicas: int = Field(ge=0)
    template: PodTemplate
    strategy: RollingUpdate = RollingUpdate()


class DeploymentStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    observed_generation: int = 0
    replicas: int = 0
    ready_replicas: int = 0
    updated_replicas: int = 0  # active pods with the current template


class Deployment(ApiObject):
    kind: Literal["Deployment"] = "Deployment"
    metadata: ObjectMeta
    spec: DeploymentSpec
    status: DeploymentStatus = DeploymentStatus()
