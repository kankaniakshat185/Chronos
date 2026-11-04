"""The Deployment kind: desired replica count of a pod template (ADR-0009, ADR-0017)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from chronos.api.meta import ObjectMeta
from chronos.api.pod import PodTemplate


class DeploymentSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    replicas: int = Field(ge=0)
    template: PodTemplate


class DeploymentStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    observed_generation: int = 0
    replicas: int = 0
    ready_replicas: int = 0


class Deployment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["Deployment"] = "Deployment"
    metadata: ObjectMeta
    spec: DeploymentSpec
    status: DeploymentStatus = DeploymentStatus()
