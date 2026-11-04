"""The Node kind: an agent and its declared capacity (ADR-0011, ADR-0014)."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict

from chronos.api.meta import ObjectMeta
from chronos.api.resources import ResourceMap


class NodeCondition(StrEnum):
    READY = "Ready"
    UNKNOWN = "Unknown"
    NOT_READY = "NotReady"


class NodeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    capacity: ResourceMap = {}
    unschedulable: bool = False


class NodeStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    condition: NodeCondition = NodeCondition.UNKNOWN
    allocated: ResourceMap = {}  # derived from bound pods, never written by clients
    agent_epoch: str = ""


class Node(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["Node"] = "Node"
    metadata: ObjectMeta
    spec: NodeSpec = NodeSpec()
    status: NodeStatus = NodeStatus()
