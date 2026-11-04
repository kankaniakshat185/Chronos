"""Request and response bodies that aren't kinds (ADR-0004, ADR-0011)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from chronos.api.node import NodeCondition
from chronos.api.resources import ResourceMap


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BindingRequest(_Body):
    node_name: str
    resource_version: int


class RegisterRequest(_Body):
    agent_epoch: str
    capacity: ResourceMap = {}


class HeartbeatRequest(_Body):
    agent_epoch: str


class HeartbeatReply(_Body):
    condition: NodeCondition


class ErrorBody(_Body):
    reason: str
    message: str
    details: dict[str, str | int | list[str]] = {}
