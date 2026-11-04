"""Request and response bodies that aren't kinds (ADR-0004, ADR-0011)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from chronos.api.node import Node, NodeCondition
from chronos.api.resources import ResourceMap


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BindingRequest(_Body):
    node_name: str
    resource_version: int


class RegisterRequest(_Body):
    agent_epoch: str
    capacity: ResourceMap = {}
    # The agent's fencing timings: the server refuses agents that would break
    # F + K + drift < E against its eviction timeout (ADR-0012).
    fence_timeout: float = 6.0
    termination_grace: float = 2.0
    drift_margin: float = 1.0


class RegisterReply(_Body):
    node: Node
    eviction_timeout: float  # the shims SIGKILL at once past this lease age


class HeartbeatRequest(_Body):
    agent_epoch: str


class HeartbeatReply(_Body):
    condition: NodeCondition


class ErrorBody(_Body):
    reason: str
    message: str
    details: dict[str, str | int | list[str]] = {}
