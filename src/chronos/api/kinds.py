"""The kinds the API serves and their URL names (ADR-0017; a registry arrives with X-6)."""

from __future__ import annotations

from chronos.api.deployment import Deployment
from chronos.api.node import Node
from chronos.api.pod import Pod

type Obj = Node | Pod | Deployment

PLURALS: dict[type[Obj], str] = {Node: "nodes", Pod: "pods", Deployment: "deployments"}
