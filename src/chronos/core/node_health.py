"""Node condition from heartbeat silence (ADR-0011). Pure: the caller supplies the time."""

from __future__ import annotations

from chronos.api.node import NodeCondition


def condition(silence: float, node_grace: float, eviction_timeout: float) -> NodeCondition:
    """Ready while silence < grace, Unknown until eviction_timeout, then NotReady."""
    if silence < node_grace:
        return NodeCondition.READY
    if silence < eviction_timeout:
        return NodeCondition.UNKNOWN
    return NodeCondition.NOT_READY
