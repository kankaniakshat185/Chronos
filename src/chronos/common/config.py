"""Timing configuration (PLAN.md section 4.5), validated at startup."""

from __future__ import annotations

from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Timings(BaseModel):
    """Defaults are the production values; the demo and tests pass faster ones.

    MVP validation only (ADR-0022): the fencing inequality F + K + drift < E is added
    with fencing in PM-1 (ADR-0012).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    heartbeat_interval: float = Field(default=1.0, gt=0)
    heartbeat_timeout: float = Field(default=0.8, gt=0)
    node_grace: float = Field(default=4.0, gt=0)
    eviction_timeout: float = Field(default=10.0, gt=0)
    lifecycle_tick: float = Field(default=0.25, gt=0)
    scheduler_tick: float = Field(default=0.25, gt=0)
    request_timeout: float = Field(default=2.0, gt=0)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.heartbeat_timeout >= self.heartbeat_interval:
            raise ValueError("heartbeat_timeout must be < heartbeat_interval")
        if self.node_grace < 3 * self.heartbeat_interval:
            raise ValueError("node_grace must be >= 3 x heartbeat_interval")
        if self.eviction_timeout <= self.node_grace:
            raise ValueError("eviction_timeout must be > node_grace")
        return self
