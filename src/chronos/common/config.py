"""Timing configuration (PLAN.md section 4.5), validated at startup."""

from __future__ import annotations

from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Timings(BaseModel):
    """Control-plane timings. Defaults are the production values (PLAN.md section 4.5);
    the demo and tests pass faster ones. The fencing inequality involves the agent's
    timings too, so it's checked when an agent registers (`FencingConfig`)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    heartbeat_interval: float = Field(default=1.0, gt=0)
    heartbeat_timeout: float = Field(default=0.8, gt=0)
    node_grace: float = Field(default=4.0, gt=0)
    eviction_timeout: float = Field(default=10.0, gt=0)
    lifecycle_tick: float = Field(default=0.25, gt=0)  # node monitor period (ADR-0011)
    resync_period: float = Field(default=30.0, gt=0)  # controllers re-queue every key (ADR-0009)
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


class FencingConfig(BaseModel):
    """An agent's fencing timings (ADR-0012), validated at agent startup; the server
    checks F + K + drift < E against its own eviction timeout at registration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    heartbeat_interval: float = Field(default=1.0, gt=0)
    fence_timeout: float = Field(default=6.0, gt=0)
    termination_grace: float = Field(default=2.0, gt=0)
    drift_margin: float = Field(default=1.0, ge=0)
    shim_poll: float = Field(default=0.1, gt=0)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.fence_timeout <= 3 * self.heartbeat_interval:
            raise ValueError("fence_timeout must be > 3 x heartbeat_interval (no fencing on blips)")
        if self.shim_poll >= self.termination_grace:
            raise ValueError("shim_poll must be < termination_grace")
        return self
