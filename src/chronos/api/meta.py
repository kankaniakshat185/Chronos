"""Object metadata shared by every kind (PLAN.md section 4.2)."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

# DNS-label-like names, as in ADR-0017.
NAME_PATTERN = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"


class ObjectMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=NAME_PATTERN, max_length=63)
    uid: str = ""  # assigned by the store, new per incarnation
    resource_version: int = Field(default=0, ge=0)  # store revision of the last write
    generation: int = Field(default=0, ge=0)  # bumped on spec change
    created_at: datetime | None = None
    deletion_timestamp: datetime | None = None
    labels: dict[str, str] = {}
    owner: str | None = None  # uid of the owning object
