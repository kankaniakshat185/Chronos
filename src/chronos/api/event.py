"""The Event kind: something noteworthy happened to an object (P7.3).

One Event per (object, reason): a repeat increments `count` and moves `last_seen`, so
a crash loop is one line saying "x14", not fourteen.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from chronos.api.meta import ApiObject, ObjectMeta


class EventSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    involved_kind: str
    involved_name: str
    reason: str  # CamelCase, e.g. Scheduled, FailedScheduling, Evicted
    type: Literal["Normal", "Warning"] = "Normal"
    source: str = ""  # who saw it: "scheduler", "node-lifecycle", "agent/slot-a"


class EventStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: str = ""  # the latest occurrence's
    count: int = Field(default=1, ge=1)
    first_seen: datetime | None = None
    last_seen: datetime | None = None


class Event(ApiObject):
    kind: Literal["Event"] = "Event"
    metadata: ObjectMeta
    spec: EventSpec
    status: EventStatus = EventStatus()
