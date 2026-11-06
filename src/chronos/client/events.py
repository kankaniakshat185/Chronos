"""Recording events, best effort (P7.3).

Events are for people reading `chronosctl describe`, never for decisions, so a failed
write is logged and dropped: recording an event must never fail or slow down the
reconcile or sync that triggered it beyond one request.
"""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Literal

from chronos.api.event import Event, EventSpec, EventStatus
from chronos.api.meta import ApiObject, ObjectMeta
from chronos.client.protocol import StoreClient
from chronos.common.clock import Clock
from chronos.store.errors import NotFound, StoreError

log = logging.getLogger(__name__)


def event_name(kind: str, name: str, reason: str) -> str:
    """Deterministic, so repeats find the same Event; a valid object name (<= 63)."""
    base = f"{kind}-{name}-{reason}".lower()
    base = re.sub(r"[^a-z0-9-]", "-", base).strip("-")
    if len(base) <= 63:
        return base
    digest = hashlib.sha256(base.encode()).hexdigest()[:8]
    return f"{base[:54].rstrip('-')}-{digest}"


class EventRecorder:
    def __init__(self, client: StoreClient, clock: Clock, source: str) -> None:
        self._client = client
        self._clock = clock
        self._source = source

    async def record(
        self,
        obj: ApiObject,
        reason: str,
        message: str,
        type_: Literal["Normal", "Warning"] = "Normal",
    ) -> None:
        name = event_name(obj.kind, obj.metadata.name, reason)
        now = self._clock.wall()
        try:
            try:
                current = await self._client.get(Event, name)
            except NotFound:
                await self._client.create(
                    Event(
                        metadata=ObjectMeta(name=name),
                        spec=EventSpec(
                            involved_kind=obj.kind,
                            involved_name=obj.metadata.name,
                            reason=reason,
                            type=type_,
                            source=self._source,
                        ),
                        status=EventStatus(message=message, first_seen=now, last_seen=now),
                    )
                )
                return
            current.status.count += 1
            current.status.message, current.status.last_seen = message, now
            await self._client.update_status(current)
        except StoreError as exc:  # Conflict with a concurrent repeat, Unavailable, ...
            log.info("event %s %s/%s not recorded: %s", reason, obj.kind, obj.metadata.name, exc)
