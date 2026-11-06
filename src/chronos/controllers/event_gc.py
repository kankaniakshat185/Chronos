"""Event garbage collection: events not seen for `ttl` are deleted (P7.3).

Events are written on every notable change, so without this they would grow forever.
"""

from __future__ import annotations

import contextlib
from datetime import timedelta

from chronos.api import Event
from chronos.client.local import LocalStoreClient
from chronos.common.clock import Clock
from chronos.controllers.runtime import Controller, Informers, register_controller
from chronos.store import NotFound

EVENT_TTL = 3600.0


class EventGarbageCollector(Controller):
    name = "event-gc"

    def __init__(
        self,
        client: LocalStoreClient,
        clock: Clock,
        informers: Informers | None = None,
        *,
        ttl: float = EVENT_TTL,
        resync_period: float = 60.0,
    ) -> None:
        informers = informers or Informers(client, clock)
        self._client = client
        self._ttl = ttl
        self._events = informers.get(Event)
        super().__init__(clock, [self._events], resync_period=resync_period)

    def keys(self) -> list[str]:
        return self._events.keys()

    async def reconcile(self, key: str) -> None:
        event = self._events.get(key)
        if event is None or event.status.last_seen is None:
            return
        if self.clock.wall() - event.status.last_seen > timedelta(seconds=self._ttl):
            with contextlib.suppress(NotFound):  # gone already
                await self._client.delete(Event, key, event.metadata.resource_version)


register_controller(
    EventGarbageCollector.name,
    lambda ctx: EventGarbageCollector(ctx.client, ctx.clock, ctx.informers),
)
