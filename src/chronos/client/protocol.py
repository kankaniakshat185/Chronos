"""The `StoreClient` protocol (ADR-0004).

Controllers in `chronosd` use `LocalStoreClient`; the agent and CLI use
`HttpStoreClient`. One contract test suite runs against both, so they can't drift.
Errors are the store's own exceptions (`chronos.store.errors`) on both sides.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from chronos.api import ApiObject, Pod
from chronos.api.wire import HeartbeatReply, RegisterReply
from chronos.common.config import FencingConfig
from chronos.store.watchlog import EventType

WATCH_TIMEOUT = 30.0  # seconds a watch waits for an event (ADR-0008)
MAX_WATCH_TIMEOUT = 60.0


@dataclass(frozen=True)
class WatchEvent[T: ApiObject]:
    type: EventType
    object: T  # for DELETED: the last state, resource_version = the deletion's revision


@dataclass(frozen=True)
class WatchBatch[T: ApiObject]:
    events: list[WatchEvent[T]]
    revision: int  # everything up to here was seen: watch again from it


class StoreClient(Protocol):
    async def get[T: ApiObject](self, kind: type[T], name: str) -> T: ...

    async def list[T: ApiObject](
        self,
        kind: type[T],
        *,
        labels: Mapping[str, str] | None = None,
        node_name: str | None = None,
    ) -> tuple[list[T], int]: ...

    async def create[T: ApiObject](self, obj: T) -> T: ...

    async def update[T: ApiObject](self, obj: T) -> T: ...

    async def update_status[T: ApiObject](self, obj: T) -> T: ...

    async def delete[T: ApiObject](
        self, kind: type[T], name: str, expected_rv: int | None = None, *, force: bool = False
    ) -> T:
        """Bound, unfinished pods are only marked terminating unless `force` (P4.7)."""
        ...

    async def bind(self, pod: str, node: str, expected_rv: int) -> Pod: ...

    async def watch[T: ApiObject](
        self,
        kind: type[T],
        since: int,
        *,
        wait: float = WATCH_TIMEOUT,
        node_name: str | None = None,
    ) -> WatchBatch[T]:
        """Changes to `kind` after revision `since`, waiting up to `wait` seconds for one
        (ADR-0008). An empty batch still advances `revision`. Raises Compacted if that
        history is gone: list again and watch from the list's revision (S6)."""
        ...

    async def register(
        self,
        node: str,
        agent_epoch: str,
        capacity: Mapping[str, int],
        fencing: FencingConfig | None = None,
    ) -> RegisterReply:
        """`capacity` is in internal units: cpu in millicores, memory in bytes.

        Refused (Invalid) if `fencing` would break F + K + drift < E on this server.
        """
        ...

    async def heartbeat(self, node: str, agent_epoch: str) -> HeartbeatReply: ...
