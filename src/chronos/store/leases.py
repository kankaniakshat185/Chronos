"""Node heartbeat leases, kept in memory on the server's monotonic clock (ADR-0011).

Heartbeats are not store writes: N nodes beating every second would otherwise
flood the revision log. Only condition *transitions* are written to the store.
"""

from __future__ import annotations

from chronos.common.clock import Clock


class Leases:
    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._last: dict[str, float] = {}

    def renew(self, node: str) -> None:
        self._last[node] = self._clock.monotonic()

    def silence(self, node: str) -> float:
        """Seconds since the node's last heartbeat.

        A node seen for the first time (e.g. created directly through the API)
        starts its lease now, so it isn't declared dead before it had a chance.
        """
        now = self._clock.monotonic()
        return now - self._last.setdefault(node, now)

    def forget(self, node: str) -> None:
        self._last.pop(node, None)
