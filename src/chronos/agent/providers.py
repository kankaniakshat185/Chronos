"""Resource providers (ADR-0023 X-5): which units of an extended resource a pod gets.

The scheduler and the store's bind only *count* units. A provider turns a count
into specific unit ids on the node when the process starts (`allocate`), frees them
when it exits (`release`), and may hand the workload environment variables naming
them (`env`; e.g. a Stage 2 device provider, deferred, would name its devices).
The ids go into `pod.status.assigned`.

Providers are registered per resource name as factories taking the node's capacity
for that resource; any advertised extended resource without one gets the
`CountingProvider`. State is in memory: after an agent restart, orphan cleanup has
killed the old processes and the restarted pods allocate afresh.

S12 (unit exclusivity): no unit is held by two live pods at once.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Protocol

from chronos.api.quantity import is_extended


class ProviderError(Exception):
    """The node can't supply the units (e.g. its capacity shrank under running pods)."""


class ResourceProvider(Protocol):
    def allocate(self, uid: str, n: int) -> list[str]: ...

    def release(self, uid: str) -> None: ...

    def env(self, units: list[str]) -> dict[str, str]: ...


type ProviderFactory = Callable[[str, int], ResourceProvider]  # (resource name, capacity)

_FACTORIES: dict[str, ProviderFactory] = {}


def register_provider(resource: str, factory: ProviderFactory) -> None:
    if resource in _FACTORIES:
        raise ValueError(f"a provider for {resource!r} is already registered")
    _FACTORIES[resource] = factory


class CountingProvider:
    """Units "0".."capacity-1"; hands out the lowest free ones. No environment."""

    def __init__(self, resource: str, capacity: int) -> None:
        self.resource = resource
        self._free = list(range(capacity))
        self._held: dict[str, list[int]] = {}

    def allocate(self, uid: str, n: int) -> list[str]:
        if uid in self._held:
            raise ProviderError(f"{self.resource}: pod {uid} already holds units")
        if n > len(self._free):
            raise ProviderError(f"{self.resource}: {n} requested, {len(self._free)} free")
        self._free.sort()
        taken, self._free = self._free[:n], self._free[n:]
        self._held[uid] = taken
        return [str(u) for u in taken]

    def release(self, uid: str) -> None:
        self._free += self._held.pop(uid, [])

    def env(self, units: list[str]) -> dict[str, str]:
        return {}


class Providers:
    """One provider per extended resource the node advertises."""

    def __init__(self, capacity: Mapping[str, int]) -> None:
        self._by_name: dict[str, ResourceProvider] = {
            name: _FACTORIES.get(name, CountingProvider)(name, units)
            for name, units in capacity.items()
            if is_extended(name)
        }

    def allocate(
        self, uid: str, requests: Mapping[str, int]
    ) -> tuple[dict[str, list[str]], dict[str, str]]:
        """All-or-nothing: on failure, nothing stays allocated."""
        assigned: dict[str, list[str]] = {}
        env: dict[str, str] = {}
        try:
            for name, n in sorted(requests.items()):
                if not is_extended(name) or n == 0:
                    continue
                provider = self._by_name.get(name)
                if provider is None:
                    raise ProviderError(f"{name}: not advertised by this node")
                assigned[name] = provider.allocate(uid, n)
                env |= provider.env(assigned[name])
        except ProviderError:
            for name in assigned:  # only what this call took
                self._by_name[name].release(uid)
            raise
        return assigned, env

    def release(self, uid: str) -> None:
        for provider in self._by_name.values():
            provider.release(uid)
