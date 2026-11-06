"""Canary bugs: each one switches off one protection (TESTING.md 6.5, P5.6).

A canary is applied only for one simulated run, and only inside the simulator: the
global patches are undone when the run ends. The simulator passes only if it finds
a violation of the expected invariant for each canary within its seed budget. If it
can't, the simulator is too weak: fix the sim, not the canary.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from chronos.agent import providers as providers_module
from chronos.agent.agent import Agent
from chronos.api.quantity import is_extended
from chronos.api.resources import insufficient
from chronos.client import local as local_module
from chronos.controllers.deployment import DeploymentController
from chronos.controllers.scheduler import SchedulerController
from chronos.core import rollout as rollout_module
from chronos.core.schedule import NodeInfo, Profile, SchedulerConfig, resolve
from chronos.store import store as store_module
from chronos.store.errors import EpochMismatch, NotFound, StoreError
from chronos.store.watchlog import WatchLog


@dataclass(frozen=True)
class Canary:
    name: str
    expect: frozenset[str]  # any of these invariants counts as detected
    budget: int  # seeds within which it must be detected
    what: str


CANARIES = {
    c.name: c
    for c in [
        Canary("disable_fencing", frozenset({"S3", "S4"}), 200, "agent and shims never fence"),
        Canary("disable_expectations", frozenset({"S3", "S7", "L2"}), 500,
               "expectations always met"),
        Canary("skip_bind_capacity_check", frozenset({"S2", "S11"}), 500,
               "no capacity re-check in bind, and no assume cache (stale scheduler cache)"),
        Canary("fence_after_eviction", frozenset({"S3", "S4"}), 200,
               "fence_timeout > eviction_timeout, with the registration check off"),
        Canary("ack_before_fsync", frozenset({"S8"}), 200, "the WAL acks without syncing"),
        Canary("measure_fence_from_ack_time", frozenset({"S3", "S4"}), 1000,
               "the lease counts from the heartbeat reply, not its send, and no heartbeat "
               "timeout (which alone would mask it)"),
        Canary("watch_skip_on_compaction", frozenset({"S6"}), 500,
               "a watch from before the history is served from the oldest event, no 410"),
        Canary("agent_only_fencing", frozenset({"S3", "S4"}), 500, "shims ignore the lease"),
        Canary("extended_resource_soft_constraint", frozenset({"S11"}), 200,
               "extended resources are ignored by the fit filter and the bind re-check"),
        Canary("provider_reuse_unit", frozenset({"S12"}), 200,
               "the counting provider always hands out unit 0"),
        Canary("rollout_ignores_max_unavailable", frozenset({"S7"}), 200,
               "a rolling update removes old pods without keeping replicas - max_unavailable "
               "ready"),
    ]
}  # fmt: skip


@dataclass
class Hooks:
    """What the world applies per object; set up by `active()`."""

    profile: Profile
    shims_enforce: bool = True
    honest_disk: bool = True
    fence_timeout: float | None = None
    heartbeat_timeout: float | None = None
    agent: list[Callable[[Agent], None]] | None = None
    scheduler: Callable[[SchedulerController], None] | None = None
    deployments: Callable[[DeploymentController], None] | None = None


class _Forgetful(dict[str, Any]):
    """An assume cache that never remembers anything."""

    def __setitem__(self, key: str, value: Any) -> None:
        return None


def _no_fence(agent: Agent) -> None:
    async def never() -> None:
        return None

    agent.fence_check = never  # type: ignore[method-assign]


def _ack_time_heartbeat(agent: Agent) -> None:
    """The bug ADR-0012 warns about: renew the lease from when the reply arrived."""

    async def heartbeat_once() -> None:
        try:
            await agent._client.heartbeat(agent.name, agent.epoch)
        except (EpochMismatch, NotFound):
            await agent.register()
            return
        except StoreError:
            return
        agent._accepted(agent._clock.monotonic())

    agent.heartbeat_once = heartbeat_once  # type: ignore[method-assign]


def _cpu_memory_fit(pod: Any, node: NodeInfo) -> str | None:
    request = {r: n for r, n in pod.requests.items() if not is_extended(r)}
    if missing := insufficient(request, node.free):
        return "Insufficient " + ", ".join(missing)
    return None


@contextlib.contextmanager
def active(name: str | None) -> Iterator[Hooks]:  # noqa: PLR0912, PLR0915 (one case per canary)
    """Apply canary `name` (or none) for one run; undo every global patch afterwards."""
    hooks = Hooks(profile=resolve(SchedulerConfig()))
    undo: list[Callable[[], None]] = []

    def patch(owner: Any, attr: str, value: Any) -> None:
        old = getattr(owner, attr)
        setattr(owner, attr, value)
        undo.append(lambda: setattr(owner, attr, old))

    match name:
        case None:
            pass
        case "disable_fencing":
            hooks.shims_enforce = False
            hooks.agent = [_no_fence]
        case "disable_expectations":

            def always_met(c: DeploymentController) -> None:
                c.expectations.satisfied = lambda key: True  # type: ignore[method-assign]

            hooks.deployments = always_met
        case "skip_bind_capacity_check":
            patch(store_module, "insufficient", lambda request, free: [])

            def forgetful(c: SchedulerController) -> None:
                c.assumed = _Forgetful()

            hooks.scheduler = forgetful
        case "fence_after_eviction":
            patch(local_module, "inequality_violation", lambda *args: None)
            hooks.fence_timeout = 6.0  # > eviction_timeout (5 s)
        case "ack_before_fsync":
            hooks.honest_disk = False
        case "measure_fence_from_ack_time":
            # The 0.4 s heartbeat timeout alone masks this bug: no reply later than that
            # is ever accepted, so ack time trails send time by < E - F - K. Lift it too.
            hooks.agent = [_ack_time_heartbeat]
            hooks.heartbeat_timeout = 10.0
        case "watch_skip_on_compaction":
            real_since = WatchLog.since

            def since(self: WatchLog, revision: int, match: Any, limit: int | None = None) -> Any:
                return real_since(self, max(revision, self.floor), match, limit)

            patch(WatchLog, "since", since)
        case "agent_only_fencing":
            hooks.shims_enforce = False
        case "extended_resource_soft_constraint":
            real = insufficient
            patch(
                store_module,
                "insufficient",
                lambda request, free: real(
                    {r: n for r, n in request.items() if not is_extended(r)}, free
                ),
            )
            base = hooks.profile
            hooks.profile = Profile(
                filters=tuple(
                    (n, _cpu_memory_fit if n == "FitsResources" else f) for n, f in base.filters
                ),
                scores=base.scores,
            )
        case "provider_reuse_unit":
            patch(providers_module.CountingProvider, "allocate", lambda self, uid, n: ["0"] * n)
        case "rollout_ignores_max_unavailable":
            real_step = rollout_module.step

            def step(dep: Any, active: Any, current_hash: str) -> Any:
                careless = dep.model_copy(deep=True)  # no validation: any value goes
                careless.spec.strategy.max_unavailable = dep.spec.replicas
                return real_step(careless, active, current_hash)

            patch(rollout_module, "step", step)
        case _:
            raise KeyError(f"unknown canary {name!r}; known: {', '.join(sorted(CANARIES))}")
    try:
        yield hooks
    finally:
        for fn in reversed(undo):
            fn()
