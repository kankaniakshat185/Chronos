"""Self-fencing decisions (ADR-0012). Pure: callers pass times from one monotonic clock.

The agent fences (stops everything, starts nothing) once `fence_timeout` F has passed
since the **send time** of its last heartbeat the server accepted. Each workload's
shim applies the same rule from the lease file the agent writes, so a dead or paused
agent still fences. Safety rests on F + K + drift < E: every workload is dead before
the server can declare the node NotReady and start replacements.
"""

from __future__ import annotations

from enum import StrEnum

# A shim's exit status when it stopped its workload because of fencing: not a crash.
FENCED_EXIT_CODE = 86


def should_fence(last_ok_send: float, now: float, fence_timeout: float) -> bool:
    return now - last_ok_send > fence_timeout


class ShimAction(StrEnum):
    RUN = "run"
    FENCE = "fence"  # SIGTERM, then SIGKILL after the termination grace
    KILL = "kill"  # SIGKILL now: replacements may already exist (the shim was frozen)


def shim_action(lease_age: float, fence_timeout: float, eviction_timeout: float) -> ShimAction:
    if lease_age > eviction_timeout:
        return ShimAction.KILL
    if lease_age > fence_timeout:
        return ShimAction.FENCE
    return ShimAction.RUN


def inequality_violation(
    fence_timeout: float, termination_grace: float, drift_margin: float, eviction_timeout: float
) -> str | None:
    """Why these timings are unsafe, or None if F + K + drift < E holds."""
    total = fence_timeout + termination_grace + drift_margin
    if total < eviction_timeout:
        return None
    return (
        f"fence_timeout + termination_grace + drift_margin = {total:g}s must be < "
        f"eviction_timeout = {eviction_timeout:g}s, or replacements could start before "
        "the old workloads are dead"
    )
