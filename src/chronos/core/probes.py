"""Probe bookkeeping (P10.1): thresholds and timing, from results to decisions. Pure.

The agent runs the probes (exec, HTTP, TCP) and feeds each result in. Readiness: a pod
becomes ready after `success_threshold` successes in a row and stops being ready after
`failure_threshold` failures in a row; it starts not ready. Liveness: `failure_threshold`
failures in a row mean the process is restarted. Nothing is probed before
`initial_delay_seconds` after the process started.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from chronos.api.pod import Probe


@dataclass(frozen=True)
class ProbeState:
    successes: int = 0  # in a row
    failures: int = 0  # in a row
    passing: bool = False  # readiness: ready; liveness: not failed (yet)
    last_run: float | None = None


def due(state: ProbeState, probe: Probe, started_at: float, now: float) -> bool:
    if now - started_at < probe.initial_delay_seconds:
        return False
    return state.last_run is None or now - state.last_run >= probe.period_seconds


def observe(state: ProbeState, probe: Probe, ok: bool, now: float) -> ProbeState:
    if ok:
        successes, failures = state.successes + 1, 0
        passing = state.passing or successes >= probe.success_threshold
    else:
        successes, failures = 0, state.failures + 1
        passing = state.passing and failures < probe.failure_threshold
    return replace(state, successes=successes, failures=failures, passing=passing, last_run=now)


def liveness_failed(state: ProbeState, probe: Probe) -> bool:
    return state.failures >= probe.failure_threshold
