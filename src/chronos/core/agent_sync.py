"""Agent sync: desired pods vs local processes -> actions (ADR-0005, ADR-0006).

Pure. The agent shell gathers the pods bound to its node and a snapshot of its
local processes, calls `plan()` with the current monotonic time, and carries the
actions out. Processes are keyed by pod uid, so a pod deleted and recreated under
the same name is a new incarnation and never adopts the old process.

Restart policy (P2.6): when a desired pod's process exits and its policy says to
restart (`Always`: any exit; `OnFailure`: a non-zero exit), it's restarted in place
after an exponential backoff over *consecutive* crashes; a run of at least
`reset_after` seconds counts as stable and resets the count. While it waits, the
pod reports `CrashLoopBackOff`. Otherwise the exit is final: `Succeeded` for code 0,
`Failed(Exited)` for anything else.

Deletion (P4.7): a pod with `deletion_timestamp` isn't desired, so its process is
stopped; once nothing runs for it, the agent finalizes (force-deletes) it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from chronos.api import Pod, PodPhase, PodStatus, RestartPolicy
from chronos.api.pod import TERMINAL_PHASES
from chronos.common.backoff import exponential
from chronos.core.fencing import FENCED_EXIT_CODE

CRASH_LOOP = "CrashLoopBackOff"


@dataclass(frozen=True)
class LocalProc:
    uid: str
    pod_name: str
    pid: int
    exit_code: int | None = None  # None while running; negative = killed by that signal
    started_at: float = 0.0  # agent monotonic clock
    exited_at: float | None = None
    restarts: int = 0  # what the pod's restart_count should say
    crashes: int = 0  # consecutive quick crashes that led to this run
    assigned: Mapping[str, list[str]] = field(default_factory=dict)  # provider units (X-5)
    reason: str = ""  # why it exited, when the agent knows better than the code (OOMKilled)

    @property
    def running(self) -> bool:
        return self.exit_code is None


@dataclass(frozen=True)
class Backoff:
    base: float = 1.0
    cap: float = 10.0
    reset_after: float = 60.0  # a run this long is stable: the crash count starts over


@dataclass(frozen=True)
class Start:
    pod: Pod
    restarts: int = 0
    crashes: int = 0


@dataclass(frozen=True)
class Stop:
    uid: str


@dataclass(frozen=True)
class Forget:
    """Drop the record of an exited process nobody needs anymore."""

    uid: str


@dataclass(frozen=True)
class Finalize:
    """The pod is terminating and its process is gone: remove it for good (P4.7)."""

    name: str
    resource_version: int


@dataclass(frozen=True)
class Report:
    """CAS-write this pod (its resource_version is the one we read) with a new status."""

    pod: Pod


type Action = Start | Stop | Forget | Report | Finalize


def is_desired(pod: Pod, node: str) -> bool:
    return (
        pod.spec.node_name == node
        and pod.status.phase not in TERMINAL_PHASES
        and pod.metadata.deletion_timestamp is None
    )


def restarts_after(pod: Pod, exit_code: int) -> bool:
    match pod.spec.restart_policy:
        case RestartPolicy.ALWAYS:
            return True
        case RestartPolicy.ON_FAILURE:
            return exit_code != 0
        case RestartPolicy.NEVER:
            return False


def _running(pod: Pod, proc: LocalProc, ready: bool) -> PodStatus:
    status = pod.status.model_copy()
    status.phase, status.reason, status.message = PodPhase.RUNNING, "", ""
    status.pid, status.exit_code, status.ready = proc.pid, None, ready
    status.restart_count = proc.restarts
    status.assigned = {name: list(units) for name, units in proc.assigned.items()}
    return status


def _final(pod: Pod, proc: LocalProc, code: int) -> PodStatus:
    status = pod.status.model_copy()
    status.pid, status.exit_code, status.ready = proc.pid, code, False
    status.restart_count, status.assigned = proc.restarts, {}  # units freed on exit
    if code == 0:
        status.phase, status.reason, status.message = PodPhase.SUCCEEDED, "Completed", ""
    elif proc.reason:
        status.phase, status.reason = PodPhase.FAILED, proc.reason
        status.message = f"{proc.reason} (exit code {code})"
    else:
        status.phase, status.reason = PodPhase.FAILED, "Exited"
        status.message = f"killed by signal {-code}" if code < 0 else f"exited with code {code}"
    return status


def _waiting(pod: Pod, proc: LocalProc, code: int, delay: float) -> PodStatus:
    status = pod.status.model_copy()
    status.phase, status.reason, status.ready = PodPhase.RUNNING, CRASH_LOOP, False
    status.pid, status.exit_code, status.restart_count = proc.pid, code, proc.restarts
    status.assigned = {}  # units freed on exit
    why = f"{proc.reason}, " if proc.reason else ""
    status.message = f"back-off {delay:g}s before restarting ({why}last exit code {code})"
    return status


def _plan_exited(pod: Pod, proc: LocalProc, code: int, now: float, b: Backoff) -> Action | None:
    if not restarts_after(pod, code):
        return Report(pod.model_copy(update={"status": _final(pod, proc, code)}, deep=True))
    exited_at = proc.exited_at if proc.exited_at is not None else now
    stable = exited_at - proc.started_at >= b.reset_after
    crashes = 1 if stable else proc.crashes + 1
    delay = exponential(crashes - 1, b.base, b.cap)
    if now - exited_at >= delay:
        return Start(pod, restarts=proc.restarts + 1, crashes=crashes)
    status = _waiting(pod, proc, code, delay)
    if status == pod.status:
        return None
    return Report(pod.model_copy(update={"status": status}, deep=True))


def plan(
    node: str,
    pods: list[Pod],
    procs: dict[str, LocalProc],
    now: float = 0.0,
    backoff: Backoff | None = None,
    *,
    ready: Mapping[str, bool] | None = None,
) -> list[Action]:
    """`ready`: readiness by pod uid from the agent's probes (P10.1); a running pod with
    no readiness probe (missing from the map) is ready."""
    b = backoff or Backoff()
    desired = {p.metadata.uid: p for p in pods if is_desired(p, node)}
    actions: list[Action] = []
    for uid, pod in sorted(desired.items()):
        proc = procs.get(uid)
        if proc is None:
            actions.append(Start(pod, restarts=pod.status.restart_count))
            continue
        if proc.exit_code == FENCED_EXIT_CODE:
            actions.append(Forget(uid))  # stopped by fencing: start again once it's safe
            continue
        if proc.exit_code is None:
            status = _running(pod, proc, True if ready is None else ready.get(uid, True))
            if status != pod.status:
                actions.append(Report(pod.model_copy(update={"status": status}, deep=True)))
            continue
        if (action := _plan_exited(pod, proc, proc.exit_code, now, b)) is not None:
            actions.append(action)
    for uid, proc in sorted(procs.items()):
        if uid not in desired:
            actions.append(Stop(uid) if proc.running else Forget(uid))
    for pod in pods:  # two-phase deletion: confirm once nothing runs for the pod
        proc = procs.get(pod.metadata.uid)
        if (
            pod.spec.node_name == node
            and pod.metadata.deletion_timestamp is not None
            and (proc is None or not proc.running)
        ):
            actions.append(Finalize(pod.metadata.name, pod.metadata.resource_version))
    return actions
