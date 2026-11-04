"""Agent sync: desired pods vs local processes -> actions (ADR-0006, P4.5 polling form).

Pure. The agent shell gathers the pods bound to its node and a snapshot of its
local processes, calls `plan()`, and carries the actions out. Processes are
keyed by pod uid, so a pod deleted and recreated under the same name is a new
incarnation and never adopts the old process.
"""

from __future__ import annotations

from dataclasses import dataclass

from chronos.api import Pod, PodPhase, PodStatus, RestartPolicy
from chronos.api.pod import TERMINAL_PHASES


@dataclass(frozen=True)
class LocalProc:
    uid: str
    pod_name: str
    pid: int
    exit_code: int | None = None  # None while running; negative = killed by that signal

    @property
    def running(self) -> bool:
        return self.exit_code is None


@dataclass(frozen=True)
class Start:
    pod: Pod


@dataclass(frozen=True)
class Stop:
    uid: str


@dataclass(frozen=True)
class Forget:
    """Drop the record of an exited process nobody needs anymore."""

    uid: str


@dataclass(frozen=True)
class Report:
    """CAS-write this pod (its resource_version is the one we read) with a new status."""

    pod: Pod


type Action = Start | Stop | Forget | Report


def is_desired(pod: Pod, node: str) -> bool:
    return (
        pod.spec.node_name == node
        and pod.status.phase not in TERMINAL_PHASES
        and pod.metadata.deletion_timestamp is None
    )


def observed_status(pod: Pod, proc: LocalProc) -> PodStatus:
    """The status the pod should have, given its local process."""
    status = pod.status.model_copy()
    status.pid = proc.pid
    if proc.exit_code is None:
        status.phase, status.reason, status.message = PodPhase.RUNNING, "", ""
        status.exit_code, status.ready = None, True  # no probes in the MVP
        return status
    status.exit_code, status.ready = proc.exit_code, False
    if proc.exit_code == 0 and pod.spec.restart_policy != RestartPolicy.ALWAYS:
        status.phase, status.reason, status.message = PodPhase.SUCCEEDED, "Completed", ""
    elif proc.exit_code < 0:
        status.phase, status.reason = PodPhase.FAILED, "Exited"
        status.message = f"killed by signal {-proc.exit_code}"
    else:
        status.phase, status.reason = PodPhase.FAILED, "Exited"
        status.message = f"exited with code {proc.exit_code}"
    return status


def plan(node: str, pods: list[Pod], procs: dict[str, LocalProc]) -> list[Action]:
    desired = {p.metadata.uid: p for p in pods if is_desired(p, node)}
    actions: list[Action] = []
    for uid, pod in sorted(desired.items()):
        proc = procs.get(uid)
        if proc is None:
            actions.append(Start(pod))
            continue
        status = observed_status(pod, proc)
        if status != pod.status:
            actions.append(Report(pod.model_copy(update={"status": status}, deep=True)))
    for uid, proc in sorted(procs.items()):
        if uid not in desired:
            actions.append(Stop(uid) if proc.running else Forget(uid))
    return actions
