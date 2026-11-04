"""agent_sync.plan: one case per rule (TESTING.md section 3)."""

from datetime import UTC, datetime

from chronos.api import ObjectMeta, Pod, PodPhase, PodSpec, RestartPolicy
from chronos.core.agent_sync import (
    CRASH_LOOP,
    Backoff,
    Finalize,
    Forget,
    LocalProc,
    Report,
    Start,
    Stop,
    plan,
)

NODE = "n1"


def pod(
    uid: str = "u1",
    node: str = NODE,
    phase: PodPhase = PodPhase.BOUND,
    policy: RestartPolicy = RestartPolicy.ALWAYS,
) -> Pod:
    p = Pod(
        metadata=ObjectMeta(name=f"pod-{uid}", uid=uid, resource_version=5),
        spec=PodSpec(command=["true"], node_name=node, restart_policy=policy),
    )
    p.status.phase = phase
    return p


def running(uid: str = "u1", pid: int = 100) -> LocalProc:
    return LocalProc(uid, f"pod-{uid}", pid)


def exited(uid: str = "u1", code: int = 0) -> LocalProc:
    return LocalProc(uid, f"pod-{uid}", 100, exit_code=code)


def test_starts_a_bound_pod_with_no_process() -> None:
    p = pod()
    assert plan(NODE, [p], {}) == [Start(p)]


def test_ignores_pods_of_other_nodes_and_unbound_pods() -> None:
    assert plan(NODE, [pod(node="n2"), pod(uid="u2", node="")], {}) == []


def test_reports_running_once() -> None:
    [action] = plan(NODE, [pod()], {"u1": running()})
    assert isinstance(action, Report)
    s = action.pod.status
    assert (s.phase, s.pid, s.ready) == (PodPhase.RUNNING, 100, True)
    assert action.pod.metadata.resource_version == 5  # CAS against what we read
    assert plan(NODE, [action.pod], {"u1": running()}) == []


def test_stops_a_process_whose_pod_is_gone() -> None:
    assert plan(NODE, [], {"u1": running()}) == [Stop("u1")]


def test_stops_a_process_whose_pod_was_recreated_under_the_same_name() -> None:
    new = pod(uid="u2")
    assert plan(NODE, [new], {"u1": running("u1")}) == [Start(new), Stop("u1")]


def test_stops_a_process_whose_pod_is_terminal() -> None:
    assert plan(NODE, [pod(phase=PodPhase.FAILED)], {"u1": running()}) == [Stop("u1")]


def test_forgets_an_exited_process_nobody_wants() -> None:
    assert plan(NODE, [], {"u1": exited()}) == [Forget("u1")]


def test_nonzero_exit_with_never_policy_fails_with_the_code() -> None:
    p = pod(phase=PodPhase.RUNNING, policy=RestartPolicy.NEVER)
    [action] = plan(NODE, [p], {"u1": exited(code=3)})
    assert isinstance(action, Report)
    s = action.pod.status
    assert (s.phase, s.reason, s.exit_code) == (PodPhase.FAILED, "Exited", 3)
    assert "code 3" in s.message


def test_signal_exit_says_which_signal() -> None:
    p = pod(phase=PodPhase.RUNNING, policy=RestartPolicy.NEVER)
    [action] = plan(NODE, [p], {"u1": exited(code=-9)})
    assert isinstance(action, Report)
    assert action.pod.status.message == "killed by signal 9"


def test_clean_exit_with_never_policy_succeeds() -> None:
    [action] = plan(NODE, [pod(policy=RestartPolicy.NEVER)], {"u1": exited(code=0)})
    assert isinstance(action, Report)
    assert (action.pod.status.phase, action.pod.status.reason) == (PodPhase.SUCCEEDED, "Completed")


def test_on_failure_restarts_crashes_but_not_clean_exits() -> None:
    p = pod(policy=RestartPolicy.ON_FAILURE)
    [clean] = plan(NODE, [p], {"u1": exited(code=0)}, now=100)
    assert isinstance(clean, Report) and clean.pod.status.phase == PodPhase.SUCCEEDED
    [crash] = plan(NODE, [p], {"u1": crashed(0)}, now=101.0, backoff=B)
    assert isinstance(crash, Start)


B = Backoff(base=1.0, cap=8.0, reset_after=60.0)


def crashed(crashes: int, ran: float = 1.0, exited_at: float = 100.0) -> LocalProc:
    return LocalProc("u1", "pod-u1", 100, 1, exited_at - ran, exited_at, crashes, crashes)


def test_always_waits_in_crash_loop_backoff_then_restarts() -> None:
    p = pod(phase=PodPhase.RUNNING)
    [wait] = plan(NODE, [p], {"u1": crashed(0)}, now=100.5, backoff=B)
    assert isinstance(wait, Report)
    s = wait.pod.status
    assert (s.phase, s.reason, s.ready, s.exit_code) == (PodPhase.RUNNING, CRASH_LOOP, False, 1)
    assert "back-off 1s" in s.message
    assert plan(NODE, [wait.pod], {"u1": crashed(0)}, now=100.9, backoff=B) == []  # no churn
    assert plan(NODE, [wait.pod], {"u1": crashed(0)}, now=101.0, backoff=B) == [
        Start(wait.pod, restarts=1, crashes=1)
    ]


def test_backoff_doubles_per_consecutive_crash_up_to_the_cap() -> None:
    delays = []
    for n in range(6):
        [wait] = plan(NODE, [pod()], {"u1": crashed(n)}, now=100.0, backoff=B)
        assert isinstance(wait, Report)
        delays.append(wait.pod.status.message.split()[1])
    assert delays == ["1s", "2s", "4s", "8s", "8s", "8s"]


def test_a_stable_run_resets_the_backoff() -> None:
    [action] = plan(NODE, [pod()], {"u1": crashed(5, ran=60.0)}, now=101.0, backoff=B)
    assert action == Start(pod(), restarts=6, crashes=1)


def test_running_reports_the_restart_count() -> None:
    proc = LocalProc("u1", "pod-u1", 100, restarts=3)
    [action] = plan(NODE, [pod()], {"u1": proc})
    assert isinstance(action, Report) and action.pod.status.restart_count == 3


def test_first_start_carries_the_stored_restart_count() -> None:
    p = pod()
    p.status.restart_count = 4  # e.g. after an agent restart
    assert plan(NODE, [p], {}) == [Start(p, restarts=4)]


def terminating(uid: str = "u1") -> Pod:
    p = pod(uid=uid, phase=PodPhase.RUNNING)
    p.metadata.deletion_timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    return p


def test_terminating_pod_with_a_running_process_is_stopped_not_finalized() -> None:
    assert plan(NODE, [terminating()], {"u1": running()}) == [Stop("u1")]


def test_terminating_pod_is_finalized_once_its_process_is_gone() -> None:
    assert plan(NODE, [terminating()], {"u1": exited(code=-15)}) == [
        Forget("u1"),
        Finalize("pod-u1", 5),
    ]
    assert plan(NODE, [terminating()], {}) == [Finalize("pod-u1", 5)]


def test_other_nodes_terminating_pods_are_not_ours_to_finalize() -> None:
    p = terminating()
    p.spec.node_name = "n2"
    assert plan(NODE, [p], {}) == []
