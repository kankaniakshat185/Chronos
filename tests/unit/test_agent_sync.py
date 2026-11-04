"""agent_sync.plan: one case per rule (TESTING.md section 3)."""

from chronos.api import ObjectMeta, Pod, PodPhase, PodSpec, RestartPolicy
from chronos.core.agent_sync import Forget, LocalProc, Report, Start, Stop, plan

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


def test_nonzero_exit_reports_failed_with_the_code() -> None:
    [action] = plan(NODE, [pod(phase=PodPhase.RUNNING)], {"u1": exited(code=3)})
    assert isinstance(action, Report)
    s = action.pod.status
    assert (s.phase, s.reason, s.exit_code) == (PodPhase.FAILED, "Exited", 3)
    assert "code 3" in s.message


def test_signal_exit_says_which_signal() -> None:
    [action] = plan(NODE, [pod(phase=PodPhase.RUNNING)], {"u1": exited(code=-9)})
    assert isinstance(action, Report)
    assert action.pod.status.message == "killed by signal 9"


def test_clean_exit_with_always_policy_is_a_failure() -> None:
    [action] = plan(NODE, [pod()], {"u1": exited(code=0)})
    assert isinstance(action, Report)
    assert action.pod.status.phase == PodPhase.FAILED


def test_clean_exit_with_never_policy_succeeds() -> None:
    [action] = plan(NODE, [pod(policy=RestartPolicy.NEVER)], {"u1": exited(code=0)})
    assert isinstance(action, Report)
    assert (action.pod.status.phase, action.pod.status.reason) == (PodPhase.SUCCEEDED, "Completed")
