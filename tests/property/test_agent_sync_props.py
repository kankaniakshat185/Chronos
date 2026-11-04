"""agent_sync properties (TESTING.md section 5): converges, then stays put; never stops a
desired pod; always stops processes nobody wants."""

from hypothesis import given
from hypothesis import strategies as st

from chronos.api import ObjectMeta, Pod, PodPhase, PodSpec, RestartPolicy
from chronos.core.agent_sync import (
    Finalize,
    Forget,
    LocalProc,
    Report,
    Start,
    Stop,
    is_desired,
    plan,
)

NODE = "n1"
UIDS = [f"u{i}" for i in range(6)]


@st.composite
def pods(draw: st.DrawFn) -> list[Pod]:
    out = []
    for uid in draw(st.lists(st.sampled_from(UIDS), unique=True, max_size=5)):
        p = Pod(
            metadata=ObjectMeta(name=f"pod-{uid}", uid=uid),
            spec=PodSpec(
                command=["true"],
                node_name=draw(st.sampled_from([NODE, NODE, "n2", ""])),
                restart_policy=draw(st.sampled_from(list(RestartPolicy))),
            ),
        )
        p.status.phase = draw(st.sampled_from(list(PodPhase)))
        out.append(p)
    return out


procs = st.dictionaries(
    st.sampled_from([*UIDS, "stranger"]),
    st.one_of(st.none(), st.integers(-15, 3)),
    max_size=6,
).map(lambda d: {u: LocalProc(u, f"pod-{u}", 100, code) for u, code in d.items()})


def apply(
    pod_list: list[Pod], local: dict[str, LocalProc], actions: list[object]
) -> tuple[list[Pod], dict[str, LocalProc]]:
    """What the agent shell does, with a process that starts and stops instantly."""
    pods_by_uid = {p.metadata.uid: p for p in pod_list}
    local = dict(local)
    for a in actions:
        match a:
            case Start(pod=p):
                local[p.metadata.uid] = LocalProc(p.metadata.uid, p.metadata.name, 200)
            case Stop(uid=u) | Forget(uid=u):
                local.pop(u)
            case Report(pod=p):
                pods_by_uid[p.metadata.uid] = p
            case Finalize(name=name):
                pods_by_uid = {u: p for u, p in pods_by_uid.items() if p.metadata.name != name}
    return list(pods_by_uid.values()), local


@given(pods(), procs)
def test_converges_within_three_rounds_and_then_stays_put(
    pod_list: list[Pod], local: dict[str, LocalProc]
) -> None:
    for _ in range(3):
        pod_list, local = apply(pod_list, local, list(plan(NODE, pod_list, local)))
    assert plan(NODE, pod_list, local) == []


@given(pods(), procs)
def test_never_stops_a_desired_pod(pod_list: list[Pod], local: dict[str, LocalProc]) -> None:
    desired = {p.metadata.uid for p in pod_list if is_desired(p, NODE)}
    stopped = {a.uid for a in plan(NODE, pod_list, local) if isinstance(a, Stop)}
    assert not stopped & desired


@given(pods(), procs)
def test_always_stops_running_processes_nobody_wants(
    pod_list: list[Pod], local: dict[str, LocalProc]
) -> None:
    desired = {p.metadata.uid for p in pod_list if is_desired(p, NODE)}
    unwanted = {u for u, proc in local.items() if proc.running and u not in desired}
    stopped = {a.uid for a in plan(NODE, pod_list, local) if isinstance(a, Stop)}
    assert stopped == unwanted


@given(pods(), procs)
def test_only_reports_pods_of_this_node_and_changes_only_status(
    pod_list: list[Pod], local: dict[str, LocalProc]
) -> None:
    by_uid = {p.metadata.uid: p for p in pod_list}
    for a in plan(NODE, pod_list, local):
        if isinstance(a, Report):
            before = by_uid[a.pod.metadata.uid]
            assert a.pod.spec == before.spec and a.pod.metadata == before.metadata
            assert a.pod.status != before.status
            assert before.spec.node_name == NODE


@given(pods(), procs)
def test_never_finalizes_a_pod_whose_process_still_runs(
    pod_list: list[Pod], local: dict[str, LocalProc]
) -> None:
    by_name = {p.metadata.name: p for p in pod_list}
    for a in plan(NODE, pod_list, local):
        if isinstance(a, Finalize):
            pod = by_name[a.name]
            assert pod.metadata.deletion_timestamp is not None and pod.spec.node_name == NODE
            proc = local.get(pod.metadata.uid)
            assert proc is None or not proc.running
