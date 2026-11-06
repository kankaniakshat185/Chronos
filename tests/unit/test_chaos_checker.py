"""The chaos runner's live checker, on synthetic states: it must see each violation."""

import itertools
from pathlib import Path

import pytest

from chronos.api import Deployment, Node, Pod, PodPhase
from chronos.testing.chaos import Chaos, Workload, plan


@pytest.fixture
def chaos(tmp_path: Path) -> Chaos:
    return Chaos(tmp_path, seed=3, nodes=2, duration=30)


def node(name: str, **capacity: int) -> Node:
    return Node.model_validate({"metadata": {"name": name}, "spec": {"capacity": capacity}})


def pod(name: str, uid: str, node_name: str = "", owner: str | None = None, **requests: int) -> Pod:
    p = Pod.model_validate(
        {"metadata": {"name": name, "uid": uid, "owner": owner},
         "spec": {"command": ["x"], "node_name": node_name, "resources": {"requests": requests}}}
    )  # fmt: skip
    if node_name:
        p.status.phase = PodPhase.RUNNING
    return p


def deployment(name: str, uid: str, replicas: int) -> Deployment:
    return Deployment.model_validate(
        {"metadata": {"name": name, "uid": uid},
         "spec": {"replicas": replicas, "template": {"command": ["x"]}}}
    )  # fmt: skip


def invariants(c: Chaos) -> list[str]:
    return [v["invariant"] for v in c.report.violations]


def test_overcommit_per_resource_name(chaos: Chaos) -> None:
    nodes = [node("a", cpu=1000, **{"x.dev/r": 1})]
    pods = [
        pod("p1", "u1", "a", **{"cpu": 600, "x.dev/r": 1}),
        pod("p2", "u2", "a", **{"cpu": 600, "x.dev/r": 1}),
    ]
    chaos._store_invariants(nodes, pods)
    assert sorted(invariants(chaos)) == ["S11", "S2"]


def test_a_pod_that_moves_node(chaos: Chaos) -> None:
    chaos._store_invariants([], [pod("p", "u1", "a")])
    chaos._store_invariants([], [pod("p", "u1", "b")])
    assert "S1" in invariants(chaos)


def test_duplicates_need_two_samples_in_a_row(chaos: Chaos) -> None:
    deps = [deployment("d1", "D", 1)]
    pods = [pod("d1-a", "u1", "s3n1", owner="D")]
    two = [Workload(10, "u1", "s3n1", "chaos-d1"), Workload(11, "rogue", "s3n2", "chaos-d1")]
    chaos._s3(pods, deps, two, 10.0)
    assert invariants(chaos) == []  # one sample could be a read race
    chaos._s3(pods, deps, two, 10.2)
    assert invariants(chaos) == ["S3"]


def test_a_terminating_pods_process_is_not_a_duplicate(chaos: Chaos) -> None:
    from datetime import UTC, datetime  # noqa: PLC0415

    deps = [deployment("d1", "D", 1)]
    old = pod("d1-old", "u0", "s3n1", owner="D")
    old.metadata.deletion_timestamp = datetime.now(UTC)
    pods = [old, pod("d1-new", "u1", "s3n2", owner="D")]
    procs = [Workload(10, "u0", "s3n1", "chaos-d1"), Workload(11, "u1", "s3n2", "chaos-d1")]
    for t in (10.0, 10.2, 10.4):
        chaos._s3(pods, deps, procs, t)
    assert invariants(chaos) == []


def test_a_workload_running_long_after_its_lease(chaos: Chaos) -> None:
    from chronos.common.clock import RealClock  # noqa: PLC0415

    lease = chaos.cluster.agent_dir("s3n1") / "lease"
    lease.parent.mkdir(parents=True)
    lease.write_text(repr(RealClock().monotonic() - 10))  # last accepted heartbeat 10 s ago
    procs = [Workload(10, "u1", "s3n1", "chaos-d1")]
    chaos._s4(procs, 10.0)
    chaos._s4(procs, 10.2)
    assert invariants(chaos) == ["S4"]


def test_two_live_processes_holding_one_unit(chaos: Chaos) -> None:
    unit = (("x.dev/r", "0"),)
    two = [Workload(1, "u1", "n", "chaos-d", unit), Workload(2, "u2", "n", "chaos-d", unit)]
    chaos._s12(two)
    assert invariants(chaos) == []  # a scan isn't atomic: one sample could be a race
    chaos._s12(two)
    assert invariants(chaos) == ["S12"]
    chaos.report.violations.clear()
    other = [Workload(1, "u1", "n", "chaos-d", unit), Workload(2, "u2", "m", "chaos-d", unit)]
    chaos._s12(other)
    chaos._s12(other)
    assert invariants(chaos) == []


def test_the_plan_is_seeded_and_one_fault_per_target_at_a_time() -> None:
    names = ["a", "b", "c"]
    assert plan(5, names, 120) == plan(5, names, 120)
    steps, resources = plan(5, names, 120)
    assert resources and all(r.startswith("chaos5.dev/") for r in resources)
    faults = sorted((s for s in steps if s.target), key=lambda s: s.at)
    for target in {s.target for s in faults}:
        mine = [s for s in faults if s.target == target]
        for x, y in itertools.pairwise(mine):
            assert y.at >= x.at + x.duration, f"overlapping faults on {target}"


def test_frozen_processes_are_not_running_until_one_poll_after_the_thaw(chaos: Chaos) -> None:
    import math  # noqa: PLC0415

    deps = [deployment("d1", "D", 1)]
    pods = [pod("d1-new", "u1", "s3n2", owner="D")]
    old_and_new = [Workload(10, "u0", "s3n1", "chaos-d1"), Workload(11, "u1", "s3n2", "chaos-d1")]
    chaos.frozen["s3n1"] = math.inf  # SIGSTOPped: the old one exists but doesn't run
    for t in (10.0, 10.2):
        chaos._s3(pods, deps, old_and_new, t)
    chaos.frozen["s3n1"] = 10.3  # thawed: the shim has one poll to kill it
    chaos._s3(pods, deps, old_and_new, 10.35)
    assert invariants(chaos) == []
    for t in (10.8, 11.0):  # still running long after the thaw: that's a duplicate
        chaos._s3(pods, deps, old_and_new, t)
    assert invariants(chaos) == ["S3"]


def test_the_lease_bound_is_f_plus_k_plus_drift_poll_and_sampling(chaos: Chaos) -> None:
    from chronos.common.clock import RealClock  # noqa: PLC0415

    t = chaos.timings
    bound = t.fence_timeout + t.termination_grace + t.drift_margin + 0.1 + 0.2
    lease = chaos.cluster.agent_dir("s3n1") / "lease"
    lease.parent.mkdir(parents=True)
    procs = [Workload(10, "u1", "s3n1", "chaos-d1")]
    lease.write_text(repr(RealClock().monotonic() - (bound - 0.2)))  # just inside the bound
    chaos._s4(procs, 1.0)
    chaos._s4(procs, 1.2)
    assert invariants(chaos) == []
    lease.write_text(repr(RealClock().monotonic() - (bound + 0.2)))  # just past it
    chaos._s4(procs, 1.4)
    chaos._s4(procs, 1.6)
    assert invariants(chaos) == ["S4"]
