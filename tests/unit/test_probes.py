"""Probes (P10.1): the pure bookkeeping, and the agent acting on it."""

import pytest

from chronos.api import ObjectMeta, Pod, PodSpec
from chronos.api.pod import HttpGet, Probe
from chronos.core.agent_sync import LocalProc
from chronos.core.probes import ProbeState, due, liveness_failed, observe

PROBE = Probe(tcp_port=8080, period_seconds=1.0, failure_threshold=3, success_threshold=2)


def run(results: list[bool]) -> ProbeState:
    state = ProbeState()
    for t, ok in enumerate(results):
        state = observe(state, PROBE, ok, float(t))
    return state


def test_readiness_needs_success_threshold_in_a_row() -> None:
    assert not run([True]).passing
    assert run([True, True]).passing
    assert not run([True, False, True]).passing  # not in a row


def test_readiness_is_lost_after_failure_threshold_in_a_row() -> None:
    assert run([True, True, False, False]).passing  # 2 failures: still ready
    assert not run([True, True, False, False, False]).passing
    assert run([True, True, False, False, True]).passing  # a success resets the count


def test_liveness_fails_after_failure_threshold() -> None:
    assert not liveness_failed(run([False, False]), PROBE)
    assert liveness_failed(run([False, False, False]), PROBE)


def test_timing_initial_delay_then_period() -> None:
    probe = Probe(tcp_port=1, period_seconds=2.0, initial_delay_seconds=5.0)
    assert not due(ProbeState(), probe, started_at=0.0, now=4.9)
    assert due(ProbeState(), probe, started_at=0.0, now=5.0)
    ran = observe(ProbeState(), probe, True, now=5.0)
    assert not due(ran, probe, 0.0, 6.9) and due(ran, probe, 0.0, 7.0)


def test_exactly_one_kind() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        Probe(tcp_port=1, http_get=HttpGet(port=2))
    with pytest.raises(ValueError, match="exactly one"):
        Probe()


# ---- the agent acting on probes -------------------------------------------------------


class ScriptedProber:
    def __init__(self) -> None:
        self.results: dict[str, list[bool]] = {}

    async def check(self, pod: Pod, probe: Probe) -> bool:
        kind = "readiness" if probe is pod.spec.readiness_probe else "liveness"
        queue = self.results.get(kind, [])
        return queue.pop(0) if queue else True


async def test_agent_reports_ready_only_after_the_probe_passes_and_restarts_on_liveness() -> None:
    from chronos.agent.agent import Agent  # noqa: PLC0415
    from chronos.common.clock import FakeClock  # noqa: PLC0415
    from tests.unit.test_agent_fencing import FENCING, FakeClient, FakeRuntime  # noqa: PLC0415

    clock = FakeClock(start=100.0)
    client, runtime, prober = FakeClient(clock), FakeRuntime(), ScriptedProber()
    agent = Agent(
        name="n1", capacity={}, client=client, runtime=runtime,  # type: ignore[arg-type]
        clock=clock, fencing=FENCING, prober=prober,
    )  # fmt: skip
    spec = PodSpec(
        command=["x"], node_name="n1",
        readiness_probe=Probe(tcp_port=1, period_seconds=1.0),
        liveness_probe=Probe(tcp_port=1, period_seconds=1.0, failure_threshold=2),
    )  # fmt: skip
    pod = Pod(metadata=ObjectMeta(name="p", uid="u1", resource_version=1), spec=spec)
    client.pods = [pod]
    await agent.register()
    await agent.pods.relist()
    runtime.procs = {"u1": LocalProc("u1", "p", 100, started_at=100.0)}

    assert agent._readiness([pod], runtime.procs) == {"u1": False}
    prober.results = {"readiness": [True], "liveness": [False]}
    await agent.probe_once()
    assert agent._readiness([pod], runtime.procs) == {"u1": True}
    assert runtime.stopped == []  # one liveness failure: not yet
    clock.advance(1.0)
    prober.results = {"liveness": [False]}
    await agent.probe_once()
    assert runtime.stopped == [("u1", False)]  # two in a row: restarted
