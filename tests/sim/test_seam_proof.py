"""Proof of modularity (PX.9 final), simulator side.

The simulator's schedules, with every extended resource renamed to the test-only
`example.com/widget`, every deployment of the test-only `test.widget-task` type, the
test-only widget provider handing out the units, and the test-only score plugin in
the scheduler (all from `tests/extensions.py`). The real store, controllers and agent
run them under faults, and every invariant is checked as on any seed: S2 and S11 per
resource name, S12 against the units the provider really handed out. Nothing in
`src/` knows about any of it.

The simulator doesn't run commands, so a workload type is exercised here through
validation at admission; `prepare` and the provider's `env` are proven on a real
process in `tests/integration/test_seams.py`.
"""

from __future__ import annotations

import contextlib
import dataclasses
from collections.abc import Iterator
from typing import Any

import pytest

from chronos.api.quantity import is_extended
from chronos.core.schedule import SchedulerConfig, resolve
from chronos.sim import canaries
from chronos.sim import run as sim_run
from chronos.sim.faults import Entry, Scenario, generate
from chronos.sim.run import run_seed
from tests.extensions import FEWEST_WIDGETS_LEFT, WIDGET, WIDGET_TASK, WidgetProvider

pytestmark = pytest.mark.sim
SEEDS = 20


def widgets(seed: int) -> Scenario:
    """Seed `seed`'s schedule, with widgets instead of its extended resources: every node
    but the first has 1-4, and every deployment needs one."""
    scenario = generate(seed)
    nodes = [
        dataclasses.replace(
            n,
            capacity={
                **{r: v for r, v in n.capacity.items() if not is_extended(r)},
                **({WIDGET: 1 + (i + seed) % 4} if i > 0 else {}),
            },
        )
        for i, n in enumerate(scenario.nodes)
    ]
    return Scenario(seed, nodes, [WIDGET], [_needs_a_widget(e) for e in scenario.entries])


def _needs_a_widget(e: Entry) -> Entry:
    if e.kind != "deploy":
        return e
    requests: dict[str, Any] = dict(e.arg["requests"])  # type: ignore[call-overload]
    requests = {r: v for r, v in requests.items() if not is_extended(r)}
    return dataclasses.replace(e, arg={**e.arg, "requests": {**requests, WIDGET: 1}})


@pytest.fixture
def plugged(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, list[str]]]:
    """Deployments of the test-only type, and the test-only plugin in the scheduler.
    Returns the provider's allocations, as they happen."""
    real_deployment = sim_run._deployment

    def deployment(e: Entry) -> Any:
        dep = real_deployment(e)
        data = dep.model_dump(mode="json")
        data["spec"]["template"]["workload_type"] = WIDGET_TASK  # validated by WidgetTask
        return type(dep).model_validate(data)

    real_active = canaries.active

    @contextlib.contextmanager
    def active(name: str | None) -> Iterator[canaries.Hooks]:
        with real_active(name) as hooks:
            hooks.profile = resolve(
                SchedulerConfig(scores={"LeastAllocated": 1, FEWEST_WIDGETS_LEFT: 10})
            )
            yield hooks

    allocations: list[tuple[str, list[str]]] = []
    real_allocate = WidgetProvider.allocate

    def allocate(self: WidgetProvider, uid: str, n: int) -> list[str]:
        units = real_allocate(self, uid, n)
        allocations.append((uid, units))
        return units

    monkeypatch.setattr(sim_run, "_deployment", deployment)
    monkeypatch.setattr(canaries, "active", active)
    monkeypatch.setattr(WidgetProvider, "allocate", allocate)
    return allocations


@pytest.mark.timeout(0)
def test_test_only_extensions_hold_every_invariant_in_the_simulator(
    plugged: list[tuple[str, list[str]]],
) -> None:
    for seed in range(SEEDS):
        before = len(plugged)
        result = run_seed(seed, scenario=widgets(seed))
        assert result.ok, result.report()
        assert len(plugged) > before, f"seed {seed}: no widget pod ever started"


@pytest.mark.timeout(0)
def test_the_simulator_sees_a_broken_test_only_provider(
    plugged: list[tuple[str, list[str]]], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The negative control: a widget provider that hands out w0 every time must be
    caught as S12, so the passing runs above mean something."""
    monkeypatch.setattr(WidgetProvider, "allocate", lambda self, uid, n: ["w0"] * n)
    for seed in range(SEEDS):
        result = run_seed(seed, scenario=widgets(seed))
        if result.violation is not None and result.violation.invariant == "S12":
            return
    pytest.fail(f"a provider reusing w0 went unnoticed in {SEEDS} seeds")
