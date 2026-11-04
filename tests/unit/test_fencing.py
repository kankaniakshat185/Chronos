"""Fencing decisions (ADR-0012): boundary tables and the timing inequality."""

import math

import pytest
from pydantic import ValidationError

from chronos.common.config import FencingConfig
from chronos.core.fencing import ShimAction, inequality_violation, shim_action, should_fence

F, E, EPS = 6.0, 10.0, 1e-9


@pytest.mark.parametrize(
    ("since_send", "fence"),
    [(0.0, False), (F - EPS, False), (F, False), (F + EPS, True), (math.inf, True)],
)
def test_should_fence_boundaries(since_send: float, fence: bool) -> None:
    assert should_fence(last_ok_send=100.0, now=100.0 + since_send, fence_timeout=F) == fence


def test_never_heard_back_means_fenced() -> None:
    assert should_fence(-math.inf, 0.0, F)


@pytest.mark.parametrize(
    ("age", "action"),
    [
        (0.0, ShimAction.RUN),
        (F, ShimAction.RUN),
        (F + EPS, ShimAction.FENCE),
        (E, ShimAction.FENCE),
        (E + EPS, ShimAction.KILL),
        (math.inf, ShimAction.KILL),  # missing or unreadable lease: fail safe
    ],
)
def test_shim_action_boundaries(age: float, action: ShimAction) -> None:
    assert shim_action(age, F, E) == action


def test_default_timings_satisfy_the_inequality() -> None:
    f = FencingConfig()
    assert inequality_violation(f.fence_timeout, f.termination_grace, f.drift_margin, 10.0) is None


@pytest.mark.parametrize(("f", "k", "d", "e"), [(6, 2, 2, 10), (6, 2, 1, 9), (8, 3, 0, 10)])
def test_inequality_rejects_unsafe_timings(f: float, k: float, d: float, e: float) -> None:
    problem = inequality_violation(f, k, d, e)
    assert problem is not None and "must be <" in problem


@pytest.mark.parametrize(
    "bad",
    [
        {"heartbeat_interval": 2.0, "fence_timeout": 6.0},  # F must be > 3 heartbeats
        {"shim_poll": 2.0, "termination_grace": 2.0},
    ],
)
def test_agent_fencing_config_validation(bad: dict[str, float]) -> None:
    with pytest.raises(ValidationError):
        FencingConfig(**bad)
