"""Node condition boundaries (ADR-0011) and timing validation (PLAN.md section 4.5)."""

import pytest
from pydantic import ValidationError

from chronos.api import NodeCondition
from chronos.common.config import Timings
from chronos.core.node_health import condition

GRACE, EVICTION, EPS = 2.0, 5.0, 1e-9


@pytest.mark.parametrize(
    ("silence", "expected"),
    [
        (0.0, NodeCondition.READY),
        (GRACE - EPS, NodeCondition.READY),
        (GRACE, NodeCondition.UNKNOWN),
        (GRACE + EPS, NodeCondition.UNKNOWN),
        (EVICTION - EPS, NodeCondition.UNKNOWN),
        (EVICTION, NodeCondition.NOT_READY),
        (EVICTION + EPS, NodeCondition.NOT_READY),
        (1e9, NodeCondition.NOT_READY),
    ],
)
def test_condition_boundaries(silence: float, expected: NodeCondition) -> None:
    assert condition(silence, GRACE, EVICTION) == expected


def test_default_timings_are_valid() -> None:
    Timings()


def test_demo_timings_are_valid() -> None:
    Timings(heartbeat_interval=0.5, heartbeat_timeout=0.4, node_grace=2.0, eviction_timeout=5.0)


@pytest.mark.parametrize(
    "bad",
    [
        {"heartbeat_timeout": 1.0},  # not < interval
        {"node_grace": 2.9},  # < 3 x interval
        {"eviction_timeout": 4.0},  # not > grace
        {"heartbeat_interval": 0},
    ],
)
def test_invalid_timings_are_rejected(bad: dict[str, float]) -> None:
    with pytest.raises(ValidationError):
        Timings(**bad)
