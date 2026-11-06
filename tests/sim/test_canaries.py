"""Canary bugs (P5.6, TESTING.md 6.5): the simulator must catch each one.

Each canary switches off one protection. The test scans seeds from 0 and passes
at the first run that breaks one of the canary's expected invariants; it fails if
the whole budget passes clean, which would mean the simulator's fault model is too
weak. `SIM_CANARY_BUDGET` can lower the budget for quick local runs.
"""

import os

import pytest

from chronos.sim.canaries import CANARIES
from chronos.sim.run import run_seed

pytestmark = pytest.mark.sim
CAP = int(os.environ.get("SIM_CANARY_BUDGET", "0")) or None


@pytest.mark.timeout(0)
@pytest.mark.parametrize("name", sorted(CANARIES))
def test_canary_is_detected(name: str) -> None:
    canary = CANARIES[name]
    budget = min(canary.budget, CAP) if CAP else canary.budget
    seen: dict[str, int] = {}
    for seed in range(budget):
        result = run_seed(seed, name)
        if result.violation is None:
            continue
        if result.violation.invariant in canary.expect:
            return
        seen[result.violation.invariant] = seen.get(result.violation.invariant, 0) + 1
    pytest.fail(
        f"canary {name} ({canary.what}) went undetected in {budget} seeds; "
        f"expected one of {sorted(canary.expect)}, saw only {seen or 'clean runs'}"
    )
