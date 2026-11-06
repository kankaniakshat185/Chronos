"""Deterministic simulation (ADR-0016, TESTING.md section 6): seeds, determinism, regressions.

`SIM_SEEDS` sets how many seeds run (default 20 locally; CI runs 200 per PR and
10 000 nightly). A failure prints the seed and the last 200 events; replay it with
`python -m chronos.sim --seed N --trace trace.txt`, shrink it with `--minimize`.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from chronos.sim.run import minimize, run_seed

SEEDS = int(os.environ.get("SIM_SEEDS", "20"))
FIRST = int(os.environ.get("SIM_FIRST_SEED", "0"))
REGRESSIONS = Path(__file__).with_name("regressions.txt")

pytestmark = pytest.mark.sim


@pytest.mark.parametrize("seed", range(FIRST, FIRST + SEEDS))
def test_seed(seed: int) -> None:
    result = run_seed(seed)
    assert result.ok, result.report()


def regressions() -> list[tuple[int, str | None]]:
    cases = []
    for line in REGRESSIONS.read_text().splitlines():
        words = line.split("#", 1)[0].split()
        if words:
            cases.append((int(words[0]), words[1] if len(words) > 1 else None))
    return cases


@pytest.mark.parametrize(("seed", "canary"), regressions())
def test_regression(seed: int, canary: str | None) -> None:
    result = run_seed(seed, canary)
    assert result.ok, result.report()


def test_same_seed_same_trace() -> None:
    a, b = run_seed(3), run_seed(3)
    assert a.events == b.events and a.trace_hash == b.trace_hash


def test_same_trace_in_fresh_processes_with_different_hash_seeds() -> None:
    """Nothing may depend on set or dict order of strings, which PYTHONHASHSEED changes."""
    code = "from chronos.sim.run import run_seed; print(run_seed(3).trace_hash)"
    hashes = {
        subprocess.run(
            [sys.executable, "-c", code],
            env={**os.environ, "PYTHONHASHSEED": str(h)},
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        for h in (1, 2)
    }
    assert hashes == {run_seed(3).trace_hash}


def test_different_seeds_explore_different_runs() -> None:
    assert run_seed(1).trace_hash != run_seed(2).trace_hash


@pytest.mark.timeout(300)
def test_minimize_shrinks_a_failure_to_what_matters() -> None:
    """Seed 1 with unit reuse: two pods sharing a unit needs only one deployment."""
    smallest = minimize(1, "provider_reuse_unit", "S12")
    assert [e.kind for e in smallest.entries] == ["deploy"]
    result = run_seed(1, "provider_reuse_unit", scenario=smallest)
    assert result.violation is not None and result.violation.invariant == "S12"
