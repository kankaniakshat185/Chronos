"""chronos-shim as a real process (ADR-0012): lease fresh, stale, very stale, missing."""

import asyncio
import os
import signal
import sys
from pathlib import Path

import pytest

from chronos.common.clock import RealClock
from chronos.core.fencing import FENCED_EXIT_CODE
from chronos.testing.cluster import alive

pytestmark = pytest.mark.integration
clock = RealClock()
WORKLOAD = [sys.executable, "-m", "chronos.testing.workload"]


async def shim(
    lease: Path, *command: str, fence: float = 30.0, evict: float = 60.0, grace: float = 0.5
) -> tuple[asyncio.subprocess.Process, int]:
    """Start a shim in its own session (it signals its whole group); return it and the
    workload's pid (the workload prints it first)."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "chronos.shim", "--lease", str(lease), "--pod-uid", "u1",
        "--fence-timeout", str(fence), "--eviction-timeout", str(evict),
        "--grace", str(grace), "--poll", "0.05", "--", *command,
        stdout=asyncio.subprocess.PIPE, start_new_session=True,
    )  # fmt: skip
    assert proc.stdout is not None
    line = (await asyncio.wait_for(proc.stdout.readline(), 10)).decode()
    pid = int(line.rsplit("pid=", 1)[1]) if "pid=" in line else 0
    return proc, pid


def lease_at(path: Path, age: float) -> Path:
    path.write_text(repr(clock.monotonic() - age))
    return path


async def wait(proc: asyncio.subprocess.Process, within: float) -> int:
    return await asyncio.wait_for(proc.wait(), within)


async def test_fresh_lease_runs_and_passes_the_exit_code_through(tmp_path: Path) -> None:
    proc, _ = await shim(lease_at(tmp_path / "lease", 0), *WORKLOAD, "--crash-after", "0.3",
                         "--exit-code", "3")  # fmt: skip
    assert await wait(proc, 5) == 3


async def test_death_by_signal_is_passed_through(tmp_path: Path) -> None:
    proc, pid = await shim(lease_at(tmp_path / "lease", 0), *WORKLOAD)
    os.kill(pid, signal.SIGKILL)
    assert await wait(proc, 5) == -signal.SIGKILL


async def test_stale_lease_fences_gracefully(tmp_path: Path) -> None:
    written = clock.monotonic()
    lease = lease_at(tmp_path / "lease", 0)
    proc, pid = await shim(lease, *WORKLOAD, fence=0.4)
    assert await wait(proc, 5) == FENCED_EXIT_CODE
    assert 0.4 <= clock.monotonic() - written <= 2.0
    assert not alive(pid)


async def test_sigterm_ignoring_workload_is_killed_after_the_grace(tmp_path: Path) -> None:
    written = clock.monotonic()  # the fence timeout counts from the lease, not the start
    lease = lease_at(tmp_path / "lease", 0)
    proc, pid = await shim(lease, *WORKLOAD, "--ignore-sigterm", fence=0.3, grace=0.6)
    assert await wait(proc, 5) == -signal.SIGKILL  # the group SIGKILL takes the shim too
    assert clock.monotonic() - written >= 0.3 + 0.6
    assert not alive(pid)


async def test_lease_past_eviction_kills_at_once(tmp_path: Path) -> None:
    """A frozen node resuming: no grace, because replacements may already be running."""
    lease = lease_at(tmp_path / "lease", 0)
    proc, pid = await shim(lease, *WORKLOAD, "--ignore-sigterm", fence=5, evict=10)
    lease_at(lease, 100)  # as if the shim had been frozen for 100 s
    started = clock.monotonic()
    assert await wait(proc, 5) == -signal.SIGKILL
    assert clock.monotonic() - started < 0.5  # one 50 ms poll, plus process overhead
    assert not alive(pid)


async def test_missing_lease_is_treated_as_stale(tmp_path: Path) -> None:
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "chronos.shim", "--lease", str(tmp_path / "none"),
        "--pod-uid", "u1", "--fence-timeout", "5", "--eviction-timeout", "10",
        "--grace", "0.5", "--poll", "0.05", "--", *WORKLOAD, start_new_session=True,
        stdout=asyncio.subprocess.DEVNULL,
    )  # fmt: skip
    assert await wait(proc, 5) == -signal.SIGKILL


async def test_shim_waits_for_its_workload_on_sigterm(tmp_path: Path) -> None:
    """The agent's own stop: SIGTERM to the group; the shim reports the workload's exit."""
    proc, pid = await shim(lease_at(tmp_path / "lease", 0), *WORKLOAD)
    os.killpg(proc.pid, signal.SIGTERM)
    assert await wait(proc, 5) == -signal.SIGTERM
    assert not alive(pid)


async def test_unstartable_command_exits_127(tmp_path: Path) -> None:
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "chronos.shim", "--lease", str(lease_at(tmp_path / "l", 0)),
        "--pod-uid", "u1", "--fence-timeout", "5", "--eviction-timeout", "10",
        "--grace", "0.5", "--", "/no/such/binary", start_new_session=True,
        stderr=asyncio.subprocess.DEVNULL,
    )  # fmt: skip
    assert await wait(proc, 5) == 127
