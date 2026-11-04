import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from chronos.common.clock import FakeClock, RealClock


async def _flush() -> None:
    # Let woken sleepers run. A zero-delay yield, not a timed wait.
    for _ in range(5):
        await asyncio.sleep(0)  # noqa: TID251


async def test_sleepers_wake_only_when_their_deadline_passes() -> None:
    clock = FakeClock()
    woke: list[str] = []

    async def sleeper(name: str, seconds: float) -> None:
        await clock.sleep(seconds)
        woke.append(name)

    tasks = [asyncio.create_task(sleeper("late", 2)), asyncio.create_task(sleeper("early", 1))]
    await _flush()
    assert clock.pending_sleepers == 2

    clock.advance(0.999)
    await _flush()
    assert woke == []

    clock.advance(0.001)
    await _flush()
    assert woke == ["early"]

    clock.advance(5)
    await _flush()
    assert woke == ["early", "late"]
    await asyncio.gather(*tasks)


async def test_non_positive_sleep_returns_immediately() -> None:
    clock = FakeClock()
    await clock.sleep(0)
    await clock.sleep(-1)
    assert clock.pending_sleepers == 0


def test_time_never_goes_backwards() -> None:
    clock = FakeClock(start=10.0)
    with pytest.raises(ValueError, match="backwards"):
        clock.advance(-0.1)
    assert clock.monotonic() == 10.0


def test_wall_time_follows_monotonic_time() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    clock = FakeClock(wall_start=start)
    clock.advance(90)
    assert clock.wall() == start + timedelta(seconds=90)


async def test_real_clock_is_monotonic() -> None:
    clock = RealClock()
    first = clock.monotonic()
    await clock.sleep(0)
    assert clock.monotonic() >= first
    assert clock.wall().tzinfo is not None
