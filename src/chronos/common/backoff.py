"""Backoff delays (P0.5). Pure functions; randomness is injected, so tests are deterministic."""

from __future__ import annotations

import random


def exponential(attempt: int, base: float, cap: float) -> float:
    """base, 2·base, 4·base, ... capped: attempt 0 waits `base`."""
    if attempt < 0:
        raise ValueError("attempt must be >= 0")
    return min(cap, base * float(2 ** min(attempt, 62)))


def full_jitter(attempt: int, base: float, cap: float, rng: random.Random) -> float:
    """Uniform in [0, exponential(attempt)]: spreads out retries that started together."""
    return rng.uniform(0.0, exponential(attempt, base, cap))
