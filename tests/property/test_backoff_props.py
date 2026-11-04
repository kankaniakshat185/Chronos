"""Backoff properties (P0.5): bounded, monotone up to the cap, jitter within bounds."""

import random

import pytest
from hypothesis import given
from hypothesis import strategies as st

from chronos.common.backoff import exponential, full_jitter

bases = st.floats(0.001, 10.0)
caps = st.floats(0.001, 1_000.0)
attempts = st.integers(0, 10_000)


@given(attempts, bases, caps)
def test_never_exceeds_the_cap(attempt: int, base: float, cap: float) -> None:
    assert 0 < exponential(attempt, base, cap) <= cap


@given(attempts, bases, caps)
def test_monotone_in_attempts(attempt: int, base: float, cap: float) -> None:
    assert exponential(attempt, base, cap) <= exponential(attempt + 1, base, cap)


@given(st.integers(0, 20), bases)
def test_doubles_below_the_cap(attempt: int, base: float) -> None:
    assert exponential(attempt + 1, base, float("inf")) == 2 * exponential(
        attempt, base, float("inf")
    )


@given(attempts, bases, caps, st.integers())
def test_jitter_stays_within_zero_and_the_exponential(
    attempt: int, base: float, cap: float, seed: int
) -> None:
    d = full_jitter(attempt, base, cap, random.Random(seed))
    assert 0 <= d <= exponential(attempt, base, cap)


def test_seeded_jitter_is_reproducible() -> None:
    a = [full_jitter(i, 0.1, 5, random.Random(7)) for i in range(5)]
    assert a == [full_jitter(i, 0.1, 5, random.Random(7)) for i in range(5)]


def test_negative_attempt_is_an_error() -> None:
    with pytest.raises(ValueError, match="attempt"):
        exponential(-1, 1, 1)
