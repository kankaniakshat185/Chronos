"""Property tests for quantities and the generic resource map (ADR-0014, ADR-0023 X-1/X-2)."""

from hypothesis import given
from hypothesis import strategies as st

from chronos.api.quantity import (
    QuantityError,
    format_cpu,
    format_memory,
    format_quantity,
    parse_cpu,
    parse_memory,
    parse_quantity,
)
from chronos.api.resources import add, fits, insufficient, sub

_ALNUM = "abcdefghijklmnopqrstuvwxyz0123456789"


@st.composite
def _resource_name(draw: st.DrawFn) -> str:
    # Built from parts rather than from_regex: deterministic to replay and much faster.
    prefix = ""
    if draw(st.booleans()):
        prefix = draw(st.sampled_from(_ALNUM)) + draw(st.text(_ALNUM + ".-", max_size=10)) + "/"
    return prefix + draw(st.sampled_from(_ALNUM)) + draw(st.text(_ALNUM + "-", max_size=10))


extended_names = _resource_name().filter(lambda n: n not in ("cpu", "memory"))
resource_names = st.one_of(st.sampled_from(["cpu", "memory"]), extended_names)
resource_maps = st.dictionaries(resource_names, st.integers(0, 10**6), max_size=5)


@given(st.integers(0, 10**12))
def test_cpu_round_trip(millicores: int) -> None:
    assert parse_cpu(format_cpu(millicores)) == millicores


@given(st.integers(0, 2**63))
def test_memory_round_trip(nbytes: int) -> None:
    assert parse_memory(format_memory(nbytes)) == nbytes


@given(resource_names, st.integers(0, 10**9))
def test_any_resource_round_trips(name: str, amount: int) -> None:
    assert parse_quantity(name, format_quantity(name, amount)) == amount


@given(extended_names, st.integers(0, 10**6))
def test_extended_accepts_whole_units_as_int_or_str(name: str, units: int) -> None:
    assert parse_quantity(name, units) == units
    assert parse_quantity(name, str(units)) == units


@given(extended_names, st.integers(0, 10**6), st.integers(1, 999))
def test_extended_rejects_fractions_and_milli_suffix(name: str, whole: int, frac: int) -> None:
    for raw in (f"{whole}.{frac:03d}", f"{whole}m" if whole else "1m"):
        try:
            parse_quantity(name, raw)
        except QuantityError:
            continue
        raise AssertionError(f"{name}={raw!r} should have been rejected")


@given(resource_maps, resource_maps)
def test_fits_matches_componentwise_comparison(request: dict[str, int], free: dict[str, int]):
    expected = all(n <= free.get(name, 0) for name, n in request.items())
    assert fits(request, free) == expected
    assert (insufficient(request, free) == []) == expected


@given(resource_maps)
def test_missing_resource_means_zero_capacity(request: dict[str, int]) -> None:
    # A node that advertises nothing fits only requests of zero.
    assert fits(request, {}) == all(n == 0 for n in request.values())


@given(resource_maps, resource_maps)
def test_add_then_sub_is_identity(a: dict[str, int], b: dict[str, int]) -> None:
    result = sub(add(a, b), b)
    assert {k: v for k, v in result.items() if v} == {k: v for k, v in a.items() if v}
    assert add(a, b) == add(b, a)
