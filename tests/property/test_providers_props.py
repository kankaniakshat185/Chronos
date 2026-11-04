"""S12: provider unit exclusivity under random allocate/release sequences (ADR-0023 X-5)."""

import contextlib

from hypothesis import given
from hypothesis import strategies as st

from chronos.agent.providers import ProviderError, Providers

PODS = [f"p{i}" for i in range(6)]
RESOURCES = ["a.dev/x", "b.dev/y"]

ops = st.lists(
    st.one_of(
        st.tuples(
            st.just("allocate"),
            st.sampled_from(PODS),
            st.dictionaries(st.sampled_from(RESOURCES), st.integers(0, 3), max_size=2),
        ),
        st.tuples(st.just("release"), st.sampled_from(PODS), st.just({})),
    ),
    max_size=40,
)


@given(st.dictionaries(st.sampled_from(RESOURCES), st.integers(0, 4)), ops)
def test_no_unit_is_ever_held_by_two_live_pods(
    capacity: dict[str, int], sequence: list[tuple[str, str, dict[str, int]]]
) -> None:
    providers = Providers(capacity)
    held: dict[str, dict[str, list[str]]] = {}  # model: pod -> resource -> units
    for op, pod, requests in sequence:
        if op == "release":
            providers.release(pod)
            held.pop(pod, None)
            continue
        if pod in held:
            continue  # a live pod doesn't allocate again before its process exits
        try:
            assigned, _ = providers.allocate(pod, requests)
        except ProviderError:
            continue
        held[pod] = assigned
        for name, units in assigned.items():
            assert len(units) == requests[name]
            assert all(0 <= int(u) < capacity[name] for u in units)
        for name in RESOURCES:  # S12
            all_units = [u for a in held.values() for u in a.get(name, [])]
            assert len(all_units) == len(set(all_units)), f"{name} unit given twice"


@given(st.integers(1, 5), st.lists(st.integers(1, 3), max_size=8))
def test_releasing_everything_restores_full_capacity(cap: int, sizes: list[int]) -> None:
    providers = Providers({"a.dev/x": cap})
    for i, n in enumerate(sizes):
        with contextlib.suppress(ProviderError):
            providers.allocate(f"p{i}", {"a.dev/x": n})
    for i in range(len(sizes)):
        providers.release(f"p{i}")
    assigned, _ = providers.allocate("all", {"a.dev/x": cap})
    assert sorted(assigned["a.dev/x"], key=int) == [str(u) for u in range(cap)]
