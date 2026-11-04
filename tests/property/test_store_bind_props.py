"""Bind never overcommits any resource on any node (S2, S11 at the store layer; X-1/X-2).

Random clusters over random resource-name sets, random pods, and binds attempted
in a random order onto random nodes. A bind must succeed exactly when the pod's
requests fit the node's free capacity, with a missing name meaning 0.
"""

from hypothesis import given
from hypothesis import strategies as st

from chronos.api import Node, NodeCondition, NodeSpec, ObjectMeta, Pod, PodSpec
from chronos.api.resources import fits, sub
from chronos.common.clock import FakeClock
from chronos.store import InsufficientCapacity, Store
from tests.strategies import resource_names


@st.composite
def scenarios(
    draw: st.DrawFn,
) -> tuple[list[dict[str, int]], list[dict[str, int]], list[tuple[int, int]]]:
    names = draw(st.lists(resource_names, min_size=1, max_size=4, unique=True))
    amounts = st.integers(0, 4)
    # Each node advertises a random subset of the names, so "missing = 0" is exercised.
    caps = draw(
        st.lists(
            st.dictionaries(st.sampled_from(names), amounts, max_size=4), min_size=1, max_size=4
        )
    )
    pods = draw(
        st.lists(
            st.dictionaries(st.sampled_from(names), amounts, max_size=4), min_size=1, max_size=12
        )
    )
    attempts = draw(
        st.lists(
            st.tuples(st.integers(0, len(pods) - 1), st.integers(0, len(caps) - 1)), max_size=30
        )
    )
    return caps, pods, attempts


@given(scenarios())
def test_bind_succeeds_exactly_when_requests_fit(
    scenario: tuple[list[dict[str, int]], list[dict[str, int]], list[tuple[int, int]]],
) -> None:
    raw_caps, raw_requests, attempts = scenario
    store = Store(FakeClock())
    caps, requests = [], []  # parsed: cpu in millicores, memory in bytes
    for i, cap in enumerate(raw_caps):
        node = store.create(Node(metadata=ObjectMeta(name=f"n{i}"), spec=NodeSpec(capacity=cap)))
        node.status.condition = NodeCondition.READY
        store.update_status(node)
        caps.append(node.spec.capacity)
    for i, req in enumerate(raw_requests):
        spec = PodSpec(command=["true"], resources={"requests": req})
        requests.append(store.create(Pod(metadata=ObjectMeta(name=f"p{i}"), spec=spec)).requests)

    for p, n in attempts:
        pod = store.get(Pod, f"p{p}")
        if pod.spec.node_name:
            continue  # bind-once is covered by the model test
        free = sub(caps[n], store.allocated(f"n{n}"))
        try:
            store.bind(pod.metadata.name, f"n{n}", pod.metadata.resource_version)
            bound = True
        except InsufficientCapacity:
            bound = False
        assert bound == fits(requests[p], free)

        for i, cap in enumerate(caps):  # S2 / S11 on every node, for every name
            used = store.allocated(f"n{i}")
            assert all(amount <= cap.get(name, 0) for name, amount in used.items())
