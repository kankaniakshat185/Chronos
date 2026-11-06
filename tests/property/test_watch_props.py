"""S6: a watch from revision r delivers every change after r, in order, or fails (ADR-0008).

For random histories (creates, updates, binds, deletes, failed writes) and a small
history buffer, so compaction happens: from **every** revision r, replaying the
watch onto the state at r gives the state now, or the watch raises Compacted. It
never answers with a partial history. Checked for the whole kind, for the node
filter (which must see a pod leave as well as arrive), and in small batches.
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from chronos.api import Node, NodeCondition, NodeSpec, ObjectMeta, Pod, PodPhase, PodSpec
from chronos.common.clock import FakeClock
from chronos.store import Compacted, Store, StoreError

type State = dict[str, dict[str, object]]

ops = st.lists(
    st.tuples(
        st.sampled_from(["create", "label", "bind", "finish", "delete", "force"]),
        st.integers(0, 4),
        st.sampled_from(["n1", "n2"]),
    ),
    max_size=40,
)


def history(steps: list[tuple[str, int, str]], keep: int) -> tuple[Store, list[State]]:
    """Run the steps; return the store and the pod state after every revision."""
    store = Store(FakeClock(), watch_history=keep)
    for name in ("n1", "n2"):
        node = store.create(Node(metadata=ObjectMeta(name=name), spec=NodeSpec()))
        node.status.condition = NodeCondition.READY
        store.update_status(node)
    states: list[State] = [{} for _ in range(store.revision + 1)]
    for op, i, node_name in steps:
        name = f"p{i}"
        try:
            match op:
                case "create":
                    store.create(Pod(metadata=ObjectMeta(name=name), spec=PodSpec(command=["x"])))
                case "label":
                    pod = store.get(Pod, name)
                    pod.metadata.labels = {"n": str(pod.metadata.resource_version)}
                    store.update(pod)
                case "bind":
                    pod = store.get(Pod, name)
                    store.bind(name, node_name, pod.metadata.resource_version)
                case "finish":
                    pod = store.get(Pod, name)
                    pod.status.phase = PodPhase.SUCCEEDED if pod.spec.node_name else PodPhase.FAILED
                    store.update_status(pod)
                case "delete" | "force":
                    store.delete(Pod, name, force=op == "force")
        except StoreError:
            pass  # a refused write changes nothing, revision included
        while len(states) <= store.revision:
            states.append({p.metadata.name: p.model_dump() for p in store.list(Pod)[0]})
    return store, states


def replay(store: Store, start: State, since: int, node: str | None, limit: int) -> State:
    state = dict(start)
    while True:
        events, seen = store.watch_since(Pod, since, node_name=node, limit=limit)
        for kind, pod in events:
            gone = kind == "DELETED" or (node is not None and pod.spec.node_name != node)
            if gone:
                state.pop(pod.metadata.name, None)
            else:
                state[pod.metadata.name] = pod.model_dump()
        assert seen >= since
        if seen == since or not events:
            return state
        since = seen


def on(state: State, node: str | None) -> State:
    if node is None:
        return state
    return {n: p for n, p in state.items() if p["spec"]["node_name"] == node}  # type: ignore[index]


@settings(max_examples=300)
@given(ops, st.integers(1, 12), st.sampled_from([None, "n1", "n2", ""]), st.integers(1, 3))
def test_replay_from_any_retained_revision_reaches_now(
    steps: list[tuple[str, int, str]], keep: int, node: str | None, limit: int
) -> None:
    store, states = history(steps, keep)
    now = on(states[store.revision], node)
    floor = store.watch.floor
    for r in range(store.revision + 1):
        if r < floor:
            try:
                store.watch_since(Pod, r, node_name=node)
            except Compacted:
                continue
            raise AssertionError(f"revision {r} < floor {floor} must be refused, not answered")
        assert replay(store, on(states[r], node), r, node, limit) == now, f"from {r}"


@given(ops)
def test_events_are_in_revision_order_with_no_gaps(steps: list[tuple[str, int, str]]) -> None:
    store, _ = history(steps, 10_000)
    revisions = [e.revision for e in store.watch.since(0, lambda _: True)]
    assert revisions == list(range(1, store.revision + 1))
