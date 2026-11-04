"""Stateful model test: the store against a dict + counter model (S1, S5; ADR-0007).

Hypothesis drives random create / update / update_status / bind / delete calls,
some with a stale resource_version, and checks after every step that the store
agrees with a trivially correct model.
"""

from dataclasses import dataclass, field

import pytest
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from chronos.api import Node, NodeCondition, NodeSpec, ObjectMeta, Pod, PodSpec
from chronos.common.clock import FakeClock
from chronos.store import AlreadyExists, Conflict, Invalid, NotFound, Store, StoreError

NAMES = st.sampled_from(["a", "b", "c", "d"])
NODES = st.sampled_from(["n1", "n2"])
# 0 = the current resource_version; anything else is stale or from the future.
RV_OFFSET = st.sampled_from([0, 0, 0, -1, 1, -7])


@dataclass
class ModelPod:
    uid: str
    rv: int
    generation: int = 1
    env: dict[str, str] = field(default_factory=dict)
    message: str = ""
    node_name: str = ""
    terminating: bool = False


class StoreModel(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.store = Store(FakeClock())
        self.revision = 0
        self.pods: dict[str, ModelPod] = {}
        self.bound_uids: dict[str, str] = {}  # uid -> node: S1 history across the run

    @initialize()
    def two_ready_nodes(self) -> None:
        for name in ("n1", "n2"):
            n = self.store.create(
                Node(metadata=ObjectMeta(name=name), spec=NodeSpec(capacity={"cpu": 10**6}))
            )
            n.status.condition = NodeCondition.READY
            self.store.update_status(n)
            self.revision += 2

    def _expect_write(self, name: str, offset: int) -> type[StoreError] | None:
        if name not in self.pods:
            return NotFound
        return Conflict if offset else None

    def _call(self, expected: type[StoreError] | None, fn: object) -> Pod | None:
        before = self.store.revision
        assert callable(fn)
        if expected is not None:
            with pytest.raises(expected):
                fn()
            assert self.store.revision == before, "a failed write changed the revision"
            return None
        result = fn()
        assert self.store.revision == before + 1, "a write must bump the revision by one"
        self.revision += 1
        assert isinstance(result, Pod)
        assert result.metadata.resource_version == self.revision
        return result

    @rule(name=NAMES)
    def create(self, name: str) -> None:
        pod = Pod(metadata=ObjectMeta(name=name), spec=PodSpec(command=["true"]))
        expected = AlreadyExists if name in self.pods else None
        if (created := self._call(expected, lambda: self.store.create(pod))) is not None:
            self.pods[name] = ModelPod(uid=created.metadata.uid, rv=self.revision)

    @rule(name=NAMES, offset=RV_OFFSET, value=st.sampled_from(["x", "y"]))
    def update_spec(self, name: str, offset: int, value: str) -> None:
        expected = self._expect_write(name, offset)
        if expected is None and self.pods[name].terminating:
            expected = Invalid  # a terminating pod's spec is frozen
        pod = Pod(metadata=ObjectMeta(name=name), spec=PodSpec(command=["true"]))
        if name in self.pods:
            pod = self.store.get(Pod, name)
            pod.metadata.resource_version += offset
            pod.spec.env = {"k": value}
        if self._call(expected, lambda: self.store.update(pod)) is not None:
            m = self.pods[name]
            if m.env != {"k": value}:
                m.generation += 1
            m.env, m.rv = {"k": value}, self.revision

    @rule(name=NAMES, offset=RV_OFFSET, message=st.sampled_from(["hello", "world"]))
    def update_status(self, name: str, offset: int, message: str) -> None:
        expected = self._expect_write(name, offset)
        pod = Pod(metadata=ObjectMeta(name=name), spec=PodSpec(command=["true"]))
        if name in self.pods:
            pod = self.store.get(Pod, name)
            pod.metadata.resource_version += offset
            pod.status.message = message
        if self._call(expected, lambda: self.store.update_status(pod)) is not None:
            self.pods[name].message, self.pods[name].rv = message, self.revision

    @rule(name=NAMES, node=NODES, offset=RV_OFFSET)
    def bind(self, name: str, node: str, offset: int) -> None:
        expected = self._expect_write(name, offset)
        if expected is None and self.pods[name].node_name:
            expected = Invalid  # bind-once (S1)
        rv = self.pods[name].rv + offset if name in self.pods else 1
        if self._call(expected, lambda: self.store.bind(name, node, rv)) is not None:
            m = self.pods[name]
            m.node_name, m.rv = node, self.revision
            m.message = ""  # bind clears a stale reason/message (e.g. Unschedulable)
            self.bound_uids[m.uid] = node

    @rule(name=NAMES, offset=st.one_of(st.none(), RV_OFFSET), force=st.booleans())
    def delete(self, name: str, offset: int | None, force: bool) -> None:
        expected = self._expect_write(name, offset or 0)
        rv = None if offset is None or name not in self.pods else self.pods[name].rv + offset
        before = self.store.revision
        if expected is not None:
            with pytest.raises(expected):
                self.store.delete(Pod, name, expected_rv=rv, force=force)
            assert self.store.revision == before
            return
        result = self.store.delete(Pod, name, expected_rv=rv, force=force)
        m = self.pods[name]
        if m.node_name and not force:  # two-phase: only marked (P4.7)
            if m.terminating:
                assert self.store.revision == before  # idempotent
            else:
                assert self.store.revision == before + 1
                self.revision += 1
                m.terminating, m.rv = True, self.revision
            assert result.metadata.deletion_timestamp is not None
            return
        assert self.store.revision == before + 1
        self.revision += 1
        del self.pods[name]

    @invariant()
    def store_matches_model(self) -> None:
        assert self.store.revision == self.revision
        items, rev = self.store.list(Pod)
        assert rev == self.revision
        assert [p.metadata.name for p in items] == sorted(self.pods)
        for pod in items:
            m = self.pods[pod.metadata.name]
            assert pod.metadata.uid == m.uid
            assert pod.metadata.resource_version == m.rv <= self.revision  # S5
            assert pod.metadata.generation == m.generation
            assert pod.spec.env == m.env
            assert pod.status.message == m.message
            assert pod.spec.node_name == m.node_name
            assert (pod.metadata.deletion_timestamp is not None) == m.terminating

    @invariant()
    def bound_pods_never_move(self) -> None:  # S1
        for pod in self.store.list(Pod)[0]:
            if pod.metadata.uid in self.bound_uids:
                assert pod.spec.node_name == self.bound_uids[pod.metadata.uid]

    @invariant()
    def index_matches_objects(self) -> None:
        for node in ("", "n1", "n2"):
            expected = sorted(n for n, m in self.pods.items() if m.node_name == node)
            assert [p.metadata.name for p in self.store.pods_on(node)] == expected


TestStoreModel = StoreModel.TestCase
