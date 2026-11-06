"""WAL properties (ADR-0015, S8): framing, torn tails, corruption, restart equivalence."""

import shutil
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from chronos.api import Node, NodeCondition, NodeSpec, ObjectMeta, Pod, PodSpec
from chronos.common.clock import FakeClock
from chronos.store import Store, StoreError
from chronos.store.codec import Record, decode, encode
from chronos.store.errors import Unavailable
from chronos.store.wal import Wal, WalCorrupt, recover, segments, snapshots

names = st.text("abcdefghij-", min_size=1, max_size=12)
json_values = st.recursive(
    st.none() | st.booleans() | st.integers(-(2**40), 2**40) | st.text(max_size=20),
    lambda inner: (
        st.lists(inner, max_size=3) | st.dictionaries(st.text(max_size=5), inner, max_size=3)
    ),
    max_leaves=10,
)
records = st.builds(
    Record,
    rev=st.integers(1, 2**40),
    op=st.just("put"),
    kind=st.sampled_from(["Node", "Pod", "Deployment"]),
    name=names,
    obj=st.dictionaries(st.text(max_size=8), json_values, max_size=4),
) | st.builds(
    Record, rev=st.integers(1, 2**40), op=st.just("delete"), kind=st.just("Pod"), name=names
)


@given(records)
def test_codec_round_trip(record: Record) -> None:
    frame = encode(record)
    assert decode(frame) == (record, len(frame))


def write_log(directory: Path, n: int) -> list[Record]:
    wal = Wal(directory, 1)
    written = [
        Record(i, "put", "Pod", f"p{i}", {"i": i, "pad": "x" * (i * 7 % 23)})
        for i in range(1, n + 1)
    ]
    for r in written:
        wal.append(r)
    wal.close()
    return written


def test_recovery_from_every_truncation_offset_is_the_complete_prefix(tmp_path: Path) -> None:
    original = tmp_path / "orig"
    original.mkdir()
    written = write_log(original, 6)
    [segment] = segments(original)
    data = segment.read_bytes()
    ends = [0]
    for _ in written:
        ends.append(decode(data, ends[-1])[1])
    for cut in range(len(data) + 1):
        trial = tmp_path / f"cut{cut}"
        trial.mkdir()
        (trial / segment.name).write_bytes(data[:cut])
        got = recover(trial).records
        complete = sum(1 for end in ends[1:] if end <= cut)
        assert got == written[:complete], f"cut at byte {cut}"
        assert (trial / segment.name).stat().st_size == ends[complete]  # torn bytes gone
        shutil.rmtree(trial)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=50)
@given(st.data())
def test_bit_flip_in_the_last_record_is_truncated(
    tmp_path_factory: pytest.TempPathFactory, data: st.DataObject
) -> None:
    d = tmp_path_factory.mktemp("wal")
    written = write_log(d, 4)
    [seg] = segments(d)
    raw = bytearray(seg.read_bytes())
    last = len(encode(written[-1]))
    byte = data.draw(st.integers(len(raw) - last, len(raw) - 1))
    raw[byte] ^= 1 << data.draw(st.integers(0, 7))
    seg.write_bytes(raw)
    assert recover(d).records == written[:-1]


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=100)
@given(st.data())
def test_bit_flip_before_the_last_record_refuses_to_start(
    tmp_path_factory: pytest.TempPathFactory, data: st.DataObject
) -> None:
    d = tmp_path_factory.mktemp("wal")
    written = write_log(d, 4)
    [seg] = segments(d)
    raw = bytearray(seg.read_bytes())
    last = len(encode(written[-1]))
    byte = data.draw(st.integers(0, len(raw) - last - 1))
    raw[byte] ^= 1 << data.draw(st.integers(0, 7))
    seg.write_bytes(raw)
    with pytest.raises(WalCorrupt, match="refusing to start"):
        recover(d)


def test_a_gap_in_revisions_refuses_to_start(tmp_path: Path) -> None:
    wal = Wal(tmp_path, 1)
    wal.append(Record(1, "delete", "Pod", "a"))
    wal.append(Record(3, "delete", "Pod", "b"))
    wal.close()
    with pytest.raises(WalCorrupt, match="expected revision 2"):
        recover(tmp_path)


# ---- the store over a WAL -----------------------------------------------------------

ops = st.lists(
    st.tuples(st.sampled_from(["create", "status", "bind", "delete", "force"]), st.integers(0, 4)),
    max_size=40,
)


def drive(store: Store, script: list[tuple[str, int]]) -> None:
    """Apply a random but valid-ish sequence of writes; rejected ones change nothing."""
    for op, i in script:
        name = f"p{i}"
        try:
            match op:
                case "create":
                    store.create(Pod(metadata=ObjectMeta(name=name), spec=PodSpec(command=["x"])))
                case "status":
                    p = store.get(Pod, name)
                    p.status.message = f"m{store.revision}"
                    store.update_status(p)
                case "bind":
                    p = store.get(Pod, name)
                    store.bind(name, "n1", p.metadata.resource_version)
                case "delete" | "force":
                    store.delete(Pod, name, force=op == "force")
        except StoreError:
            pass


def state(store: Store) -> tuple[int, list[tuple[str, dict[str, object]]]]:
    snap = store.snapshot()
    return snap.revision, snap.objects


def ready_node(store: Store) -> None:
    n = store.create(Node(metadata=ObjectMeta(name="n1"), spec=NodeSpec(capacity={"cpu": 10**6})))
    n.status.condition = NodeCondition.READY
    store.update_status(n)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(ops, st.integers(1, 6))
def test_reopening_gives_back_exactly_the_same_state(
    tmp_path_factory: pytest.TempPathFactory, script: list[tuple[str, int]], every: int
) -> None:
    d = tmp_path_factory.mktemp("store")
    store = Store.open(d, FakeClock(), snapshot_every=every, segment_size=2048)
    ready_node(store)
    drive(store, script)
    before = state(store)
    store.close()
    again = Store.open(d, FakeClock(), snapshot_every=every, segment_size=2048)
    assert state(again) == before
    # and it keeps going from the recovered revision (U2.3)
    n = again.get(Node, "n1")
    assert again.update_status(n).metadata.resource_version == before[0] + 1
    again.close()
    assert len(snapshots(d)) <= 2


def test_snapshots_prune_old_segments(tmp_path: Path) -> None:
    store = Store.open(tmp_path, FakeClock(), snapshot_every=10, segment_size=1 << 20)
    ready_node(store)
    drive(store, [("create", i % 5) for i in range(60)] + [("status", i % 5) for i in range(60)])
    store.close()
    assert len(snapshots(tmp_path)) == 2
    assert len(segments(tmp_path)) <= 3  # since the older kept snapshot, plus the current one


def test_damaged_newest_snapshot_falls_back_to_the_previous_one(tmp_path: Path) -> None:
    store = Store.open(tmp_path, FakeClock(), snapshot_every=5)
    ready_node(store)
    drive(store, [("create", i) for i in range(5)] + [("status", i) for i in range(5)] * 2)
    before = state(store)
    store.close()
    newest = snapshots(tmp_path)[-1]
    newest.write_bytes(newest.read_bytes()[:-3] + b"xx}")
    again = Store.open(tmp_path, FakeClock(), snapshot_every=5)
    assert state(again) == before


def test_interrupted_snapshot_tmp_is_ignored(tmp_path: Path) -> None:
    store = Store.open(tmp_path, FakeClock())
    ready_node(store)
    before = state(store)
    store.close()
    (tmp_path / "snap-00000000000000000099.tmp").write_bytes(b"half a snapshot")
    assert state(Store.open(tmp_path, FakeClock())) == before
    assert not list(tmp_path.glob("*.tmp"))


def test_a_failed_log_write_changes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Disk full (ADR-0015): the write fails with Unavailable and memory is untouched,
    because the log comes first."""
    store = Store.open(tmp_path, FakeClock())
    ready_node(store)
    pod = store.create(Pod(metadata=ObjectMeta(name="p"), spec=PodSpec(command=["x"])))
    before = state(store)

    def disk_full(record: Record) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(store._wal, "append", disk_full)
    pod.status.message = "lost"
    with pytest.raises(Unavailable, match="No space left"):
        store.update_status(pod)
    with pytest.raises(Unavailable):
        store.create(Pod(metadata=ObjectMeta(name="q"), spec=PodSpec(command=["x"])))
    with pytest.raises(Unavailable):
        store.delete(Pod, "p")
    assert state(store) == before
