"""Write-ahead log, snapshots and recovery (ADR-0015).

Files in the data dir:

    wal-<first rev:020d>.log    segments of framed records (codec.py), appended in order
    snap-<rev:020d>.json        "<crc32>\\n<json>": every object at that revision

Write path (called by the store before it changes memory): encode → write → durable
sync → return. Only then does the store apply the change and acknowledge it, so an
acknowledged write is on disk (S8). Every `snapshot_every` records the store
snapshots its state; the log then rolls to a new segment, and segments older than the
previous snapshot are deleted (two snapshots are kept, so a damaged newest one can
fall back to the older one).

Recovery: newest snapshot that parses and passes its CRC, then every record after it,
in order and contiguous. An invalid frame at the end of the last segment is a write
torn by a crash: it's truncated away. An invalid frame with an intact one anywhere
after it is corruption: recovery refuses to start rather than silently drop data.
"""

from __future__ import annotations

import itertools
import json
import logging
import os
import sys
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from chronos.common import crashpoint
from chronos.store.codec import HEADER, InvalidFrame, Record, decode, encode

log = logging.getLogger(__name__)

DEFAULT_SEGMENT_SIZE = 64 * 1024 * 1024

# full: sync every write before returning. group: write now, sync in batches via
# flush() (the caller holds acks until then). none: never sync (benchmarks only).
type SyncMode = Literal["full", "group", "none"]
KEEP_SNAPSHOTS = 2


class WalCorrupt(Exception):
    """The log is damaged somewhere other than a torn final record."""


def durable_sync(fd: int) -> None:
    """Flush to stable storage. macOS needs F_FULLFSYNC: its fsync only reaches the drive."""
    if sys.platform == "darwin":
        import fcntl  # noqa: PLC0415 (POSIX-only module)

        fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
    else:
        os.fsync(fd)


def sync_dir(path: Path) -> None:
    """Make a create/rename/delete in `path` durable."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _segment_name(first_rev: int) -> str:
    return f"wal-{first_rev:020d}.log"


def _snapshot_name(rev: int) -> str:
    return f"snap-{rev:020d}.json"


def _rev_of(path: Path) -> int:
    return int(path.stem.split("-", 1)[1])


def segments(data_dir: Path) -> list[Path]:
    return sorted(data_dir.glob("wal-*.log"), key=_rev_of)


def snapshots(data_dir: Path) -> list[Path]:
    return sorted(data_dir.glob("snap-*.json"), key=_rev_of)


# ---- snapshots -------------------------------------------------------------------


@dataclass(frozen=True)
class Snapshot:
    revision: int
    objects: list[tuple[str, dict[str, object]]]  # (kind, object JSON)


def write_snapshot(data_dir: Path, snap: Snapshot) -> Path:
    body = json.dumps(
        {"revision": snap.revision, "objects": [[k, o] for k, o in snap.objects]},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    final = data_dir / _snapshot_name(snap.revision)
    tmp = final.with_suffix(".tmp")
    with tmp.open("wb") as f:
        f.write(str(zlib.crc32(body)).encode() + b"\n" + body)
        f.flush()
        durable_sync(f.fileno())
    crashpoint.hit("snapshot.before_rename")
    os.replace(tmp, final)
    crashpoint.hit("snapshot.after_rename_before_dirsync")
    sync_dir(data_dir)
    return final


def read_snapshot(path: Path) -> Snapshot:
    raw = path.read_bytes()
    crc, sep, body = raw.partition(b"\n")
    if not sep or not crc.isdigit() or int(crc) != zlib.crc32(body):
        raise WalCorrupt(f"{path.name}: checksum mismatch")
    data = json.loads(body)
    return Snapshot(int(data["revision"]), [(str(k), o) for k, o in data["objects"]])


# ---- recovery ----------------------------------------------------------------------


@dataclass
class Recovered:
    snapshot: Snapshot | None
    records: list[Record] = field(default_factory=list)
    truncated: tuple[str, int] | None = None  # (segment, offset) of a torn tail

    @property
    def revision(self) -> int:
        if self.records:
            return self.records[-1].rev
        return self.snapshot.revision if self.snapshot else 0


def _intact_frame_after(data: bytes, offset: int) -> int | None:
    for p in range(offset + 1, len(data) - HEADER.size + 1):
        try:
            decode(data, p)
        except InvalidFrame:
            continue
        return p
    return None


def recover(data_dir: Path, *, repair: bool = True) -> Recovered:
    """`repair=False` only reports (e.g. `chronosd wal-verify`): nothing is truncated or deleted."""
    for tmp in data_dir.glob("snap-*.tmp"):  # a snapshot interrupted before its rename
        if repair:
            tmp.unlink()
    snap = None
    for path in reversed(snapshots(data_dir)):
        try:
            snap = read_snapshot(path)
            break
        except (WalCorrupt, ValueError, KeyError) as exc:
            log.warning("ignoring snapshot %s: %s", path.name, exc)
    result = Recovered(snap)
    expected = (snap.revision if snap else 0) + 1
    segs = segments(data_dir)
    for i, seg in enumerate(segs):
        data = seg.read_bytes()
        offset = 0
        while offset < len(data):
            try:
                record, nxt = decode(data, offset)
            except InvalidFrame as exc:
                later = _intact_frame_after(data, offset)
                if i == len(segs) - 1 and later is None:
                    if repair:
                        log.warning("truncating torn write at %s:%d (%s)", seg.name, offset, exc)
                        with seg.open("r+b") as f:
                            f.truncate(offset)
                            durable_sync(f.fileno())
                    result.truncated = (seg.name, offset)
                    break
                problem = f"corruption at {seg.name}:{offset} ({exc})"
                if later is not None:
                    problem += f"; an intact record follows at offset {later}"
                raise WalCorrupt(problem + "; refusing to start") from None
            offset = nxt
            if record.rev < expected:
                continue  # already in the snapshot
            if record.rev != expected:
                raise WalCorrupt(
                    f"gap in the log at {seg.name}: expected revision {expected}, "
                    f"found {record.rev}"
                )
            result.records.append(record)
            expected += 1
    return result


# ---- the writer --------------------------------------------------------------------


class Wal:
    """Appends records durably. Open it after `recover()` has validated the directory."""

    def __init__(
        self,
        data_dir: Path,
        next_rev: int,
        *,
        sync: SyncMode = "full",
        segment_size: int = DEFAULT_SEGMENT_SIZE,
    ) -> None:
        self.dir = data_dir
        self.sync_mode: SyncMode = sync
        self._segment_size = segment_size
        self._dirty = False
        if sync == "none":
            log.warning("WAL sync is OFF: acknowledged writes can be lost (benchmarks only)")
        existing = segments(data_dir)
        if existing:
            path = existing[-1]
            self._fd = os.open(path, os.O_WRONLY | os.O_APPEND)
            self._size = path.stat().st_size
        else:
            self._open_segment(next_rev)

    def _open_segment(self, first_rev: int) -> None:
        path = self.dir / _segment_name(first_rev)
        self._fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        self._size = 0
        sync_dir(self.dir)

    def append(self, record: Record) -> None:
        """In "full" mode, durable when this returns; in "group" mode, once the next
        flush() returns. Raises OSError (e.g. disk full) on failure."""
        frame = encode(record)
        if self._size and self._size + len(frame) > self._segment_size:
            crashpoint.hit("segment.rotate")
            self.roll(record.rev)
        crashpoint.hit("wal.before_write")
        if crashpoint.armed("wal.torn_write"):  # simulate a crash halfway through a write
            os.write(self._fd, frame[: len(frame) // 2])
            os._exit(crashpoint.CRASH_EXIT)
        os.write(self._fd, frame)
        self._size += len(frame)
        if self.sync_mode == "full":
            crashpoint.hit("wal.before_fsync")
            durable_sync(self._fd)
            crashpoint.hit("wal.after_fsync_before_apply")
        else:
            self._dirty = True

    def flush(self) -> None:
        """Group mode: make everything appended so far durable, with one sync."""
        if self.sync_mode != "group" or not self._dirty:
            return
        crashpoint.hit("wal.before_fsync")
        durable_sync(self._fd)
        self._dirty = False
        crashpoint.hit("wal.after_fsync_before_apply")

    def roll(self, next_rev: int) -> None:
        self.flush()  # the old segment's pending records must not be left unsynced
        os.close(self._fd)
        self._open_segment(next_rev)

    def snapshot(self, snap: Snapshot) -> None:
        """Write a snapshot, start a new segment, and drop what's no longer needed."""
        write_snapshot(self.dir, snap)
        self.roll(snap.revision + 1)
        snaps = snapshots(self.dir)
        for old in snaps[:-KEEP_SNAPSHOTS]:
            old.unlink()
        oldest_kept = _rev_of(snaps[-KEEP_SNAPSHOTS]) if len(snaps) >= KEEP_SNAPSHOTS else 0
        segs = segments(self.dir)
        for seg, nxt in itertools.pairwise(segs):
            if _rev_of(nxt) <= oldest_kept + 1:  # every record in `seg` is <= oldest_kept
                seg.unlink()
        sync_dir(self.dir)

    def close(self) -> None:
        self.flush()
        os.close(self._fd)
