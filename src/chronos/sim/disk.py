"""A disk that forgets what wasn't synced (ADR-0016: synced vs unsynced bytes).

The real WAL writes real files in a scratch directory. Its `durable_sync` is replaced
by a recorder that notes, per file (device, inode), how many bytes the sync covered,
and doesn't actually sync (the sim never loses power for real, and real fsyncs would
be slow). A control-plane crash then truncates every file to its last synced size,
which is what a power loss would leave, and the real recovery code reads the result.

A process crash keeps the page cache; this models the harder case on purpose: it's
the only way a missing fsync becomes visible (testing-the-tests.md 3.5).
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator
from pathlib import Path

from chronos.store import wal as wal_module


class SimDisk:
    def __init__(self, data_dir: Path, *, honest: bool = True) -> None:
        self.dir = data_dir
        self.honest = honest  # False: the `ack_before_fsync` canary, syncs do nothing
        self._synced: dict[tuple[int, int], int] = {}

    def _record(self, fd: int) -> None:
        if self.honest:
            st = os.fstat(fd)
            self._synced[(st.st_dev, st.st_ino)] = st.st_size

    @contextlib.contextmanager
    def installed(self) -> Iterator[None]:
        real = wal_module.durable_sync
        wal_module.durable_sync = self._record
        try:
            yield
        finally:
            wal_module.durable_sync = real

    def crash(self) -> list[str]:
        """Lose every unsynced byte. Returns what was cut, for the trace."""
        cut = []
        for path in sorted(self.dir.iterdir()):
            st = path.stat()
            keep = self._synced.get((st.st_dev, st.st_ino), 0)
            if st.st_size > keep:
                os.truncate(path, keep)
                cut.append(f"{path.name}:{st.st_size}->{keep}")
        return cut
