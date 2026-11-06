# ADR-0015: Persistence via WAL with CRC framing and snapshots

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Accepted | 2026-11-06 | P6 | — | — |

## 1. What
Every store mutation is appended to a **write-ahead log** and **fsynced before it is applied in memory or acknowledged**. Records are length-prefixed with a CRC. Periodic **atomic snapshots** bound recovery time. Recovery **truncates a torn final record** but **refuses to start on corruption anywhere else**.

## 2. Why
### Problem
The store is in memory. Without durability, a `chronosd` crash loses deployments and bindings, and on restart the controllers act on amnesia. Concrete failure: after a crash the store has forgotten that pod P is bound to node-2, so the deployment controller creates a fresh pod, while P's process is still running on node-2 → duplicates (S3), or the deployment disappears entirely.
### Forces & constraints
- Contract: **an acknowledged write is never lost; a partial write is never applied** (S8).
- Must work on macOS (where plain `fsync` doesn't flush the drive cache) and Linux.
- The author built a WAL in Strata, so deliberately reusing and improving that experience is part of the story.
- Debuggability (inspect the log by hand).

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: WAL (fsync-before-ack) + CRC framing + snapshots** | Precise contract; shows the mechanism; bounded recovery | fsync latency caps throughput | ✅ Chosen |
| A. SQLite | ACID for free | Hides the mechanism (see ADR-0007) | ❌ |
| B. Periodic snapshot only | Simple | Loses writes since the last snapshot (violates S8) | ❌ |
| C. Ack before fsync (async durability) | Fast | Loses acked writes on crash/power loss | ⏸ Opt-in mode for B5 only |
| D. Binary payload (msgpack/protobuf) | Smaller, faster | Not human-inspectable; fsync dominates anyway | ❌ (revisit if B5 says otherwise) |

**A** is the pragmatic choice in industry, but here it would remove one of the project's showcase mechanisms. **B** breaks the contract outright. **C** is a common real-world tradeoff; it exists as a benchmark mode, and canary `ack_before_fsync` proves the harness would catch it. **D** optimizes the wrong thing: encoding is microseconds, fsync is milliseconds.

## 4. How
### Components & files
- `src/chronos/store/wal.py`: segment writer, `append_and_sync`, group commit.
- `src/chronos/store/snapshot.py`: write/load.
- `src/chronos/store/recovery.py`: startup procedure.
- `src/chronos/store/codec.py`: record framing.
- `src/chronos/common/crashpoint.py`: fault hooks.
- CLI: `chronosd wal dump|verify`.
### Mechanism
**Record format (little-endian):**
```
┌──────────────┬──────────────┬───────────────────────────────────────────────┐
│ u32 length   │ u32 crc32    │ payload (JSON): {rev, op, kind, term, object} │
└──────────────┴──────────────┴───────────────────────────────────────────────┘
```
`term` is reserved for HA (ADR-0018), default 0.
**Write path** (inside the store's single-writer transaction, ADR-0007):
```
validate → encode → write(segment) → durable_sync(fd) → apply to memory + watch log → ack
durable_sync = fcntl(fd, F_FULLFSYNC) on macOS, os.fsync(fd) on Linux
```
**Group commit (optional):** writers enqueue encoded records; a committer task writes all pending records, syncs once, then resolves every waiter's future and applies the records in order. Window = max wait before syncing.
**Segments:** `wal-<first_rev:020d>.log`, rotated at 64 MiB; after creating a new file, fsync the directory.
**Snapshots:** every `snapshot_every` records:
```
serialize(state, revision) → snap-<rev>.tmp → fsync → rename → snap-<rev>.json → fsync(dir)
→ delete segments whose last rev ≤ snapshot rev → keep the newest 2 snapshots
```
Snapshot content is a consistent copy taken under the store lock (serialization happens outside the lock on a deep copy).
**Recovery:**
```
1. snap = newest snapshot that parses and passes its checksum (fall back to the previous one)
2. for each segment in order, for each record with rev > snap.rev:
       if record is invalid:
           if it is in the LAST segment and nothing valid follows → truncate here (torn write)
           else → refuse to start: "corruption at <file>:<offset>"  (override: --force-truncate-at)
       assert rev == expected_next_rev   (contiguity)
       apply
3. revision = last applied rev; watch log empty (clients get 410, ADR-0008)
4. node lifecycle startup grace (ADR-0011)
```
**Crash points** (`CHRONOS_CRASHPOINT=<name>[:<nth>]` → `os._exit(137)` on the nth hit): `wal.before_write`, `wal.before_fsync`, `wal.after_fsync_before_apply`, `segment.rotate`, `snapshot.before_rename`, `snapshot.after_rename_before_dirsync`.
### Configuration
| Param | Default |
|-------|---------|
| `data_dir` | `~/.chronos/server` |
| `wal.sync` | `full` (fsync per write) \| `group` \| `none` (bench only, logs a loud warning) |
| `wal.group_window` | 2 ms |
| `wal.segment_size` | 64 MiB |
| `snapshot_every` | 5 000 records |
### Edge cases & failure handling
- **Disk full / write error:** the write fails with `Unavailable`; memory is untouched (WAL first); `chronosd` stays up for reads.
- **Crash between fsync and apply:** the record is durable but unacked → it's applied on recovery. That's fine: the client got no ack and must re-read (at-least-once visible, never lost).
- **Crash during snapshot:** the `.tmp` file is ignored on recovery; the old snapshot + WAL are still complete.
- **Clock/term:** not used for ordering; revisions are the only order.

### Implementation status (2026-11-06)
Implemented: `store/codec.py` (the record format above), `store/wal.py` (segments, `durable_sync` with `F_FULLFSYNC` on macOS, atomic snapshots keeping the newest two, recovery with torn-tail truncation and refusal on any other corruption or a revision gap), `common/crashpoint.py` (the six points above plus `wal.torn_write`, which writes half a frame and exits), `Store.open()` and `chronosd run --data-dir`, and `chronosd wal-verify` (report only; it never truncates). A failed append raises `Unavailable` with memory untouched. Deviations: **group commit (P6.2) is deferred to the PM-6 benchmarks (B5)**: with one synchronous writer every write is synced on its own (about 3 ms with `F_FULLFSYNC` on this Mac), which is enough at Stage 1's scale, and B5 is where per-write vs batched syncing gets measured. Snapshots are written synchronously by the writer (small state; revisit with B6). The `ack_before_fsync` canary belongs to the simulator (PM-4): a process crash can't lose page-cache data, so only a modelled power loss can catch a missing fsync. Verification: 7 crash points x 50 seeds (`make test-durability`), recovery from every truncation offset, bit flips, disk full, restart equivalence, and the control-plane restart integration tests (U2.2).

## 5. Consequences
**Positive:** crash safety with a precise contract; the recovery path runs in every chaos run; a direct "what I did differently from Strata" story.
**Negative / accepted costs:** fsync latency (≈ 0.1–5 ms on Linux SSDs; `F_FULLFSYNC` on macOS can be 5–20 ms) caps write throughput → group commit, measured in B5. Recovery time grows with the WAL since the last snapshot (B6).
**Risks & mitigations:** a subtle recovery bug → exhaustive truncation-offset property tests plus crash-point matrix runs.
**Limitation:** single disk, no replication (ADR-0018).

## 6. Verification
- **S8:** codec round-trip property; recovery from **every byte truncation offset** equals the prefix of complete records; bit-flip in the final record → truncated; bit-flip mid-file → refuses to start.
- Durability harness: every crash point × 50 seeds; acked-write log compared after restart.
- Canary `ack_before_fsync` detected by the harness/sim.
- Chaos: `kill -9 chronosd` mid-scale-up.
- **B5** (throughput & p99 latency per sync mode); **B6** (recovery time vs WAL size, with/without snapshots).

## 7. Revisit when
B5 shows write throughput limits convergence at the target scale (tune group commit, consider a binary encoding), or HA work replaces local durability with a replicated log.

## 8. Related & references
ADR-0007, ADR-0008, ADR-0011, ADR-0018; Strata (author's prior WAL); Pillai et al., "All File Systems Are Not Created Equal" (OSDI 2014); *DDIA* ch. 3 & 7; Apple `fsync(2)` man page on `F_FULLFSYNC`.
