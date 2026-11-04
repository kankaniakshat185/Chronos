# ADR-0018: Control-plane high availability

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Deferred (Backlog, not scheduled; Option A drafted as P11.1–P11.6) | 2026-10-21 | Backlog | — | — |

## 1. What
Control-plane HA is **not built in Stage 1**; it lives in the PLAN.md **Backlog (not scheduled)**, and the design keeps it possible. When Phase 11 starts, the tentative choice is **active/passive `chronosd` with a lease and fencing tokens (terms) on shared storage**. A from-scratch **Raft** replicated log is a possible separate follow-on project.

## 2. Why
### Problem
`chronosd` is a single process with a local WAL. If it dies, nothing heals, and after `fence_timeout` every workload stops (ADR-0012). That's the main availability limitation.

The trap: "add leader election" sounds small, but election is only half the problem. A new leader must have **all acknowledged state**, which requires replicated or shared storage, and a **deposed leader that doesn't know it's deposed** (e.g. after a GC pause) must be prevented from writing. Election without replicated state just moves the single point of failure; election without fencing allows split-brain writes.
### Forces & constraints
- Out of Stage 1 scope (Backlog).
- Must not be faked: any HA claim needs the same sim + chaos rigor as everything else.
- Should reuse what already exists (WAL, leases, fencing, simulator).

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Tentative: A. Active/passive + lease + fencing tokens on shared storage** | 15–20 h; reuses the WAL, leases, fencing; honest | Tolerates process failure, not machine/disk loss | ✅ If Phase 11 happens |
| B. Raft replicated log from scratch | Real machine-level fault tolerance; strong resume item | 40–60 h; a project in itself | ⏸ Separate follow-on |
| C. etcd / existing Raft library | Production-grade | Undercuts the from-scratch value | ❌ |
| D. Leader election only (no shared/replicated state) | Easy | The new leader has no state, so it's pointless | ❌ |
| E. Do nothing | Zero cost | Documented limitation remains | ✅ Default for the core plan |

**B** is the "real" answer, and the simulator is the right tool to test it, but it would consume the whole stretch budget and more. **C** contradicts the project's premise. **D** is the trap described above.

## 4. How (Option A, for when Phase 11 starts)
### Components & files
- `src/chronos/ha/lease.py`: lease acquire/renew.
- `src/chronos/store/wal.py`: term check on append.
- `src/chronos/server/`: `503 NotLeader` + leader hint.
- `src/chronos/client/http.py`: endpoint list, follow the leader hint.
### Mechanism
**Lease record** `<data_dir>/leader.json` = `{leader_id, term, renewed_at_mono, duration}`, written with tmp + rename + dir fsync under an `flock` on `<data_dir>/leader.lock`.
```
standby loop:
    every 1 s: read lease; if (now - renewed_at) > duration + margin:
        flock; re-read; if still expired: term += 1; write {me, term}; unlock
        → recover store from WAL; start controllers; serve
leader loop:
    every duration/3: flock; re-read; if leader_id != me or term != my_term: step down (exit)
                      else renewed_at = now; write; unlock
WAL append (every write):
    flock(leader.lock, shared); read lease.term; if term != my_term → reject (NotLeader); append(record.term = my_term); unlock
```
The **term is the fencing token**: a paused old leader that wakes up finds a newer term under the lock and can't append. Recovery also rejects records with a term lower than one already seen later in the log.
**Agents/CLI:** `client.endpoints = [cp1, cp2]`; on `503 NotLeader {leader: cp2}` or a connection error, try the next endpoint.
**Interaction with fencing (ADR-0012):** failover must complete within `fence_timeout`, or workloads fence. Lease duration 2 s + recovery ≈ 1 s < F = 6 s.
### Configuration (proposed)
| Param | Default |
|-------|---------|
| `ha.lease_duration` | 2 s |
| `ha.renew_interval` | 0.66 s |
| `ha.takeover_margin` | 0.5 s |
### Edge cases
- Both processes think the lease is expired at the same moment → the `flock` serializes them; the second sees the new term and stays standby.
- `renewed_at_mono` relies on a shared monotonic clock → valid on a **single host only**, which is consistent with Option A's honest scope.
- Shared storage itself fails → out of scope (that's Option B's job).
### Design constraints already honoured now
- One `Store` API with a total order (ADR-0007), so a replicated log can slot in underneath.
- WAL records carry a reserved `term` field (ADR-0015).
- `HttpStoreClient` takes a list of endpoints; `503 NotLeader` is reserved (ADR-0004).
- Controllers are restart-safe via level triggering (ADR-0009), so a new leader just starts reconciling.

## 5. Consequences
**Positive (if built):** the cluster survives control-plane process crashes and pauses without stopping workloads; strong fencing-token story.
**Negative / accepted costs:** process-level HA only; extra complexity in the write path (a lease check per append, mitigated by caching the term and re-checking under the lock only on renew).
**Now (deferred):** documented limitation, "a control-plane outage longer than `fence_timeout` stops all workloads".

## 6. Verification (when built)
- Sim: leader crash / pause / partition schedules; invariants S1–S8 plus **H1: at most one leader appends per term**, and **H2: no record from a stale term is ever applied**.
- Chaos: SIGSTOP the leader past the lease → the standby takes over → SIGCONT the old leader → its writes are rejected (term mismatch), and it steps down.
- Benchmark: failover time distribution vs `fence_timeout`.

## 7. Revisit when
Tier 1 + 2 and the write-up are done (PLAN.md Phase 11 gate), or interviews show consensus experience is the gap to close (then choose Option B as a separate project).

## 8. Related & references
ADR-0004, ADR-0007, ADR-0009, ADR-0012, ADR-0015, ADR-0016; Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm" (2014); Kleppmann, "How to do distributed locking"; Burrows, "The Chubby lock service" (2006).
