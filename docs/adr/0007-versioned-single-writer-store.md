# ADR-0007: Versioned single-writer store with optimistic concurrency

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Proposed | 2026-10-21 | P1 | — | — |

## 1. What
Cluster state lives in one `Store` that applies mutations **one at a time**, stamps each with a **global, strictly increasing revision**, and accepts updates only as **compare-and-swap** on the object's `resource_version`. Spec and status are written through separate operations, and structural invariants (immutable `uid`, bind-once `node_name`, node capacity) are enforced inside the store's transaction.

## 2. Why
### Problem
Several actors write concurrently: the CLI (spec), the deployment controller (creates/deletes pods), the scheduler (binds), the node lifecycle controller (conditions, eviction) and agents (pod status, heartbeats). Without concurrency control you get **lost updates**. Concrete scenario: the scheduler binds pod P to node-2; a moment later an agent writes a status update for P based on a copy read *before* the bind, and the binding is silently erased, so P gets scheduled again on node-3 (S1 violated).
### Forces & constraints
- A total order of changes is needed for watch resumption (ADR-0008) and the WAL (ADR-0015).
- Correctness must hold with stale caches everywhere (ADR-0009).
- Should mirror etcd/kube-apiserver semantics (interview relevance) without importing them.
- Throughput target is modest (hundreds to low thousands of writes/s).

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: single writer + global revision + CAS** | No lost updates; total order; simple watch | Retry logic for every writer; single-writer throughput cap | ✅ Chosen |
| A. Pessimistic per-object locks/leases | Familiar | Lock timeouts, deadlocks, fragile across network failures | ❌ |
| B. Last-writer-wins with timestamps | Trivial | Silently loses updates; clock-dependent | ❌ |
| C. Per-object version counters only | CAS works | No global order → watch resumption needs vectors | ❌ |
| D. SQLite | ACID for free | Hides the mechanisms the project exists to show; watch still custom | ❌ |

**A** makes every writer hold distributed locks, and a partitioned lock holder blocks everyone until the lock times out. CAS needs no holding. **B** is the textbook lost-update bug. **C** works for CAS but makes "give me everything after X" awkward. **D** is a fine engineering choice in general, but it removes revisions, the WAL and recovery from the project, and those are part of its value.

## 4. How
### Components & files
- `src/chronos/store/store.py`: `Store` (sync core) + `AsyncStore` (façade with an `asyncio.Lock`).
- `src/chronos/store/errors.py`: `Conflict`, `NotFound`, `AlreadyExists`, `Invalid`, `InsufficientCapacity`, `Compacted`.
- `src/chronos/store/validation.py`: per-kind transition checks.
### Mechanism
```python
class Store:
    revision: Revision                       # starts at 0
    objects: dict[tuple[Kind, str], Obj]
    log: WatchLog                            # ADR-0008
    wal: Wal | None                          # ADR-0015

    def _commit(self, op, new: Obj | None, old: Obj | None) -> Event:
        rev = self.revision + 1
        if new: new.metadata.resource_version = rev
        if self.wal: self.wal.append_and_sync(rev, op, new or old)   # durable BEFORE visible
        self.revision = rev
        apply_to(self.objects, op, new, old)
        return self.log.append(Event(rev, op, deepcopy(new or old)))

    def update(self, obj: Obj) -> Obj:
        cur = self._get_or_404(obj.kind, obj.name)
        if obj.metadata.resource_version != cur.metadata.resource_version:
            raise Conflict(current=cur.metadata.resource_version)
        validate_spec_update(cur, obj)       # uid immutable, node_name bind-once, etc.
        new = merge_spec(cur, obj); new.metadata.generation += spec_changed(cur, new)
        return deepcopy(self._commit("MODIFIED", new, cur).obj)
```
- **Operations:** `create`, `get`, `list(kind, selector) -> (items, revision)`, `update` (spec + labels), `update_status` (status only), `bind` (ADR-0013), `delete(expected_rv)` (pods: sets `deletion_timestamp`; others: removes), `force_delete`.
- **Copies:** deep copy on the way in and out, so callers can never mutate stored state.
- **Single writer:** the sync core has no `await`; the async façade serializes calls with one lock. The sim calls the sync core directly.
- **Client helper:**
```python
async def retry_on_conflict(fn, attempts=5):   # re-read → recompute → write
    for i in range(attempts):
        try: return await fn()
        except Conflict: await clock.sleep(backoff(i))
    raise
```
### Configuration
None besides WAL settings (ADR-0015).
### Edge cases & failure handling
- **Delete of a pod with `deletion_timestamp` already set:** idempotent, returns current.
- **Update on a Terminating object:** spec updates rejected (`Invalid`); status updates allowed.
- **Revision overflow:** Python ints are unbounded; not an issue.
- **WAL append fails (disk full):** raise `Unavailable`; in-memory state unchanged (because the WAL comes first).

## 5. Consequences
**Positive:** lost updates are impossible by construction; linearizable (a single writer gives a total order); the watch is trivial; mirrors etcd/K8s semantics.
**Negative / accepted costs:** every writer needs retry-on-conflict; throughput is capped by one writer (+ fsync); a single point of failure (ADR-0018).
**Risks & mitigations:** forgetting CAS somewhere → no blind-write API exists at all (except `create`, `force_delete`).

## 6. Verification
- **S1, S5:** Hypothesis `RuleBasedStateMachine` comparing `Store` against a dict + counter model under random create/update/CAS/delete.
- Unit tests for every error path and every validation rule.
- Sim: S1/S5 checked after every event.
- B5 measures write throughput.

## 7. Revisit when
Write throughput limits B1/B4 at the target scale, or HA work (ADR-0018) needs the store layered on a replicated log.

## 8. Related & references
ADR-0008, ADR-0013, ADR-0015, ADR-0018; Kubernetes "API concepts: resource versions"; etcd data model (revisions).
