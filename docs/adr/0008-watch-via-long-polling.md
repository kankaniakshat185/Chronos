# ADR-0008: Watch via long-polling on resource versions

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Accepted | 2026-11-06 | P1 | — | — |

## 1. What
Clients learn about changes with `GET /api/v1/watch/{kind}?since=<rev>`: the server returns all matching events after `rev` immediately, or **blocks** until one arrives or a timeout passes. If `rev` is older than the retained history, it returns **410 Gone** and the client must **relist**. Events are never silently skipped.

## 2. Why
### Problem
Controllers and agents must react to changes quickly without listing the whole cluster repeatedly, and after a disconnect they must either resume *exactly* where they left off or know for certain that they missed something. Concrete failure without this: an agent reconnects after a partition, misses the DELETED event for its pod, and keeps the workload running forever.
### Forces & constraints
- Latency: sub-second reaction for the demo and for B1.
- Must be easy to fault-inject (the proxy can break it) and to reason about under partitions.
- Bounded server memory.
- Restart of the control plane loses in-memory history (ADR-0015).

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: long-poll + revision + 410** | Simple, explicit, bounded requests, curl-able | Extra round trip per batch | ✅ Chosen |
| A. Streaming (chunked HTTP / SSE) | Lower latency, fewer requests; what K8s does | Long-lived connection semantics harder under partitions and fault injection | ⏸ Later upgrade |
| B. Periodic full list | Trivial | O(cluster) per poll; latency = poll interval | ❌ (kept only as resync) |
| C. Server push to clients | Lowest latency | Server must reach clients (rejected in ADR-0010) | ❌ |

**A** is the real-world design, but a half-open TCP stream during a blackhole partition is exactly the kind of ambiguous state that makes reasoning hard. Long-poll gives each round trip an explicit start, end and timeout. It's upgradeable later behind the informer. **B** doesn't scale and adds latency, so it survives only as the periodic resync safety net (ADR-0009). **C** conflicts with the pull model.

## 4. How
### Components & files
- `src/chronos/store/watchlog.py`: ring buffer + waiters.
- `src/chronos/server/routes/watch.py`: endpoint.
- `src/chronos/controllers/informer.py`: client side (list → watch → relist).
### Mechanism
**Server:**
```python
class WatchLog:
    buf: deque[Event]                     # maxlen = watch_history
    cond: asyncio.Condition

    def since(self, rev, pred) -> list[Event]:
        if self.buf and rev < self.buf[0].revision - 1: raise Compacted
        return [e for e in self.buf if e.revision > rev and pred(e)]   # bisect in practice

async def watch(kind, since, timeout, filters):
    deadline = clock.monotonic() + timeout
    while True:
        evs = log.since(since, match(kind, filters))[:BATCH]
        if evs or clock.monotonic() >= deadline:
            return {"events": evs, "revision": evs[-1].revision if evs else store.revision}
        await wait_for(log.cond.wait(), deadline - clock.monotonic())
```
**Client (informer):**
```
items, rev = list(kind)            # replace cache wholesale
loop:
    try: batch = watch(kind, since=rev, timeout=30)
    except Gone: goto relist
    apply batch.events to cache in order; rev = batch.revision
```
- The returned `revision` lets clients advance even when no matching events occurred (filtered watch), so they don't fall behind compaction just by being idle.
- The `node=` filter matches pods whose `spec.node_name == node` **in either the old or new version** of the object, so an agent sees its pod's DELETED event and the unbind.
### Configuration
| Param | Default | Notes |
|-------|---------|-------|
| `watch_history` | 10 000 events | ring buffer size |
| `watch.timeout` | 30 s (max 60) | per request |
| `watch.batch` | 500 events | |
### Edge cases & failure handling
- **Control-plane restart:** the buffer is empty and the revision is restored from the WAL. Any `since` < current → 410 → relist. Correct, and it exercises the relist path in every chaos run.
- **Client slower than the event rate:** eventually 410 → relist (self-correcting).
- **Request lost in a partition:** the client times out (`timeout + 5 s`) and retries from the same `since`. This is idempotent.

### Implementation status (2026-11-06)
Implemented as designed: `store/watchlog.py` (a ring of the last `watch_history` events; one event per revision, so revision r sits in slot `(r - 1) % N` and no search is needed), `Store.watch_since`, `GET /api/v1/watch/{plural}?since=&timeout=&node=` for every registered kind, `StoreClient.watch` on both clients, and `Compacted` (410). Deviations and additions: the events carry objects **as stored**, so a Node's derived `status.allocated` is empty in watch events (it's computed on read; watchers that need it sum it from pods, as the scheduler does). `since` **newer** than the store also gets 410: that only happens when a store was restored from an older state (`--wal-sync none`), where revision numbers are being reused. In-process watches wait for the WAL's durable revision like every other read (group commit, ADR-0015), so no event a crash could take back is ever delivered. On shutdown, open HTTP watches are answered at once with an empty batch rather than cut. One real bug found by a test on the way: the in-process watch wrapped its wait in a task, which registered its waiter only when first scheduled, so a write landing in between went unnoticed until the 30 s timeout (a lost wakeup); the waiter is now registered synchronously, with no `await` between checking and registering, and a deterministic test pins it. Verification: the S6 replay property from **every** revision of random histories (whole kind, node filter, batches of 1–3) with compaction, its boundary checked by mutation; watch contract tests over both clients; informer relist on 410 and after a restart.

## 5. Consequences
**Positive:** an explicit "you missed events" signal (S6); idempotent retries; partitions handled naturally; simple to test.
**Negative / accepted costs:** one request per batch; idle clients poll every 30 s.
**Risks & mitigations:** off-by-one at the compaction boundary → property tests at exact boundary revisions.

## 6. Verification
- **S6:** property test: for random histories and any retained `r`, replaying `since(r)` onto `list@r` equals `list@now`; for `r` below retention → `Compacted`, never a partial result.
- Canary `watch_skip_on_compaction` must be caught by the sim.
- Integration: restart `chronosd` → informers relist; no stuck agents.

## 7. Revisit when
B1 shows watch round trips dominate convergence latency, or request counts matter at scale. Then switch to streaming behind the same informer.

## 8. Related & references
ADR-0007, ADR-0009, ADR-0010; Kubernetes "Efficient detection of changes" (watch, 410 Gone, bookmarks).
