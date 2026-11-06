# ADR-0009: Level-triggered reconciliation with informers, work queue and resync

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Accepted | 2026-11-06 | P4 | — | — |

## 1. What
Controllers are **level-triggered**: events only say "something about key K may have changed"; the controller then reads the *current* desired and observed state for K and computes the full difference. A shared runtime provides **informers** (cached list+watch), a **deduplicating, rate-limited work queue**, **expectations** to tolerate stale caches, and **periodic resync**.

## 2. Why
### Problem
An **edge-triggered** design ("on pod-died event, create a pod") breaks the moment one event is lost, duplicated or reordered, or the controller restarts mid-way, and then the cluster is permanently wrong. Concrete scenario: the controller crashes after creating 2 of 3 replacement pods; with edge triggering nobody ever creates the 3rd.
### Forces & constraints
- Caches are always somewhat stale (watch lag).
- Controllers restart (control-plane crash, ADR-0015) and must just carry on.
- Multiple events for the same key in a burst should cause one reconcile, not N.
- Must be the recognizable Kubernetes design (client-go), so it's explainable in interviews.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: level-triggered + informer + work queue + expectations + resync** | Self-healing; dedup; per-key backoff; K8s-faithful | More machinery | ✅ Chosen |
| A. Edge-triggered handlers | Simple at first | Lost/duplicate events → permanent drift | ❌ |
| B. Global polling loop ("every 1 s, reconcile everything") | Level-triggered, very simple | O(cluster) per tick; no per-key backoff/dedup; latency = tick | ⏸ OK for a first spike only |
| C. CAS only, no expectations | Less state | Creates don't conflict, so a stale cache causes duplicate pods | ❌ |

**A** is fragile by construction. **B** has the right semantics but scales poorly and gives no per-key error handling; it's acceptable as a throwaway spike to validate `core.reconcile` before building the runtime. **C** is subtle and worth explaining: CAS protects updates to *one existing object*, but "create pod #3" doesn't touch any existing object, so two reconciles on a stale cache both create one. The canary `disable_expectations` demonstrates this.

## 4. How
### Components & files
- `src/chronos/controllers/runtime.py`: `Informer`, `WorkQueue`, `Controller` base, `Expectations`.
- `src/chronos/core/reconcile.py`: pure deployment logic.
- `src/chronos/controllers/deployment.py`, `node_lifecycle.py`, `scheduler.py`.
### Mechanism
**Work queue semantics:**
```
queue:       FIFO of keys, each key present at most once      (dedup)
processing:  set of keys being reconciled                       (≤1 worker per key)
dirty:       keys re-added while processing → requeued on done
failures:    per-key count → delay = min(5ms · 2^n, 30s) + jitter (rate limit)
```
**Controller loop:**
```python
async def worker(self):
    while key := await self.queue.get():
        try:
            res = await self.reconcile(key)
            self.queue.forget(key)                         # reset backoff
            if res.requeue_after: self.queue.add_after(key, res.requeue_after)
        except Conflict:   self.queue.add_rate_limited(key)
        except Exception:  log.exception(...); self.queue.add_rate_limited(key)
        finally:           self.queue.done(key)
```
**Deployment reconcile (pure core):**
```python
def reconcile(dep, pods, exp) -> list[Action]:
    if not exp.satisfied(): return []                      # wait for cache to catch up
    active = [p for p in pods if p.is_active()]            # not Failed/Succeeded/Terminating
    diff = dep.spec.replicas - len(active)
    if diff > 0: return [CreatePod(dep.template_pod()) for _ in range(diff)]
    if diff < 0: return [DeletePod(p) for p in pick_victims(active, -diff)]
    return [UpdateStatus(...)] if status_changed else []
```
`pick_victims` order: Pending → Bound → Running; within a group, newest first. It's deterministic.
**Expectations:** before issuing N creates, `exp.expect_creates(key, N)`. Each pod ADDED event observed by the informer (with an owner of `key`) calls `exp.observed_create(key)`. Satisfied when the counts reach zero **or** `expectations_ttl` passes (in case an event was genuinely lost; resync then corrects).
**Event → key mapping:** Deployment event → its name; Pod event → `pod.metadata.owner` name; Node NotReady → the owners of its pods.
**Resync:** every `resync_period`, enqueue every key in the informer cache.
### Configuration
| Param | Default |
|-------|---------|
| `resync_period` | 30 s |
| `workers` per controller | 1 (deterministic; raise later if B1 says so) |
| `queue.backoff` | 5 ms → 30 s |
| `expectations_ttl` | 30 s |
### Edge cases & failure handling
- **Controller crash mid-reconcile:** on restart, informers relist and a full reconcile recomputes the diff. Partially executed actions are simply observed state.
- **Owner deleted:** pods with a missing owner are garbage-collected by the deployment controller (DeletePod).
- **Scale-down racing with scheduling:** victims prefer Pending pods, so binding races mostly don't matter; a CAS conflict just requeues.

### Implementation status (2026-11-06)
Implemented in `controllers/runtime.py`: `Informer` (list, then long-poll watch; relist on 410; handlers get `(old, new)`, and a relist reports exactly the difference; optional indexes), `Informers` (one shared informer per kind, like client-go's SharedInformerFactory), `WorkQueue` (dedup, one in-flight per key, a key re-added while processing is queued again when done, per-key backoff 5 ms → 30 s), `Expectations` (creates counted, deletes tracked by name so a pod seen terminating and then gone counts once; TTL 30 s), `Controller` (one worker, resync every `resync_period`, 30 s), and the controller registry (X-6). Deviations: the deployment controller's keys are deployment **uids** (what pods carry in `metadata.owner`), so a pod event maps straight to its key and a deleted deployment's key still finds its pods; garbage collection of orphans confirms the owner is gone with a live read before deleting. The node lifecycle controller resyncs every `lifecycle_tick` (250 ms), not 30 s, because heartbeat silence grows without any store event; it also confirms NotReady with a live read before evicting, so a stale node cache can't evict pods from a node that has already come back. The agent uses the same `Informer` on `pods?node=self` and syncs on every cache change, every local process start or exit, and at least every `--resync-interval` (1 s; crash-loop backoffs expire with no event). No jitter in the queue backoff (deterministic for the simulator, PM-4). Verification: work-queue and expectations unit tests on a FakeClock; the expectations stale-cache test (the `disable_expectations` canary, checked by mutation: 3 pods become 6 without it); informer relist tests; the MVP integration suite passing on the new runtime, with one change: two tests now wait for the workload's first output line before deleting it, because `Running` is reported the moment the process starts (polling used to add ≥ 100 ms) and the SIGTERM-ignoring workload hadn't installed its handler yet; `test_watch.py` runs the agent with a 30 s resync to prove the watch, not a timer, drives it.

## 5. Consequences
**Positive:** self-healing from lost events, crashes and even controller bugs (resync); each part is unit-testable; maps 1:1 onto client-go vocabulary.
**Negative / accepted costs:** convergence latency = watch lag + queue + reconcile; expectations add state with a TTL.
**Risks & mitigations:** an expectations leak blocks a deployment → the TTL plus a metric `expectations_expired_total`.

## 6. Verification
- Property: `reconcile` is **idempotent** (apply actions to a model → reconcile again → `[]`) and **convergent** (≤ 2 rounds).
- Unit: work queue (dedup, single in-flight, dirty requeue, backoff) with FakeClock; expectations TTL.
- Sim: S3 after every event, L2 after quiescence; canary `disable_expectations` → S3 violation found.
- B1: convergence latency.

## 7. Revisit when
B1 shows queue latency dominating (add workers per controller, which needs per-key locking already provided by `processing`), or reconcile costs grow superlinearly.

## 8. Related & references
ADR-0006, ADR-0007, ADR-0008; Kubernetes sample-controller & client-go `workqueue`; *Programming Kubernetes* (Hausenblas & Schimanski), ch. 1 & 3; ReplicaSet controller expectations.
