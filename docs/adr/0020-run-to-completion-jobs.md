# ADR-0020: Run-to-completion `Job` kind

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Deferred – Stage 2 (after Stage 1 exit; hardware lined up) | 2026-10-23 | Stage 2 (P9) | — | — |

> **Deferred to Stage 2 (GPU/CUDA)** on 2026-11-04: the plan was split into Stage 1 (Orchestration) and Stage 2 (GPU/CUDA). This decision is kept unchanged and is not scheduled. It plugs into the Stage 1 seams in [ADR-0023](0023-extension-seams.md); see [PHASE2-EXTENSION-POINTS.md](../PHASE2-EXTENSION-POINTS.md).

## 1. What
Add a `Job` kind for workloads that **finish**: one pod at a time, `completions: 1`, retried on genuine failure up to `backoff_limit`, and **rescheduled without consuming the retry budget** when its pod is lost to node failure. The workload's exit code is the success signal.

## 2. Why
### Problem
The GPU extension runs CUDA tasks that compute a result and exit. Deployments (ADR-0009) keep `replicas` *active* pods forever: a pod that exits 0 becomes `Succeeded`, which no longer counts as active, so the controller creates another, and the CUDA task re-runs endlessly. There's also no place to record "this task succeeded/failed" as a final outcome, which the spec needs ("capture its result/exit status as the workload's success/failure signal").
### Forces & constraints
- Must reuse the controller runtime, expectations and pure-core pattern (ADR-0006, ADR-0009).
- Node loss mid-run must reschedule (spec's failure/recovery test), and that must not be confused with the task itself failing.
- Fencing (ADR-0012) must still guarantee at most one running copy during partitions.
- Keep it small: no parallelism, no indexed completions, no cron.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: minimal `Job` kind (completions = 1, backoff_limit)** | Correct semantics; small; mirrors K8s Job | One more controller | ✅ Chosen |
| A. Deployment whose CUDA task loops forever | No new kind | Not how batch GPU work behaves; no success/failure signal | ❌ |
| B. Bare pods with `restart_policy: Never` | No controller | Nothing reschedules after node loss, which fails the spec's recovery test | ❌ |
| C. Full K8s Job (parallelism, completions > 1, indexed) | Feature-complete | Scope creep; no extra insight | ❌ |
| D. Count NodeLost against backoff_limit | Simpler counter | Infrastructure failures would fail user jobs | ❌ |

**A** fakes the workload's nature to fit the abstraction. **B** has no owner to react to eviction. **C** multiplies the state space without teaching anything new. **D** conflates "my code crashed" with "the machine died", and K8s Jobs distinguish these too (pod failure policy / disruption conditions).

## 4. How
### Components & files
- `src/chronos/api/job.py`: `JobSpec`, `JobStatus`.
- `src/chronos/core/job.py`: pure reconcile.
- `src/chronos/controllers/job.py`: shell (runtime from ADR-0009).
- `chronosctl get|describe|delete job`.
### Mechanism
```python
def reconcile_job(job: Job, pods: Sequence[Pod], exp: Expectations) -> list[Action]:
    if job.status.condition in (COMPLETE, FAILED) or not exp.satisfied(): return []
    if any(p.phase == SUCCEEDED for p in pods):
        return [SetJobCondition(COMPLETE, exit_code=0, message=last_message(pods))]
    genuine_failures = [p for p in pods if p.phase == FAILED and p.status.reason != "NodeLost"]
    if len(genuine_failures) > job.spec.backoff_limit:
        return [SetJobCondition(FAILED, exit_code=last_exit(genuine_failures), message=...)]
    if not any(p.is_active() for p in pods):
        return [CreatePod(job.template_pod(attempt=len(pods)))]    # after retry backoff delay
    return [UpdateStatus(...)] if status_changed else []
```
- **At most one active pod:** guaranteed by expectations (no duplicate creates on a stale cache) plus fencing (an evicted pod's process is dead before its replacement is created, ADR-0012).
- **Retry delay:** exponential (10 s → 5 min cap; shrunk in tests) between genuine failures, via `requeue_after`.
- **Restart policy:** `Never` (default; each retry is a new pod, and attempts stay visible) or `OnFailure` (the agent restarts in place, counted in `restart_count`; still bounded by `backoff_limit`). `Always` is rejected for Jobs.
- **GC:** finished Jobs keep their pods for inspection (`describe`); deleting the Job deletes them.
- **Status:** `active`, `succeeded`, `failed` (genuine), `evictions`, `condition`, `last_exit_code`, `message` (last stdout line from the pod, ≤ 256 chars).
### Configuration
| Param | Default |
|-------|---------|
| `spec.backoff_limit` | 3 |
| `spec.completions` | 1 (only value allowed) |
| `job.retry_backoff` | 10 s → 300 s |
### Edge cases & failure handling
- **Node dies mid-run:** pod → `Failed/NodeLost` → a new pod is created (budget untouched) → the scheduler places it on another feasible node (for GPU jobs, only GPU nodes, ADR-0019) → the job **reruns from scratch** (no checkpointing; documented).
- **No feasible node:** the new pod stays `Pending/Unschedulable`; the Job stays `Running` (not Failed). It's a placement problem, not a task failure.
- **Control-plane crash:** the Job and pods are in the WAL; level-triggered reconcile resumes.
- **Job deleted while running:** two-phase pod deletion stops the process.

## 5. Consequences
**Positive:** correct batch semantics for CUDA tasks; a clear success/failure signal; a clean separation of infrastructure failure from task failure; reuses all existing machinery.
**Negative / accepted costs:** one more kind and controller (~150 LOC + tests); reruns from scratch after node loss.
**Risks & mitigations:** an off-by-one in the backoff counting → property tests at the exact boundary.

## 6. Verification
- **L6:** property tests on `reconcile_job` (terminates; ≤ 1 active pod; NodeLost never counted; exactly `backoff_limit + 1` genuine attempts before Failed); sim with node crashes during jobs; canary `nodelost_counts_as_failure` detected.
- Integration (simulated runner, CI): success, retry-then-fail, kill-agent-mid-run → completes elsewhere.
- Real-GPU suite: the same with real kernels.
- S3 extended to Jobs: never more than 1 running process per Job (process table).

## 7. Revisit when
Workloads need parallel completions or checkpoint/resume (a new ADR; checkpointing is explicitly out of scope).

## 8. Related & references
ADR-0009, ADR-0012, ADR-0019, ADR-0021; Kubernetes Job (backoffLimit, pod failure policy, disruption conditions).
