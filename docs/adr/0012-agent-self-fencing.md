# ADR-0012: Self-fencing (agent + per-pod shim) to bound duplicate replicas during partitions

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Accepted | 2026-11-05 | P4 | — | — |

## 1. What
A node whose lease with the control plane has expired **kills its own workloads before the control plane can evict and replace them**. The lease is measured from the **send time** of the last acknowledged heartbeat. It's enforced by two independent parties: the **agent**, and a tiny **`chronos-shim`** wrapping each workload. The shim watches a lease file and enforces fencing even if the agent itself is dead. Config validation guarantees `fence_timeout + termination_grace + drift_margin < eviction_timeout`.

**Scope of the guarantee (stated exactly this way in the README):** no duplicate replicas under **agent pause, agent crash and network partition**. For a **whole-node freeze** (a VM pause that also stops the shims), there's a bounded window of **at most one shim poll (100 ms)** after resume before the stale workload is SIGKILLed. **Accepted cost (Option A):** a control-plane outage longer than `fence_timeout` stops every workload.

## 2. Why
### Problem
When a node goes silent, the control plane evicts its pods after `eviction_timeout` and starts replacements (ADR-0011). If the node is **partitioned, paused or its agent crashed**, but the workloads are still alive, the deployment now has more than `R` running replicas. For a workload that assumes it's a singleton (a leader, a cron job, a writer to an external DB), that's a correctness bug.

Two concrete scenarios:
1. **Partition / SIGSTOP of the agent:** the agent can't reach the control plane, but its workloads keep running.
2. **`kill -9` of the agent:** the workloads keep running (own sessions, ADR-0005), and a *dead* agent can't fence anything. Real Kubernetes has the same gap; container runtimes use per-container shims (containerd-shim, conmon) so containers don't depend on the kubelet process.

Kubernetes Deployments don't guarantee at-most-R under partitions (StatefulSets avoid duplicates by never rescheduling until the node is confirmed gone). Chronos takes an explicit, provable position instead.
### Forces & constraints
- Must be provable with stated assumptions, not "probably fine".
- Must not depend on the agent being alive.
- Must not depend on synchronized wall clocks.
- Cost: availability during control-plane outages (see Consequences).

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: lease-based self-fencing in agent + per-pod shim** | Provable bound; survives agent death | Workloads stop during long CP outages; one extra process per pod | ✅ Chosen |
| A. Do nothing (K8s Deployment semantics) | Simplest; maximally available | Duplicates under partition; S3 can't be stated | ❌ |
| B. Never reschedule until the node is confirmed dead (StatefulSet-style) | Safe | Not live: a partitioned node blocks its workloads forever | ⏸ Possible per-deployment option |
| C. Agent-only fencing (no shim) | Fewer processes | Fails if the agent is killed or crashes | ❌ |
| D. Fencing tokens checked by downstream resources | Gold standard for shared resources | Needs workload/downstream cooperation; outside Chronos's control | ⏸ Documented as the complete solution |
| E. STONITH (power off the node) | Definitive | Needs out-of-band control; not applicable to processes | ❌ |
| F. Linux `PR_SET_PDEATHSIG` | Workload dies with its parent | Linux-only; doesn't cover partition/pause; parent = thread semantics are tricky | ❌ |
| G. Smarter fencing: fence only when the control plane is reachable but rejects the node, or when the node is cut off but its peers aren't | Workloads survive a plain control-plane outage | Needs peer-to-peer health checks or a third-party witness; many more cases to prove | ❌ (too complex; HA is the real fix, Backlog P11.x) |

**A** gives up the project's most interesting guarantee. **B** is safe but trades liveness away completely. **C** was the first draft; it fails scenario 2, which led to the shim. **D** is what you'd tell an interviewer is needed for *end-to-end* safety: fencing bounds when old processes die, but a delayed write already in flight could still land; tokens make the resource reject it. Chronos exposes a token (`CHRONOS_POD_UID` plus the binding revision) that workloads *may* use. **F** covers only half of one scenario, on one OS. **G** is the tempting fix for the availability cost, but an agent can't tell "the control plane is dead" from "I am partitioned" without extra machinery (peer gossip or a witness), and every new branch weakens the proof. Chosen instead (**Option A**): accept the cost, document it, and leave availability to control-plane HA (ADR-0018, Backlog).

## 4. How
### Components & files
- `src/chronos/core/fencing.py`: pure `should_fence(last_ok_send, now, cfg)`.
- `src/chronos/agent/heartbeat.py`: tracks `last_ok_send`, writes the lease file.
- `src/chronos/shim/main.py`: `chronos-shim` entry point (~100 LOC, stdlib only).
- `src/chronos/common/config.py`: inequality validation.
### Mechanism
**Agent side:**
```python
async def heartbeat_loop():
    while True:
        t_send = clock.monotonic()
        try:
            await client.heartbeat(node, epoch, timeout=cfg.heartbeat_timeout)
            last_ok_send = max(last_ok_send, t_send)        # SEND time, not ack time
            lease_file.write_atomic(last_ok_send)            # tmp + rename
        except (Unavailable, Timeout): pass
        if core.fencing.should_fence(last_ok_send, clock.monotonic(), cfg):
            enter_fenced()                                   # stop all pods, start nothing
        await clock.sleep(cfg.heartbeat_interval)
```
**Shim side** (one per pod; the process-group leader; the workload is its child):
```python
child = Popen(cmd)                                          # same process group
forward SIGTERM → child
every 100 ms:
    if child exited: exit(child.returncode)
    last_ok_send = read(lease_file)                          # missing/garbled ⇒ treat as stale
    age = time.monotonic() - last_ok_send
    if age > E:                                              # frozen past the eviction point: replacements may exist
        child.kill(); exit(FENCED_EXIT_CODE)                 # SIGKILL now, no grace
    if age > F:
        child.terminate(); wait K; child.kill(); exit(FENCED_EXIT_CODE)
```
`time.monotonic()` is a **system-wide** clock on Linux (CLOCK_MONOTONIC) and macOS (mach_absolute_time), so agent and shim compare values from the same clock.

**Why send time, and the safety proof sketch.** Let `t_s` be the server's receipt time of the last heartbeat it accepted from this node. Its request was sent at `t_send ≤ t_s`. The server can't mark the node NotReady before `t_s + E`. The agent's and shim's `last_ok_send ≤ t_send ≤ t_s`, so every workload is dead by
```
last_ok_send + F + K  ≤  t_s + F + K  <  t_s + E  ≤  eviction time       ⟸   F + K < E
```
Replacements are created only after eviction, so old and new instances never overlap. Measuring from **ack arrival** instead would let a slow response push `last_ok` past `t_s` by up to the request timeout, which breaks the inequality (canary `measure_fence_from_ack_time`).

**Clock rate drift** between hosts (ρ, typically < 10⁻⁴): the condition becomes `(F + K)(1 + ρ) < E(1 − ρ)`, covered by `drift_margin`. On one host ρ = 0.

**Why the immediate-SIGKILL rule:** normally the lease age crosses `F` first and the graceful path finishes before `E` (the proof above). The `age > E` branch only fires when the shim itself was frozen (whole-node freeze): on resume it sees an already-expired lease, and it kills without the `K` grace, so the overlap with replacements is at most one poll interval.

**Whole-node freeze bound.** If the agent, shims and workloads are all frozen, nothing can act during the freeze. On resume, each shim's next poll (≤ 100 ms) sees `age > E` and SIGKILLs. So S3/S4 hold except for that window. This assumes the node's monotonic clock advanced across the freeze: true on Chronos's single host (shared clock); on real VMs it depends on the hypervisor.

**Leaving Fenced:** only via a successful heartbeat. Pods evicted meanwhile no longer exist in the store, so they aren't restarted; pods that are still bound (e.g. after a control-plane outage, when startup grace prevented evictions) are restarted. The shims of fenced pods have already exited.
### Configuration
| Param | Default | Rule |
|-------|---------|------|
| `fence_timeout` F | 6.0 s | > 3 × `heartbeat_interval` (avoid fencing on blips) |
| `termination_grace` K | 2.0 s | |
| `drift_margin` | 1.0 s | |
| `eviction_timeout` E | 10.0 s | **F + K + drift_margin < E** (validated at startup by both `chronosd` and agents; the server rejects agents registering with incompatible values) |
| `shim.poll` | 100 ms | adds ≤ 100 ms; included in K's budget; also the whole-node-freeze window |
| shim `--eviction-timeout` | = E | enables the immediate-SIGKILL branch |
### Edge cases & failure handling
- **Shim killed by `kill -9`:** its child keeps running → out of the fault model (documented). Chaos targets agents/control plane, not shims. Orphan cleanup on agent restart still catches it.
- **Lease file unreadable/corrupt:** the shim treats it as stale and fences (fail safe).
- **Process ignoring SIGTERM / in D-state:** SIGKILL after K handles the first; uninterruptible sleep is a documented residual assumption.
- **Control plane restarted within F:** agents never fence; workloads keep their PIDs. Startup grace (ADR-0011) means the server never evicts earlier than `E` after restart, so the inequality still holds.
- **Control plane down longer than F (accepted, Option A):** every agent and shim fences, so **all workloads stop**. Agents can't distinguish this from a partition, so this is the safe outcome. When the control plane returns, startup grace prevents evictions; agents heartbeat, leave Fenced mode and restart the pods still bound to them (new PIDs). Real Kubernetes differs here: the kubelet keeps pods running while the API server is down.
- **Whole-node freeze:** see the bound above.
- **Agent restarted with a new epoch while shims are running:** orphan cleanup kills the old shims; fresh pods start only for pods still bound to this node.

### Implementation status (2026-11-05)
Implemented as described: `core/fencing.py`, `shim/__main__.py` (run as `python -m chronos.shim`), the agent's heartbeat, fence-watch and sync loops, and registration-time validation of F + K + drift < E (the server returns E, which the shims use). Deviations: the fence watch is its own loop (the heartbeat loop can sit in a register retry while the server is down); a stop caused by fencing is recorded as `FENCED_EXIT_CODE` (86) so the pod is restarted, not failed, after the agent leaves Fenced mode; when the lease is past E the shim SIGKILLs its whole process group, itself included. Integration tests cover agent pause, agent `kill -9`, a cut partition (a minimal TCP proxy; the full fault proxy is PM-6), a whole-node freeze (about 65 ms window measured) and a short pause that must not fence. Still to come: the sim and chaos canaries (PM-4, PM-6) and the control-plane outage path (U2.2, PM-2).

### Amendment (2026-11-06, found by the simulator, ADR-0016)
The agent applies the shim's second rule too: if its lease is older than `eviction_timeout`, it SIGKILLs every workload at once (no grace), in every fence check and **before writing any new lease**. Without this, after a whole-node freeze the agent's re-register could renew the lease before the shim's next poll, and the shim would then let a workload whose replacement already runs live for up to K. That broke the "at most one shim poll" bound. Also: after any registration, the agent acts only once its pod cache has reached the registration's store revision, so it never restarts pods the server evicted while it was away.

## 5. Consequences
**Positive:** a precise, provable safety property (S3, S4) that holds even when the agent dies; a strong interview topic (leases, send-time rule, clock assumptions, shims, fencing tokens).
**Negative / accepted costs:**
- **Availability (decided: Option A):** a control-plane outage longer than F stops **every workload** in the cluster. This is the same tradeoff as any lease-based leader, and unlike the Kubernetes kubelet. It is an explicit Stage 1 limitation (PLAN.md B.5 #1); the fix is control-plane HA (ADR-0018, Backlog). A future option: `fencing: none | strict` per deployment.
- **Scope:** "no duplicates" covers agent pause, agent crash and network partition; a whole-node freeze has a bounded one-poll window (PLAN.md B.5 #2). The README must not claim "under any pause".
- One extra process (~15 MB RSS) per pod.
**Risks & mitigations:** fencing on transient blips → F ≫ heartbeat interval; B3/B-fence measures the false-fence rate.

## 6. Verification
- Unit: `should_fence` boundary table; send-vs-ack rule; config inequality validation.
- Shim tests: lease fresh → child runs; stale (> F) → SIGTERM, then SIGKILL after K; very stale (> E) → immediate SIGKILL; missing file → fence; exit code propagation.
- **Sim:** S3/S4 after every event under pause, partition, delay and agent-crash schedules. Canaries `disable_fencing`, `fence_after_eviction`, `measure_fence_from_ack_time` and `agent_only_fencing` (no shim, plus agent crash) must all be detected.
- **Integration:** SIGSTOP agent → workloads gone *before* replacements start; **kill -9 agent → workloads gone before replacements start** (the shim's job); **whole-node freeze** (SIGSTOP agent + shims + workloads past E, then SIGCONT) → stale workloads SIGKILLed within one shim poll of resume; `chronosd` restarted within F → same PIDs; `chronosd` down longer than F → all workloads fenced, then reconverge after restart (L4).
- **Chaos:** proxy `blackhole` + live process-table S3 checker.
- **Benchmark:** availability impact of control-plane outages of 2 / 5 / 8 / 15 s.

## 7. Revisit when
HA (ADR-0018) makes control-plane outages rare enough to lengthen F, or a workload class needs availability over safety (add a per-deployment `fencing: none`).

## 8. Related & references
ADR-0005, ADR-0011, ADR-0018; Gray & Cheriton, "Leases" (1989); Kleppmann, "How to do distributed locking" (fencing tokens); *DDIA* ch. 8; containerd-shim / conmon design.
