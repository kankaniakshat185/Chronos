# ADR-0010: Pull-based node agents

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Proposed | 2026-10-21 | P2 | — | — |

## 1. What
Agents **pull** their work: they register, heartbeat, and watch for pods bound to them. The control plane **never connects to an agent**. Each agent is itself level-triggered: it continuously makes its local processes match the pod list the control plane holds for it.

## 2. Why
### Problem
Bound pods must reach the right agent reliably, including after agent restarts and partitions. With push, the control plane has to track agent addresses, keep per-node retry queues, and still reconcile anyway when a push is lost. Concrete failure: a push of "delete pod P" is lost during a blip, and with no pull-based reconciliation P runs forever.
### Forces & constraints
- The control plane should hold no per-connection state (it's simpler to make crash-safe).
- An agent must be able to rebuild its desired state from scratch after a restart.
- Must be faithful to kubelet (interview relevance).

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: agent pulls via long-poll watch** | Stateless control plane; restart-safe agent; partition-friendly | Latency = watch round trip; CP can't force-stop a partitioned node | ✅ Chosen |
| A. Control plane pushes (`POST agent:port/pods`) | Lowest latency | Address tracking, reachability, retry queues, still needs reconciliation | ❌ |
| B. Agent polls with plain GET every N s | Simplest | Latency = N; load = nodes/N | ❌ |

**A** moves delivery state into the control plane and still needs a pull-style resync to be correct, so it's strictly more machinery. **B** is the long-poll design without the "block until change" part, trading latency for nothing.

## 4. How
### Components & files
- `src/chronos/agent/main.py`: wiring + `TaskGroup` of the loops below.
- `src/chronos/agent/heartbeat.py`: registration, heartbeat, lease file (ADR-0011/0012).
- `src/chronos/agent/sync_loop.py`: pod informer + local sync.
- `src/chronos/core/agent_sync.py`: pure diff.
### Mechanism
Agent loops (all in one `asyncio.TaskGroup`):
1. **Register:** `create Node` or, if it exists, CAS-update `spec.capacity` and `status.agent_epoch`.
2. **Heartbeat:** every `heartbeat_interval` (ADR-0011).
3. **Pod informer:** list+watch `pods?node=<self>` (ADR-0008).
4. **Sync** (triggered by informer changes, process exits, and a 1 s tick):
```python
def agent_sync(desired: list[Pod], local: dict[PodUID, LocalProc], fenced: bool) -> list[LocalAction]:
    if fenced: return [StopProcess(u) for u in local]
    want = {p.uid: p for p in desired if not p.is_terminal()}
    acts = [StopProcess(u) for u in local if u not in want]                 # unknown / rebound / deleted
    acts += [StopProcess(p.uid) for p in want.values() if p.is_terminating() and p.uid in local]
    acts += [StartProcess(p) for u, p in want.items() if u not in local and not p.is_terminating()
             and restart_allowed(p, local)]
    acts += [ReportStatus(p, observe(local.get(p.uid))) for p in want.values() if status_differs(...)]
    acts += [RemovePod(p) for p in want.values() if p.is_terminating() and p.uid not in local]
    return acts
```
5. **Status reporting:** `update_status` with CAS, coalesced per pod (only the latest observation is sent).
### Configuration
| Param | Default |
|-------|---------|
| `agent.sync_tick` | 1.0 s |
| `agent.status_coalesce` | 100 ms |
### Edge cases & failure handling
- **Agent restart:** orphan cleanup (ADR-0005), then register with a new epoch, relist, and sync from scratch.
- **Pod rebound elsewhere after a partition heals:** it's no longer in `want` → `StopProcess`. (Normally it's already dead via fencing, ADR-0012.)
- **Status update Conflict:** re-read the pod and re-send if still relevant (the pod may have been deleted meanwhile).

## 5. Consequences
**Positive:** the control plane is stateless about connections; the agent is crash-safe; partitions degrade gracefully; mirrors kubelet.
**Negative / accepted costs:** delivery latency ≈ one watch round trip; the control plane can't instantly stop a partitioned node's workloads, which is exactly why self-fencing exists (ADR-0012).
**Risks & mitigations:** sync bugs that kill wanted processes → `agent_sync` is pure and property-tested (never stops a desired, running, non-terminating pod while not fenced).

## 6. Verification
- Unit/property: `agent_sync` (idempotence; never stops wanted pods unless fenced; always stops unknown ones).
- Integration: bind → process running within ~1 s; agent restart → resync; delete → two-phase removal.
- Sim: S3 after rebind scenarios.

## 7. Revisit when
B1 shows delivery latency matters (switch to streaming watch, ADR-0008), or a pushed command is genuinely needed (e.g. `logs`/`exec`, which would be a separate agent API, as kubelet does).

## 8. Related & references
ADR-0005, ADR-0008, ADR-0011, ADR-0012; kubelet sync loop (`syncLoop`, `SyncPod`).
