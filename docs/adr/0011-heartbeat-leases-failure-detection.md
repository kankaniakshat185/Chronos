# ADR-0011: Heartbeat leases and a node condition state machine

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Proposed | 2026-10-21 | P2 | — | — |

## 1. What
Agents heartbeat every second. The control plane, using **only its own monotonic clock**, moves each node through **Ready → Unknown → NotReady** based on how long it's been silent: Unknown stops new placements, and NotReady triggers eviction. Agent restarts are detected by an **epoch**, the control plane gives everyone a grace period after its own restart, and a guard pauses mass eviction.

## 2. Why
### Problem
To self-heal, the control plane must decide that a node is gone. In an asynchronous network a crashed node and a slow or partitioned one look identical, so every detector trades **detection latency** against **false positives**. False positives are expensive: evicting a live node's pods creates duplicates unless the node fences (ADR-0012), and replacement churn costs capacity.
### Forces & constraints
- Fencing (ADR-0012) needs a **hard upper bound** on when eviction can happen, so the detector must be bounded and simple.
- Short hiccups (GC pause, blip) must not cause eviction.
- Agent clocks can't be trusted by the server.
- A control-plane outage must not look like every node dying.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: fixed timeouts, two thresholds (Unknown / NotReady)** | Bounded, simple, tunable, explainable | Doesn't adapt to jitter | ✅ Chosen |
| A. Single threshold | Simplest | One number must serve both "stop scheduling" and "evict" | ❌ |
| B. Phi-accrual detector | Adapts to observed jitter; suspicion level | No hard bound → complicates the fencing inequality | ⏸ Backlog P12.2 (benchmark vs fixed) |
| C. Gossip (SWIM) | Decentralized, scalable | Chronos has a central control plane; adds a protocol for no gain | ❌ |
| D. Agent-reported timestamps | Less server work | Trusts remote clocks, which skew | ❌ |

**A** forces a bad compromise: a short threshold evicts on blips, a long one schedules onto dead nodes. **B** is more principled and a great comparison study, but the fencing proof needs `eviction_time ≥ t_last_hb + E` as a hard fact. **C** solves a problem Chronos doesn't have. **D** breaks under clock skew.

## 4. How
### Components & files
- `src/chronos/core/node_health.py`: pure condition function.
- `src/chronos/controllers/node_lifecycle.py`: tick loop, transitions, eviction (P4), guards.
- `src/chronos/server/routes/nodes.py`: heartbeat endpoint.
### Mechanism
**Heartbeat endpoint:**
```python
POST /nodes/{n}/heartbeat {agent_epoch}
  node = store.get(Node, n)
  if node.status.agent_epoch != agent_epoch: → 409 {reason: "EpochMismatch"}  # agent must re-register
  last_heartbeat[n] = clock.monotonic()       # server clock, kept in memory (not WAL-ed per beat)
  return {condition: node.status.condition}
```
`last_heartbeat` lives in memory, not in the store, so 1 Hz × N nodes doesn't hit the WAL. The **condition** is persisted only on transitions.

**Pure condition function:**
```python
def condition(silence: float, cfg) -> Cond:
    if silence < cfg.node_grace:       return READY
    if silence < cfg.eviction_timeout: return UNKNOWN
    return NOT_READY
```
**Lifecycle tick (every 0.5 s):** for each node compute `condition(now - last_hb)`; on a change, CAS-write `status.condition` + emit an Event; on → NOT_READY, run eviction (P4: mark pods `Failed/NodeLost`, force-delete).

**Guards:**
- **Startup grace:** after `chronosd` starts (or recovers), set `last_heartbeat[n] = now` for every node. No node can be NotReady before `eviction_timeout` after startup.
- **Mass-eviction guard:** if > 50 % of nodes would transition to NOT_READY in one window, hold them at UNKNOWN, emit `MassEvictionSuspended`, and keep checking. (Assumption: a control-plane-side network fault is likelier than half the cluster dying at once.)
- **Epochs:** a new agent process gets a new `agent_epoch` (uuid4) and re-registers; the node becomes Ready with a fresh lease.
### Configuration
| Param | Default | Constraint |
|-------|---------|------------|
| `heartbeat_interval` | 1.0 s | |
| `heartbeat_timeout` | 0.8 s | < interval |
| `node_grace` | 4.0 s | ≥ 3 × interval |
| `eviction_timeout` | 10.0 s | > `fence_timeout + termination_grace + drift_margin` (ADR-0012) |
| `lifecycle_tick` | 0.5 s | adds ≤ tick to detection latency |
| `mass_eviction_threshold` | 0.5 | |
Config validation refuses to start if the constraints are violated.
### Edge cases & failure handling
- **Heartbeat arrives for an unknown node:** 404 → agent registers.
- **Agent restarts quickly (< grace):** the new epoch replaces the old; the old pods are gone locally (orphan cleanup) and the agent reports the missing processes; the deployment controller replaces them.
- **Network flaps:** Ready ↔ Unknown oscillation only affects scheduling, never eviction.

### Implementation status (2026-11-06)
Implemented: the heartbeat endpoint with epochs, the pure `condition()` function, the lifecycle controller (250 ms tick, CAS transitions), revoking the epoch on NotReady, level-triggered eviction, and **startup grace**: leases live in memory and start empty at boot, so every node's silence is measured from when the restarted server first sees it, and none is NotReady earlier than `eviction_timeout` after a restart (tested by U2.2 (b)). Not yet: the mass-eviction guard.

## 5. Consequences
**Positive:** simple, bounded, explainable; the latency-vs-false-positive tradeoff is measured (B3).
**Negative / accepted costs:** fixed timeouts don't adapt; detection ≥ 4 s and eviction ≥ 10 s by default (tests use smaller values).
**Risks & mitigations:** the mass-eviction guard can delay a genuine large failure → it's logged and visible via Events/metrics; the threshold is configurable.

## 6. Verification
- **L1:** unit boundary table for `condition` (grace ± ε, eviction ± ε); integration: kill -9 agent → Unknown at ≈ grace, NotReady at ≈ eviction (asserted as windows).
- SIGSTOP shorter than grace → no transition; between grace and eviction → Unknown, no eviction.
- Sim: control-plane crash/restart → no premature eviction (startup grace); mass partition → guard holds.
- **B3:** detection latency vs false-eviction rate under injected pause distributions.

## 7. Revisit when
B3 shows an unacceptable tradeoff under realistic jitter (try phi-accrual, Backlog P12.2, keeping a hard cap for fencing), or the cluster size makes per-node heartbeats expensive (batch them, or use K8s-style Lease objects).

## 8. Related & references
ADR-0012, ADR-0018; Hayashibara et al., "The φ Accrual Failure Detector" (2004); Kubernetes `node-monitor-grace-period`, node Lease objects; Chandra & Toueg, unreliable failure detectors.
