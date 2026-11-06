# ADR-0016: Deterministic simulation testing for control logic

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Accepted | 2026-11-06 | P5 | — | — |

## 1. What
A **discrete-event simulator** runs the *real* store and core logic under **virtual time** with a **single seeded RNG**, injecting network, process, node and disk faults. It checks every safety invariant after **every event** and liveness after quiescence. Any failure is reproducible from its seed and can be minimized automatically.

## 2. Why
### Problem
The bugs that matter in an orchestrator live in rare interleavings: a heartbeat response delayed just past a threshold, a watch event arriving after a CAS retry, a crash between fsync and apply. Real-process tests take seconds per scenario, can't control interleavings, and when they do find a bug it often never reproduces. Concrete case: the "measure the fence from ack time" bug (ADR-0012) only shows up when a response is delayed by more than `E − F − K`, which is almost impossible to hit on purpose with real processes.
### Forces & constraints
- Needs thousands of scenarios per minute.
- Must exercise the **real** decision code, not a model of it.
- Must be bit-for-bit reproducible.
- Build budget: ~18 h.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: discrete-event sim over the pure core + real store** | Fast, reproducible, real logic, minimizable | Doesn't cover the async shell; fault model is an assumption | ✅ Chosen |
| A. Real-process chaos only | Fully real | Slow, non-reproducible, low interleaving coverage | ⏸ Kept as T6, not sufficient alone |
| B. Model checking (TLA+) | Exhaustive for the spec | Checks a spec, not the code | ⏸ Optional spec for fencing |
| C. Virtual-time asyncio loop over the real shell | Covers shell code | Scheduling not seed-controllable; faults still simulated | ⏸ Possible later layer |

**A** stays as the reality check, but on its own it can't give coverage or reproducibility. **B** would beautifully verify the fencing *protocol*; the sim verifies the *implementation*, so they're complementary. **C** might be added if chaos runs keep finding shell-only bugs.

## 4. How
### Components & files
```
sim/
  engine.py       # event queue, virtual clock, RNG
  network.py      # links, delay/drop/partition
  nodes.py        # SimAgent (runs core.agent_sync / fencing), SimProcess
  control.py      # SimControlPlane: real Store + core controllers driven step-wise
  disk.py         # SimDisk: synced vs unsynced bytes (for WAL)
  faults.py       # fault + action schedule generator
  invariants.py   # shared with chaos checker (chronos.testing.invariants)
  __main__.py     # CLI: --seed --seeds --trace --minimize --canary
```
### Mechanism
```python
class Sim:
    def __init__(self, seed):
        self.rng = random.Random(seed); self.now = 0.0; self.q = []   # heap of (t, seq, event)
    def at(self, t, ev): heappush(self.q, (t, next(self.seq), ev))   # seq ⇒ deterministic tie-break
    def run(self, until):
        while self.q and self.q[0][0] <= until:
            self.now, _, ev = heappop(self.q)
            ev.fire(self)                     # may schedule more events
            self.trace.append(ev.describe())
            check_safety(self.state())        # S1–S8 after EVERY event
```
- **Messages** (heartbeat, watch response, bind, status) are events delivered after `link.delay(rng)`, or dropped with probability `p`, or held if partitioned.
- **Agent pause** = the agent stops processing events but its SimProcesses keep "running" (the dangerous case).
- **Controller shell in the sim:** the informer cache is updated by delivered watch events (so it can be stale); the work queue is drained one key per event, so interleavings between controllers are controlled by the RNG.
- **Control-plane crash:** discard everything except `SimDisk.synced`, run the real recovery code, rebuild controllers.
- **Run shape:** generate cluster + schedule (120 s virtual) → run → heal all → run to the liveness bound → check L1–L5.
- **Determinism rules:** no `dict`/`set` iteration where order matters without sorting; no `id()`/hash-based ordering; no wall time. A test runs the same seed twice and compares SHA-256 hashes of the traces.
- **Minimization:** greedy delta-debugging over the fault/action list: try removing each entry; keep the removal if the same invariant still fails.
- **Canaries:** `--canary <name>` flips one protection off (TESTING.md §6.5); CI asserts each is detected within its budget.
### Configuration
| Param | PR | Nightly |
|-------|----|---------|
| seeds | 200 | 10 000 |
| virtual duration | 120 s | 120–600 s |
| nodes | 2–8 | 2–32 |
### Edge cases & failure handling
- **Sim bug vs system bug:** each failure report includes the trace, so you decide which; sim bugs get fixed in `sim/`, with a note in the devlog.
- **Divergence from the real shell:** every chaos-found bug must be reproduced in the sim (new fault type if needed) before it's considered fixed.

### Implementation status (2026-11-06)
Implemented as `src/chronos/sim/` (`python -m chronos.sim --seed N [--trace F] [--minimize] [--canary C]`): `engine.py` (event heap ordered by time and sequence, one seeded RNG, SHA-256 of the trace), `network.py` (per-direction links: delay, drop, spikes, cuts; requests and replies are separate messages; client timeouts as in `HttpStoreClient`), `disk.py` (fsync recorded, not performed; a crash truncates every file to its last synced size), `control.py`, `nodes.py`, `faults.py`, `invariants.py`, `canaries.py`, `run.py`. Deviations from the sketch: the **real `Agent`** runs too, not a re-implementation. Its coroutines are sim `Task`s that may only await sim futures, and the control plane's coroutines are driven by `run_sync()`, which works because in full-sync mode they never suspend. That needed four injection points in production code (a heap-based `WorkQueue`, `rng`/`new_epoch` on `Agent`, `rng` on `Informer`, `new_name` on `DeploymentController`) and one read-only accessor, `Store.view()`. Process starts take 1-20 ms; the engine raises on livelock or runaway. The sim uses a 16-event watch history so compaction is common. `measure_fence_from_ack_time` also removes the heartbeat timeout, which alone masks it (testing-the-tests.md 4.10). Results: the sim found three real bugs on its first runs (an agent renewing its lease before the shim could kill a frozen node's replaced workload; a resumed agent restarting evicted pods from a stale cache; an informer race between a relist and an in-flight watch), all fixed with unit tests and regression seeds in `tests/sim/regressions.txt`. All 10 canaries are detected within the first 6 seeds. Budget: about 1 s per seed per core; CI runs 200 seeds, the regressions and every canary per PR, and 10 000 seeds nightly.

## 5. Consequences
**Positive:** orders of magnitude more interleavings than real tests; every bug reproducible from a seed; small counterexamples; measurable test strength (canaries); a standout resume and interview point.
**Negative / accepted costs:** ~18 h to build; must be kept in step with the shell; only as good as its fault model.
**Risks & mitigations:** false confidence → canaries plus the chaos layer checking the same invariants.

## 6. Verification
- Determinism test (same seed → same trace hash).
- Every canary detected within its seed budget.
- Nightly 10 k seeds green; `tests/sim/regressions.txt` replayed on every PR.

## 7. Revisit when
Chaos keeps finding bugs the sim can't express (extend the fault model, or add the virtual-time asyncio layer), or sim runtime exceeds the PR budget (parallelize with xdist, trim scenarios).

## 8. Related & references
ADR-0006, ADR-0012, ADR-0015; Will Wilson, "Testing Distributed Systems w/ Deterministic Simulation" (Strange Loop 2014); TigerBeetle VOPR; Zeller, "Simplifying and Isolating Failure-Inducing Input" (delta debugging).
