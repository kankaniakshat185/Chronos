# ADR-0006: Functional core, imperative shell, injected clock

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Proposed | 2026-10-21 | P0 | — | — |

## 1. What
All decision logic lives in **pure functions** in `chronos.core` that take state, desired state, `now` and config, and return a list of **actions**. The **shell** (controllers, agent, server) does all I/O and executes those actions. **Time is always injected** through a `Clock` protocol; nothing reads the system clock directly.

## 2. Why
### Problem
Distributed control logic written directly against asyncio, sockets and `time.monotonic()` can only be tested in real time, with mocks. That makes the tests slow, timing-dependent and flaky, and the rare interleavings (a heartbeat delayed *exactly* past a threshold) are never explored. The project's strongest claims (S3/S4 under partitions) need thousands of reproducible interleavings, and that requires running the real logic under simulated time (ADR-0016).
### Forces & constraints
- Must support deterministic simulation and property-based testing.
- The shell still needs asyncio for real concurrency.
- Discipline must be enforceable mechanically, not by memory.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: pure core + thin shell + injected clock** | Millisecond tests, sim-able, Hypothesis-friendly | Some indirection (actions as data) | ✅ Chosen |
| A. asyncio everywhere, tested with real time + mocks | Fastest to write at first | Flaky, slow, no simulation | ❌ |
| B. Virtual-time asyncio loop for everything | Runs real async code under fake time | Scheduling order not seedable; faults still need simulating | ⏸ Possible complement |
| C. Actor framework | Message-passing structure | New concept + dependency; testing not easier | ❌ |

**A** is the usual source of flaky distributed tests, and it rules out simulation. **B** is attractive and could complement this design later, but asyncio's ready-queue order isn't a controllable source of nondeterminism, so it can't drive seed-based exploration. **C** adds a framework without solving the testing problem.

## 4. How
### Components & files
```
src/chronos/core/          # PURE: reconcile.py, schedule.py, node_health.py, fencing.py, agent_sync.py, rollout.py, actions.py
src/chronos/common/clock.py
src/chronos/controllers/, agent/, server/   # shell
tests/unit/test_architecture.py             # enforcement
```
### Mechanism
```python
# common/clock.py
class Clock(Protocol):
    def monotonic(self) -> float: ...       # for every timeout/lease
    def wall(self) -> datetime: ...         # display only
    async def sleep(self, s: float) -> None: ...

class FakeClock:                            # tests
    def advance(self, s: float) -> None: ...  # wakes sleepers whose deadline passed
```
```python
# core/reconcile.py: pure
def reconcile(dep: Deployment, pods: Sequence[Pod], expectations: Expectations) -> list[Action]: ...

# controllers/deployment.py: shell
async def reconcile_key(self, key: str) -> Result:
    dep, pods = self.cache.deployment(key), self.cache.pods_owned_by(key)
    for action in core.reconcile(dep, pods, self.expectations.get(key)):
        await self.execute(action)          # StoreClient calls, CAS
```
- **Actions** are frozen dataclasses (`CreatePod`, `DeletePod`, `MarkCondition`, `StartProcess`, `StopProcess`, `ReportStatus`, `Fence`, ...), so tests assert on data, not on mock call sequences.
- **Randomness** (jitter, tie-breaks if any) is passed in as `random.Random`.
- **Enforcement:** `test_architecture.py` walks the AST of every `chronos.core` module and fails if it imports `asyncio`, `time`, `os`, `subprocess`, `socket`, `httpx`, `random` (the module; the type is passed in), `datetime.now` callers, etc. A ruff `TID251` rule bans `time.monotonic`/`time.time` outside `common/clock.py`.
- **Store:** synchronous transactional core (`Store.apply(mutation) -> Event`) with no awaits inside, wrapped by an async façade. This lets the sim call it directly.
### Configuration
None; `Clock` is chosen at process start (`RealClock`) or test setup (`FakeClock`, sim clock).
### Edge cases
- Logic that "needs" I/O mid-decision (e.g. read another object): restructure so the shell fetches it first and passes it in.
- Long-running pure functions (scheduling 5 000 pods) are fine, but the shell must call them in bounded batches so the event loop isn't blocked for long.

## 5. Consequences
**Positive:** unit tests in milliseconds; Hypothesis generates states directly; the simulator reuses the real logic; reasoning is local.
**Negative / accepted costs:** indirection (action types + executor); some up-front design for each feature.
**Risks & mitigations:** bugs in the glue aren't covered by the sim → integration and chaos tests check the same invariants against the real shell; any shell bug found gets a regression test at the lowest possible layer.

## 6. Verification
- `test_architecture.py` (import ban) on every PR.
- T1/T2 suites run with no sockets and no sleeping (wall-time budget asserted: < 30 s total).
- Sim determinism test: same seed → same trace hash.

## 7. Revisit when
Chaos tests keep finding shell-only bugs that the sim can't express. Then consider a virtual-time asyncio loop (alternative B) as an additional layer.

## 8. Related & references
ADR-0016; Gary Bernhardt, "Boundaries" (2012); "Functional core, imperative shell".
