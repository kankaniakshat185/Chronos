# ADR-0022: MVP scope (formerly "Week-1 MVP"): temporary shortcuts and their pay-down plan

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Accepted (2026-11-04, `v0.1-mvp`; → Deprecated at end of PM-3; → Superseded at Stage 1 exit) | 2026-10-24 (revised 2026-11-04) | Stage 1, Part A | — | — |

> **Revised 2026-11-04:** the plan was split into Stage 1 (Orchestration) and Stage 2 (GPU/CUDA, deferred). The MVP now demonstrates the hard-constraint rule with a generic extended resource (`chronos.dev/slot`) instead of a simulated GPU. The original one-week framing is retired; the plan now defines order and scope only, with no hour estimates.

## 1. What
The MVP deliberately ships a **vertical slice with documented, temporary shortcuts**:
- polling loops instead of watch/informers;
- no expectations or assume cache;
- no fencing or shim;
- no WAL;
- one-phase deletion;
- "replace" instead of restart-in-place;
- a crash-stop-only node failure model;
- extended resources demonstrated with a generic `chronos.dev/slot` (no GPU code at all; GPU is Stage 2, deferred).

Every shortcut is listed, justified, flagged in the README's Limitations and scheduled for removal (PLAN.md B.2), mostly within PM-1 to PM-3.

## 2. Why
### Problem
The MVP was originally framed as a short slice for an internship-interview demo. That framing is retired, but the reason for a thin, honest slice still holds: it puts a working, tested system in place before the larger blocks start. Two bad ways to get a fast slice:
1. **Demo-ware:** hard-coded or untested code that only works on stage. It's dishonest and collapses under one probing question.
2. **Silently thinner design:** shortcuts nobody writes down, which become permanent debt and over-claims.
### Forces & constraints
- **Cut features, never tests** (PLAN.md §5.1).
- The pure core (`core/*`) must be the real, final version, so later blocks replace *shells*, not logic.
- Every claim made in the demo must be backed by a test; every gap must be stated (PLAN.md A.8).
- No GPU work in Stage 1 (GPU is Stage 2, deferred): the hard-constraint story is told with a generic extended resource through the X-1/X-2 seams (ADR-0023).
- Very scoped: no complexity beyond the listed tasks.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: thin, tested vertical slice + explicit shortcut register; `chronos.dev/slot` demo** | Real system; honest; keeps the final core; no GPU code in Stage 1 (GPU deferred to Stage 2) | Some shells rewritten in PM-3 | ✅ Chosen |
| A. Follow the original phase order and demo whatever exists | No rework | Early phases have no demoable behaviour | ❌ |
| B. Build the whole Tier 1 shallowly (fencing, WAL, chaos) without tests | Bigger feature list | Violates the project's core standard; over-claims | ❌ |
| C. Slideware / design-only presentation | Zero build risk | No running system | ❌ |
| D. Keep the simulated-GPU demo (the pre-2026-11-04 version) | GPU-flavoured story | Puts GPU-specific code into Stage 1, against the Stage 1/2 split | ❌ (the slot demo makes the same point; GPUs plug in at Stage 2) |

**A** gives the right order for a long project but no early slice. **B** is exactly what this project exists to avoid. **C** wastes the chance to show real code. **D** told a GPU-flavoured story, but the extended-resource rule is the real mechanism: `chronos.dev/slot` shows it with no GPU code, and Stage 2 reuses it unchanged.

## 4. How
### Components & files
All MVP code lives in the final module layout (PLAN.md §4.6). The MVP-only parts are the polling `run()`/`tick()` loops in `controllers/deployment.py`, `controllers/scheduler.py`, `controllers/node_lifecycle.py` and the sync loop in `agent/agent.py`; PM-3 (U3.1) replaces them with informers and watches while the pure `core/*` functions stay unchanged.
### Mechanism: the shortcuts and why each is safe *for now*
| Shortcut | Why it's safe in the MVP | Becomes unsafe when | Removed in |
|----------|-------------------------|---------------------|------------|
| Polling over the **authoritative** store (no watch) | Level-triggered full recompute each tick | Never unsafe; just unscalable | PM-3 |
| No expectations / assume cache | Reads aren't cached, so there's no stale-read window | **The moment any controller reads from a cache.** They must ship in the same change as informers. | PM-3 |
| No fencing / shim | Fault model is crash-stop of the whole node | Partition, pause, or agent-only crash | ~~PM-1~~ paid down (2026-11-05) |
| No WAL | Documented: restart = total state loss | Any `chronosd` restart | PM-2 |
| One-phase deletion | Agent stops unknown processes on its next poll | Never unsafe; bounded run window | ~~PM-1~~ paid down (2026-11-05) |
| Crash → replace (no backoff) | Converges | Crash loops churn pods | ~~PM-1~~ paid down (2026-11-05) |
| No workload-type registry / resource providers yet (X-4/X-5) | Only `generic` workloads; extended resources are counted, not assigned | Claims about per-unit assignment, which aren't made | ~~PM-1~~ paid down (2026-11-05) |
| Demo timings without the fencing inequality | No fencing to violate | When fencing lands (validation added in U1.1) | ~~PM-1~~ paid down (2026-11-05) |
| Extensions in-process only (no plugin loader) | All code is in-tree | A third party needs out-of-tree plugins | Stage 2 entry, if needed (ADR-0023) |
### ADR implementation status at `v0.1-mvp`
| ADR | Status at MVP | What's missing |
|-----|---------------|----------------|
| 0001–0004, 0006, 0007 | **Accepted** | — |
| 0005, 0010, 0012 | **Accepted** at the end of PM-1 (2026-11-05) | — |
| 0009 | Proposed (partial) | informer, work queue, expectations, resync |
| 0011 | Proposed (partial) | startup grace, mass-eviction guard |
| 0013 | Proposed (partial) | assume cache, `MostAllocated` (X-3 registry done) |
| 0014 | Proposed (partial) | memory-limit enforcement (X-1/X-2 done) |
| 0017 | Proposed (partial) | `workload_type` validation needs X-4 (PM-1); kind dispatch via X-6 (PM-3) |
| 0023 | Proposed (partial) | X-1–X-5, X-8 and early X-9 done (X-4/X-5/X-8 in PM-1); X-6, X-7 and final X-9 later |
| 0008, 0015, 0016 | Proposed | not started |
| 0018 | Deferred (Backlog) | not scheduled |
| 0019, 0020, 0021 | Deferred – Stage 2 | not scheduled |
### Configuration
MVP demo config: `heartbeat 0.5 s`, `node_grace 2 s`, `eviction 5 s`, controller tick 250 ms, agent poll 500 ms. Validation: `grace ≥ 3 × interval`, `eviction > grace`.
### Edge cases & failure handling
- **Someone asks about an unsupported scenario:** answer from PLAN.md A.8 "If asked directly".
- **A shortcut turns out to hide a real bug during the MVP:** fix it if it's small; otherwise add it to A.8 and the B.2 register, never paper over it.

## 5. Consequences
**Positive:** a real, tested, demoable system early; honest scope; the final core logic is written once; a clear pay-down plan.
**Negative / accepted costs:** the PM-3 shell rewrite; the MVP is unsafe outside crash-stop until PM-1; state is lost on restart until PM-2; no GPU-specific demo (by design; GPU is Stage 2, deferred).
**Risks & mitigations:** shortcuts becoming permanent → B.2 register with owners and blocks, and the buffer rule never drops PM-1–PM-3; over-claiming → the A.4 ✅/⚠️/❌ table and A.8 shown with the demo.

## 6. Verification
- MVP exit criteria (PLAN.md A.9), including early PX.9.
- Each B.2 row closes only with its listed test passing (row 11 is carried into Stage 2 explicitly).
- PM-3 regression gate: the MVP integration suite and the pure-core property tests pass **unchanged** after the shells are replaced (TESTING.md §12).

## 7. Revisit when
At the end of PM-1, PM-2 and PM-3 (status review of each row); and immediately if a deadline forces new shortcuts, which go into this ADR's register, not into silence.

## 8. Related & references
ADR-0005, 0006, 0008, 0009, 0010, 0011, 0012, 0013, 0014, 0015, 0017, 0023; PLAN.md §5, Stage 1 Part A/B; TESTING.md §12.
