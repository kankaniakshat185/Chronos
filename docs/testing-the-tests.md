# Testing the tests: a mutation-testing log

Chronos makes strong claims: "never more than R replicas", "an acknowledged write survives a crash", "a pod never lands on a node without enough slots". Each claim is backed by tests. But a passing test only proves something if it *would fail* when the claim breaks. This log records how that was checked: by deliberately breaking the code that upholds each claim and confirming that a test notices.

It covers the MVP through PM-9a (67 hand-made mutations plus the simulator's 11 canaries so far), and gets a new entry every time a guarantee is added.

---

## 1. Method

**One rule:** for every safety or liveness property, find the line or lines of code that make it true, break them in the most plausible way a real bug would, and run the tests that are supposed to protect that property.

**The loop, for each mutation:**

1. **Pick the guarantee and the code that enforces it.** For example, S2 ("no node is overcommitted") is enforced by the capacity check inside `Store.bind()`.
2. **Write down which tests should catch it,** *before* running anything. If the answer is "I'm not sure", that's already a finding.
3. **Inject the bug** with a one-line, marked edit, so the change is obvious and easy to revert:
   ```bash
   cp src/chronos/store/store.py /tmp/store.bak
   sed -i '' 's/if missing := insufficient(pod.requests, free):/if missing := []:/' src/chronos/store/store.py
   grep -c MUTATION src/chronos/store/store.py   # injected edits carry a "MUTATION" comment
   ```
4. **Run only the tests that should catch it** (fast feedback), and record how many fail.
5. **Restore and prove it's restored:**
   ```bash
   cp /tmp/store.bak src/chronos/store/store.py
   grep -c MUTATION src/chronos/store/store.py   # must print 0
   uv run pytest -q                              # the full suite must be green again
   ```
6. **If the mutation survived,** a test is missing or weak. Write or fix the test until the mutation is caught, then restore the code. The test changes, never the bar.

**What this is not:** an automated mutation tool (mutmut, cosmic-ray) generating thousands of random operator swaps. Those mostly produce trivial or equivalent mutants. Here every mutation is a hand-picked, *plausible* bug against a *named* invariant, which is slower but gives a direct answer to "would my tests catch the bug that matters?". Automating a curated set of these as "canary bugs" behind feature flags is planned for the deterministic simulator (PM-4).

**Mutations are never committed.** Each one lives for a single test run; the backup-and-`grep` step is what guarantees that.

---

## 2. Catalogue

Columns: the guarantee (IDs from the plan's invariant list), the injected bug, the tests expected to catch it, and the result.

### MVP: versioned store (Day 2)

| # | Guarantee | Injected bug | Expected to catch | Result |
|---|---|---|---|---|
| 1 | S5: a stale compare-and-swap never succeeds | `_check_rv`: `if expected != current.rv` → `if False` | stateful model test | ✅ caught |
| 2 | S1: a pod is bound once | `bind`: `if pod.spec.node_name:` → `if False:` | stateful model test | ✅ caught |
| 3 | S2/S11: bind never overcommits | `bind`: `if missing := insufficient(...)` → `if missing := []` | bind property | ✅ caught |

### MVP: scheduler (Day 5)

| # | Guarantee | Injected bug | Expected to catch | Result |
|---|---|---|---|---|
| 4 | S11: extended resources are a hard constraint | `FitsResources` only checks cpu/memory | scheduler properties | ✅ caught ("pending pods fit nowhere") |
| 5 | Placement is deterministic | tie-break `max(sorted(feasible))` → `max(feasible)` | determinism property | ❌ **survived the properties**, caught by a unit test (see 3.1) |
| 6 | Never schedule onto unhealthy nodes | `NodeReady` accepts every node | scheduler properties | ✅ caught |
| 7 | S11, defence in depth | remove the extended check from **both** the filter and the store's bind | S11 property | ✅ caught |

### MVP: reconciliation (Day 6)

| # | Guarantee | Injected bug | Expected to catch | Result |
|---|---|---|---|---|
| 8 | L2: count only active pods | terminating pods counted as active | reconcile unit + properties | ✅ caught |
| 9 | Scale-down disturbs running work last | victim order reversed (Running first) | victim-order unit test, properties | ✅ caught (2 tests) |
| 10 | Keep at most 3 finished pods | keep 4 | GC unit test, property | ✅ caught (2 tests) |

### PM-1: self-fencing and crash handling

| # | Guarantee | Injected bug | Expected to catch | Result |
|---|---|---|---|---|
| 11 | S3/S4 under agent crash | the shim ignores the lease (the plan's `agent_only_fencing` canary) | killed-agent and freeze tests | ⚠️ killed-agent test caught it; **the freeze test passed** (see 3.2) |
| 12 | Whole-node-freeze window is one shim poll | remove the "lease older than E → SIGKILL now" branch | freeze test | ❌ **survived at first** (see 3.3); caught after the test was redesigned |
| 13 | Fencing is measured from heartbeat *send* time | `_accepted(sent)` → `_accepted(clock.now())` (ack time; the plan's `measure_fence_from_ack_time` canary) | agent unit test with a slow reply | ✅ caught |
| 14 | No duplicates after a quick agent restart | remove `cleanup_orphans()` before registering | quick-restart integration test | ✅ caught |
| 15 | Crash-loop backoff grows | constant delay instead of exponential | unit "doubling" + integration "growing gaps" | ✅ caught (2 tests) |
| 16 | Two-phase deletion: the pod stays until its process is gone | finalize even while the process runs | finalize property + integration | ✅ caught |
| 17 | S12: no unit held by two live pods | counting provider doesn't remove handed-out units | S12 property + 2 integration tests | ✅ caught (3 tests) |

### PM-2: durability

| # | Guarantee | Injected bug | Expected to catch | Result |
|---|---|---|---|---|
| 18 | S8: acknowledged writes survive | `update_status` applies to memory without logging | crash harness | ✅ caught (9 cases) |
| 19 | A torn final write is cut off, not fatal | never truncate (always "corrupt") | harness (`wal.torn_write`), truncation property | ✅ caught (5 tests) |
| 20 | Log before memory | `_commit`: apply to memory, *then* log | harness, WAL properties | ❌ **survived** (see 3.4); caught after a disk-full test was added |
| 21 | U2.1: no evictions right after a restart | leases start "infinitely old" (no startup grace) | long-outage restart test | ✅ caught |
| 22 | Group commit: answer only after the sync | `_durable()` returns at once | group-commit "nothing answered before durable" | ✅ caught (2 tests) |
| 23 | Group commit actually syncs | `flush()` skips `durable_sync` | all crash/WAL/group tests | ❌ **survived 56 tests** (see 3.5); caught after a sync-accounting test was added |
| 24 | Full mode actually syncs | `append()` skips `durable_sync` | sync-accounting test | ✅ caught (added together with #23's fix) |
| 25 | (harness check) every harness case really crashes | none: measured instead | — | ❌ **40% of runs never crashed** (see 4.1) |

### PM-3: watch, informers, work queue, expectations, assume cache

For this block the mutations were run by a small script (kept outside the repo) that, for each entry, swaps one exact anchor string for the bug, runs the named tests with `-x`, restores the file and asserts that no `MUTATION` marker is left. Same method, less typing.

| # | Guarantee | Injected bug | Expected to catch | Result |
|---|---|---|---|---|
| 26 | S6: refuse history you don't have | compaction boundary off by one (`revision < floor - 1`) | S6 replay property | ✅ caught. At first it was the watch log's own `slot.revision == rev` assertion that fired; re-run with that assertion removed too, the property caught it on its own (`revision 2 < floor 3 must be refused`) |
| 27 | S6: no silent skips (the plan's `watch_skip_on_compaction` canary) | below the floor, serve from the oldest retained event instead of 410 | S6 replay property | ✅ caught |
| 28 | Node filter sees a pod *leave* | match only the new version of an object | S6 property (node `""` watcher), contract tests | ✅ caught by the property |
| 29 | A watch can't sleep through a write | register the waiter in a task (the original lost-wakeup bug, put back) | deterministic race test | ✅ caught (timeout) |
| 30 | Group commit: no event before it's durable | watch skips `_durable()` | group-commit ordering test (extended to watches) | ✅ caught (`watch answered` came before `sync`) |
| 31 | S3: no duplicate creates from a stale cache (the `disable_expectations` canary) | `Expectations.satisfied()` always true | stale-cache deployment test | ✅ caught (3 tests; 3 pods become 6) |
| 32 | No lost binds from a stale cache | the scheduler never records an assumption | assume-cache test, scheduler properties | ✅ caught (unit test + 2 properties) |
| 33 | S6 at the client: a cache is never patched over a gap | informer logs `Compacted` but doesn't relist | informer relist test | ❌ **survived** (see 3.6); caught after the test was rewritten |
| 34 | One reconcile per change, never zero | work queue drops a key re-added while it was being processed | work-queue unit test | ✅ caught |
| 35 | Bursts collapse into one reconcile | work queue without dedup | work-queue unit test | ✅ caught |
| 36 | The agent is driven by its watch | informer changes don't wake the agent's sync | `test_watch.py` (agent with a 30 s resync) | ✅ caught |
| 37 | A started pod is reported Running promptly | no wake-up after a local start (the original bug, put back) | `test_watch.py` | ✅ caught |
| 38 | Freed capacity reaches waiting pods | a pod that frees capacity doesn't requeue the pending ones | scheduling integration tests | ✅ caught (2 tests) |
| 39 | A new node reaches waiting pods | node events don't requeue pending pods | integration suite | ⚠️ caught, but **only by the end-to-end demo test**; a focused unit test was added and catches it alone |
| 40 | Never evict from a node that came back | evict on the cached NotReady, no live read | lifecycle tests, integration suite | ❌ **survived** (see 3.7); caught after a stale-node-cache test was added |

### PM-4: the simulator's fixes, by hand

| # | Guarantee | Injected bug | Expected to catch | Result |
|---|---|---|---|---|
| 41 | S3 after a freeze: SIGKILL before the lease looks fresh | the agent's `_kill_if_past_eviction` removed | unit test; simulator seed 9 | ✅ caught by both |
| 42 | No acting on a cache older than the registration | the registration-revision gate removed | unit test; simulator seed 25 | ✅ caught by both |
| 43 | A watch batch never applies to a cache that was relisted under it | the informer's generation check removed | informer race test | ❌ **survived** (see 3.9); caught after the test was made damaging |
| 44 | A list older than the cache never replaces it | the informer's "older list" check removed | informer race test | ✅ caught |

### PM-5: observability and the CLI

| # | Guarantee | Injected bug | Expected to catch | Result |
|---|---|---|---|---|
| 45 | Metric names are an API | `chronos_nodes` renamed `chronos_node` | catalog golden test | ✅ caught |
| 46 | The dashboard uses only real metrics | a panel queries `chronos_pod` | dashboard lint | ✅ caught |
| 47 | Repeated events are one counted line | the recorder never increments `count` | event tests | ✅ caught |
| 48 | `pack` packs | `MostAllocated` scores free space (spreads) | scheduler unit tests | ✅ caught (the first attempt was a `sed` that broke the syntax: an "error" is not a catch, so it was redone) |
| 49 | Exported in base units | CPU in millicores | metrics test (`... cpu"} 2.0`) | ✅ caught |
| 50 | Recurring events survive GC | GC by `first_seen` instead of `last_seen` | event GC test | ❌ **survived**: every test event was recorded once, so first and last seen were equal; caught after an "old but still recurring" event was added |

### PM-6: the chaos checker

| # | Guarantee | Injected bug | Expected to catch | Result |
|---|---|---|---|---|
| 51 | Frozen processes aren't "running" until one poll after the thaw | the checker counts SIGSTOPped workloads | checker unit tests | ❌ **survived** at first (no test had a frozen node); caught after one was added |
| 52 | A one-sample read race isn't a violation | S3/S4/S12 flag on the first sample | checker unit tests | ✅ caught |
| 53 | The S4 bound is F + K + poll + sampling, not looser | 1.7 s of extra slack | checker unit tests | ❌ **survived** at first (the only S4 test was 10 s past the bound); caught after a boundary test |
| 54 | A terminating pod's process isn't a duplicate | the checker counts terminating pods | checker unit tests | ✅ caught |
| 55 | The checker sees violations on real processes, not just synthetic ones | a rogue workload started behind Chronos's back | `test_the_checker_catches_a_rogue_workload` | ✅ caught live as S3, and as an L5 orphan after the heal |

### PM-9a: Tier 2 (probes, rolling updates, memory limits) and S7

| # | Guarantee | Injected bug | Expected to catch | Result |
|---|---|---|---|---|
| 56 | A rollout keeps `replicas - max_unavailable` ready (S7) | `may_take = ready`: ready old pods removed regardless of the floor | rollout properties, the real-process rollout test | ✅ caught by both (the real-process test saw `worst_ready` fall below 3); the simulator's `rollout_ignores_max_unavailable` canary is the same bug |
| 57 | A rollout never runs more than `replicas + max_surge` pods (S7) | `create = replicas - len(new)`: new pods created without the surge bound | rollout properties, the simulator | ✅ caught; the simulator found it on 6 of the first 40 seeds as S7 ("10 active pods > 6") |
| 58 | A rollout with `max_surge = 0` still makes progress | the removal budget counted from `replicas` instead of `replicas - max_unavailable` | the progress property | ✅ caught. This was a real bug in my first draft, spotted while reading the code before any test ran; the mutation confirms the property would have found it |
| 59 | Old pods that are still starting aren't all thrown away at once (at least `replicas - max_unavailable` pods stay) | no floor on the removal budget | rollout properties, the simulator, the real-process test | ❌ **survived** everything; caught after a new property (see 3.10) |
| 60 | Ready only after `success_threshold` successes in a row | ready on the first success | probe unit tests | ✅ caught |
| 61 | `failure_threshold` liveness failures restart the process | liveness failures ignored | the agent's probe unit test, the real-process liveness test | ✅ caught by both |
| 62 | A process over its memory limit is killed, recorded as OOMKilled | the RSS check never fires | the real-process memory test | ✅ caught |
| 63 | Upgrading Chronos doesn't roll out every deployment | the template hash includes the new probe fields even when they're unset | unit and property tests | ❌ **survived**; caught after a test pinned the hash the code before probes computed (see 3.11) |
| 64 | Readiness lost under a fault isn't blamed on the controller | the chaos checker judges availability while faults are active | chaos checker unit tests | ✅ caught |
| 65 | A rollout's surge isn't counted as duplicate processes | S3 without the surge allowance | chaos checker unit tests | ✅ caught |
| 66 | The chaos checker sees a surge breach | S7 surge never flagged | chaos checker unit tests | ✅ caught |
| 67 | The chaos checker sees ready pods pushed below the floor in a calm window | S7 availability never flagged | chaos checker unit tests | ✅ caught |

### PM-4: canaries (the simulator's own "test the tests", TESTING.md 6.5)

Each canary switches off one protection inside the simulator, and the simulator must report a violation of the expected invariant. Measured over the first 40 seeds:

| Canary | Expected | Detected | First seed |
|---|---|---|---|
| `disable_fencing` (agent and shims never fence) | S3/S4 | 17/40 | 2 |
| `agent_only_fencing` (shims ignore the lease) | S3/S4 | 15/40 | 2 |
| `fence_after_eviction` (F > E, registration check off) | S3/S4 | 9/40 | 2 |
| `measure_fence_from_ack_time` (+ no heartbeat timeout, see 4.10) | S3/S4 | 5/40 | 0 |
| `disable_expectations` | S3/L2 | 2/40 | 2 |
| `skip_bind_capacity_check` (+ no assume cache) | S2/S11 | 10/40 | 0 |
| `extended_resource_soft_constraint` | S11 | 24/40 | 0 |
| `provider_reuse_unit` (counting provider always returns unit 0) | S12 | 13/40 | 1 |
| `ack_before_fsync` (the WAL acks without syncing) | S8 | 13/40 | 5 |
| `watch_skip_on_compaction` (no 410) | S6 | 2/40 | 4 |
| `rollout_ignores_max_unavailable` (PM-9a: old pods removed as if `max_unavailable = replicas`) | S7 | 22/40 | 0 |

Since PM-9a, `disable_expectations` is also caught as S7 (10 of its 14 detections in the first 40 seeds): without expectations, the controller over-creates during a rollout too, which breaks the surge bound. S7 was added to its expected set.

`tests/sim/test_canaries.py` scans seeds from 0 for each canary and fails if its whole budget (200-1 000 seeds) passes clean. Two canaries started at 0/40; neither could be fixed by changing the canary (see 4.10 and 4.11).

**Score:** of 65 code mutations, 52 were caught on the first try and 13 survived (5 up to PM-2, 2 in PM-3, 1 in PM-4, 1 in PM-5, 2 in PM-6, 2 in PM-9a). Every survivor led to a new or redesigned test, and all 65 are caught now. (#55 plants a fault instead of changing code.) One more (#39) was caught only by the slow end-to-end test and got a focused one. Entry 25 checked the test harness itself rather than the code.

---

## 3. When the mutation survived

These are the valuable cases: each one is a test that *looked* like it covered something and didn't.

### 3.1 The determinism property that couldn't see order (#5)
**Bug:** the scheduler's tie-break used the order nodes arrived in instead of their names.
**Why it survived:** the property test checked "same input → same placement" by building the same cluster twice. But the store always lists nodes sorted by name, so the input order never varied, and the bug could never show.
**Fix:** none needed for coverage: a unit test that passes nodes in shuffled order already caught it. **Lesson:** a property is only as strong as the variation its generator produces. If the system under test normalises its input, a "randomized" test may never explore the dimension you care about.

### 3.2 The freeze test that the agent rescued (#11)
**Bug:** the shim ignores the lease, so a dead agent's workloads never get fenced.
**Why it survived (in one test):** the whole-node-freeze test froze the agent, the shims and the workloads, then resumed *everything*. On resume, the *agent* noticed its stale lease and stopped the workload itself, fast enough to stay inside the bound. The test was meant to prove the shim works, but it passed without the shim.
**Fix:** resume only the shims and workloads and keep the agent frozen, so the shim is the only thing that can act. **Lesson:** when two mechanisms provide the same safety (defence in depth), a test has to switch one off to prove the other.

### 3.3 The freeze test that the workload rescued (#12)
**Bug:** remove the shim's "lease older than eviction timeout → SIGKILL immediately" rule, so a frozen node's shim falls back to the graceful path: SIGTERM, then a 0.5 s grace.
**Why it survived:** the test workload exits on SIGTERM straight away, so the graceful path was just as fast as SIGKILL.
**Fix:** use a workload that ignores SIGTERM, so only an immediate SIGKILL can end it quickly, and measure the window. Real path: **about 65 ms** (one 100 ms poll at most). Mutated path: **about 550 ms** (the grace). The bound was set to 0.3 s, which leaves room for slow CI runners and still tells the two apart. **Lesson:** test inputs must exercise the behaviour that differs; a cooperative workload hides the difference between "polite" and "forceful".

### 3.4 Apply-before-log: invisible to crashes (#20)
**Bug:** the store applies a change to memory and *then* writes the log record (the wrong order).
**Why it survived:** the crash harness kills the process. After a crash, memory is gone anyway, so the order in which memory and disk were updated can't be observed. The only time the order matters is when the log write **fails without a crash**, as with a full disk: memory would then show a change that isn't durable, and the client would get an error for a write that's visible.
**Fix:** a "disk full" test makes `append` raise `ENOSPC` and asserts that memory and revision are unchanged. **Lesson:** crash testing covers *crashes*. Error paths that don't crash need their own tests.

### 3.5 The fsync nobody could see (#23)
**Bug:** group-commit `flush()` "forgets" to call fsync.
**Why it survived 56 tests:** every crash test kills the *process*. The kernel's page cache survives a process crash, so data that was written but never synced is still read back after "recovery". From inside a process-crash test, "written" and "synced" can't be told apart. Only power loss can tell them apart.
**Fix:** a **sync-accounting** test that wraps `durable_sync` (recording how far into the log file each fsync covered) and `append` (recording where each record ended). It asserts that when a write is acknowledged, an fsync has already covered its record. It catches the mutation in group mode and in full mode. The planned `ack_before_fsync` canary in the simulator (PM-4) will model power loss directly.
**Lesson:** know what your fault injection *can't* inject. `kill -9` is not power loss; for the property "synced before acknowledged", check the order directly.

### 3.6 The informer test that relisted by itself (#33)
**Bug:** the informer logs `Compacted` but carries on watching from the same revision, never relisting. Its cache would silently miss every change in the gap.
**Why it survived:** the test called `informer.relist()` by hand, made enough writes to overflow the history, then started `informer.run()` and waited for `relists == 2`. But `run()` *starts* with a relist. That was the second relist, and the 410 path was never reached.
**Fix:** the loop was split into `step()` (one list or one watch; a 410 only sets `needs_relist`), and the test drives it step by step: list → overflow → step (asserts 410 was seen and nothing applied) → step (asserts the relist and the exact handler calls). **Lesson:** count the events your test *causes*, not just the ones you expect; a setup step can produce the very signal you're waiting for.

### 3.7 Nothing covered a stale node cache (#40)
**Bug:** the lifecycle controller evicts based on its *cached* node condition, without the live read.
**Why it survived:** the dangerous case is a cache that still says NotReady after the node came back *and* a new pod was bound to it. That is a narrow race the integration suite never produces.
**Fix:** a unit test builds that exact state by hand: it relists the node informer while the node is NotReady, then re-registers the node, makes it Ready, binds a pod, and refreshes only the pod cache. Then it asserts the pod is still Bound. **Lesson:** races that real runs rarely hit need tests that *construct* the interleaving instead of waiting for it.

### 3.8 Caught by the wrong test (#39, #40, first run)
In the first batch, #39 and #40 were both reported "caught", by `test_sigterm_ignoring_workload…` and `test_two_phase_deletion…`. Neither test has anything to do with node events or eviction. Running them on *unmutated* code showed they were failing anyway (see 4.7). With that fixed, #40 survived and #39 was caught only by the demo test. **Lesson:** a mutation counts as caught only if the failing test is one that *should* fail, and the same test passes without the mutation. Check flakiness first.

### 3.9 A race test whose stale batch was harmless (#43)
**Bug:** the informer applies a watch batch even though a relist replaced the cache while the watch was in flight.
**Why it survived:** the test's stale batch was a DELETED event for an object the newer list held at a higher `resource_version`, so the per-object version check skipped it anyway. That's defence in depth again (see 3.2). The damaging case is a stale MODIFIED for an object the newer list no longer has: with no current object to compare against, it would be resurrected.
**Fix:** the test now uses exactly that case. **Lesson:** a race test has to deliver the stale message that does damage, not just *a* stale message.

### 3.10 A rollout floor no test could see (#59)
**Bug:** without the floor on the removal budget, a rollout that starts while the old pods are still starting (not ready yet) deletes all of them at once.
**Why it survived:** every test started its rollout from old pods that were all ready, and for ready pods a second guard (`may_take`) gives the same answer. The budget only matters for old pods that aren't ready, which no test had. The real-process test and the simulator didn't reach it either.
**Fix:** a property starts the rollout from old pods with random readiness and checks that at least `replicas - max_unavailable` pods remain after one step (Kubernetes bounds its clean-up of unhealthy old pods the same way). **Lesson:** when two guards overlap, each needs a test where only it stands between the code and the bug.

### 3.11 An upgrade that would roll out every deployment (#63)
**Bug:** adding the probe fields to the pod template changes the JSON every template hashes to, even when they're unset, so after an upgrade every pod looks outdated and every deployment rolls out for nothing.
**Why it survived:** every test computes its hashes with the code under test, so old and new always agree with each other.
**Fix:** a unit test pins the hash that the previous commit's code computes for a fixed template (`340494c604`, computed by running the old code from `git archive`). **Lesson:** compatibility across versions can't be tested inside one version; pin a value from the old one.

---

## 4. Tests that were wrong

Sometimes the test, not the code, was broken. These are found by the same habit: when something fails or passes surprisingly, find out which side is wrong before changing anything.

### 4.1 The harness that didn't crash (#25)
The crash harness drew "crash at the nth hit of crash point P" with a guessed number of hits. A measurement over 35 runs showed that **about 40% of runs never reached their crash point** and simply finished: those tests passed without testing anything. **Fix:** each seed now runs once crash-free to *count* the real hits per crash point, draws n from that range, and the test asserts the process really exited with 137. **Lesson:** fault-injection tests must assert that the fault happened.

### 4.2 "One write per operation" (PM-2)
The harness's reference model assumed each operation is one store write, but operation 0 made two (create a node, then mark it Ready). A crash between them produced a legitimate durable state that the model didn't list, and the test reported a durability failure that wasn't one. **Fix:** split the operation. **Lesson:** a reference model's granularity must match the atomicity of the system it checks.

### 4.3 The orphan test that passed by accident (PM-1)
"After an agent restart, the pods come back with new processes" passed instantly, because right after the restart the store still listed the *old*, still-alive processes, which already satisfied "2 Running and matching the OS". It then failed for the right reason once the agent caught up. **Fix:** wait explicitly for process groups that differ from the old ones.

### 4.4 Too strict a property (Day 5)
"A second scheduler tick writes nothing" failed. The scheduler was right: a pending pod's honest reason changes when another pod is bound in the same tick, so one rewrite is correct. **Fix:** the property became "ticks reach a fixed point" (the third tick writes nothing). **Lesson:** when a property fails, decide which is wrong, the code or the specification, before touching either.

### 4.5 Timing measured from the wrong moment (PM-1)
Two shim tests timed the fence timeout from *after* the shim process started (about 0.25 s of Python startup) instead of from when the lease was written, which is when the timeout actually starts. **Fix:** measure from the lease write.

### 4.6 Waiting for something impossible (PM-1)
The first freeze test waited until "the store and the live processes agree" while the old workload was *frozen*, and frozen processes count as alive, so they could never agree. It failed before resuming the frozen processes, teardown then hung on them, and processes were left behind. **Fix:** check the store only during the freeze; teardown now SIGCONTs everything first and SIGKILLs agents that don't stop.

### 4.7 Running is not "ready" (PM-3)
Two MVP tests delete a SIGTERM-ignoring workload and check that it survives SIGTERM until the grace period ends. After the move to watches, the workload died from SIGTERM 50 ms after starting. The agent now reports Running the moment the process starts (polling used to add ≥ 100 ms), and the tests deleted the pod before the Python workload had installed its `SIG_IGN` handler. **Fix:** the tests wait for the workload's first output line, which it prints after installing the handler. The product was right; the tests relied on a delay that polling happened to provide. **Lesson:** making a system faster exposes tests that depended on it being slow. (The same assumption turned up again in PM-6, in a shim test: under load, the lease's 0.3 s fence timeout ran out before the Python workload had installed its handler. The test now keeps the lease fresh until the workload reports ready, then lets it age.)

### 4.8 Two "stale cache" tests whose caches weren't stale (PM-3)
Two tests, caught while writing them: (a) the orphan-GC test was meant to show that a controller whose *deployment* cache is behind doesn't delete the deployment's pods, but its deployment cache had in fact been refreshed, so it showed nothing; it now uses a second controller whose deployment cache is empty. (b) The node-event test failed with a Conflict because its *pod* cache was stale as well (it hadn't seen the `Unschedulable` status write), which a live system's watch would have delivered. **Lesson:** in a test about stale caches, write down which cache is stale and which is fresh, and set each one up deliberately.

### 4.9 My own fix was the next bug (PM-4)
The first fix for the simulator's seed 25 made the agent relist after registering. The simulator then failed seed 21 with an S6 violation. The fix relisted *while the informer's own watch was in flight*, the replies crossed, and an older list wiped a pod. It surfaced under an unrelated canary (`skip_bind_capacity_check`), and that oddity is what made it worth chasing. **Lesson:** run the whole sweep again after every fix, including the canaries, and treat an "unexpected" invariant in a canary run as a finding, not noise.

### 4.10 A canary the system made impossible (PM-4)
`measure_fence_from_ack_time` (renew the lease from when the heartbeat *reply* arrives, ADR-0012's classic bug) went undetected in 40 seeds. That wasn't the simulator being weak. In Chronos as built the bug can't cause harm: the heartbeat client times out after 0.4 s, so no reply later than that is ever accepted, and ack time trails send time by at most 0.4 s. A violation needs a gap above E − F − K − poll = 1.4 s. **Fix:** the canary removes both protections, and the generator gained a "degrade" fault (the link slows down, then dies), which is when the bug matters; now 5/40. **Lesson:** an undetectable canary means either a weak simulator or a second protection. Find out which, and write it down. Here it documents that the heartbeat timeout must stay below E − F − K.

### 4.11 History too long to fall out of (PM-4)
`watch_skip_on_compaction` was also 0/40: with 64 events of history, an agent that was cut off rarely fell far enough behind to need a 410. The simulator now keeps 16 (production keeps 10 000; what matters is how far behind an agent gets relative to the history), and detects it in 2/40.

### 4.13 A unit-exclusivity "violation" that was the checker reading stale status (PM-6)
The first 10-minute chaos runs reported S12, two live pods holding the same unit of an extended resource, on and off for about 10 seconds at a time. The hits were intermittent, but a real double allocation would show on every 200 ms sample. The checker read units from `pod.status.assigned`, which the agent reports after the fact: a pod that was fenced keeps its old `assigned` in the store until its next report. **Fix:** check the real thing. The runtime now puts each process's units in its environment (`CHRONOS_ASSIGNED`), and the checker compares those across the live processes on a node. **Lesson:** an invariant checker should read the ground truth, not a report about it, wherever the ground truth is reachable.

### 4.14 A real-time bound on an overloaded machine (PM-6)
Running three 5-node chaos runs in parallel on the laptop (about 150 Python processes polling every 50-100 ms, plus macOS's Spotlight updater) pushed the load average to 80-190 on 8 cores. S4 then fired: workloads ran up to 0.85 s past the F + K bound, because the shims and agents weren't scheduled on time. The bound wasn't loosened; the setup was wrong. CI runs each nightly seed on its own runner, local runs now go one at a time, and every report records the maximum load average, so a starved machine is visible as such. **Lesson:** a real-time check is only meaningful on a machine that can keep time; record the conditions alongside the result.

### 4.15 A timing guarantee whose margin wasn't met (PM-6)
After the parallel runs were stopped, a single 10-minute run still reported S3 and S4: a workload ran 3.6 s after its node's last accepted heartbeat (bound 2.1 s), and one deployment briefly had 6 processes for 5 replicas. The trace showed the machine stalling: load average 74, three nodes going Unknown in the same instant, a restarted agent taking 2 s just to start Python. The shims fenced, but a second late. Fencing is a timing guarantee, F + K + drift < E, and the **drift margin** is the slack for exactly this; the chaos runner had been using the test fixture's 0.2 s. **Fix:** chaos runs on the demo's timings (drift margin 1 s; production also uses 1 s, with E = 10 s), and the S4 bound includes the drift margin. **Lesson:** when a real-time guarantee fails, check its preconditions before its implementation, and say which one failed.

### 4.16 A liveness check that demanded the impossible (PM-9a)
The first sweep with rolling updates failed seeds 8 and 9 with L2: "6 active pods, wants 4". The traces showed a full cluster: the new pods were Pending (Unschedulable) because the old pods held the resources, and with the old pods at the floor (`replicas - max_unavailable` ready) the controller may not remove another. No move both makes progress and keeps S7. Kubernetes stalls the same way and reports the rollout as past its progress deadline. **Fix:** an unfinished rollout passes liveness only while a new pod is pending and fits no node (L3 still checks that it really doesn't fit), with at most `replicas + max_surge` pods; in both the simulator and the chaos harness. **Lesson:** a liveness property has to allow for the states where safety forbids every way forward.

### 4.17 A new check that took over an old canary (PM-9a)
The first version of the S7 surge check counted active pods against `replicas` even outside rollouts. `disable_expectations` then went undetected in 500 seeds: it was caught 334 times, but always as S7, which isn't in its expected set. Outside a rollout the bound is S3's, so the surge check now runs only during rollouts. Inside one, the over-creation really is an S7 breach, so S7 joined the canary's expected set.

### 4.18 Regression seeds that silently changed meaning (PM-9a)
`tests/sim/regressions.txt` replays seeds by number, and a seed's schedule comes from the generator. Adding the Tier 2 actions to the generator's random stream gave every seed a new schedule, so seeds 9, 24 and 25 no longer replayed the schedules that found their bugs, and nothing failed. A test also broke: the minimizer could no longer shrink seed 1's failure to a single deploy. **Fix:** the Tier 2 actions come from a second random stream (`scenario-<seed>-tier2`). Every seed keeps its old schedule entry for entry, checked across 300 seeds against the previous commit's generator, and only gains entries. **Lesson:** a regression list that stores seeds depends on the generator; extend the generator without disturbing what it already produces.

### 4.12 The simulator hung (PM-4)
One seed under `fence_after_eviction` pegged a CPU forever. A simulated process start took zero time, so "kill past E" and "start again" alternated at one virtual instant without end. Starts now take 1-20 ms (fork + exec), and the engine raises a `SimError` for a livelock (100 000 events at one instant) or a runaway (2 million events) instead of hanging. **Lesson:** a test harness needs its own guard against running forever.

---

## 5. Real bugs the tests found

Bugs in Chronos itself, each caught by a test before it shipped:

| When | Bug | Caught by |
|---|---|---|
| Day 1 | cpu stored as millicores (2000) was serialized as `2000` and parsed back as 2000 *cores* | model JSON round-trip test |
| Day 2 | free capacity computed as `capacity − (capacity + allocated)`: every bind failed | store unit tests |
| Day 3 | the same unit trap: `register()` capacity in millicores re-parsed as cores (HTTP *and* local client) | the contract suite running against both clients |
| Day 6 | the same unit trap a third time, in `pod_for` (`model_dump()` hands back millicores) | caught in review, then pinned with a unit test |
| Day 7 | `chronosctl get pods` crashed on an empty list (`max(int)`) | running the demo for real |
| PM-1 | `signal.signal(SIGKILL, …)` raises, so the shim couldn't pass on "killed by SIGKILL" | shim exit-code test |
| PM-1 | proxy deadlock: on Python 3.12 `Server.wait_closed()` waits for open connections | partition test hanging |
| PM-2 | `chronosd wal-verify` would have truncated a torn tail while "verifying" | review while writing the CLI |
| PM-3 | **lost wakeup**: the in-process watch registered its waiter inside a task, so a write landing before the task first ran went unnoticed until the 30 s timeout | the X-6 widget test (it creates an object right at startup), then pinned by a deterministic race test (#29) |
| PM-6 | **a metastable failure: fencing storms from a too-impatient heartbeat** (agents gave up on a heartbeat after 160 ms; an overloaded `chronosd`, whose WAL fsyncs block its event loop, missed that; the agents fenced, restarted every workload at once, and overloaded it again; one run never converged after the heal) | chaos, seed 102 (L2). Fixed: the agent waits up to F/2 for a reply, which is safe because the lease counts from the send time |
| PM-4 | **after a whole-node freeze, the agent renewed its lease before the shim's next poll**, so the shim let a replaced workload run up to K alongside its replacement (S3; the "one shim poll" claim was false) | the simulator, seed 9 (and 24) |
| PM-4 | **a resumed agent restarted evicted pods from its stale pod cache** after re-registering (S3; came in with PM-3's caching) | the simulator, seed 25 |
| PM-4 | **concurrent relist and watch:** an older list wiped a pod the watch had delivered (S6; came in with the first fix for seed 25) | the simulator's S6 check, seed 21 |
| PM-3 | a pod started by the agent was never reported Running: reporting needs a second sync, and nothing triggered one without polling | `test_watch.py` (#37) |
| PM-3 | chronosd took more than 10 s to stop: uvicorn waited for the agents' 30 s long-poll watches | integration teardown timing out |
| PM-3 | the first fix for that hung chronosd for good: answering *every* watch at once on shutdown made the in-process informers spin without ever yielding to the event loop | integration teardown again; about 30 leaked test servers at 20% CPU each (load average 180), which is how it was noticed. `Cluster.close()` now SIGKILLs a chronosd that ignores SIGTERM and fails the test |

**Found only on Linux** (macOS was fine), all in the test harness:
- macOS `tar` adds AppleDouble `._*` files to a copy; ruff failed on 79 of them.
- Linux `ps` truncates output to 80 columns when it isn't writing to a terminal, so the workload's label fell off the command line. Fixed with `ps -A -ww`.
- On Linux `killpg(pgid, 0)` reports a group containing only a **zombie** as alive. A fenced shim whose parent agent is SIGSTOPped can't be reaped, so the test's `alive()` check must ignore zombies.

---

## 6. Known blind spots (left on purpose, with a plan)

- **Power loss** is modelled only in the simulator: its disk keeps just the synced bytes, and the `ack_before_fsync` canary proves the check works. Real power loss on real hardware is not tested.
- **The simulator drives the agent's real code, but not its asyncio shell** (`Agent.run`, the `Signal` wake-ups, the real `ProcessRuntime`). Those stay covered by the real-process integration tests; the chaos harness (PM-6) re-checks the same invariants on real processes.
- **Network faults on real sockets** (delay, loss, one-way partitions through a proxy) come with the chaos fault proxy in PM-6; the simulator already models them on its own links.
- **Equivalent mutants and coverage:** the catalogue is curated, not exhaustive; the 10 canaries now run on every PR (`tests/sim/test_canaries.py`), so the most important protections are re-checked automatically.

---

## 7. Talking points

- *"How do you know your tests work?"* I break the code on purpose. 39 hand-picked, realistic bugs against named invariants; 32 caught on the first try, 7 survived, and each of those taught me something specific about my tests (sections 3.1–3.7).
- *A mutation caught by a flaky test isn't caught* (3.8): check that the failing test is the one that should fail.
- *Construct the race, don't wait for it* (3.7, and the lost-wakeup test): a stale cache or a write in a 1-line gap won't show up in a real run often enough to trust.
- *Making the system faster broke two tests* (4.7): they relied on polling's delay.
- *The simulator found three real bugs in its first hour* (section 5), two of them introduced by my own recent changes (PM-3's caching, and a fix).
- *An undetectable canary taught me a design rule* (4.10): the heartbeat timeout must stay below E − F − K, or the "send time" rule stops mattering.
- *Check the ground truth, not a report of it* (4.13): the chaos checker's S12 false alarm came from reading status instead of the processes.
- *Chaos found a metastable failure the simulator couldn't* (section 5): an impatient heartbeat timeout, an fsync that blocks the event loop, and a restart storm feeding each other. The simulator doesn't model CPU contention; real processes do.
- *When a timing guarantee fails, check its preconditions first* (4.15): the drift margin wasn't met on an overloaded laptop.
- *Defence in depth hides bugs from tests* (3.2): the agent and the shim both fence, so a test must switch one off to prove the other works.
- *`kill -9` is not power loss* (3.5): crash tests can't see a missing fsync; sync accounting can.
- *Crash tests don't cover error paths* (3.4): disk full is not a crash.
- *Assert that the fault happened* (4.1): 40% of my crash tests weren't crashing.
- *Run it on the other OS* (section 5): three harness bugs existed only on Linux.

### Blog outline
"I broke my orchestrator 43 times on purpose, then built a simulator to do it for me": the method (section 1) → three stories (3.2 the agent rescuing the shim, 3.5 the invisible fsync, 4.1 the harness that didn't crash) → the benchmark showing why group commit exists (10× throughput at 32 writers) → what I'd automate next (the simulator's canaries).
