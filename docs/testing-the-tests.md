# Testing the tests: a mutation-testing log

Chronos makes strong claims: "never more than R replicas", "an acknowledged write survives a crash", "a pod never lands on a node without enough slots". Each claim is backed by tests. But a passing test only proves something if it *would fail* when the claim breaks. This log records how that was checked: by deliberately breaking the code that upholds each claim and confirming that a test notices.

It covers the MVP through PM-2 (25 mutations so far), and gets a new entry every time a guarantee is added.

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

**Score:** of 24 code mutations, 19 were caught on the first try and 5 survived. All 5 led to a new or redesigned test, and all are caught now. Entry 25 checked the test harness itself rather than the code.

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

**Found only on Linux** (macOS was fine), all in the test harness:
- macOS `tar` adds AppleDouble `._*` files to a copy; ruff failed on 79 of them.
- Linux `ps` truncates output to 80 columns when it isn't writing to a terminal, so the workload's label fell off the command line. Fixed with `ps -A -ww`.
- On Linux `killpg(pgid, 0)` reports a group containing only a **zombie** as alive. A fenced shim whose parent agent is SIGSTOPped can't be reaped, so the test's `alive()` check must ignore zombies.

---

## 6. Known blind spots (left on purpose, with a plan)

- **Power loss** is not simulated (see 3.5). Covered for now by the sync-accounting test; the simulator's `ack_before_fsync` canary comes in PM-4.
- **Equivalent mutants and coverage:** the catalogue is curated, not exhaustive. The deterministic simulator (PM-4) adds flag-gated canary bugs (`disable_fencing`, `disable_expectations`, `skip_bind_capacity_check`, …) that run on every PR across hundreds of seeds, which turns this manual log into a regression gate.
- **Network faults beyond a clean cut** (delay, loss, one-way partitions) come with the chaos fault proxy in PM-6.

---

## 7. Talking points

- *"How do you know your tests work?"* I break the code on purpose. 24 hand-picked, realistic bugs against named invariants; 19 caught on the first try, 5 survived, and each of those taught me something specific about my tests (sections 3.1–3.5).
- *Defence in depth hides bugs from tests* (3.2): the agent and the shim both fence, so a test must switch one off to prove the other works.
- *`kill -9` is not power loss* (3.5): crash tests can't see a missing fsync; sync accounting can.
- *Crash tests don't cover error paths* (3.4): disk full is not a crash.
- *Assert that the fault happened* (4.1): 40% of my crash tests weren't crashing.
- *Run it on the other OS* (section 5): three harness bugs existed only on Linux.

### Blog outline
"I broke my orchestrator 24 times on purpose": the method (section 1) → three stories (3.2 the agent rescuing the shim, 3.5 the invisible fsync, 4.1 the harness that didn't crash) → the benchmark showing why group commit exists (10× throughput at 32 writers) → what I'd automate next (the simulator's canaries).
