# What breaking my orchestrator 10,000 times taught me

Chronos is a small container orchestrator in the spirit of Kubernetes, built from scratch in Python. It has a versioned store with a write-ahead log, a scheduler, level-triggered controllers on watches and informers, node agents that run workloads as real processes, and self-fencing nodes. The interesting part wasn't building those pieces. It was finding out where they were wrong.

Three tools did most of that work: a **deterministic simulator** that runs the real control plane and agent through seeded fault schedules (10,000 seeds every night), a **chaos harness** that does the same to real processes through a TCP fault proxy, and the habit of **breaking the code on purpose** to check that a test notices. This is what they found.

## The promise that needed defending

The central promise is easy to state: a deployment of R replicas never has more than R copies running. That's hard to keep when the control plane can't tell a dead node from a slow or cut-off one. If it starts replacements while the "dead" node's workloads are still running, you briefly have two copies of something that might be writing to a database.

Chronos's answer is **self-fencing**. Every workload runs under a small `chronos-shim` that reads a lease file the agent keeps fresh with heartbeats. Once no heartbeat has been accepted for F seconds, the agent *and* every shim stop their workloads within K seconds, even if the agent itself is dead. The control plane only declares the node lost after E seconds, and both sides refuse to start unless F + K + clock drift < E. With the defaults (6 + 2 + 1 < 10), the old copies are dead before the new ones start.

That inequality looks airtight. The bugs below lived in the gaps between "the rule" and "the code that applies the rule".

## Bug 1: the lease that was renewed too early

**What happened.** Seed 9 of the simulator reported S3: a deployment briefly had one more process than it should. The schedule froze a whole node (agent, shims and workloads all stopped, like a VM pause) for longer than E. The control plane declared the node lost and started replacements on other nodes, exactly as designed.

**The timeline.** When the node thawed, its agent sent a heartbeat. The server rejected it: the node's epoch had been revoked when it went NotReady. So the agent re-registered, which took a few milliseconds, and the registration's reply **renewed the lease file**. The shim polls the lease every 100 ms. By the time it looked, the lease was fresh, so it let its workload keep running: the old copy, alongside its replacement. The agent eventually stopped it gracefully, up to K seconds later.

**Root cause.** The shim has two rules. Past F, stop the workload gracefully. Past E, **SIGKILL it immediately**, because replacements may already exist. The second rule only works if the shim sees the stale lease. The agent, waking from the same freeze, raced it and won: it renewed the lease before the shim's next poll. The README's claim that a freeze leaves "at most one shim poll" of overlap was false.

**Fix.** The agent applies the shim's second rule itself. If its own lease is older than E, it SIGKILLs every workload in every fence check, and, crucially, **before** writing any new lease. The rule now holds whoever wakes up first.

**Why it took a simulator.** On real hardware the window is milliseconds wide, and it needs a freeze longer than E followed by a thaw in exactly the wrong order. No integration test was going to hit it. The simulator explores schedules like this on every seed, and checks S3 after every single event, not just at the end.

**Lesson.** A safety rule enforced in two places is only as strong as the order in which they run. When two components share a rule, look for the interleaving where one undoes the other's precondition.

## Bug 2: the fix that caused the next bug

**What happened.** Seed 25 also broke S3, through a different path. An agent was paused across a control-plane restart. When it resumed it re-registered, then **started pods from its pod cache** that the control plane had already evicted and replaced elsewhere.

**Root cause.** This bug came in with the move from polling to watches. The original agent listed its pods fresh on every poll, so it couldn't act on stale state. The watch-based agent had a cache, and the watch feeding it had died with the old control plane; it wouldn't time out for another 15 seconds. Registration succeeded, the cache looked fine, and the agent trusted it.

**First fix.** After registering, relist the pods before acting.

**And then seed 21** reported S6, the watch invariant: an informer's cache was missing a pod the store had. The relist from my fix ran **while the informer's own watch request was in flight**. The replies crossed: the watch delivered a new pod, then the older list response arrived and replaced the cache, wiping it.

**Real fix.** The agent remembers the store revision of its registration and acts only once its cache has reached that revision. The informer became safe under concurrent use: a list older than the cache is dropped, and a watch batch that was in flight across a relist is discarded.

**The odd part.** Seed 21 surfaced while I was running a *canary* (a deliberately planted bug) that had nothing to do with watches. It looked like noise. It was a real bug that only that canary's schedule happened to reach.

**Lesson.** Caches move bugs from "wrong answer" to "right answer, too late", and every fix to a cache is itself a concurrency change. Run the whole invariant suite after a fix, not just the test that failed.

## Bug 3: the outage that sustained itself

**What happened.** On a long chaos run on real processes, one seed never converged after the faults healed: the cluster never got back to its desired state.

**The loop.**
1. The agent gave up on a heartbeat after 160 ms (0.8 × the heartbeat interval in the test timings).
2. Under load, `chronosd` missed that deadline. With full durability on, each write's fsync blocks its event loop, and heartbeats wait behind it.
3. Unanswered heartbeats meant no lease renewal, so agents fenced and stopped their workloads.
4. When heartbeats got through again, every agent restarted every workload at once: status reports and events, each one a synced write.
5. That load made `chronosd` miss the deadline again.

The faults had healed, but the system kept its own outage going. That's a **metastable failure**: stable in the bad state, and the trigger is no longer needed to stay there.

**Fix.** The agent now waits up to F/2 for a heartbeat reply. That looks like it weakens fencing, but it doesn't. The lease is measured from when the heartbeat was **sent**, not from when the reply arrived. A late reply is still proof that the server heard the node at send time, so waiting longer for it can't stretch the lease. That send-time rule was a decision made for safety (a canary confirms that counting from the reply breaks S3). Here it also made the system patient for free.

**What's still true.** The fsync on the event loop remains. Benchmarks put a durable write at about 3 ms on the test machine (about 300 writes per second), and group commit raises the ceiling about tenfold under concurrent load. Moving the sync off the event loop is the fix if write rates grow.

**Lesson.** Timeouts are a load-feedback mechanism. A timeout shorter than the system's worst latency under load turns a slow moment into a retry storm, and a retry storm into an outage.

## The smaller ones

*   **A seam that only worked for one owner.** Stage 2 will add Jobs, which own pods. To prove the extension seams were real, I gave a test-only controller a pod to own. The deployment controller deleted it within two seconds: it garbage-collected *every* pod whose owner wasn't a Deployment. An earlier proof had registered a test-only controller that didn't own anything, so it never noticed. A seam isn't proven until the fake extension does what the real one will.
*   **macOS only, on CI only.** Every agent on GitHub's macOS runners failed to register, and their logs were empty. The empty log was the clue: the agent always logs on shutdown, so it had been stuck before its event loop started. The only new code in that stretch was the metrics server, and Python's `HTTPServer.server_bind` does a **reverse DNS lookup** of its own bind address, which stalled on those runners. Locally it took 6 ms.
*   **`fsync` that doesn't sync.** On macOS, plain `fsync` is about as fast as no sync at all, because it only reaches the drive's cache. The WAL uses `F_FULLFSYNC`, which is ten times slower: that's the real price of durability on that platform.

## Testing the tests

Every guarantee in Chronos has a test. I checked each one by breaking the code that upholds it, one plausible bug at a time, and seeing whether a test failed. Of 68 such mutations, 13 survived the first time. The survivors were more instructive than the catches:

*   A determinism test that compared two runs' outputs but couldn't see their **order**.
*   A freeze test that passed because the agent rescued the situation the shim was supposed to handle.
*   An upgrade-compatibility test that could never fail, because both sides of its comparison were computed by the code under test. It now pins a value produced by the previous version.

The simulator has its own version of this: **11 canary bugs**, each switching off one protection (fencing, expectations, the bind capacity check, fsync before acknowledging, ...). Each must be caught as the right invariant within a seed budget, on every pull request. Two canaries started at zero detections in 40 seeds, and neither was fixed by weakening the check. One watch bug needed the simulator to keep less history, so agents fell far enough behind to hit it. The other, counting the lease from the reply instead of the send, turned out to be masked by a *second* protection, the heartbeat timeout. The canary now switches off both, the simulator gained a fault where a link slows down and then dies, and the masking itself is documented as a constraint. An undetectable canary means either a weak simulator or a hidden second protection, and both are worth knowing.

## What I'd tell myself at the start

1.  **Write the invariants down before the code.** "At most R processes", "a watch never skips", "an acknowledged write survives a crash". Each one became a property test, a simulator check and a chaos check. Without the list there's nothing to check.
2.  **Make the logic pure.** Scheduling, reconciliation, rollouts, fencing and the agent's decisions are pure functions with the clock passed in. That's what made it possible to run the *same* code under Hypothesis, under virtual time in the simulator, and in production.
3.  **A simulator is worth more than its cost, but only with canaries.** Without them, a green run means nothing.
4.  **Real processes still find things the simulator can't.** The metastable failure needed a real event loop blocked by a real fsync.
5.  **Check a guarantee's preconditions before its implementation.** When chaos first reported duplicates on an overloaded laptop, the fencing code was fine. The run had violated F + K + drift < E, because the clock-drift margin was too small for a machine at load 74.

The code, the [architecture](ARCHITECTURE.md), the [benchmarks](BENCHMARKS.md) and the [full mutation log](testing-the-tests.md) are in the repository.
