# ADR-0005: Workloads are OS processes supervised by node agents

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Proposed | 2026-10-21 | P2 | — | — |

## 1. What
Each pod runs as **one OS process (and its children) in its own process group**, started, stopped and monitored by the agent of the node it is bound to. "Nodes" are agent processes with a **declared** capacity, usually all on one machine. The runtime sits behind a `Runtime` protocol so Docker could be added later.

## 2. Why
### Problem
A scheduled pod has to *be* something. If it's fake, there are no real crashes, signals, orphans or partitions, so the fault-injection story is hollow. If it's a container, the project grows a container runtime (explicitly out of scope) or depends on Docker everywhere, including in CI.
### Forces & constraints
- Fault injection must be real: `kill -9`, `SIGSTOP`, crashes, ignored SIGTERM.
- Tests must be fast (hundreds of process starts per test run).
- No heavy dependency (Docker Desktop on macOS) for the core path.
- Must work identically on macOS and Linux.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: OS processes in process groups** | Real failure modes, ~20–50 ms start, no dependencies | No isolation; nodes share a kernel | ✅ Chosen |
| A. Docker containers (Docker SDK) | Isolation, "real" feel | Heavy dependency; ~0.5–1 s start; daemon failure modes | ⏸ Future `Runtime` |
| B. Pure simulation | Fast, deterministic | Fake failure modes; weak demo | ❌ (only inside the sim, ADR-0016) |
| C. Threads/tasks inside the agent | Trivial | Can't be killed independently; useless for fencing tests | ❌ |

**A** would make every integration test 10–50× slower and tie CI to Docker, while adding no new insight about orchestration. **B** is right *for the simulator* but would make the real system a toy. **C** can't express "the agent died but the workload kept running", which is the exact scenario fencing exists for.

## 4. How
### Components & files
- `src/chronos/agent/runtime.py`: `Runtime` protocol + `ProcessRuntime`.
- `src/chronos/agent/supervisor.py`: per-pod state machine (start, monitor, restart with backoff, stop).
- `src/chronos/testing/workload.py`: controllable test workload.
### Mechanism
```python
class Runtime(Protocol):
    async def start(self, pod: Pod) -> Handle: ...          # returns pid/pgid
    async def stop(self, h: Handle, grace: float) -> Exit: ...
    def poll(self, h: Handle) -> Exit | None: ...
    def rss_bytes(self, h: Handle) -> int: ...              # Tier 2 limits
```
- **Start:** `asyncio.create_subprocess_exec("chronos-shim", "--lease", lease_path, "--fence-timeout", F, "--", *cmd, start_new_session=True, env=base_env | pod_env, stdout=log, stderr=log)`. `start_new_session=True` makes the shim a session and process-group leader (pgid == shim pid); the workload is its child in the same group.
- **Env injected:** `CHRONOS_POD_UID`, `CHRONOS_POD_NAME`, `CHRONOS_NODE_NAME`, optional `PORT` (ADR-0017).
- **Stop:** `os.killpg(pgid, SIGTERM)` → wait up to `termination_grace` → `os.killpg(pgid, SIGKILL)` → reap. `ProcessLookupError` = already gone, so it's treated as success.
- **Exit detection:** `await proc.wait()`. Negative return code = killed by a signal.
- **Logs:** `<agent-data>/pods/<uid>/stdout.log`, `stderr.log`.
- **Pidfiles:** `<agent-data>/pids/<uid>` holds `pgid`. Written after start, removed after reap.
- **Orphan cleanup on agent start:** for each pidfile, verify the process still exists *and* has the matching `CHRONOS_POD_UID` in its environment (psutil; guards against PID reuse), then `killpg(SIGKILL)` and remove the pidfile, before registering.
### Configuration
| Param | Default |
|-------|---------|
| `agent.data_dir` | `~/.chronos/agents/<node>` |
| `termination_grace` | 2.0 s |
| `agent.capacity.cpu` / `.mem` | required flags |
### Edge cases & failure handling
- **Agent `kill -9`:** workloads keep running (they're in their own sessions), and a dead agent can't fence them. This is why every workload is launched **under `chronos-shim`** (ADR-0012 §4): the shim is the process-group leader, watches the agent's lease file, and kills its workload when the lease goes stale, whether or not the agent is alive. On agent restart, orphan cleanup also removes any leftovers.
- **Workload forks and its parent exits:** `killpg` still reaches the children (same group) unless they call `setsid`. Documented limitation.
- **PID reuse:** pidfile checks are verified against `CHRONOS_POD_UID`.
- **Zombie processes:** asyncio's child watcher reaps them; the supervisor always awaits `wait()`.

## 5. Consequences
**Positive:** real crashes, exit codes, signals, orphans and pauses; fast; no dependencies.
**Negative / accepted costs:** no isolation (resources are advisory, ADR-0014); node failure is simulated by killing or pausing agents, not machines.
**Risks & mitigations:** leaked processes during development → process groups, orphan cleanup, and a session-scoped test fixture that asserts no `CHRONOS_POD_UID` processes remain.
**README limitation:** "Nodes are processes, not machines. Killing an agent leaves its workloads running. This is the *harder* case for safety, and it's tested deliberately."

## 6. Verification
- Unit: supervisor state machine with a fake `Runtime`.
- Integration: crash/restart, `--ignore-sigterm` → SIGKILL after grace, agent `kill -9` + restart → orphans cleaned, L5 teardown check.
- Chaos: SIGSTOP/kill of agents with workloads running.

## 7. Revisit when
You want isolation or a Docker demo. Then add `DockerRuntime` behind the same protocol and run the integration suite against both.

## 8. Related & references
ADR-0010, ADR-0012, ADR-0014, ADR-0016; `setsid(2)`, `killpg(2)`.
