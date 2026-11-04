# ADR-0021: CUDA workloads via numba-cuda, with a simulate mode

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Deferred – Stage 2 (after Stage 1 exit; hardware lined up) | 2026-10-23 | Stage 2 (P9) | — | — |

> **Deferred to Stage 2 (GPU/CUDA)** on 2026-11-04: the plan was split into Stage 1 (Orchestration) and Stage 2 (GPU/CUDA). This decision is kept unchanged and is not scheduled. It plugs into the Stage 1 seams in [ADR-0023](0023-extension-seams.md); see [PHASE2-EXTENSION-POINTS.md](../PHASE2-EXTENSION-POINTS.md).

## 1. What
Real GPU workloads are a bundled command, **`chronos-cuda-task`**, written in Python with **`numba-cuda`**. It has exactly two kernels (**vector add** and **parallel sum reduction**), verifies its result against NumPy, and reports through its **exit code**. A **`--simulate`** mode exercises the full orchestration path without a GPU, so everything except the kernel itself is tested on macOS and in CI.

## 2. Why
### Problem
The extension has to prove that GPU-aware scheduling works end to end with *real* CUDA, not just counters. That brings in a GPU toolchain, which can derail the project: a second language, nvcc, driver mismatches, and a dev machine (macOS) that can't run CUDA at all. If the CUDA path is the only way to test GPU scheduling, every scheduling bug becomes a hardware/driver debugging session.
### Forces & constraints
- The project is Python-native (ADR-0002), so avoid a second toolchain if possible.
- Dev on macOS (no CUDA), CI without GPUs; real validation on an occasional Linux + NVIDIA machine.
- Spec guardrails: one or two kernels, one task per GPU slot, no memory manager.
- The result must be *checked*, not just "ran without crashing".

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: numba-cuda kernels in Python + `--simulate`** | One language; no nvcc; JIT on the target; testable without a GPU | JIT compile adds ~1 s on first launch; Numba's CUDA subset | ✅ Chosen |
| A. CUDA C++ compiled with nvcc | The canonical CUDA path; resume signal | Second language + toolchain; build step per machine; C++ error handling | ⏸ Optional later swap (same CLI contract) |
| B. CuPy `RawKernel` (CUDA C source string, compiled by NVRTC) | Real CUDA C kernels without nvcc | Large wheel tied to the CUDA version; kernel code as strings | ❌ (close second) |
| C. PyTorch tensor ops | Easy | Not writing kernels at all; a huge dependency | ❌ |
| D. No real CUDA, simulate only | Zero hardware need | Doesn't meet the spec's end-to-end requirement | ❌ (but kept as a mode) |

**A** is the most "authentic", but it doubles the toolchain for two tiny kernels. Because the orchestrator only sees a command plus an exit code, a C++ binary can replace the Python runner later without touching Chronos. **B** is good, but heavier to install and pin than numba-cuda. **C** hides the kernel. **D** alone fails the requirement, though as a *mode* it lets 90 % of the GPU work be developed and tested on a Mac.

## 4. How
### Components & files
- `src/chronos/workloads/cuda_task.py`: entry point `chronos-cuda-task`.
- `pyproject.toml`: optional dependency group `gpu = ["numba-cuda", "numpy"]` (pin exact versions when implementing; match the machine's CUDA driver).
- `docs/GPU.md`: setup and run guide.
### Mechanism
**CLI contract (all the orchestrator depends on):**
```
chronos-cuda-task --kernel {vector_add,reduce_sum} [--n 16777216] [--duration-s 0] [--seed 0] [--simulate] [--fail]
stdout (last line): {"kernel":..., "n":..., "iterations":..., "ms_per_iter":..., "ok":true, "device":"Tesla T4"}
exit: 0 ok | 1 wrong result | 3 no CUDA device/driver | 4 CUDA runtime error | 2 bad args
```
**Kernels:**
```python
@cuda.jit
def vector_add(a, b, out):
    i = cuda.grid(1)
    if i < out.size: out[i] = a[i] + b[i]

@cuda.jit
def block_sum(x, partial):                       # tree reduction in shared memory, one partial per block
    s = cuda.shared.array(TPB, float32); t = cuda.threadIdx.x; i = cuda.grid(1)
    s[t] = x[i] if i < x.size else 0.0
    cuda.syncthreads()
    stride = TPB // 2
    while stride > 0:
        if t < stride: s[t] += s[t + stride]
        cuda.syncthreads(); stride //= 2
    if t == 0: partial[cuda.blockIdx.x] = s[0]
# host: launch block_sum, then sum the partials (a second launch or on host); compare with np.sum (float64 reference, rtol 1e-4)
```
**Run loop:** check `cuda.is_available()` (else exit 3) → generate inputs from `--seed` → copy to device once → launch repeatedly until `--duration-s` has elapsed (at least once) → copy back → verify → print summary → exit code. `--duration-s` lets tests kill the task *mid-execution* (the spec's recovery test), because a single vector add finishes in milliseconds.
**Simulate mode:** asserts `CUDA_VISIBLE_DEVICES` is non-empty (proving slot assignment reached the process), sleeps `--duration-s`, prints a summary with `"device": "simulated"`, and exits 0 (or 1 with `--fail`). It needs no numba or numpy, so it runs everywhere.
**Signals:** SIGTERM → stop after the current launch, exit 143. This works with the shim's fencing (ADR-0012), with SIGKILL as the backstop.
### Configuration
Defaults: `--n 2**24`, threads per block 256, `--duration-s 0`.
### Edge cases & failure handling
- **Driver/toolkit mismatch:** exit 3 with the error message on stderr; the Job retries (counts as a genuine failure) and then fails with a clear `message`. GPU.md lists the `nvidia-smi` check.
- **Several logical slots on one physical GPU:** kernels share the device; tiny workloads make this harmless (documented).
- **First-launch JIT latency (~1 s):** excluded from `ms_per_iter` (a warm-up launch).

## 5. Consequences
**Positive:** a single language; real CUDA kernels with verified results; GPU scheduling fully testable without a GPU; the orchestrator stays agnostic (command + exit code), so swapping in a C++ kernel later costs nothing.
**Negative / accepted costs:** Numba's kernel subset; ~1 s JIT; the real path is only validated on a borrowed or rented GPU machine, so its evidence is a committed results file, not CI.
**Risks & mitigations:** numba-cuda packaging changes → pin versions in the `gpu` group and document the tested CUDA driver in GPU.md.

## 6. Verification
- Unit (no GPU): the CLI argument contract; simulate mode exit codes; the reduction host logic against NumPy on CPU (kernel math is mirrored in a pure-Python reference).
- Real-GPU suite (`-m gpu`): both kernels verified correct; the exit-3 path checked by running with `CUDA_VISIBLE_DEVICES=""`; kill-mid-execution via `--duration-s`.
- Results file `bench/results/gpu-suite-<date>.json` with GPU model, driver and timings.

## 7. Revisit when
You want CUDA C++ on the resume (swap the runner, same CLI contract), or more complex GPU workloads are needed (a new ADR, mindful of the scope guardrails).

## 8. Related & references
ADR-0002, ADR-0005, ADR-0012, ADR-0019, ADR-0020; Numba CUDA documentation (`numba-cuda`); NVIDIA, "Optimizing Parallel Reduction in CUDA" (Mark Harris).
