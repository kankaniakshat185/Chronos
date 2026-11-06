"""Shared by the benchmarks (TESTING.md section 9): machine info, raw samples, summaries.

Every result is `bench/results/<name>-<date>.json` with the machine, the config and the
raw samples. Summaries are p50/p95/p99 with the sample count, never only a mean.
"""

from __future__ import annotations

import json
import os
import platform
import statistics
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

RESULTS = Path(__file__).parent / "results"


def machine() -> dict[str, Any]:
    cpu = platform.processor() or platform.machine()
    if sys.platform == "darwin":
        out = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"],
            capture_output=True,
            text=True,
            check=False,
        )
        cpu = out.stdout.strip() or cpu
    mem = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") if hasattr(os, "sysconf") else 0
    return {
        "cpu": cpu,
        "cores": os.cpu_count(),
        "memory_gib": round(mem / 2**30, 1),
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
    }


def pct(samples: list[float], q: float) -> float:
    if not samples:
        return float("nan")
    ordered = sorted(samples)
    i = min(len(ordered) - 1, max(0, round(q / 100 * (len(ordered) - 1))))
    return ordered[i]


def summary(samples: list[float]) -> dict[str, float]:
    return {
        "n": len(samples),
        "p50": pct(samples, 50),
        "p95": pct(samples, 95),
        "p99": pct(samples, 99),
        "mean": statistics.fmean(samples) if samples else float("nan"),
    }


def save(name: str, config: dict[str, Any], results: dict[str, Any]) -> Path:
    RESULTS.mkdir(exist_ok=True)
    date = datetime.now(UTC).strftime("%Y-%m-%d")
    path = RESULTS / f"{name}-{date}.json"
    path.write_text(json.dumps({"name": name, "date": date, "machine": machine(), "config": config,
                                "results": results}, indent=2))  # fmt: skip
    print(f"wrote {path}")
    return path


def latest(name: str) -> dict[str, Any]:
    files = sorted(RESULTS.glob(f"{name}-*.json"))
    if not files:
        raise FileNotFoundError(f"no results for {name}: run bench/{name}*.py first")
    data: dict[str, Any] = json.loads(files[-1].read_text())
    return data
