"""Generates dashboards/chronos.json (ADR-0024).

    uv run python deploy/observability/grafana/build_dashboard.py

Five rows: cluster overview, control loop, node health, resources, durability.
Every query may use only catalog metrics (tests/golden/metrics.txt); the dashboard
lint test checks that, and that this script and the committed JSON agree.
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).parent / "dashboards" / "chronos.json"
DS = {"type": "prometheus", "uid": "prometheus"}


def p95(metric: str, by: str = "") -> str:
    group = f"le, {by}" if by else "le"
    return f"histogram_quantile(0.95, sum by ({group}) (rate({metric}_bucket[1m])))"


ROWS: list[tuple[str, list[tuple[str, str, list[tuple[str, str]], str]]]] = [
    # (row title, [(panel title, kind, [(query, legend)], unit)])
    ("Cluster overview", [
        ("Nodes by condition", "timeseries", [("chronos_nodes", "{{condition}}")], "short"),
        ("Pods by phase", "timeseries", [("chronos_pods", "{{phase}}")], "short"),
        ("Unschedulable pods by reason", "timeseries",
         [("chronos_unschedulable_pods", "{{reason}}")], "short"),
    ]),
    ("Control loop", [
        ("Reconcile p95", "timeseries",
         [(p95("chronos_reconcile_duration_seconds", "controller"), "{{controller}}")], "s"),
        ("Work queue depth", "timeseries",
         [("chronos_workqueue_depth", "{{controller}}")], "short"),
        ("Scheduling p95", "timeseries",
         [(p95("chronos_scheduling_duration_seconds"), "p95")], "s"),
        ("Bind conflicts / s", "timeseries",
         [("rate(chronos_bind_conflicts_total[1m])", "conflicts")], "short"),
    ]),
    ("Node health", [
        ("Heartbeat age", "timeseries",
         [("chronos_node_heartbeat_age_seconds", "{{node}}")], "s"),
        ("Fence events (5 m)", "timeseries",
         [("increase(chronos_agent_fence_events_total[5m])", "{{instance}}")], "short"),
        ("Heartbeat failures / s", "timeseries",
         [("rate(chronos_agent_heartbeat_failures_total[1m])", "{{instance}}")], "short"),
        ("Agent processes", "timeseries",
         [("chronos_agent_processes", "{{instance}} {{state}}")], "short"),
    ]),
    ("Resources", [
        ("Allocated / capacity", "timeseries",
         [("chronos_resource_allocated / chronos_resource_capacity", "{{node}} {{resource}}")],
         "percentunit"),
        ("Capacity", "timeseries",
         [("chronos_resource_capacity", "{{node}} {{resource}}")], "short"),
    ]),
    ("Durability", [
        ("WAL fsync p95", "timeseries",
         [(p95("chronos_wal_fsync_duration_seconds"), "p95")], "s"),
    ]),
]  # fmt: skip


def build() -> dict[str, object]:
    panels: list[dict[str, object]] = []
    y, pid = 0, 1
    for title, row in ROWS:
        panels.append({"type": "row", "title": title, "id": pid, "collapsed": False,
                       "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}})  # fmt: skip
        pid, y = pid + 1, y + 1
        width = 24 // len(row)
        for i, (name, kind, queries, unit) in enumerate(row):
            panels.append(
                {
                    "type": kind,
                    "title": name,
                    "id": pid,
                    "datasource": DS,
                    "gridPos": {"h": 8, "w": width, "x": i * width, "y": y},
                    "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
                    "targets": [
                        {"refId": chr(65 + j), "expr": q, "legendFormat": legend, "datasource": DS}
                        for j, (q, legend) in enumerate(queries)
                    ],
                }
            )
            pid += 1
        y += 8
    return {
        "uid": "chronos",
        "title": "Chronos",
        "tags": ["chronos"],
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 1,
        "refresh": "1s",
        "time": {"from": "now-5m", "to": "now"},
        "timepicker": {"refresh_intervals": ["1s", "5s", "10s", "30s"]},
        "panels": panels,
    }


def render() -> str:
    return json.dumps(build(), indent=2) + "\n"


if __name__ == "__main__":
    OUT.write_text(render())
    print(f"wrote {OUT}")
