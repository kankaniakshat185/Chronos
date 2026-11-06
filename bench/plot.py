"""Render the latest bench/results/*.json into docs/img/b1.png ... b6.png.

uv run python bench/plot.py
"""

from __future__ import annotations

from pathlib import Path

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt
from common import latest

OUT = Path(__file__).parents[1] / "docs" / "img"


def b1(ax: plt.Axes) -> None:
    r = latest("b1")["results"]
    xs = sorted(int(k) for k in r)
    for q, style in (("p50", "o-"), ("p95", "s--"), ("p99", "^:")):
        ax.plot(xs, [r[str(x)][q] for x in xs], style, label=q)
    ax.set(
        xscale="log", xlabel="replicas", ylabel="apply -> all Running (s)", title="B1 convergence"
    )
    ax.legend()


def b2(ax: plt.Axes) -> None:
    r = latest("b2")["results"]
    phases = ["detection", "eviction", "reconcile", "schedule", "start"]
    left = 0.0
    for p in phases:
        v = r[p]["p50"]
        ax.barh(["p50"], [v], left=left, label=f"{p} {v:.2f}s")
        left += v
    ax.set(
        xlabel="seconds after kill -9",
        title=f"B2 recovery (total p50 {r['total']['p50']:.2f}s, p99 {r['total']['p99']:.2f}s)",
    )
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.25), ncol=3, fontsize=8)


def b3(ax: plt.Axes) -> None:
    r = latest("b3")["results"]
    for e, v in sorted(r.items(), key=lambda kv: float(kv[0])):
        x, y = v["detection"]["p50"], 100 * v["false_eviction_rate"]
        ax.plot(x, y, "o")
        ax.annotate(f"E={e}s", (x, y), textcoords="offset points", xytext=(6, 4))
    ax.set(
        xlabel="detection latency p50 (s)",
        ylabel="false evictions (% of pauses)",
        title="B3 detection vs false positives",
    )


def b4(ax: plt.Axes) -> None:
    r = latest("b4")["results"]
    xs = sorted(int(k) for k in r)
    ax.plot(xs, [r[str(x)]["pods_per_s"] for x in xs], "o-")
    ax.set(xscale="log", xlabel="nodes", ylabel="pods/s", title="B4 scheduler throughput")
    twin = ax.twinx()
    twin.plot(xs, [r[str(x)]["decision"]["p99"] * 1e3 for x in xs], "s--", color="tab:red")
    twin.set_ylabel("decision p99 (ms)", color="tab:red")


def b5(ax: plt.Axes) -> None:
    r = latest("b5")["results"]
    keys = [k for k in r if k.endswith("/32")]
    ax.bar([k.split("/")[0] for k in keys], [r[k]["writes_per_s"]["p50"] for k in keys])
    ax.set(yscale="log", ylabel="writes/s (32 writers)", title="B5 WAL sync modes")
    ax.tick_params(axis="x", rotation=30)


def b6(ax: plt.Axes) -> None:
    r = latest("b6")["results"]
    for label, style in (("without", "o-"), ("with", "s--")):
        rows = sorted(
            (v["records"], v["seconds"]["p50"]) for k, v in r.items() if k.endswith(f"/{label}")
        )
        ax.plot([a for a, _ in rows], [b for _, b in rows], style, label=f"{label} snapshots")
    ax.set(
        xscale="log",
        yscale="log",
        xlabel="WAL records",
        ylabel="Store.open (s)",
        title="B6 recovery time",
    )
    ax.legend()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, draw in (("b1", b1), ("b2", b2), ("b3", b3), ("b4", b4), ("b5", b5), ("b6", b6)):
        try:
            fig, ax = plt.subplots(figsize=(6, 3.6), layout="constrained")
            draw(ax)
            fig.savefig(OUT / f"{name}.png", dpi=120)
            plt.close(fig)
            print(f"wrote {OUT / name}.png")
        except FileNotFoundError as exc:
            print(f"skipped {name}: {exc}")


if __name__ == "__main__":
    main()
