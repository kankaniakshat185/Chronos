"""Plain-text tables for `chronosctl get` (pure: the caller passes the time)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from chronos.api import Deployment, Node, Pod
from chronos.api.quantity import CPU, MEMORY, format_quantity, is_extended

NONE = "—"


def age(created: datetime | None, now: datetime) -> str:
    if created is None:
        return NONE
    seconds = max(int((now - created).total_seconds()), 0)
    for unit, size in (("d", 86_400), ("h", 3_600), ("m", 60)):
        if seconds >= size:
            return f"{seconds // size}{unit}"
    return f"{seconds}s"


def table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    widths = [max([len(h), *(len(r[i]) for r in rows)]) for i, h in enumerate(headers)]
    lines = [headers, *rows]
    return "\n".join("   ".join(c.ljust(w) for c, w in zip(line, widths, strict=True)).rstrip()
                     for line in lines)  # fmt: skip


def _usage(node: Node, name: str) -> str:
    if name not in node.spec.capacity:
        return NONE  # the node doesn't advertise it: capacity 0, nothing can use it
    used = node.status.allocated.get(name, 0)
    return f"{format_quantity(name, used)}/{format_quantity(name, node.spec.capacity[name])}"


def nodes(items: Sequence[Node], now: datetime) -> str:
    """One allocated/capacity column per resource name seen on any node."""
    extended = sorted({n for node in items for n in node.spec.capacity if is_extended(n)})
    headers = ["NAME", "STATUS", "CPU", "MEMORY", *(n.upper() for n in extended), "AGE"]
    rows = [
        [
            node.metadata.name,
            node.status.condition + (",Unschedulable" if node.spec.unschedulable else ""),
            *(_usage(node, r) for r in (CPU, MEMORY, *extended)),
            age(node.metadata.created_at, now),
        ]
        for node in items
    ]
    return table(headers, rows)


def _status(pod: Pod) -> str:
    return "Terminating" if pod.metadata.deletion_timestamp is not None else pod.status.phase


def pods(items: Sequence[Pod], now: datetime, *, wide: bool = False) -> str:
    if not wide:
        rows = [[p.metadata.name, _status(p), age(p.metadata.created_at, now)] for p in items]
        return table(["NAME", "STATUS", "AGE"], rows)
    rows = [
        [
            p.metadata.name,
            _status(p),
            p.spec.node_name or NONE,
            str(p.status.pid) if p.status.pid else NONE,
            p.status.reason or NONE,
            age(p.metadata.created_at, now),
            p.status.message,
        ]
        for p in items
    ]
    return table(["NAME", "STATUS", "NODE", "PID", "REASON", "AGE", "MESSAGE"], rows)


def deployments(items: Sequence[Deployment], now: datetime) -> str:
    rows = [
        [
            d.metadata.name,
            f"{d.status.ready_replicas}/{d.spec.replicas}",
            str(d.status.replicas),
            age(d.metadata.created_at, now),
        ]
        for d in items
    ]
    return table(["NAME", "READY", "ACTIVE", "AGE"], rows)
