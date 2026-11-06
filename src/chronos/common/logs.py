"""Structured logging (P7.2): one JSON object per line, with context.

`--log-format json` on `chronosd` and the agent switches the root handler to
`JsonFormatter`. Context set with `log_context(...)` (the agent's `node`, a
reconcile's `controller`, `key` and `trace_id`) is attached to every line logged
inside it, so the chaos harness (PM-6) can rebuild a timeline per object. The text
format stays the default for humans.

Deviation from the plan: the standard library plus `contextvars`, not structlog. It
does the same job here without a dependency.
"""

from __future__ import annotations

import contextlib
import contextvars
import json
import logging
from collections.abc import Iterator
from datetime import UTC, datetime

_CONTEXT: contextvars.ContextVar[dict[str, object] | None] = contextvars.ContextVar(
    "chronos_log_context", default=None
)


def _current() -> dict[str, object]:
    return _CONTEXT.get() or {}


@contextlib.contextmanager
def log_context(**fields: object) -> Iterator[None]:
    token = _CONTEXT.set({**_current(), **fields})
    try:
        yield
    finally:
        _CONTEXT.reset(token)


def bind(**fields: object) -> None:
    """Add context for the rest of this task (e.g. the agent's node name, at startup)."""
    _CONTEXT.set({**_current(), **fields})


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        line: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "component": record.name,
            "msg": record.getMessage(),
            **_current(),
        }
        if record.exc_info:
            line["exc"] = self.formatException(record.exc_info)
        return json.dumps(line, default=str)


def setup(fmt: str, text_prefix: str = "") -> None:
    """Configure the root logger: `fmt` is "text" or "json"."""
    handler = logging.StreamHandler()
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(f"%(asctime)s {text_prefix}%(name)s %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)  # not one line per request
