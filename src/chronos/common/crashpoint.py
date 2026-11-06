"""Crash-point injection for durability tests (ADR-0015, P6.5).

`CHRONOS_CRASHPOINT=<name>[:<nth>]` makes the nth hit of crash point <name> exit the
process at once with status 137 (like `kill -9`): no cleanup, no flushing.
Unset (the normal case), `hit()` costs one dict lookup.
"""

from __future__ import annotations

import os

CRASH_EXIT = 137

_target: tuple[str, int] | None = None
_hits: dict[str, int] = {}


def _parse(spec: str | None) -> tuple[str, int] | None:
    if not spec:
        return None
    name, _, nth = spec.partition(":")
    return name, int(nth) if nth else 1


_target = _parse(os.environ.get("CHRONOS_CRASHPOINT"))


def armed(name: str) -> bool:
    """Count a hit of `name`; True if this is the hit that should crash."""
    _hits[name] = _hits.get(name, 0) + 1
    return _target is not None and _target[0] == name and _hits[name] == _target[1]


def hits() -> dict[str, int]:
    """How often each crash point was passed in this process (to calibrate tests)."""
    return dict(_hits)


def reset() -> None:
    _hits.clear()


def hit(name: str) -> None:
    if armed(name):
        os._exit(CRASH_EXIT)
