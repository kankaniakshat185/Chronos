"""Store errors (ADR-0007). The server maps them to HTTP status codes (ADR-0004)."""

from __future__ import annotations


class StoreError(Exception):
    """Base class: every store failure leaves state and revision unchanged."""


class NotFound(StoreError):
    def __init__(self, kind: str, name: str) -> None:
        super().__init__(f"{kind} {name!r} not found")
        self.kind = kind
        self.name = name


class AlreadyExists(StoreError):
    def __init__(self, kind: str, name: str) -> None:
        super().__init__(f"{kind} {name!r} already exists")
        self.kind = kind
        self.name = name


class Conflict(StoreError):
    """A compare-and-swap lost: the caller's resource_version is stale."""

    def __init__(self, kind: str, name: str, expected: int, current: int) -> None:
        super().__init__(
            f"{kind} {name!r}: resource_version {expected} is stale (current {current})"
        )
        self.expected = expected
        self.current = current


class Invalid(StoreError):
    """The write breaks a structural rule (immutable field, bind-once, ...)."""


class InsufficientCapacity(StoreError):
    """Binding would overcommit at least one resource on the node (S2, S11)."""

    def __init__(self, node: str, resources: list[str]) -> None:
        super().__init__(f"node {node!r}: Insufficient {', '.join(resources)}")
        self.node = node
        self.resources = resources
