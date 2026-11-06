"""Store errors (ADR-0007), shared by the server and both clients (ADR-0004).

Each error knows its HTTP status and can round-trip through an error body
(`reason`, `message`, `details`), so `HttpStoreClient` raises the same exception
the store raised on the server.
"""

from __future__ import annotations

from chronos.api.wire import ErrorBody


class StoreError(Exception):
    """Base class: every store failure leaves state and revision unchanged."""

    status = 500

    def body(self) -> ErrorBody:
        return ErrorBody(reason=type(self).__name__, message=str(self))


class NotFound(StoreError):
    status = 404

    def __init__(self, kind: str, name: str) -> None:
        super().__init__(f"{kind} {name!r} not found")
        self.kind = kind
        self.name = name

    def body(self) -> ErrorBody:
        return ErrorBody(
            reason="NotFound", message=str(self), details={"kind": self.kind, "name": self.name}
        )


class AlreadyExists(StoreError):
    status = 409

    def __init__(self, kind: str, name: str) -> None:
        super().__init__(f"{kind} {name!r} already exists")
        self.kind = kind
        self.name = name

    def body(self) -> ErrorBody:
        return ErrorBody(
            reason="AlreadyExists",
            message=str(self),
            details={"kind": self.kind, "name": self.name},
        )


class Conflict(StoreError):
    """A compare-and-swap lost: the caller's resource_version is stale."""

    status = 409

    def __init__(self, kind: str, name: str, expected: int, current: int) -> None:
        super().__init__(
            f"{kind} {name!r}: resource_version {expected} is stale (current {current})"
        )
        self.kind = kind
        self.name = name
        self.expected = expected
        self.current = current

    def body(self) -> ErrorBody:
        details: dict[str, str | int | list[str]] = {
            "kind": self.kind,
            "name": self.name,
            "expected": self.expected,
            "current": self.current,
        }
        return ErrorBody(reason="Conflict", message=str(self), details=details)


class Invalid(StoreError):
    """The write breaks a structural rule (immutable field, bind-once, ...)."""

    status = 422


class InsufficientCapacity(StoreError):
    """Binding would overcommit at least one resource on the node (S2, S11)."""

    status = 409

    def __init__(self, node: str, resources: list[str]) -> None:
        super().__init__(f"node {node!r}: Insufficient {', '.join(resources)}")
        self.node = node
        self.resources = resources

    def body(self) -> ErrorBody:
        return ErrorBody(
            reason="InsufficientCapacity",
            message=str(self),
            details={"node": self.node, "resources": self.resources},
        )


class EpochMismatch(StoreError):
    """A heartbeat from an agent incarnation the server no longer recognizes (ADR-0011).

    The agent must re-register before its heartbeats count again.
    """

    status = 409


class Compacted(StoreError):
    """A watch asked for history the server no longer has (ADR-0008): relist, then
    watch from the list's revision. Never a partial answer (S6)."""

    status = 410


class Unavailable(StoreError):
    """The control plane couldn't be reached (timeout or connection error). Client-side only."""

    status = 503


def from_body(body: ErrorBody) -> StoreError:
    """Rebuild the exception the server raised; unknown reasons become a plain StoreError."""
    d = body.details

    def s(key: str) -> str:
        return str(d.get(key, ""))

    def i(key: str) -> int:
        value = d.get(key, 0)
        return value if isinstance(value, int) else 0

    match body.reason:
        case "NotFound":
            return NotFound(s("kind"), s("name"))
        case "AlreadyExists":
            return AlreadyExists(s("kind"), s("name"))
        case "Conflict":
            return Conflict(s("kind"), s("name"), i("expected"), i("current"))
        case "InsufficientCapacity":
            resources = d.get("resources", [])
            return InsufficientCapacity(s("node"), resources if isinstance(resources, list) else [])
    return _MESSAGE_ONLY.get(body.reason, StoreError)(body.message)


_MESSAGE_ONLY: dict[str, type[StoreError]] = {
    cls.__name__: cls for cls in (Invalid, EpochMismatch, Compacted, Unavailable)
}
