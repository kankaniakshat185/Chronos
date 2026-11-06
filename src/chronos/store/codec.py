"""WAL record framing (ADR-0015).

    ┌────────────┬────────────┬──────────────────────────────────────┐
    │ u32 length │ u32 crc32  │ payload: JSON {rev, op, kind, name,  │
    │ (payload)  │ (payload)  │          object?}                    │
    └────────────┴────────────┴──────────────────────────────────────┘

Little-endian. `op` is "put" (object written whole) or "delete". A frame either
decodes completely with a matching CRC, or it is invalid: there is no partial read.
"""

from __future__ import annotations

import json
import struct
import zlib
from dataclasses import dataclass

HEADER = struct.Struct("<II")
MAX_PAYLOAD = 64 * 1024 * 1024


@dataclass(frozen=True)
class Record:
    rev: int
    op: str  # "put" | "delete"
    kind: str
    name: str
    obj: dict[str, object] | None = None  # the object's JSON for "put"


class InvalidFrame(ValueError):
    pass


def encode(record: Record) -> bytes:
    body: dict[str, object] = {
        "rev": record.rev,
        "op": record.op,
        "kind": record.kind,
        "name": record.name,
    }
    if record.obj is not None:
        body["object"] = record.obj
    payload = json.dumps(body, separators=(",", ":"), sort_keys=True).encode()
    return HEADER.pack(len(payload), zlib.crc32(payload)) + payload


def decode(buf: bytes | memoryview, offset: int = 0) -> tuple[Record, int]:
    """The record at `offset` and the offset just after it; InvalidFrame if there's
    no complete, intact record there."""
    end_header = offset + HEADER.size
    if end_header > len(buf):
        raise InvalidFrame("short header")
    length, crc = HEADER.unpack_from(buf, offset)
    if length > MAX_PAYLOAD or end_header + length > len(buf):
        raise InvalidFrame("length past the end")
    payload = bytes(buf[end_header : end_header + length])
    if zlib.crc32(payload) != crc:
        raise InvalidFrame("crc mismatch")
    try:
        body = json.loads(payload)
        record = Record(
            rev=int(body["rev"]),
            op=str(body["op"]),
            kind=str(body["kind"]),
            name=str(body["name"]),
            obj=body.get("object"),
        )
    except (ValueError, KeyError, TypeError) as exc:
        raise InvalidFrame(f"bad payload: {exc}") from None
    if record.op not in ("put", "delete"):
        raise InvalidFrame(f"bad op {record.op!r}")
    return record, end_header + length
