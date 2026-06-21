"""Opaque cursor helpers for additive list pagination."""

from __future__ import annotations

import base64
import json
from typing import Any

CURSOR_VERSION = 1


class CursorDecodeError(ValueError):
    """Raised when an opaque pagination cursor cannot be decoded safely."""


class MatchCursorCodec:
    """Encode and decode small opaque cursor payloads.

    The payload is intentionally generic so the same helper can support match,
    pipeline-run, and operational blocker lists without creating parallel
    endpoint-specific cursor machinery.
    """

    @staticmethod
    def encode(kind: str, **payload: Any) -> str:
        data = {"v": CURSOR_VERSION, "kind": kind, **payload}
        raw = json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    @staticmethod
    def decode(cursor: str | None, *, expected_kind: str) -> dict[str, Any] | None:
        if not cursor:
            return None
        padding = "=" * (-len(cursor) % 4)
        try:
            raw = base64.urlsafe_b64decode((cursor + padding).encode("ascii"))
            data = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise CursorDecodeError("Invalid cursor.") from exc
        if not isinstance(data, dict):
            raise CursorDecodeError("Invalid cursor.")
        if data.get("v") != CURSOR_VERSION or data.get("kind") != expected_kind:
            raise CursorDecodeError("Cursor does not apply to this list.")
        return data
