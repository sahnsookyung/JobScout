"""Stable hashing utilities for resume variant freshness checks."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from datetime import date, datetime
from typing import Any


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def canonical_json_bytes(value: Any) -> bytes:
    """Return a stable UTF-8 JSON representation for hashing and tests."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    ).encode("utf-8")


def canonical_json_hash(value: Any) -> str:
    """Hash JSON-like data without depending on dict insertion order."""
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()
