"""Redacted, capped JSONL events for OCI critical-only logging."""

from __future__ import annotations

import fcntl
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_DEFAULT_LOG_DIR = "/var/log/jobscout/oci-critical"
_DEFAULT_DAILY_CAP_MB = 5
_MAX_STRING_LENGTH = 300
_SAFE_KEY = re.compile(r"[^a-zA-Z0-9_.-]+")
_SAFE_SERVICE = re.compile(r"[^a-zA-Z0-9_-]+")
_SENSITIVE_KEY_PARTS = (
    "api_key",
    "authorization",
    "body",
    "content",
    "cookie",
    "credential",
    "description",
    "email",
    "job_description",
    "password",
    "prompt",
    "raw",
    "resume",
    "secret",
    "token",
    "url",
)


def _enabled() -> bool:
    return os.getenv("OCI_CRITICAL_LOG_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _service_name(value: str | None = None) -> str:
    raw = value or os.getenv("JOBSCOUT_SERVICE_NAME") or "app"
    safe = _SAFE_SERVICE.sub("_", raw.strip().lower().replace("-", "_")).strip("_")
    return safe[:80] or "app"


def _log_dir() -> Path:
    return Path(os.getenv("OCI_CRITICAL_LOG_DIR", _DEFAULT_LOG_DIR))


def _disabled_by_budget_marker() -> bool:
    return (_log_dir() / ".oci-critical-logging-disabled").exists()


def _daily_cap_bytes() -> int:
    raw = os.getenv("OCI_CRITICAL_LOG_DAILY_BYTES_CAP_MB")
    try:
        mb = int(raw) if raw is not None else _DEFAULT_DAILY_CAP_MB
    except ValueError:
        mb = _DEFAULT_DAILY_CAP_MB
    return max(mb, 0) * 1024 * 1024


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _sanitize_key(key: Any) -> str:
    safe = _SAFE_KEY.sub("_", str(key).strip()).strip("_")
    return safe[:80] or "field"


def _is_sensitive_key(key: str) -> bool:
    lowered = key.lower()
    return any(part in lowered for part in _SENSITIVE_KEY_PARTS)


def _sanitize_value(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    text = str(value)
    if len(text) > _MAX_STRING_LENGTH:
        return text[:_MAX_STRING_LENGTH] + "...truncated"
    return text


def _sanitize_fields(fields: dict[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for raw_key, raw_value in fields.items():
        key = _sanitize_key(raw_key)
        if _is_sensitive_key(key):
            continue
        sanitized[key] = _sanitize_value(raw_value)
    return sanitized


def _read_state(state_file: Path) -> dict[str, Any]:
    if not state_file.exists():
        return {"date": _today(), "bytes": 0}
    try:
        payload = json.loads(state_file.read_text(encoding="utf-8") or "{}")
    except (OSError, json.JSONDecodeError):
        return {"date": _today(), "bytes": 0}
    if payload.get("date") != _today():
        return {"date": _today(), "bytes": 0}
    try:
        used = int(payload.get("bytes") or 0)
    except (TypeError, ValueError):
        used = 0
    return {"date": _today(), "bytes": max(used, 0)}


def _write_state(state_file: Path, state: dict[str, Any]) -> None:
    state_file.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")


def _record_metric(event_type: str, outcome: str, byte_count: int = 0, reason: str | None = None) -> None:
    try:
        from core.metrics import (
            observe_oci_critical_log_bytes,
            record_oci_critical_log_drop,
            record_oci_critical_log_event,
        )

        record_oci_critical_log_event(event_type, outcome)
        if byte_count > 0:
            observe_oci_critical_log_bytes(event_type, byte_count)
        if reason:
            record_oci_critical_log_drop(reason)
    except Exception:
        pass


def emit_oci_critical_event(
    event_type: str,
    *,
    severity: str = "info",
    service: str | None = None,
    **fields: Any,
) -> bool:
    """Append one redacted critical event if enabled and below the daily cap."""
    if not _enabled():
        _record_metric(event_type, "disabled", reason="disabled")
        return False
    if _disabled_by_budget_marker():
        _record_metric(event_type, "disabled", reason="cap_exceeded")
        return False

    service_name = _service_name(service)
    event = {
        "timestamp": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "schema_version": 1,
        "service": service_name,
        "event_type": _sanitize_key(event_type),
        "severity": _sanitize_key(severity),
        **_sanitize_fields(fields),
    }
    line = json.dumps(event, sort_keys=True, separators=(",", ":"), default=str) + "\n"
    encoded = line.encode("utf-8")
    cap = _daily_cap_bytes()
    if cap <= 0:
        _record_metric(event_type, "dropped", reason="cap_disabled")
        return False

    directory = _log_dir()
    log_file = directory / f"{service_name}.jsonl"
    state_file = directory / f".{service_name}.state.json"
    lock_file = directory / f".{service_name}.lock"
    try:
        directory.mkdir(parents=True, exist_ok=True)
        with lock_file.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            state = _read_state(state_file)
            used = int(state.get("bytes") or 0)
            if used + len(encoded) > cap:
                ratio = min((used / cap) if cap else 1.0, 1.0)
                try:
                    from core.metrics import set_oci_critical_log_budget_usage_ratio

                    set_oci_critical_log_budget_usage_ratio(service_name, ratio)
                except Exception:
                    pass
                _record_metric(event_type, "dropped", reason="cap_exceeded")
                return False
            with log_file.open("a", encoding="utf-8") as log_handle:
                log_handle.write(line)
            state["bytes"] = used + len(encoded)
            _write_state(state_file, state)
            try:
                from core.metrics import set_oci_critical_log_budget_usage_ratio

                set_oci_critical_log_budget_usage_ratio(service_name, state["bytes"] / cap)
            except Exception:
                pass
        _record_metric(event_type, "written", len(encoded))
        return True
    except OSError:
        _record_metric(event_type, "error", reason="write_error")
        return False
