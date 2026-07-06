from __future__ import annotations

import json
import re

from core.oci_critical_logging import emit_oci_critical_event


def _read_events(path):
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_critical_logging_disabled_by_default(monkeypatch, tmp_path):
    monkeypatch.delenv("OCI_CRITICAL_LOG_ENABLED", raising=False)
    monkeypatch.setenv("OCI_CRITICAL_LOG_DIR", str(tmp_path))

    assert emit_oci_critical_event("provider_canary", status="succeeded") is False

    assert list(tmp_path.iterdir()) == []


def test_critical_logging_writes_redacted_jsonl(monkeypatch, tmp_path):
    monkeypatch.setenv("OCI_CRITICAL_LOG_ENABLED", "true")
    monkeypatch.setenv("OCI_CRITICAL_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("JOBSCOUT_SERVICE_NAME", "llm-evaluation-worker")
    monkeypatch.setenv("OCI_CRITICAL_LOG_DAILY_BYTES_CAP_MB", "1")

    assert emit_oci_critical_event(
        "provider_canary",
        status="failed",
        provider="nvidia",
        api_key="secret",
        authorization="Bearer secret",
        prompt="do not log this",
        job_description="do not log this either",
        error_category="timeout",
    )

    events = _read_events(tmp_path / "llm_evaluation_worker.jsonl")
    assert events[0]["event_type"] == "provider_canary"
    assert events[0]["service"] == "llm_evaluation_worker"
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", events[0]["timestamp"])
    assert events[0]["provider"] == "nvidia"
    assert events[0]["error_category"] == "timeout"
    assert "api_key" not in events[0]
    assert "authorization" not in events[0]
    assert "prompt" not in events[0]
    assert "job_description" not in events[0]
    assert "secret" not in str(events[0])


def test_critical_logging_enforces_daily_byte_cap(monkeypatch, tmp_path):
    monkeypatch.setenv("OCI_CRITICAL_LOG_ENABLED", "true")
    monkeypatch.setenv("OCI_CRITICAL_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("JOBSCOUT_SERVICE_NAME", "app")
    monkeypatch.setenv("OCI_CRITICAL_LOG_DAILY_BYTES_CAP_MB", "1")

    assert emit_oci_critical_event("readiness_check", status="ok", padding="x" * 200)
    state_file = tmp_path / ".app.state.json"
    state = json.loads(state_file.read_text(encoding="utf-8"))
    state["bytes"] = 1024 * 1024 - 20
    state_file.write_text(json.dumps(state), encoding="utf-8")

    assert emit_oci_critical_event("readiness_check", status="ok", padding="x" * 200) is False

    assert len(_read_events(tmp_path / "app.jsonl")) == 1


def test_critical_logging_respects_budget_disable_marker(monkeypatch, tmp_path):
    monkeypatch.setenv("OCI_CRITICAL_LOG_ENABLED", "true")
    monkeypatch.setenv("OCI_CRITICAL_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("JOBSCOUT_SERVICE_NAME", "app")
    (tmp_path / ".oci-critical-logging-disabled").write_text("disabled", encoding="utf-8")

    assert emit_oci_critical_event("provider_canary", status="succeeded") is False

    assert not (tmp_path / "app.jsonl").exists()
