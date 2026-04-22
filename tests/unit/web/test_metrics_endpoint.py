"""Smoke test: the backend exposes /metrics in Prometheus text format."""

from __future__ import annotations

from fastapi.testclient import TestClient

from web.backend.app import app


def test_metrics_endpoint_returns_prometheus_text():
    client = TestClient(app)
    response = client.get("/metrics")

    assert response.status_code == 200
    assert "text/plain" in response.headers.get("content-type", "")

    # Every declared metric must be present in the exposition payload
    # (HELP/TYPE lines are emitted even when counters are zero).
    body = response.text
    for name in (
        "jobscout_scorer_route_total",
        "jobscout_scorer_degraded_reason_total",
        "jobscout_evidence_rerank_latency_ms",
        "jobscout_selection_tier_items_total",
        "jobscout_preference_reranker_status_total",
        "jobscout_email_verification_events_total",
        "jobscout_worker_running",
    ):
        assert name in body, f"missing metric in /metrics output: {name}"
