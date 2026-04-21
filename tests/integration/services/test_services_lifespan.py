#!/usr/bin/env python3
"""Smoke coverage for the Prometheus /metrics endpoint on each microservice.

Replaces the deleted JSON worker-liveness /metrics endpoints. The consumer
task state that those previously surfaced is now covered by /health plus
the Prometheus counters themselves.
"""

from fastapi.testclient import TestClient


def _assert_prometheus_metrics(app) -> None:
    client = TestClient(app)
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers.get("content-type", "")
    assert b"jobscout_scorer_route_total" in response.content


class TestEmbeddingsMetrics:
    def test_metrics_endpoint_prometheus(self):
        from services.embeddings.main import app
        _assert_prometheus_metrics(app)


class TestExtractionMetrics:
    def test_metrics_endpoint_prometheus(self):
        from services.extraction.main import app
        _assert_prometheus_metrics(app)


class TestMatcherMetrics:
    def test_metrics_endpoint_prometheus(self):
        from services.scorer_matcher.main import app
        _assert_prometheus_metrics(app)
