"""
Tests for web/backend/exceptions.py

Covers the three async exception handlers via a FastAPI TestClient app
so the full request/response cycle is exercised without asyncio boilerplate.
"""

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from web.backend.exceptions import (
    ServiceException,
    MatchNotFoundException,
    JobNotFoundException,
    PipelineLockedException,
    InvalidPolicyException,
    NotificationException,
    service_exception_handler,
    http_exception_handler,
    general_exception_handler,
)


# ---------------------------------------------------------------------------
# Shared app fixture that registers all handlers and exposes one route per
# exception type.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client():
    app = FastAPI()
    app.add_exception_handler(ServiceException, service_exception_handler)
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(Exception, general_exception_handler)

    @app.get("/match_not_found")
    def _match_not_found():
        raise MatchNotFoundException("match 123 not found")

    @app.get("/job_not_found")
    def _job_not_found():
        raise JobNotFoundException("job 456 not found")

    @app.get("/invalid_policy")
    def _invalid_policy():
        raise InvalidPolicyException("min_fit out of range")

    @app.get("/pipeline_locked")
    def _pipeline_locked():
        raise PipelineLockedException("pipeline already running")

    @app.get("/notification_error")
    def _notification_error():
        raise NotificationException("discord webhook failed")

    @app.get("/base_service_error")
    def _base_service():
        raise ServiceException("generic service failure")

    @app.get("/http_error")
    def _http_error():
        raise HTTPException(status_code=403, detail="Forbidden")

    @app.get("/unexpected_error")
    def _unexpected():
        raise RuntimeError("something went very wrong")

    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Exception class hierarchy
# ---------------------------------------------------------------------------

class TestExceptionHierarchy:
    def test_all_are_service_exceptions(self):
        for cls in (
            MatchNotFoundException,
            JobNotFoundException,
            PipelineLockedException,
            InvalidPolicyException,
            NotificationException,
        ):
            assert issubclass(cls, ServiceException)

    def test_service_exception_is_exception(self):
        assert issubclass(ServiceException, Exception)


# ---------------------------------------------------------------------------
# service_exception_handler — status codes
# ---------------------------------------------------------------------------

class TestServiceExceptionHandler:
    def test_match_not_found_returns_404(self, client):
        response = client.get("/match_not_found")
        assert response.status_code == 404

    def test_job_not_found_returns_404(self, client):
        response = client.get("/job_not_found")
        assert response.status_code == 404

    def test_invalid_policy_returns_400(self, client):
        response = client.get("/invalid_policy")
        assert response.status_code == 400

    def test_pipeline_locked_returns_400(self, client):
        response = client.get("/pipeline_locked")
        assert response.status_code == 400

    def test_notification_exception_returns_500(self, client):
        response = client.get("/notification_error")
        assert response.status_code == 500

    def test_base_service_exception_returns_500(self, client):
        response = client.get("/base_service_error")
        assert response.status_code == 500

    def test_response_body_has_success_false(self, client):
        data = client.get("/match_not_found").json()
        assert data["success"] is False

    def test_response_body_has_error_message(self, client):
        data = client.get("/match_not_found").json()
        assert "match 123 not found" in data["error"]

    def test_response_body_has_type_field(self, client):
        data = client.get("/match_not_found").json()
        assert data["type"] == "MatchNotFoundException"

    def test_invalid_policy_type_field(self, client):
        data = client.get("/invalid_policy").json()
        assert data["type"] == "InvalidPolicyException"


# ---------------------------------------------------------------------------
# http_exception_handler
# ---------------------------------------------------------------------------

class TestHttpExceptionHandler:
    def test_http_403_status_code(self, client):
        response = client.get("/http_error")
        assert response.status_code == 403

    def test_http_response_body_success_false(self, client):
        data = client.get("/http_error").json()
        assert data["success"] is False

    def test_http_response_body_error_message(self, client):
        data = client.get("/http_error").json()
        assert data["error"] == "Forbidden"

    def test_http_response_body_type_field(self, client):
        data = client.get("/http_error").json()
        assert data["type"] == "HTTPException"


# ---------------------------------------------------------------------------
# general_exception_handler
# ---------------------------------------------------------------------------

class TestGeneralExceptionHandler:
    def test_unexpected_error_returns_500(self, client):
        response = client.get("/unexpected_error")
        assert response.status_code == 500

    def test_unexpected_error_success_false(self, client):
        data = client.get("/unexpected_error").json()
        assert data["success"] is False

    def test_unexpected_error_generic_message(self, client):
        data = client.get("/unexpected_error").json()
        assert "Internal server error" in data["error"]

    def test_unexpected_error_type_field(self, client):
        data = client.get("/unexpected_error").json()
        assert data["type"] == "InternalError"
