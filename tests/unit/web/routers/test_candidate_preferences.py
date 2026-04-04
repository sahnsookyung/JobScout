"""Unit tests for web/backend/routers/candidate_preferences.py"""

from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.backend.dependencies import get_current_user
from web.backend.routers.candidate_preferences import (
    get_candidate_preferences_service,
    router,
)


_PREFS_PAYLOAD = {
    "remote_mode": "remote",
    "target_locations": [],
    "visa_sponsorship_required": False,
    "salary_min": None,
    "employment_types": [],
    "soft_preferences": "",
    "soft_preference_summary": None,
    "preference_mode": "semantic_rerank",
    "allowed_preference_modes": ["semantic_rerank"],
    "effective_preference_mode": "semantic_rerank",
    "revision": 1,
}


class _User:
    def __init__(self):
        self.id = "user-1"


@pytest.fixture
def mock_service():
    svc = Mock()
    svc.get_preferences.return_value = _PREFS_PAYLOAD
    svc.update_preferences.return_value = _PREFS_PAYLOAD
    return svc


@pytest.fixture
def app(mock_service):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_candidate_preferences_service] = lambda: mock_service
    app.dependency_overrides[get_current_user] = lambda: _User()
    yield app
    app.dependency_overrides.clear()


@pytest.fixture
def client(app):
    return TestClient(app, raise_server_exceptions=True)


class TestCandidatePreferencesRouter:
    def test_get_returns_preferences(self, client, mock_service):
        response = client.get("/api/v1/candidate-preferences")
        assert response.status_code == 200
        mock_service.get_preferences.assert_called_once()
        assert response.json()["remote_mode"] == "remote"

    def test_put_updates_and_returns_preferences(self, client, mock_service):
        body = {
            "remote_mode": "remote",
            "target_locations": [],
            "visa_sponsorship_required": False,
            "salary_min": None,
            "employment_types": [],
            "soft_preferences": "Python FastAPI",
            "preference_mode": "semantic_rerank",
        }
        response = client.put("/api/v1/candidate-preferences", json=body)
        assert response.status_code == 200
        mock_service.update_preferences.assert_called_once()
        assert response.json()["revision"] == 1
