"""Unit tests for backend auth dependency safety rules."""

import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi import HTTPException
import pytest


def test_dev_bypass_is_rejected_outside_dev_test(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "dev-bypass")
    monkeypatch.setenv("JOBSCOUT_ENV", "production")

    module = importlib.import_module("web.backend.dependencies")

    with pytest.raises(RuntimeError, match="only allowed in development/test environments"):
        module._ensure_dev_bypass_allowed()


def test_dev_bypass_is_allowed_in_test_environment(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "dev-bypass")
    monkeypatch.setenv("JOBSCOUT_ENV", "test")

    module = importlib.import_module("web.backend.dependencies")

    module._ensure_dev_bypass_allowed()


def test_current_environment_prefers_jobscout_env(monkeypatch):
    monkeypatch.setenv("JOBSCOUT_ENV", "Production")
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("ENVIRONMENT", "test")

    module = importlib.import_module("core.auth")

    assert module._current_environment() == "production"


def test_auth_mode_is_normalized(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", " DEV-BYPASS ")

    module = importlib.import_module("web.backend.dependencies")

    assert module._auth_mode() == "dev-bypass"


def test_ensure_dev_user_returns_existing_user(monkeypatch):
    monkeypatch.setenv("DEV_BYPASS_EMAIL", "existing@example.com")

    module = importlib.import_module("web.backend.dependencies")
    session = MagicMock()
    existing_user = SimpleNamespace(id="user-1", email="existing@example.com")
    session.execute.return_value.scalar_one_or_none.return_value = existing_user

    result = module._ensure_dev_user(session)

    assert result is existing_user
    session.add.assert_not_called()
    session.commit.assert_not_called()


def test_ensure_dev_user_creates_seed_user(monkeypatch):
    monkeypatch.setenv("DEV_BYPASS_EMAIL", "new-user@example.com")
    monkeypatch.setenv("DEV_BYPASS_NAME", "Dev Seed")
    monkeypatch.setenv("DEV_BYPASS_USER_ID", "00000000-0000-0000-0000-000000000123")

    module = importlib.import_module("web.backend.dependencies")
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None

    created_user = module._ensure_dev_user(session)

    assert str(created_user.id) == "00000000-0000-0000-0000-000000000123"
    assert created_user.email == "new-user@example.com"
    assert created_user.display_name == "Dev Seed"
    assert session.add.call_count == 2
    session.commit.assert_called_once()
    session.refresh.assert_called_once()


def test_get_current_user_returns_seeded_user_in_dev_bypass(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "dev-bypass")
    monkeypatch.setenv("JOBSCOUT_ENV", "test")

    module = importlib.import_module("web.backend.dependencies")
    fake_session = MagicMock()
    fake_user = SimpleNamespace(id="user-1")

    with patch.object(module, "_ensure_dev_user", return_value=fake_user) as mock_ensure_user:
        module._db_manager = SimpleNamespace(session_local=lambda: fake_session)
        result = module.get_current_user()

    assert result is fake_user
    mock_ensure_user.assert_called_once_with(fake_session)
    fake_session.expunge.assert_called_once_with(fake_user)
    fake_session.close.assert_called_once()


def test_get_current_user_requires_auth_when_not_in_dev_bypass(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "google")
    monkeypatch.setenv("JOBSCOUT_ENV", "test")

    module = importlib.import_module("web.backend.dependencies")

    with pytest.raises(HTTPException) as exc_info:
        module.get_current_user()

    assert exc_info.value.status_code == 401
