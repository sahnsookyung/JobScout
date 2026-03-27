"""Unit tests for backend auth dependency safety rules."""

import importlib

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
