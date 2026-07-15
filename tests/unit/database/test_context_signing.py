from __future__ import annotations

import pytest

from database.context_signing import (
    database_context_secret_is_configured,
    sign_database_context,
    sign_database_readiness_probe,
    sign_google_login_provisioning,
)


def test_database_context_signature_binds_user_and_tenant(monkeypatch) -> None:
    monkeypatch.setenv(
        "JOBSCOUT_DB_CONTEXT_SECRET",
        "unit-test-context-secret-at-least-32-characters",
    )

    user_a = sign_database_context(user_id="user-a", tenant_id="tenant")
    user_b = sign_database_context(user_id="user-b", tenant_id="tenant")
    other_tenant = sign_database_context(user_id="user-a", tenant_id="other")

    assert len(user_a) == 64
    assert user_a != user_b
    assert user_a != other_tenant


def test_database_context_secret_rejects_documented_placeholder(monkeypatch) -> None:
    monkeypatch.setenv(
        "JOBSCOUT_DB_CONTEXT_SECRET",
        "replace-with-at-least-32-random-characters",
    )

    assert database_context_secret_is_configured() is False

    with pytest.raises(RuntimeError, match="non-placeholder"):
        sign_database_readiness_probe()


def test_google_login_signature_binds_every_privileged_argument(monkeypatch) -> None:
    monkeypatch.setenv(
        "JOBSCOUT_DB_CONTEXT_SECRET",
        "unit-test-context-secret-at-least-32-characters",
    )
    parameters = {
        "provider_subject": " google-subject ",
        "verified_email": "USER@example.com",
        "display_name": "Example User",
        "allow_create": True,
        "public_tenant_id": "tenant-id",
        "retention_seconds": 14_400,
    }

    signature = sign_google_login_provisioning(**parameters)
    altered = sign_google_login_provisioning(
        **{**parameters, "retention_seconds": 86_400}
    )

    assert len(signature) == 64
    assert signature != altered


def test_signing_fails_closed_for_short_secret(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_DB_CONTEXT_SECRET", "too-short")

    with pytest.raises(RuntimeError, match="at least 32"):
        sign_database_context(user_id="user", tenant_id="tenant")
