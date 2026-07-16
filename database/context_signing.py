"""HMAC envelopes for database identities and privileged auth operations."""

from __future__ import annotations

import hashlib
import hmac
import os
from collections.abc import Iterable

MIN_DATABASE_CONTEXT_SECRET_LENGTH = 32
DATABASE_CONTEXT_SECRET_ENV = "JOBSCOUT_DB_CONTEXT_SECRET"
RLS_CONTEXT_NAMESPACE = "jobscout-rls-v1"
GOOGLE_LOGIN_NAMESPACE = "jobscout-google-login-v1"
READINESS_NAMESPACE = "jobscout-readiness-v1"
INSECURE_DATABASE_CONTEXT_SECRETS = frozenset({"change-me", "replace-me"})


def database_context_signing_enabled() -> bool:
    """Return whether database-enforced RLS activation is enabled."""
    return os.getenv("JOBSCOUT_CLOUD_ACTIVATE_RLS", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def database_context_secret_is_configured() -> bool:
    """Return whether a non-placeholder signing secret is configured."""
    return database_context_secret_is_valid(
        os.getenv(DATABASE_CONTEXT_SECRET_ENV, ""),
    )


def database_context_secret_is_valid(secret: str) -> bool:
    """Reject short and documented placeholder signing secrets."""
    normalized = secret.strip()
    return (
        len(normalized) >= MIN_DATABASE_CONTEXT_SECRET_LENGTH
        and normalized not in INSECURE_DATABASE_CONTEXT_SECRETS
        and not normalized.startswith("replace-")
    )


def _database_context_secret() -> bytes:
    secret = os.getenv(DATABASE_CONTEXT_SECRET_ENV, "").strip()
    if not database_context_secret_is_valid(secret):
        raise RuntimeError(
            f"{DATABASE_CONTEXT_SECRET_ENV} must be a non-placeholder secret with at least "
            f"{MIN_DATABASE_CONTEXT_SECRET_LENGTH} characters when RLS is active."
        )
    return secret.encode("utf-8")


def _canonical_payload(namespace: str, values: Iterable[object | None]) -> str:
    components = [namespace]
    for value in values:
        normalized = "" if value is None else str(value)
        components.append(f"{len(normalized)}:{normalized}")
    return "|".join(components)


def _sign(payload: str) -> str:
    return hmac.new(
        _database_context_secret(),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def sign_database_context(
    *,
    user_id: object | None,
    tenant_id: object | None,
) -> str:
    """Sign the exact transaction-local identity consumed by PostgreSQL RLS."""
    payload = _canonical_payload(RLS_CONTEXT_NAMESPACE, (user_id, tenant_id))
    return _sign(payload)


def sign_database_readiness_probe() -> str:
    """Prove that the application and database use the same signing secret."""
    return _sign(READINESS_NAMESPACE)


def sign_google_login_provisioning(
    *,
    provider_subject: str,
    verified_email: str,
    display_name: str,
    allow_create: bool,
    public_tenant_id: object | None,
    retention_seconds: int,
) -> str:
    """Sign all caller-controlled arguments to privileged login provisioning."""
    payload = _canonical_payload(
        GOOGLE_LOGIN_NAMESPACE,
        (
            provider_subject.strip(),
            verified_email.strip().lower(),
            display_name,
            str(allow_create).lower(),
            public_tenant_id,
            retention_seconds,
        ),
    )
    return _sign(payload)
