from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from core.ephemeral_quota import (
    EphemeralQuotaExceeded,
    consume_ephemeral_quota,
)


def test_account_quota_is_indexed_without_a_fixed_expiry(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_PUBLIC_TESTING_QUOTAS_ENABLED", "true")
    client = Mock()
    client.get.return_value = None
    client.eval.return_value = [1, 1]

    remaining = consume_ephemeral_quota(
        "owner-1",
        "resume_uploads",
        default_limit=3,
        client=client,
    )

    assert remaining == 2
    eval_args = client.eval.call_args.args
    assert eval_args[1:] == (
        2,
        "jobscout-cloud:account-quota:owner-1:resume_uploads",
        "jobscout-cloud:user-quota-keys:owner-1",
        3,
    )
    assert "EXPIRE" not in eval_args[0]


def test_account_quota_rejects_operations_after_the_lifetime_limit(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_PUBLIC_TESTING_QUOTAS_ENABLED", "true")
    client = Mock()
    client.get.return_value = None
    client.eval.return_value = [0, 3]

    with patch("core.ephemeral_quota.record_public_security_event") as record_event:
        with pytest.raises(EphemeralQuotaExceeded, match="quota exceeded"):
            consume_ephemeral_quota(
                "owner-1",
                "resume_uploads",
                default_limit=3,
                client=client,
            )

    record_event.assert_called_once_with("quota_exhausted")


@pytest.mark.parametrize("operation", ["llm_evaluations", "matching_runs", "resume_variants", "resume_uploads"])
def test_verified_admin_quota_survives_expired_login_cache(monkeypatch, operation):
    from database.database import database_context
    from core import ephemeral_quota

    monkeypatch.setenv("JOBSCOUT_PUBLIC_TESTING_QUOTAS_ENABLED", "true")
    monkeypatch.setattr(ephemeral_quota, "admin_budget_exempt", lambda: True)
    client = Mock()
    client.get.return_value = None
    client.eval.side_effect = AssertionError("Admin must not consume a capped account allocation")
    with database_context(user_id="admin", tenant_id="shared"):
        assert ephemeral_quota.consume_ephemeral_quota("admin", operation, default_limit=3, client=client) is None
    client.get.assert_not_called()
    client.eval.assert_not_called()


def test_admin_context_does_not_exempt_another_owners_quota(monkeypatch):
    from database.database import database_context
    from core import ephemeral_quota

    monkeypatch.setenv("JOBSCOUT_PUBLIC_TESTING_QUOTAS_ENABLED", "true")
    verify = Mock(return_value=True)
    monkeypatch.setattr(ephemeral_quota, "admin_budget_exempt", verify)
    client = Mock()
    client.get.return_value = None
    client.eval.return_value = [0, 3]
    with database_context(user_id="admin", tenant_id="shared"):
        with pytest.raises(ephemeral_quota.EphemeralQuotaExceeded):
            ephemeral_quota.consume_ephemeral_quota("ordinary", "llm_evaluations", default_limit=3, client=client)
    verify.assert_not_called()


def test_admin_identity_lookup_failure_does_not_fall_back_to_stale_cache(monkeypatch):
    from database.database import database_context
    from core import ephemeral_quota

    monkeypatch.setenv("JOBSCOUT_PUBLIC_TESTING_QUOTAS_ENABLED", "true")
    monkeypatch.setattr(ephemeral_quota, "admin_budget_exempt", Mock(side_effect=RuntimeError("db unavailable")))
    client = Mock()
    client.get.return_value = "1"
    with database_context(user_id="admin", tenant_id="shared"):
        with pytest.raises(ephemeral_quota.EphemeralQuotaUnavailable, match="identity verification"):
            ephemeral_quota.consume_ephemeral_quota("admin", "llm_evaluations", default_limit=3, client=client)
    client.get.assert_not_called()
