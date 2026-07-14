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
