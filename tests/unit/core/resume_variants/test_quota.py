from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from core.resume_variants.quota import (
    ResumeVariantConcurrencyError,
    ResumeVariantQuota,
    ResumeVariantQuotaExceeded,
    ResumeVariantQuotaUnavailable,
)


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.counts: dict[str, int] = {}
        self.ttls: dict[str, int] = {}
        self.renewed = threading.Event()
        self._lock = threading.Lock()

    def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None):
        with self._lock:
            if nx and key in self.values:
                return False
            self.values[key] = value
            if ex is not None:
                self.ttls[key] = ex
            return True

    def get(self, key: str):
        with self._lock:
            return self.values.get(key)

    def sadd(self, key: str, *values: str) -> int:
        del key
        return len(values)

    def expire(self, key: str, ttl: int) -> bool:
        with self._lock:
            self.ttls[key] = ttl
        return True

    def eval(self, script: str, numkeys: int, *args):
        with self._lock:
            if numkeys == 1:
                if "resume-variant-check" in script:
                    key, token = args
                    return int(self.values.get(key) == token)
                if "EXPIRE" in script:
                    key, token, ttl = args
                    if self.values.get(key) == token:
                        self.ttls[key] = int(ttl)
                        self.renewed.set()
                        return 1
                    return 0
                key, token = args
                if self.values.get(key) == token:
                    del self.values[key]
                    self.ttls.pop(key, None)
                    return 1
                return 0

            daily_key, hourly_key, daily_limit, hourly_limit, *_ttl = args
            daily_limit = int(daily_limit)
            hourly_limit = int(hourly_limit)
            daily_count = self.counts.get(daily_key, 0)
            hourly_count = self.counts.get(hourly_key, 0)
            if daily_count >= daily_limit:
                return [0, "daily", daily_count, 3600]
            if hourly_count >= hourly_limit:
                return [0, "hourly", hourly_count, 3600]
            daily_count += 1
            hourly_count += 1
            self.counts[daily_key] = daily_count
            self.counts[hourly_key] = hourly_count
            return [1, "ok", daily_limit - daily_count, hourly_limit - hourly_count]


class _EvalFailureRedis(_FakeRedis):
    def __init__(self, *, fail_release: bool = False) -> None:
        super().__init__()
        self.fail_release = fail_release
        self.release_attempts = 0

    def eval(self, script: str, numkeys: int, *args):
        if numkeys == 1:
            self.release_attempts += 1
            if self.fail_release:
                raise RuntimeError("release unavailable")
            return super().eval(script, numkeys, *args)
        raise RuntimeError("quota unavailable")


class _BytesQuotaExceededRedis(_FakeRedis):
    def eval(self, script: str, numkeys: int, *args):
        del script, numkeys, args
        return [0, b"hourly", 3, 45]


@pytest.mark.concurrency
def test_quota_enforces_hourly_limit_under_parallel_consumers() -> None:
    fake = _FakeRedis()
    quota = ResumeVariantQuota(lambda: fake, daily_limit=10, hourly_limit=3)

    def consume() -> bool:
        try:
            quota.consume_generation(owner_id="owner-1")
            return True
        except ResumeVariantQuotaExceeded:
            return False

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: consume(), range(8)))

    assert sum(results) == 3


@pytest.mark.concurrency
def test_lock_release_uses_owner_token_and_does_not_delete_newer_lock() -> None:
    fake = _FakeRedis()
    quota = ResumeVariantQuota(lambda: fake)
    lease = quota.lease("owner-1")

    lease.__enter__()
    fake.values[lease.lock_key] = "newer-token"
    lease.__exit__(None, None, None)

    assert fake.values[lease.lock_key] == "newer-token"
    assert "owner-1" not in lease.lock_key


@pytest.mark.concurrency
def test_generation_lease_fails_closed_after_lock_ownership_is_lost() -> None:
    fake = _FakeRedis()
    quota = ResumeVariantQuota(lambda: fake)

    with quota.lease("owner-1") as lease:
        fake.values[lease.lock_key] = "newer-token"
        with pytest.raises(ResumeVariantConcurrencyError, match="ownership was lost"):
            lease.assert_owned()

    assert fake.values[lease.lock_key] == "newer-token"


@pytest.mark.concurrency
def test_generation_lease_renews_lock_until_context_exits() -> None:
    fake = _FakeRedis()
    quota = ResumeVariantQuota(
        lambda: fake,
        lock_ttl_seconds=1,
        renew_interval_seconds=0.01,
    )

    with quota.lease("owner-1") as lease:
        assert fake.renewed.wait(timeout=0.5)
        assert fake.values[lease.lock_key] == lease.token
        assert fake.ttls[lease.lock_key] == 1

    assert lease.lock_key not in fake.values


@pytest.mark.security
def test_quota_keys_hash_owner_identifiers() -> None:
    fake = _FakeRedis()
    quota = ResumeVariantQuota(lambda: fake)

    quota.consume_generation(owner_id="00000000-0000-4000-8000-000000000201")

    assert fake.counts
    assert all("00000000-0000-4000-8000-000000000201" not in key for key in fake.counts)


@pytest.mark.concurrency
def test_concurrent_generation_lock_blocks_second_owner_request() -> None:
    fake = _FakeRedis()
    quota = ResumeVariantQuota(lambda: fake)
    first = quota.lease("owner-1")
    first.__enter__()

    with pytest.raises(ResumeVariantConcurrencyError):
        quota.lease("owner-1").__enter__()

    first.__exit__(None, None, None)


@pytest.mark.security
def test_quota_backend_unavailable_fails_closed_before_generation() -> None:
    class _FailingRedis:
        def set(self, *args, **kwargs):
            raise RuntimeError("down")

    quota = ResumeVariantQuota(lambda: _FailingRedis())

    with pytest.raises(ResumeVariantQuotaUnavailable):
        quota.lease("owner-1").__enter__()


@pytest.mark.security
def test_quota_consume_generation_fails_closed_when_redis_eval_fails() -> None:
    quota = ResumeVariantQuota(_EvalFailureRedis)

    with pytest.raises(ResumeVariantQuotaUnavailable):
        quota.consume_generation(owner_id="owner-1")


@pytest.mark.security
def test_lease_releases_lock_when_quota_consume_fails() -> None:
    fake = _EvalFailureRedis()
    quota = ResumeVariantQuota(lambda: fake)

    with pytest.raises(ResumeVariantQuotaUnavailable):
        quota.lease("owner-1").__enter__()

    assert fake.release_attempts == 1
    assert fake.values == {}


@pytest.mark.security
def test_lease_exit_swallows_release_backend_errors() -> None:
    fake = _EvalFailureRedis(fail_release=True)
    quota = ResumeVariantQuota(lambda: fake)
    lease = quota.lease("owner-1")
    lease.token = "token"
    fake.values[lease.lock_key] = lease.token

    lease.__exit__(None, None, None)

    assert fake.release_attempts == 1
    assert fake.values[lease.lock_key] == "token"


def test_quota_exceeded_decodes_byte_bucket_and_retry_after() -> None:
    quota = ResumeVariantQuota(_BytesQuotaExceededRedis)

    with pytest.raises(ResumeVariantQuotaExceeded) as exc_info:
        quota.consume_generation(owner_id="owner-1")

    assert exc_info.value.bucket == "hourly"
    assert exc_info.value.retry_after == 45


def test_public_testing_uses_lifetime_account_quota(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_PUBLIC_TESTING_QUOTAS_ENABLED", "true")
    fake = _FakeRedis()
    quota = ResumeVariantQuota(lambda: fake)

    with patch(
        "core.resume_variants.quota.consume_ephemeral_quota",
        return_value=1,
    ) as consume:
        status = quota.consume_generation(owner_id="owner-1")

    consume.assert_called_once_with(
        "owner-1",
        "resume_variants",
        default_limit=2,
        client=fake,
    )
    assert status.daily_remaining == 1
    assert status.hourly_remaining == 1


def test_public_testing_lifetime_limit_maps_to_variant_quota_error(monkeypatch) -> None:
    from core.ephemeral_quota import EphemeralQuotaExceeded

    monkeypatch.setenv("JOBSCOUT_PUBLIC_TESTING_QUOTAS_ENABLED", "true")
    quota = ResumeVariantQuota(_FakeRedis)

    with patch(
        "core.resume_variants.quota.consume_ephemeral_quota",
        side_effect=EphemeralQuotaExceeded("limit reached"),
    ), pytest.raises(ResumeVariantQuotaExceeded) as exc_info:
        quota.consume_generation(owner_id="owner-1")

    assert exc_info.value.bucket == "account"
