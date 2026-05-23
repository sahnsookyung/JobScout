from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

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
        self._lock = threading.Lock()

    def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None):
        del ex
        with self._lock:
            if nx and key in self.values:
                return False
            self.values[key] = value
            return True

    def eval(self, script: str, numkeys: int, *args):
        del script
        with self._lock:
            if numkeys == 1:
                key, token = args
                if self.values.get(key) == token:
                    del self.values[key]
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
