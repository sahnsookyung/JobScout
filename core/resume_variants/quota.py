"""Redis quota and token-owned concurrency locks for resume variants."""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from types import TracebackType
from typing import Any, Callable

from core.ephemeral_quota import (
    EphemeralQuotaExceeded,
    EphemeralQuotaUnavailable,
    consume_ephemeral_quota,
    public_testing_quotas_enabled,
)
from core.redis_streams import get_redis_client

logger = logging.getLogger(__name__)

DAILY_LIMIT = int(os.getenv("RESUME_VARIANT_DAILY_LIMIT", "10"))
HOURLY_LIMIT = int(os.getenv("RESUME_VARIANT_HOURLY_LIMIT", "3"))
LOCK_TTL_SECONDS = 120
USER_QUOTA_INDEX_PREFIX = "jobscout-cloud:user-quota-keys"
QUOTA_EXEMPT_PREFIX = "jobscout-cloud:quota-exempt"

_QUOTA_SCRIPT = """
local daily_count = tonumber(redis.call('GET', KEYS[1]) or '0')
local hourly_count = tonumber(redis.call('GET', KEYS[2]) or '0')
local daily_limit = tonumber(ARGV[1])
local hourly_limit = tonumber(ARGV[2])
if daily_count >= daily_limit then
  local ttl = redis.call('TTL', KEYS[1])
  return {0, 'daily', daily_count, ttl}
end
if hourly_count >= hourly_limit then
  local ttl = redis.call('TTL', KEYS[2])
  return {0, 'hourly', hourly_count, ttl}
end
daily_count = redis.call('INCR', KEYS[1])
if daily_count == 1 then redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3])) end
hourly_count = redis.call('INCR', KEYS[2])
if hourly_count == 1 then redis.call('EXPIRE', KEYS[2], tonumber(ARGV[4])) end
return {1, 'ok', daily_limit - daily_count, hourly_limit - hourly_count}
"""

_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

_RENEW_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return 0
"""

_CHECK_SCRIPT = """
-- resume-variant-check
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return 1
end
return 0
"""


class ResumeVariantQuotaUnavailable(RuntimeError):
    """Raised when Redis quota enforcement cannot make a safe decision."""


class ResumeVariantQuotaExceeded(RuntimeError):
    """Raised when a user exceeds hourly or daily generation budget."""

    def __init__(self, bucket: str, retry_after: int | None = None) -> None:
        super().__init__(f"Resume variant {bucket} quota exceeded.")
        self.bucket = bucket
        self.retry_after = retry_after


class ResumeVariantConcurrencyError(RuntimeError):
    """Raised when another generation for the owner is already in progress."""


@dataclass(frozen=True)
class QuotaStatus:
    daily_remaining: int
    hourly_remaining: int


class ResumeVariantQuotaLease:
    def __init__(self, quota: "ResumeVariantQuota", owner_id: str) -> None:
        self.quota = quota
        self.owner_id = owner_id
        self.owner_scope = _owner_scope(owner_id)
        self.lock_key = f"resume-variant:lock:{self.owner_scope}"
        self.token = secrets.token_urlsafe(32)
        self.status: QuotaStatus | None = None
        self._exempt = False
        self._renew_stop = threading.Event()
        self._renew_thread: threading.Thread | None = None

    def __enter__(self) -> "ResumeVariantQuotaLease":
        client = self.quota.client_factory()
        try:
            if client.get(f"{QUOTA_EXEMPT_PREFIX}:{self.owner_id}"):
                self._exempt = True
                self.status = QuotaStatus(
                    daily_remaining=self.quota.daily_limit,
                    hourly_remaining=self.quota.hourly_limit,
                )
                return self
            acquired = client.set(
                self.lock_key,
                self.token,
                nx=True,
                ex=self.quota.lock_ttl_seconds,
            )
        except Exception as exc:
            raise ResumeVariantQuotaUnavailable("Redis lock backend unavailable.") from exc
        if not acquired:
            raise ResumeVariantConcurrencyError("Resume variant generation already in progress.")
        owner_index_key = f"{USER_QUOTA_INDEX_PREFIX}:{self.owner_id}"
        try:
            client.sadd(owner_index_key, self.lock_key)
            client.expire(owner_index_key, 2 * 24 * 60 * 60)
        except Exception as exc:
            try:
                self._release(client)
            except ResumeVariantQuotaUnavailable:
                logger.warning("Could not release an unindexed resume variant lock")
            raise ResumeVariantQuotaUnavailable(
                "Redis quota index backend unavailable."
            ) from exc

        try:
            self.status = self.quota.consume_generation(owner_id=self.owner_id, client=client)
            self._start_renewal()
        except Exception:
            try:
                self._release(client)
            except ResumeVariantQuotaUnavailable:
                logger.warning("Could not release resume variant lock after quota failure")
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        if self._exempt:
            return
        self._stop_renewal()
        try:
            self._release(self.quota.client_factory())
        except ResumeVariantQuotaUnavailable:
            return

    def _start_renewal(self) -> None:
        self._renew_stop.clear()
        thread = threading.Thread(
            target=self._renew_loop,
            name="resume-variant-lease-renewal",
            daemon=True,
        )
        self._renew_thread = thread
        try:
            thread.start()
        except Exception as exc:
            self._renew_thread = None
            raise ResumeVariantQuotaUnavailable("Redis lock renewal could not be started.") from exc

    def _stop_renewal(self) -> None:
        self._renew_stop.set()
        thread = self._renew_thread
        if thread is None or thread is threading.current_thread():
            return
        thread.join(timeout=5)
        if thread.is_alive():
            logger.warning("Resume variant lock renewal did not stop promptly")
        self._renew_thread = None

    def _renew_loop(self) -> None:
        while not self._renew_stop.wait(self.quota.renew_interval_seconds):
            try:
                renewed = self._renew(self.quota.client_factory())
            except ResumeVariantQuotaUnavailable:
                logger.warning("Resume variant lock renewal failed; retrying")
                continue
            if not renewed:
                logger.warning("Resume variant lock ownership was lost; stopping renewal")
                return

    def _renew(self, client: Any) -> bool:
        try:
            return bool(
                client.eval(
                    _RENEW_SCRIPT,
                    1,
                    self.lock_key,
                    self.token,
                    self.quota.lock_ttl_seconds,
                )
            )
        except Exception as exc:
            raise ResumeVariantQuotaUnavailable("Redis lock renewal failed.") from exc

    def assert_owned(self) -> None:
        """Fail closed when the generation lease no longer belongs to this request."""
        try:
            owned = self.quota.client_factory().eval(
                _CHECK_SCRIPT,
                1,
                self.lock_key,
                self.token,
            )
        except Exception as exc:
            raise ResumeVariantQuotaUnavailable("Redis lock ownership check failed.") from exc
        if not owned:
            raise ResumeVariantConcurrencyError(
                "Resume variant generation lock ownership was lost."
            )

    def _release(self, client: Any) -> None:
        try:
            client.eval(_RELEASE_SCRIPT, 1, self.lock_key, self.token)
        except Exception as exc:
            raise ResumeVariantQuotaUnavailable("Redis lock release failed.") from exc


class ResumeVariantQuota:
    """Small Redis-backed guard for paid/expensive generation work."""

    def __init__(
        self,
        client_factory: Callable[[], Any] = get_redis_client,
        *,
        daily_limit: int = DAILY_LIMIT,
        hourly_limit: int = HOURLY_LIMIT,
        lock_ttl_seconds: int = LOCK_TTL_SECONDS,
        renew_interval_seconds: float | None = None,
    ) -> None:
        if lock_ttl_seconds <= 0:
            raise ValueError("lock_ttl_seconds must be positive")
        self.client_factory = client_factory
        self.daily_limit = daily_limit
        self.hourly_limit = hourly_limit
        self.lock_ttl_seconds = lock_ttl_seconds
        self.renew_interval_seconds = (
            renew_interval_seconds
            if renew_interval_seconds is not None
            else max(1.0, lock_ttl_seconds / 3)
        )
        if self.renew_interval_seconds <= 0:
            raise ValueError("renew_interval_seconds must be positive")

    def lease(self, owner_id: str) -> ResumeVariantQuotaLease:
        return ResumeVariantQuotaLease(self, owner_id)

    def consume_generation(self, *, owner_id: str, client: Any | None = None) -> QuotaStatus:
        client = client or self.client_factory()
        if public_testing_quotas_enabled():
            try:
                remaining = consume_ephemeral_quota(
                    owner_id,
                    "resume_variants",
                    default_limit=2,
                    client=client,
                )
            except EphemeralQuotaExceeded as exc:
                raise ResumeVariantQuotaExceeded(bucket="account") from exc
            except EphemeralQuotaUnavailable as exc:
                raise ResumeVariantQuotaUnavailable(str(exc)) from exc
            lifetime_remaining = int(remaining or 0)
            return QuotaStatus(
                daily_remaining=lifetime_remaining,
                hourly_remaining=lifetime_remaining,
            )

        now = datetime.now(timezone.utc)
        owner_scope = _owner_scope(owner_id)
        day_key = f"resume-variant:daily:{owner_scope}:{now:%Y-%m-%d}"
        hour_key = f"resume-variant:hour:{owner_scope}:{now:%Y-%m-%d-%H}"
        owner_index_key = f"{USER_QUOTA_INDEX_PREFIX}:{owner_id}"
        try:
            if client.get(f"{QUOTA_EXEMPT_PREFIX}:{owner_id}"):
                return QuotaStatus(
                    daily_remaining=self.daily_limit,
                    hourly_remaining=self.hourly_limit,
                )
            raw = client.eval(
                _QUOTA_SCRIPT,
                2,
                day_key,
                hour_key,
                self.daily_limit,
                self.hourly_limit,
                2 * 24 * 60 * 60,
                2 * 60 * 60,
            )
            client.sadd(owner_index_key, day_key, hour_key)
            client.expire(owner_index_key, 2 * 24 * 60 * 60)
        except Exception as exc:
            raise ResumeVariantQuotaUnavailable("Redis quota backend unavailable.") from exc

        allowed = int(raw[0]) == 1
        bucket = _decode(raw[1])
        if not allowed:
            retry_after = int(raw[3]) if len(raw) > 3 and int(raw[3]) > 0 else None
            raise ResumeVariantQuotaExceeded(bucket=bucket, retry_after=retry_after)
        return QuotaStatus(daily_remaining=int(raw[2]), hourly_remaining=int(raw[3]))


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _owner_scope(owner_id: str) -> str:
    return hashlib.sha256(owner_id.encode("utf-8")).hexdigest()[:32]
