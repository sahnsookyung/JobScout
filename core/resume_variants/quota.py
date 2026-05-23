"""Redis quota and token-owned concurrency locks for resume variants."""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from types import TracebackType
from typing import Any, Callable

from core.redis_streams import get_redis_client

DAILY_LIMIT = 10
HOURLY_LIMIT = 3
LOCK_TTL_SECONDS = 120

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

    def __enter__(self) -> "ResumeVariantQuotaLease":
        client = self.quota.client_factory()
        try:
            acquired = client.set(self.lock_key, self.token, nx=True, ex=LOCK_TTL_SECONDS)
        except Exception as exc:
            raise ResumeVariantQuotaUnavailable("Redis lock backend unavailable.") from exc
        if not acquired:
            raise ResumeVariantConcurrencyError("Resume variant generation already in progress.")

        try:
            self.status = self.quota.consume_generation(owner_id=self.owner_id, client=client)
        except Exception:
            self._release(client)
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        try:
            self._release(self.quota.client_factory())
        except ResumeVariantQuotaUnavailable:
            return

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
    ) -> None:
        self.client_factory = client_factory
        self.daily_limit = daily_limit
        self.hourly_limit = hourly_limit

    def lease(self, owner_id: str) -> ResumeVariantQuotaLease:
        return ResumeVariantQuotaLease(self, owner_id)

    def consume_generation(self, *, owner_id: str, client: Any | None = None) -> QuotaStatus:
        now = datetime.now(timezone.utc)
        owner_scope = _owner_scope(owner_id)
        day_key = f"resume-variant:daily:{owner_scope}:{now:%Y-%m-%d}"
        hour_key = f"resume-variant:hour:{owner_scope}:{now:%Y-%m-%d-%H}"
        client = client or self.client_factory()
        try:
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
