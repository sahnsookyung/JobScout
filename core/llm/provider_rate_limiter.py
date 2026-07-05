from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from redis import Redis

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_SECONDS = 60

_SLIDING_WINDOW_SCRIPT = """
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, 0, now_ms - window_ms)
local count = tonumber(redis.call('ZCARD', key) or '0')
if count < limit then
  redis.call('ZADD', key, now_ms, member)
  redis.call('PEXPIRE', key, window_ms * 2)
  return {1, 0, limit - count - 1}
end

local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local retry_after_ms = window_ms
if oldest[2] then
  retry_after_ms = math.max(1, tonumber(oldest[2]) + window_ms - now_ms)
end
redis.call('PEXPIRE', key, window_ms * 2)
return {0, retry_after_ms, 0}
"""


class ProviderRateLimitExceeded(RuntimeError):
    """Raised when an LLM provider quota slot is unavailable."""

    error_category = "rate_limit"
    retryable = True

    def __init__(self, provider_name: str, retry_after_seconds: float) -> None:
        self.provider_name = provider_name
        self.retry_after_seconds = max(float(retry_after_seconds), 0.0)
        super().__init__(
            f"LLM provider rate limit reached for {provider_name}; "
            f"retry after {self.retry_after_seconds:.2f}s."
        )


@dataclass(frozen=True)
class ProviderRateLimitDecision:
    allowed: bool
    retry_after_seconds: float = 0.0
    remaining: int = 0


def _default_client_factory() -> Any:
    from core.config_loader import load_config

    return Redis.from_url(load_config().orchestrator.redis_url)


def _provider_scope(provider_name: str) -> str:
    normalized = "".join(
        character if character.isalnum() else "-"
        for character in str(provider_name).strip().lower()
    ).strip("-")
    return normalized or "unknown"


class ProviderRateLimiter:
    """Redis-backed sliding-window limiter for external LLM providers."""

    def __init__(
        self,
        *,
        client_factory: Callable[[], Any] = _default_client_factory,
        time_func: Callable[[], float] = time.time,
        sleep_func: Callable[[float], None] = time.sleep,
        window_seconds: int = DEFAULT_WINDOW_SECONDS,
    ) -> None:
        self.client_factory = client_factory
        self.time_func = time_func
        self.sleep_func = sleep_func
        self.window_seconds = int(window_seconds)

    def acquire(
        self,
        *,
        provider_name: str,
        requests_per_minute: int,
    ) -> ProviderRateLimitDecision:
        limit = int(requests_per_minute)
        if limit <= 0:
            raise ValueError("requests_per_minute must be positive")
        now_ms = int(self.time_func() * 1000)
        window_ms = max(int(self.window_seconds), 1) * 1000
        key = f"llm-provider-rate:{_provider_scope(provider_name)}"
        member = f"{now_ms}:{uuid.uuid4().hex}"
        try:
            raw = self.client_factory().eval(
                _SLIDING_WINDOW_SCRIPT,
                1,
                key,
                now_ms,
                window_ms,
                limit,
                member,
            )
        except Exception as exc:
            logger.warning("LLM provider rate limiter unavailable for %s", provider_name)
            raise ProviderRateLimitExceeded(provider_name, self.window_seconds) from exc

        allowed = int(raw[0]) == 1
        retry_after_seconds = max(float(raw[1]) / 1000.0, 0.0)
        remaining = max(int(raw[2]), 0)
        return ProviderRateLimitDecision(
            allowed=allowed,
            retry_after_seconds=retry_after_seconds,
            remaining=remaining,
        )

    def wait_for_slot(
        self,
        *,
        provider_name: str,
        requests_per_minute: int,
        max_wait_seconds: int,
    ) -> ProviderRateLimitDecision:
        deadline = self.time_func() + max(int(max_wait_seconds), 0)
        while True:
            decision = self.acquire(
                provider_name=provider_name,
                requests_per_minute=requests_per_minute,
            )
            if decision.allowed:
                return decision

            now = self.time_func()
            remaining_wait = deadline - now
            if remaining_wait <= 0 or decision.retry_after_seconds > remaining_wait:
                raise ProviderRateLimitExceeded(
                    provider_name,
                    decision.retry_after_seconds,
                )

            self.sleep_func(max(decision.retry_after_seconds, 0.001))
