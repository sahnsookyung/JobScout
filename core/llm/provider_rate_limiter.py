from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from redis import Redis

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_SECONDS = 60
DEFAULT_CIRCUIT_FAILURE_THRESHOLD = 3
DEFAULT_CIRCUIT_COOLDOWN_SECONDS = 120

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


class ProviderCircuitOpen(RuntimeError):
    """Raised when a provider is temporarily skipped by circuit state."""

    error_category = "circuit_open"
    retryable = True

    def __init__(
        self,
        provider_name: str,
        retry_after_seconds: float,
        *,
        model: str | None = None,
    ) -> None:
        self.provider_name = provider_name
        self.model = model
        self.retry_after_seconds = max(float(retry_after_seconds), 0.0)
        scope = provider_name if not model else f"{provider_name}/{model}"
        super().__init__(
            f"LLM provider circuit is open for {scope}; "
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


def _safe_scope_component(value: str | None) -> str:
    normalized = "".join(
        character if character.isalnum() else "-"
        for character in str(value or "").strip().lower()
    ).strip("-")
    return normalized or "unknown"


def _provider_scope(provider_name: str, model: str | None = None) -> str:
    provider_scope = _safe_scope_component(provider_name)
    if model is None:
        return provider_scope
    return f"{provider_scope}:{_safe_scope_component(model)}"


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
            try:
                from core.metrics import observe_llm_judge_provider_wait_seconds

                observe_llm_judge_provider_wait_seconds(
                    provider_name,
                    "unavailable",
                    self.window_seconds,
                )
            except Exception:
                pass
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
                try:
                    from core.metrics import observe_llm_judge_provider_wait_seconds

                    observe_llm_judge_provider_wait_seconds(
                        provider_name,
                        "retry_after",
                        decision.retry_after_seconds,
                    )
                except Exception:
                    pass
                raise ProviderRateLimitExceeded(
                    provider_name,
                    decision.retry_after_seconds,
                )

            sleep_seconds = max(decision.retry_after_seconds, 0.001)
            try:
                from core.metrics import observe_llm_judge_provider_wait_seconds

                observe_llm_judge_provider_wait_seconds(
                    provider_name,
                    "waited",
                    sleep_seconds,
                )
            except Exception:
                pass
            self.sleep_func(sleep_seconds)


class ProviderCircuitBreaker:
    """Redis-backed short cooldown circuit for transient provider failures."""

    def __init__(
        self,
        *,
        client_factory: Callable[[], Any] = _default_client_factory,
        failure_threshold: int = DEFAULT_CIRCUIT_FAILURE_THRESHOLD,
        cooldown_seconds: int = DEFAULT_CIRCUIT_COOLDOWN_SECONDS,
    ) -> None:
        self.client_factory = client_factory
        self.failure_threshold = max(int(failure_threshold), 1)
        self.cooldown_seconds = max(int(cooldown_seconds), 1)

    def _failure_key(self, provider_name: str, model: str | None = None) -> str:
        return f"llm-provider-circuit-failures:{_provider_scope(provider_name, model)}"

    def _open_key(self, provider_name: str, model: str | None = None) -> str:
        return f"llm-provider-circuit-open:{_provider_scope(provider_name, model)}"

    def assert_available(self, provider_name: str, model: str | None = None) -> None:
        try:
            client = self.client_factory()
            open_key = self._open_key(provider_name, model)
            if not client.exists(open_key):
                return
            ttl = client.ttl(open_key)
            retry_after_seconds = self.cooldown_seconds if ttl is None or int(ttl) < 0 else int(ttl)
            try:
                from core.metrics import record_llm_judge_provider_circuit_event

                record_llm_judge_provider_circuit_event(provider_name, "skip")
            except Exception:
                pass
            raise ProviderCircuitOpen(provider_name, retry_after_seconds, model=model)
        except ProviderCircuitOpen:
            raise
        except Exception:
            logger.warning("LLM provider circuit unavailable for %s", provider_name)

    def record_success(self, provider_name: str, model: str | None = None) -> None:
        try:
            deleted = self.client_factory().delete(
                self._failure_key(provider_name, model),
                self._open_key(provider_name, model),
            )
            if int(deleted or 0) > 0:
                try:
                    from core.metrics import record_llm_judge_provider_circuit_event

                    record_llm_judge_provider_circuit_event(provider_name, "closed")
                except Exception:
                    pass
        except Exception:
            logger.warning("Could not clear LLM provider circuit for %s", provider_name)

    def record_failure(self, provider_name: str, model: str | None = None) -> None:
        try:
            client = self.client_factory()
            failure_key = self._failure_key(provider_name, model)
            count = int(client.incr(failure_key))
            client.expire(failure_key, self.cooldown_seconds)
            if count >= self.failure_threshold:
                client.setex(
                    self._open_key(provider_name, model),
                    self.cooldown_seconds,
                    "1",
                )
                try:
                    from core.metrics import record_llm_judge_provider_circuit_event

                    record_llm_judge_provider_circuit_event(provider_name, "opened")
                except Exception:
                    pass
        except Exception:
            logger.warning("Could not record LLM provider circuit failure for %s", provider_name)

    def reset(self, provider_name: str, model: str | None = None) -> dict[str, int | str | None | bool]:
        deleted = 0
        try:
            deleted = int(
                self.client_factory().delete(
                    self._failure_key(provider_name, model),
                    self._open_key(provider_name, model),
                )
                or 0
            )
            if deleted:
                try:
                    from core.metrics import record_llm_judge_provider_circuit_event

                    record_llm_judge_provider_circuit_event(provider_name, "manual_reset")
                except Exception:
                    pass
        except Exception:
            logger.warning("Could not reset LLM provider circuit for %s", provider_name)
        return {
            "provider": provider_name,
            "model": model,
            "circuit_open": False,
            "circuit_retry_after_seconds": None,
            "circuit_failure_count": 0,
            "deleted_keys": deleted,
        }

    def status(self, provider_name: str, model: str | None = None) -> dict[str, int | str | None | bool]:
        try:
            client = self.client_factory()
            open_key = self._open_key(provider_name, model)
            failure_key = self._failure_key(provider_name, model)
            circuit_open = bool(client.exists(open_key))
            ttl = client.ttl(open_key) if circuit_open else None
            retry_after_seconds = (
                self.cooldown_seconds
                if ttl is not None and int(ttl) < 0
                else int(ttl)
                if ttl is not None
                else None
            )
            raw_count = client.get(failure_key)
            failure_count = int(raw_count or 0)
        except Exception:
            logger.warning("Could not inspect LLM provider circuit for %s", provider_name)
            circuit_open = False
            retry_after_seconds = None
            failure_count = 0
        return {
            "provider": provider_name,
            "model": model,
            "circuit_open": circuit_open,
            "circuit_retry_after_seconds": retry_after_seconds,
            "circuit_failure_count": failure_count,
        }
