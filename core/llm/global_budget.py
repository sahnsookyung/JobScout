"""Fail-closed Redis budget for all remote LLM and embedding calls."""

from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional

from core.llm.interfaces import LLMProvider
from core.metrics import record_public_security_event, set_global_llm_budget_usage
from core.redis_streams import get_redis_client

_RESERVE_SCRIPT = """
local requests = tonumber(redis.call('GET', KEYS[1]) or '0')
local tokens = tonumber(redis.call('GET', KEYS[2]) or '0')
local request_limit = tonumber(ARGV[1])
local token_limit = tonumber(ARGV[2])
local reserve_tokens = tonumber(ARGV[3])
if requests + 1 > request_limit then
  return {0, 'requests', requests, tokens}
end
if tokens + reserve_tokens > token_limit then
  return {0, 'tokens', requests, tokens}
end
requests = redis.call('INCR', KEYS[1])
tokens = redis.call('INCRBY', KEYS[2], reserve_tokens)
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[4]))
return {1, 'ok', requests, tokens}
"""

_CHECK_CAPACITY_SCRIPT = """
local requests = tonumber(redis.call('GET', KEYS[1]) or '0')
local tokens = tonumber(redis.call('GET', KEYS[2]) or '0')
local request_limit = tonumber(ARGV[1])
local token_limit = tonumber(ARGV[2])
local required_requests = tonumber(ARGV[3])
local required_tokens = tonumber(ARGV[4])
if requests + required_requests > request_limit then
  return {0, 'requests', requests, tokens}
end
if tokens + required_tokens > token_limit then
  return {0, 'tokens', requests, tokens}
end
return {1, 'ok', requests, tokens}
"""

_RESERVE_ADDITIONAL_REQUEST_SCRIPT = """
local requests = tonumber(redis.call('GET', KEYS[1]) or '0')
local request_limit = tonumber(ARGV[1])
if requests + 1 > request_limit then
  return {0, requests}
end
requests = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return {1, requests}
"""

_RECONCILE_SCRIPT = """
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local reserved = tonumber(ARGV[1])
local actual = tonumber(ARGV[2])
local adjusted = math.max(current - reserved + actual, 0)
redis.call('SET', KEYS[1], adjusted, 'KEEPTTL')
return adjusted
"""


class GlobalLlmBudgetExceeded(RuntimeError):
    """Raised before a provider call would exceed a configured daily ceiling."""


class GlobalLlmBudgetUnavailable(RuntimeError):
    """Raised when the budget backend cannot make a safe decision."""


@dataclass(frozen=True)
class GlobalLlmBudgetReservation:
    client: Any
    tokens_key: str
    reserved_tokens: int
    token_limit: int
    reset_at: int


_PREPAID_REQUEST_UNITS: ContextVar[int] = ContextVar(
    "jobscout_global_llm_prepaid_request_units",
    default=0,
)
_BUDGET_LANE: ContextVar[str] = ContextVar(
    "jobscout_global_llm_budget_lane",
    default="interactive",
)


def global_llm_budget_enabled() -> bool:
    return os.getenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _positive_env(name: str) -> int:
    try:
        value = int(os.environ[name])
    except (KeyError, ValueError) as exc:
        raise GlobalLlmBudgetUnavailable(f"Missing or invalid global LLM budget: {name}.") from exc
    if value <= 0:
        raise GlobalLlmBudgetUnavailable(f"Global LLM budget must be positive: {name}.")
    return value


def _nonnegative_env(name: str, default: int = 0) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError) as exc:
        raise GlobalLlmBudgetUnavailable(f"Invalid global LLM budget: {name}.") from exc
    if value < 0:
        raise GlobalLlmBudgetUnavailable(f"Global LLM budget must be non-negative: {name}.")
    return value


def _request_limit_for_current_lane(request_limit: int) -> int:
    if _BUDGET_LANE.get() != "background":
        return request_limit
    interactive_reserve = _nonnegative_env(
        "JOBSCOUT_CLOUD_GLOBAL_LLM_INTERACTIVE_REQUESTS_RESERVE"
    )
    return max(request_limit - interactive_reserve, 0)


@contextmanager
def global_llm_budget_lane(lane: str) -> Iterator[None]:
    """Apply the request ceiling for interactive or background provider work."""
    if lane not in {"interactive", "background"}:
        raise ValueError(f"Unsupported global LLM budget lane: {lane}.")
    token = _BUDGET_LANE.set(lane)
    try:
        yield
    finally:
        _BUDGET_LANE.reset(token)


def _seconds_until_next_utc_day() -> int:
    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).date()
    reset = datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc)
    return max(int((reset - now).total_seconds()) + 300, 300)


def _next_utc_day_timestamp() -> int:
    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).date()
    reset = datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc)
    return int(reset.timestamp())


def reserve_global_llm_budget(
    estimated_tokens: int,
    *,
    client: Any | None = None,
) -> GlobalLlmBudgetReservation | None:
    if not global_llm_budget_enabled():
        return None
    request_limit = _positive_env("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY")
    effective_request_limit = _request_limit_for_current_lane(request_limit)
    token_limit = _positive_env("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY")
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    requests_key = f"jobscout-cloud:llm-budget:{day}:requests"
    tokens_key = f"jobscout-cloud:llm-budget:{day}:tokens"
    resolved_client = client or get_redis_client()
    reserved_tokens = max(int(estimated_tokens), 1)
    reset_at = _next_utc_day_timestamp()
    try:
        raw = resolved_client.eval(
            _RESERVE_SCRIPT,
            2,
            requests_key,
            tokens_key,
            effective_request_limit,
            token_limit,
            reserved_tokens,
            _seconds_until_next_utc_day(),
        )
    except Exception as exc:
        raise GlobalLlmBudgetUnavailable("Global LLM budget backend is unavailable.") from exc
    current_requests = int(raw[2])
    current_tokens = int(raw[3])
    set_global_llm_budget_usage(
        "requests",
        current_requests,
        request_limit,
        reset_at=reset_at,
    )
    set_global_llm_budget_usage(
        "tokens",
        current_tokens,
        token_limit,
        reset_at=reset_at,
    )
    if int(raw[0]) != 1:
        bucket = raw[1].decode("utf-8") if isinstance(raw[1], bytes) else str(raw[1])
        record_public_security_event("global_budget_exhausted")
        scope = "Background daily" if _BUDGET_LANE.get() == "background" else "Global daily"
        raise GlobalLlmBudgetExceeded(f"{scope} LLM {bucket} budget exhausted.")
    return GlobalLlmBudgetReservation(
        client=resolved_client,
        tokens_key=tokens_key,
        reserved_tokens=reserved_tokens,
        token_limit=token_limit,
        reset_at=reset_at,
    )


def consume_global_llm_request(*, client: Any | None = None) -> None:
    """Count one actual provider attempt, including retries and batch chunks."""
    if not global_llm_budget_enabled():
        return

    prepaid = _PREPAID_REQUEST_UNITS.get()
    if prepaid > 0:
        _PREPAID_REQUEST_UNITS.set(prepaid - 1)
        return

    request_limit = _positive_env("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY")
    effective_request_limit = _request_limit_for_current_lane(request_limit)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    requests_key = f"jobscout-cloud:llm-budget:{day}:requests"
    resolved_client = client or get_redis_client()
    reset_at = _next_utc_day_timestamp()
    try:
        raw = resolved_client.eval(
            _RESERVE_ADDITIONAL_REQUEST_SCRIPT,
            1,
            requests_key,
            effective_request_limit,
            _seconds_until_next_utc_day(),
        )
    except Exception as exc:
        raise GlobalLlmBudgetUnavailable("Global LLM budget backend is unavailable.") from exc
    current_requests = int(raw[1])
    set_global_llm_budget_usage(
        "requests",
        current_requests,
        request_limit,
        reset_at=reset_at,
    )
    if int(raw[0]) != 1:
        record_public_security_event("global_budget_exhausted")
        scope = "Background daily" if _BUDGET_LANE.get() == "background" else "Global daily"
        raise GlobalLlmBudgetExceeded(f"{scope} LLM requests budget exhausted.")


def ensure_global_llm_budget_available(
    *,
    estimated_requests: int = 1,
    estimated_tokens: int = 1,
    client: Any | None = None,
) -> None:
    """Fail closed when an interactive operation cannot fit in today's budget."""
    if not global_llm_budget_enabled():
        return

    request_limit = _positive_env("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY")
    token_limit = _positive_env("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY")
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    requests_key = f"jobscout-cloud:llm-budget:{day}:requests"
    tokens_key = f"jobscout-cloud:llm-budget:{day}:tokens"
    resolved_client = client or get_redis_client()
    required_requests = max(int(estimated_requests), 1)
    required_tokens = max(int(estimated_tokens), 1)
    reset_at = _next_utc_day_timestamp()
    try:
        raw = resolved_client.eval(
            _CHECK_CAPACITY_SCRIPT,
            2,
            requests_key,
            tokens_key,
            request_limit,
            token_limit,
            required_requests,
            required_tokens,
        )
    except Exception as exc:
        raise GlobalLlmBudgetUnavailable("Global LLM budget backend is unavailable.") from exc

    current_requests = int(raw[2])
    current_tokens = int(raw[3])
    set_global_llm_budget_usage(
        "requests",
        current_requests,
        request_limit,
        reset_at=reset_at,
    )
    set_global_llm_budget_usage(
        "tokens",
        current_tokens,
        token_limit,
        reset_at=reset_at,
    )
    if int(raw[0]) != 1:
        bucket = raw[1].decode("utf-8") if isinstance(raw[1], bytes) else str(raw[1])
        record_public_security_event("global_budget_exhausted")
        raise GlobalLlmBudgetExceeded(f"Global daily LLM {bucket} budget exhausted.")


def _provider_actual_tokens(provider: LLMProvider) -> int | None:
    usage = getattr(provider, "last_usage", None)
    if usage is None:
        return None
    if isinstance(usage, dict):
        value = usage.get("total_tokens")
    else:
        value = getattr(usage, "total_tokens", None)
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def reconcile_global_llm_budget(
    reservation: GlobalLlmBudgetReservation | None,
    provider: LLMProvider,
) -> None:
    """Replace a conservative token reservation with provider-reported usage."""
    if reservation is None:
        return
    actual_tokens = _provider_actual_tokens(provider)
    if actual_tokens is None:
        return
    try:
        adjusted_tokens = reservation.client.eval(
            _RECONCILE_SCRIPT,
            1,
            reservation.tokens_key,
            reservation.reserved_tokens,
            actual_tokens,
        )
        set_global_llm_budget_usage(
            "tokens",
            int(adjusted_tokens),
            reservation.token_limit,
            reset_at=reservation.reset_at,
        )
    except Exception as exc:
        raise GlobalLlmBudgetUnavailable(
            "Global LLM budget reconciliation backend is unavailable."
        ) from exc


def _estimate_tokens(*values: Any, output_reserve: int = 0) -> int:
    character_count = sum(len(str(value)) for value in values if value is not None)
    return max(character_count // 4, 1) + max(output_reserve, 0)


class BudgetedLLMProvider(LLMProvider):
    """Interface-preserving provider decorator with conservative pre-call reservations."""

    def __init__(self, provider: LLMProvider) -> None:
        self.provider = provider

    def __getattr__(self, name: str) -> Any:
        return getattr(self.provider, name)

    def _run_with_budget(self, estimated_tokens: int, operation: Callable[[], Any]) -> Any:
        reservation = reserve_global_llm_budget(estimated_tokens)
        context_token = _PREPAID_REQUEST_UNITS.set(1 if reservation is not None else 0)
        try:
            result = operation()
        finally:
            _PREPAID_REQUEST_UNITS.reset(context_token)
        reconcile_global_llm_budget(reservation, self.provider)
        return result

    def extract_structured_data(
        self,
        text: str,
        schema_spec: Dict,
        system_prompt: Optional[str] = None,
        user_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        estimated_tokens = _estimate_tokens(
            text,
            schema_spec,
            system_prompt,
            user_message,
            output_reserve=4096,
        )
        return self._run_with_budget(
            estimated_tokens,
            lambda: self.provider.extract_structured_data(
                text,
                schema_spec,
                system_prompt=system_prompt,
                user_message=user_message,
            ),
        )

    def extract_resume_data(self, text: str) -> Dict[str, Any]:
        return self._run_with_budget(
            _estimate_tokens(text, output_reserve=4096),
            lambda: self.provider.extract_resume_data(text),
        )

    def extract_requirements_data(self, text: str) -> Dict[str, Any]:
        return self._run_with_budget(
            _estimate_tokens(text, output_reserve=4096),
            lambda: self.provider.extract_requirements_data(text),
        )

    def generate_embedding(self, text: str) -> List[float]:
        return self._run_with_budget(
            _estimate_tokens(text),
            lambda: self.provider.generate_embedding(text),
        )

    def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        return self._run_with_budget(
            _estimate_tokens(*texts),
            lambda: self.provider.generate_embeddings_batch(texts),
        )
