"""Fail-closed Redis budget for all remote LLM and embedding calls."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from core.llm.interfaces import LLMProvider
from core.metrics import record_public_security_event
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


def _seconds_until_next_utc_day() -> int:
    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).date()
    reset = datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc)
    return max(int((reset - now).total_seconds()) + 300, 300)


def reserve_global_llm_budget(
    estimated_tokens: int,
    *,
    client: Any | None = None,
) -> GlobalLlmBudgetReservation | None:
    if not global_llm_budget_enabled():
        return None
    request_limit = _positive_env("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY")
    token_limit = _positive_env("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY")
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    requests_key = f"jobscout-cloud:llm-budget:{day}:requests"
    tokens_key = f"jobscout-cloud:llm-budget:{day}:tokens"
    resolved_client = client or get_redis_client()
    reserved_tokens = max(int(estimated_tokens), 1)
    try:
        raw = resolved_client.eval(
            _RESERVE_SCRIPT,
            2,
            requests_key,
            tokens_key,
            request_limit,
            token_limit,
            reserved_tokens,
            _seconds_until_next_utc_day(),
        )
    except Exception as exc:
        raise GlobalLlmBudgetUnavailable("Global LLM budget backend is unavailable.") from exc
    if int(raw[0]) != 1:
        bucket = raw[1].decode("utf-8") if isinstance(raw[1], bytes) else str(raw[1])
        record_public_security_event("global_budget_exhausted")
        raise GlobalLlmBudgetExceeded(f"Global daily LLM {bucket} budget exhausted.")
    return GlobalLlmBudgetReservation(
        client=resolved_client,
        tokens_key=tokens_key,
        reserved_tokens=reserved_tokens,
    )


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
        reservation.client.eval(
            _RECONCILE_SCRIPT,
            1,
            reservation.tokens_key,
            reservation.reserved_tokens,
            actual_tokens,
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

    def extract_structured_data(
        self,
        text: str,
        schema_spec: Dict,
        system_prompt: Optional[str] = None,
        user_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        reservation = reserve_global_llm_budget(
            _estimate_tokens(
                text,
                schema_spec,
                system_prompt,
                user_message,
                output_reserve=4096,
            )
        )
        result = self.provider.extract_structured_data(
            text,
            schema_spec,
            system_prompt=system_prompt,
            user_message=user_message,
        )
        reconcile_global_llm_budget(reservation, self.provider)
        return result

    def extract_resume_data(self, text: str) -> Dict[str, Any]:
        reservation = reserve_global_llm_budget(_estimate_tokens(text, output_reserve=4096))
        result = self.provider.extract_resume_data(text)
        reconcile_global_llm_budget(reservation, self.provider)
        return result

    def extract_requirements_data(self, text: str) -> Dict[str, Any]:
        reservation = reserve_global_llm_budget(_estimate_tokens(text, output_reserve=4096))
        result = self.provider.extract_requirements_data(text)
        reconcile_global_llm_budget(reservation, self.provider)
        return result

    def generate_embedding(self, text: str) -> List[float]:
        reservation = reserve_global_llm_budget(_estimate_tokens(text))
        result = self.provider.generate_embedding(text)
        reconcile_global_llm_budget(reservation, self.provider)
        return result

    def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        reservation = reserve_global_llm_budget(_estimate_tokens(*texts))
        result = self.provider.generate_embeddings_batch(texts)
        reconcile_global_llm_budget(reservation, self.provider)
        return result
