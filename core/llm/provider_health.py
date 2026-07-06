from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any

from redis import Redis

from core.config_loader import LlmJudgeProviderRuntimeConfig, load_config
from core.llm.provider_chain import (
    CIRCUIT_FAILURE_CATEGORIES,
    classify_llm_provider_error,
    configured_provider_entries,
    llm_error_is_retryable,
    runtime_config_from_provider_entry,
    sanitized_provider_config,
)
from core.llm.provider_factory import build_llm_provider
from core.llm.provider_rate_limiter import (
    ProviderCircuitBreaker,
    ProviderCircuitOpen,
    ProviderRateLimitExceeded,
    ProviderRateLimiter,
)
from core.metrics import record_llm_judge_provider_canary

_CANARY_SCHEMA = {
    "name": "jobscout_llm_provider_canary",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "ok": {"type": "boolean"},
            "echo": {"type": "string"},
        },
        "required": ["ok", "echo"],
        "additionalProperties": False,
    },
}

_CANARY_SYSTEM_PROMPT = (
    "You are a health-check responder. Return only the requested JSON object."
)
_CANARY_USER_MESSAGE = (
    "Return a JSON object with ok=true and echo='jobscout-canary'. "
    "Do not include private data or any additional fields."
)
_CANARY_STATUS_KEY = "llm-provider-canary:last"


def _canary_ttl_seconds() -> int:
    try:
        return max(int(os.getenv("LLM_PROVIDER_CANARY_TTL_SECONDS", "900")), 60)
    except ValueError:
        return 900


def _redis_conn() -> Redis:
    return Redis.from_url(load_config().orchestrator.redis_url)


def _provider_key(name: str, model: str) -> str:
    return f"{name}:{model}"


def _last_canary_results() -> dict[str, dict[str, Any]]:
    try:
        raw = _redis_conn().get(_CANARY_STATUS_KEY)
        if raw is None:
            return {}
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        payload = json.loads(str(raw))
        results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(results, list):
            return {}
        return {
            _provider_key(str(result.get("name") or ""), str(result.get("model") or "")): result
            for result in results
            if isinstance(result, dict)
        }
    except Exception:
        return {}


def _store_canary_results(results: list[dict[str, Any]]) -> None:
    try:
        payload = {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "results": results,
        }
        _redis_conn().setex(
            _CANARY_STATUS_KEY,
            _canary_ttl_seconds(),
            json.dumps(payload, sort_keys=True, default=str),
        )
    except Exception:
        pass


def _entry_name(entry: LlmJudgeProviderRuntimeConfig) -> str:
    return str(entry.name or entry.provider)


def _provider_status_payload(
    entry: LlmJudgeProviderRuntimeConfig,
    circuit_breaker: ProviderCircuitBreaker,
    last_canary_by_provider: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    sanitized = sanitized_provider_config(entry)
    name = _entry_name(entry)
    model = str(entry.model or "")
    circuit = circuit_breaker.status(name, model=model)
    payload = {
        **sanitized,
        "configured": True,
        "circuit_open": bool(circuit.get("circuit_open")),
        "circuit_retry_after_seconds": circuit.get("circuit_retry_after_seconds"),
        "circuit_failure_count": int(circuit.get("circuit_failure_count") or 0),
    }
    last_canary = (last_canary_by_provider or {}).get(_provider_key(name, model))
    if isinstance(last_canary, dict):
        payload.update(
            {
                "last_canary_status": last_canary.get("status"),
                "last_canary_error_category": last_canary.get("error_category"),
                "last_canary_retryable": bool(last_canary.get("retryable", False)),
                "last_canary_retry_after_seconds": last_canary.get("retry_after_seconds"),
                "last_canary_elapsed_ms": last_canary.get("elapsed_ms"),
                "last_canary_checked_at": last_canary.get("checked_at"),
                "last_canary_error": last_canary.get("error"),
            }
        )
    return payload


def configured_llm_provider_status() -> dict[str, Any]:
    """Return passive, non-secret status for configured LLM judge providers."""
    runtime = load_config().matching.llm_judge.runtime
    entries = configured_provider_entries(runtime)
    circuit_breaker = ProviderCircuitBreaker()
    last_canary_by_provider = _last_canary_results()
    return {
        "success": True,
        "count": len(entries),
        "providers": [
            _provider_status_payload(entry, circuit_breaker, last_canary_by_provider)
            for entry in entries
        ],
    }


def reset_llm_provider_circuit(*, provider: str, model: str) -> dict[str, Any]:
    """Clear transient circuit state for one configured provider/model pair."""
    provider = str(provider or "").strip()
    model = str(model or "").strip()
    if not provider or not model:
        raise ValueError("provider and model are required")
    reset = ProviderCircuitBreaker().reset(provider, model=model)
    return {"success": True, **reset}


def _run_entry_canary(
    entry: LlmJudgeProviderRuntimeConfig,
    *,
    circuit_breaker: ProviderCircuitBreaker,
    rate_limiter: ProviderRateLimiter,
) -> dict[str, Any]:
    name = _entry_name(entry)
    model = str(entry.model or "")
    payload = _provider_status_payload(entry, circuit_breaker)
    checked_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    try:
        circuit_breaker.assert_available(name, model=model)
        if entry.requests_per_minute is not None:
            rate_limiter.wait_for_slot(
                provider_name=name,
                requests_per_minute=int(entry.requests_per_minute),
                max_wait_seconds=int(entry.rate_limit_max_wait_seconds),
            )
        provider = build_llm_provider(runtime_config_from_provider_entry(entry))
        result = provider.extract_structured_data(
            "jobscout provider canary",
            _CANARY_SCHEMA,
            system_prompt=_CANARY_SYSTEM_PROMPT,
            user_message=_CANARY_USER_MESSAGE,
        )
        if result.get("ok") is not True or result.get("echo") != "jobscout-canary":
            raise ValueError("Provider canary returned an invalid structured payload")
        circuit_breaker.record_success(name, model=model)
        record_llm_judge_provider_canary(str(entry.provider), "succeeded")
        return {
            **payload,
            "status": "succeeded",
            "error_category": None,
            "retryable": False,
            "elapsed_ms": int((time.monotonic() - started) * 1000),
            "checked_at": checked_at,
        }
    except ProviderCircuitOpen as exc:
        record_llm_judge_provider_canary(str(entry.provider), "circuit_open", "circuit_open")
        return {
            **payload,
            "status": "circuit_open",
            "error_category": "circuit_open",
            "retryable": True,
            "retry_after_seconds": exc.retry_after_seconds,
            "elapsed_ms": int((time.monotonic() - started) * 1000),
            "checked_at": checked_at,
        }
    except ProviderRateLimitExceeded as exc:
        record_llm_judge_provider_canary(str(entry.provider), "rate_limited", "rate_limit")
        return {
            **payload,
            "status": "rate_limited",
            "error_category": "rate_limit",
            "retryable": True,
            "retry_after_seconds": exc.retry_after_seconds,
            "elapsed_ms": int((time.monotonic() - started) * 1000),
            "checked_at": checked_at,
        }
    except Exception as exc:
        category = classify_llm_provider_error(exc)
        if category in CIRCUIT_FAILURE_CATEGORIES:
            circuit_breaker.record_failure(name, model=model)
        record_llm_judge_provider_canary(str(entry.provider), "failed", category)
        return {
            **payload,
            "status": "failed",
            "error_category": category,
            "retryable": llm_error_is_retryable(category),
            "elapsed_ms": int((time.monotonic() - started) * 1000),
            "error": str(exc)[:300],
            "checked_at": checked_at,
        }


def run_llm_provider_canaries() -> dict[str, Any]:
    """Run explicit structured-output canaries against configured providers."""
    runtime = load_config().matching.llm_judge.runtime
    entries = configured_provider_entries(runtime)
    circuit_breaker = ProviderCircuitBreaker()
    rate_limiter = ProviderRateLimiter()
    results = [
        _run_entry_canary(
            entry,
            circuit_breaker=circuit_breaker,
            rate_limiter=rate_limiter,
        )
        for entry in entries
    ]
    _store_canary_results(results)
    return {
        "success": True,
        "count": len(results),
        "results": results,
    }
