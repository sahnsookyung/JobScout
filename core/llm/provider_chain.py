from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List

from core.config_loader import LlmJudgeProviderRuntimeConfig, LlmJudgeRuntimeConfig
from core.llm.interfaces import LLMProvider
from core.llm.provider_factory import RuntimeLLMConfig, build_llm_provider
from core.llm.provider_rate_limiter import ProviderRateLimitExceeded, ProviderRateLimiter

TRANSIENT_ERROR_CATEGORIES = frozenset(
    {"rate_limit", "timeout", "connection_error", "server_error"}
)
RETRYABLE_ERROR_CATEGORIES = TRANSIENT_ERROR_CATEGORIES | {"unknown"}


@dataclass(frozen=True)
class LLMProviderCandidate:
    name: str
    provider_name: str
    model: str
    provider: LLMProvider
    requests_per_minute: int | None = None
    rate_limit_max_wait_seconds: int = 0
    fallback_on_rate_limit: bool = False


class LLMProviderChainError(RuntimeError):
    """Raised when every eligible provider in an ordered chain fails."""

    def __init__(
        self,
        message: str,
        *,
        error_category: str,
        attempts: list[dict[str, Any]],
        retryable: bool,
    ) -> None:
        super().__init__(message)
        self.error_category = error_category
        self.attempts = attempts
        self.retryable = retryable


def _status_code(exc: BaseException) -> int | None:
    raw_status = getattr(exc, "status_code", None)
    if raw_status is None:
        response = getattr(exc, "response", None)
        raw_status = getattr(response, "status_code", None)
    try:
        return int(raw_status)
    except (TypeError, ValueError):
        return None


def classify_llm_provider_error(exc: BaseException) -> str:
    """Map provider exceptions to bounded categories used for fallback decisions."""
    status_code = _status_code(exc)
    message = str(exc).lower()
    class_name = exc.__class__.__name__.lower()
    if status_code == 413 or "request too large" in message or "context length" in message:
        return "input_too_large"
    if (
        status_code == 429
        or "rate limit" in message
        or "token_quota_exceeded" in message
        or "tokens per minute" in message
    ):
        return "rate_limit"
    if status_code in {408, 504} or "timeout" in class_name or "timed out" in message:
        return "timeout"
    if "connection" in class_name or "connection" in message or "network" in message:
        return "connection_error"
    if status_code is not None and 500 <= status_code <= 599:
        return "server_error"
    if status_code in {401, 403} or "invalid api key" in message or "unauthorized" in message:
        return "invalid_auth"
    if status_code == 404 or "unsupported model" in message or "model not found" in message:
        return "unsupported_model"
    if status_code in {400, 422}:
        return "invalid_request"
    if (
        isinstance(exc, ValueError)
        and ("schema" in message or "json" in message or "validation" in message)
    ):
        return "schema_error"
    return "unknown"


def llm_error_is_transient(category: str) -> bool:
    return category in TRANSIENT_ERROR_CATEGORIES


def llm_error_is_retryable(category: str) -> bool:
    return category in RETRYABLE_ERROR_CATEGORIES


def _has_auth(entry: LlmJudgeProviderRuntimeConfig) -> bool:
    if entry.api_key or entry.api_secret or entry.headers:
        return True
    base_url = str(entry.base_url or "").lower()
    return any(host in base_url for host in ("localhost", "127.0.0.1", "ollama"))


def configured_provider_entries(
    config: LlmJudgeRuntimeConfig,
) -> list[LlmJudgeProviderRuntimeConfig]:
    entries = [
        entry
        for entry in list(getattr(config, "providers", None) or [])
        if str(entry.base_url or "").strip()
        and str(entry.model or "").strip()
        and _has_auth(entry)
    ]
    return entries


def runtime_config_from_provider_entry(
    entry: LlmJudgeProviderRuntimeConfig,
) -> RuntimeLLMConfig:
    provider = "openai_compatible" if entry.provider == "nvidia" else entry.provider
    return RuntimeLLMConfig(
        provider=provider,
        base_url=entry.base_url,
        api_key=entry.api_key,
        api_secret=entry.api_secret,
        headers=entry.headers,
        model=entry.model,
        temperature=entry.temperature,
        timeout_seconds=entry.timeout_seconds,
        structured_output_mode=entry.structured_output_mode,
    )


def sanitized_provider_config(entry: LlmJudgeProviderRuntimeConfig) -> dict[str, Any]:
    requests_per_minute = getattr(entry, "requests_per_minute", None)
    rate_limit_max_wait_seconds = getattr(entry, "rate_limit_max_wait_seconds", 0)
    fallback_on_rate_limit = getattr(entry, "fallback_on_rate_limit", False)
    return {
        "name": str(entry.name or entry.provider),
        "provider": str(entry.provider),
        "base_url": str(entry.base_url or ""),
        "model": str(entry.model or ""),
        "structured_output_mode": str(entry.structured_output_mode),
        "timeout_seconds": int(entry.timeout_seconds),
        "max_input_tokens": int(entry.max_input_tokens),
        "requests_per_minute": (
            int(requests_per_minute)
            if requests_per_minute is not None
            else None
        ),
        "rate_limit_max_wait_seconds": int(rate_limit_max_wait_seconds),
        "fallback_on_rate_limit": bool(fallback_on_rate_limit),
        "api_key_env": str(entry.api_key_env or "") or None,
    }


def build_match_judge_provider(config: LlmJudgeRuntimeConfig) -> LLMProvider | None:
    entries = configured_provider_entries(config)
    if not entries:
        return None
    candidates: list[LLMProviderCandidate] = []
    for entry in entries:
        candidates.append(
            LLMProviderCandidate(
                name=str(entry.name or entry.provider),
                provider_name=str(entry.provider),
                model=str(entry.model or ""),
                provider=build_llm_provider(runtime_config_from_provider_entry(entry)),
                requests_per_minute=(
                    int(entry.requests_per_minute)
                    if entry.requests_per_minute is not None
                    else None
                ),
                rate_limit_max_wait_seconds=int(entry.rate_limit_max_wait_seconds),
                fallback_on_rate_limit=bool(entry.fallback_on_rate_limit),
            )
        )
    return LLMProviderChain(candidates)


class LLMProviderChain(LLMProvider):
    """Ordered fallback wrapper for match-level LLM judging."""

    def __init__(
        self,
        candidates: list[LLMProviderCandidate],
        *,
        rate_limiter: ProviderRateLimiter | None = None,
    ) -> None:
        if not candidates:
            raise ValueError("LLMProviderChain requires at least one provider candidate.")
        self._candidates = candidates
        self._rate_limiter = rate_limiter or ProviderRateLimiter()
        self.last_attempts: list[dict[str, Any]] = []
        self.last_success: dict[str, str] | None = None

    def _record_fallback(
        self,
        current: LLMProviderCandidate,
        next_candidate: LLMProviderCandidate,
        category: str,
    ) -> None:
        try:
            from core.metrics import record_llm_judge_provider_fallback

            record_llm_judge_provider_fallback(
                current.provider_name,
                next_candidate.provider_name,
                category,
            )
        except Exception:
            pass

    def _rate_limit_provider(self, candidate: LLMProviderCandidate) -> None:
        if candidate.requests_per_minute is None:
            return
        self._rate_limiter.wait_for_slot(
            provider_name=candidate.name,
            requests_per_minute=int(candidate.requests_per_minute),
            max_wait_seconds=int(candidate.rate_limit_max_wait_seconds),
        )

    def _append_failure_attempt(
        self,
        candidate: LLMProviderCandidate,
        *,
        started: float,
        status: str,
        error_category: str,
        retryable: bool,
        retry_after_seconds: float | None = None,
    ) -> None:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        attempt = {
            "provider": candidate.name,
            "provider_type": candidate.provider_name,
            "model": candidate.model,
            "status": status,
            "error_category": error_category,
            "retryable": retryable,
            "elapsed_ms": max(elapsed_ms, 0),
        }
        if retry_after_seconds is not None:
            attempt["retry_after_seconds"] = max(float(retry_after_seconds), 0.0)
        self.last_attempts.append(attempt)

    def _call(self, operation: Callable[[LLMProvider], Any]) -> Any:
        self.last_attempts = []
        self.last_success = None
        last_error: BaseException | None = None
        last_category = "unknown"
        for index, candidate in enumerate(self._candidates):
            started = time.monotonic()
            try:
                self._rate_limit_provider(candidate)
                result = operation(candidate.provider)
            except ProviderRateLimitExceeded as exc:
                last_error = exc
                last_category = "rate_limit"
                self._append_failure_attempt(
                    candidate,
                    started=started,
                    status="rate_limited",
                    error_category=last_category,
                    retryable=True,
                    retry_after_seconds=exc.retry_after_seconds,
                )
                if candidate.fallback_on_rate_limit and index < len(self._candidates) - 1:
                    self._record_fallback(
                        candidate,
                        self._candidates[index + 1],
                        last_category,
                    )
                    continue
                raise LLMProviderChainError(
                    "LLM provider chain failed.",
                    error_category=last_category,
                    attempts=list(self.last_attempts),
                    retryable=True,
                ) from exc
            except Exception as exc:
                last_error = exc
                last_category = classify_llm_provider_error(exc)
                self._append_failure_attempt(
                    candidate,
                    started=started,
                    status="failed",
                    error_category=last_category,
                    retryable=llm_error_is_retryable(last_category),
                )
                should_fallback = (
                    llm_error_is_transient(last_category)
                    and index < len(self._candidates) - 1
                    and (last_category != "rate_limit" or candidate.fallback_on_rate_limit)
                )
                if should_fallback:
                    self._record_fallback(
                        candidate,
                        self._candidates[index + 1],
                        last_category,
                    )
                    continue
                raise LLMProviderChainError(
                    "LLM provider chain failed.",
                    error_category=last_category,
                    attempts=list(self.last_attempts),
                    retryable=llm_error_is_retryable(last_category),
                ) from exc
            elapsed_ms = int((time.monotonic() - started) * 1000)
            self.last_success = {
                "provider": candidate.name,
                "provider_type": candidate.provider_name,
                "model": candidate.model,
            }
            self.last_attempts.append(
                {
                    "provider": candidate.name,
                    "provider_type": candidate.provider_name,
                    "model": candidate.model,
                    "status": "succeeded",
                    "error_category": None,
                    "retryable": False,
                    "elapsed_ms": max(elapsed_ms, 0),
                }
            )
            return result
        raise LLMProviderChainError(
            "LLM provider chain failed.",
            error_category=last_category,
            attempts=list(self.last_attempts),
            retryable=llm_error_is_retryable(last_category),
        ) from last_error

    def extract_structured_data(
        self,
        text: str,
        schema_spec: Dict,
        system_prompt: str | None = None,
        user_message: str | None = None,
    ) -> Dict[str, Any]:
        return self._call(
            lambda provider: provider.extract_structured_data(
                text,
                schema_spec,
                system_prompt=system_prompt,
                user_message=user_message,
            )
        )

    def extract_resume_data(self, text: str) -> Dict[str, Any]:
        return self._call(lambda provider: provider.extract_resume_data(text))

    def extract_requirements_data(self, text: str) -> Dict[str, Any]:
        return self._call(lambda provider: provider.extract_requirements_data(text))

    def generate_embedding(self, text: str) -> List[float]:
        return self._call(lambda provider: provider.generate_embedding(text))
