from __future__ import annotations

from typing import Any

import pytest

from core.config_loader import LlmJudgeProviderRuntimeConfig
from core.llm.interfaces import LLMProvider
from core.llm.provider_chain import (
    LLMProviderCandidate,
    LLMProviderChain,
    LLMProviderChainError,
    classify_llm_provider_error,
    configured_provider_entries,
)
from core.llm.provider_rate_limiter import ProviderRateLimitExceeded


class _Provider(LLMProvider):
    def __init__(self, *, response: dict[str, Any] | None = None, error: Exception | None = None):
        self.calls = 0
        self.response = response or {"ok": True}
        self.error = error

    def extract_structured_data(self, text, schema_spec, system_prompt=None, user_message=None):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.response

    def extract_resume_data(self, text):
        raise NotImplementedError

    def extract_requirements_data(self, text):
        raise NotImplementedError

    def generate_embedding(self, text):
        raise NotImplementedError


def _candidate(
    name: str,
    provider: _Provider,
    *,
    requests_per_minute: int | None = None,
    rate_limit_max_wait_seconds: int = 0,
    fallback_on_rate_limit: bool = False,
) -> LLMProviderCandidate:
    return LLMProviderCandidate(
        name=name,
        provider_name=name,
        model=f"{name}-model",
        provider=provider,
        requests_per_minute=requests_per_minute,
        rate_limit_max_wait_seconds=rate_limit_max_wait_seconds,
        fallback_on_rate_limit=fallback_on_rate_limit,
    )


class _RateLimiter:
    def __init__(self, error: ProviderRateLimitExceeded | None = None):
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def wait_for_slot(
        self,
        *,
        provider_name: str,
        requests_per_minute: int,
        max_wait_seconds: int,
    ) -> None:
        self.calls.append(
            {
                "provider_name": provider_name,
                "requests_per_minute": requests_per_minute,
                "max_wait_seconds": max_wait_seconds,
            }
        )
        if self.error is not None:
            raise self.error


def test_provider_chain_falls_back_after_transient_failure() -> None:
    primary = _Provider(error=TimeoutError("timed out"))
    fallback = _Provider(response={"score": 90})
    chain = LLMProviderChain([_candidate("nvidia", primary), _candidate("groq", fallback)])

    result = chain.extract_structured_data("SAFE PAYLOAD", {}, user_message="SAFE PAYLOAD")

    assert result == {"score": 90}
    assert primary.calls == 1
    assert fallback.calls == 1
    assert [attempt["status"] for attempt in chain.last_attempts] == ["failed", "succeeded"]
    assert chain.last_attempts[0]["error_category"] == "timeout"
    assert "SAFE PAYLOAD" not in str(chain.last_attempts)
    assert chain.last_success == {
        "provider": "groq",
        "provider_type": "groq",
        "model": "groq-model",
    }


def test_provider_chain_does_not_fallback_after_terminal_auth_failure() -> None:
    error = RuntimeError("invalid api key")
    error.status_code = 401
    primary = _Provider(error=error)
    fallback = _Provider(response={"score": 90})
    chain = LLMProviderChain([_candidate("nvidia", primary), _candidate("groq", fallback)])

    with pytest.raises(LLMProviderChainError) as exc_info:
        chain.extract_structured_data("SAFE PAYLOAD", {})

    assert exc_info.value.error_category == "invalid_auth"
    assert exc_info.value.retryable is False
    assert primary.calls == 1
    assert fallback.calls == 0
    assert len(chain.last_attempts) == 1


def test_provider_chain_does_not_fallback_after_rate_limit_by_default() -> None:
    error = RuntimeError("tokens per minute")
    error.status_code = 429
    primary = _Provider(error=error)
    fallback = _Provider(response={"score": 90})
    chain = LLMProviderChain([_candidate("nvidia", primary), _candidate("groq", fallback)])

    with pytest.raises(LLMProviderChainError) as exc_info:
        chain.extract_structured_data("SAFE PAYLOAD", {})

    assert exc_info.value.error_category == "rate_limit"
    assert exc_info.value.retryable is True
    assert primary.calls == 1
    assert fallback.calls == 0
    assert chain.last_attempts[0]["error_category"] == "rate_limit"


def test_provider_chain_can_fallback_after_rate_limit_when_explicitly_enabled() -> None:
    error = RuntimeError("tokens per minute")
    error.status_code = 429
    primary = _Provider(error=error)
    fallback = _Provider(response={"score": 90})
    chain = LLMProviderChain(
        [
            _candidate("nvidia", primary, fallback_on_rate_limit=True),
            _candidate("groq", fallback),
        ]
    )

    result = chain.extract_structured_data("SAFE PAYLOAD", {})

    assert result == {"score": 90}
    assert primary.calls == 1
    assert fallback.calls == 1


def test_provider_chain_applies_configured_rate_limiter_before_provider_call() -> None:
    primary = _Provider(response={"score": 90})
    limiter = _RateLimiter()
    chain = LLMProviderChain(
        [
            _candidate(
                "nvidia",
                primary,
                requests_per_minute=40,
                rate_limit_max_wait_seconds=90,
            )
        ],
        rate_limiter=limiter,
    )

    result = chain.extract_structured_data("SAFE PAYLOAD", {})

    assert result == {"score": 90}
    assert primary.calls == 1
    assert limiter.calls == [
        {
            "provider_name": "nvidia",
            "requests_per_minute": 40,
            "max_wait_seconds": 90,
        }
    ]


def test_provider_chain_rate_limiter_backpressure_does_not_call_fallback() -> None:
    primary = _Provider(response={"score": 90})
    fallback = _Provider(response={"score": 75})
    limiter = _RateLimiter(ProviderRateLimitExceeded("nvidia", 12.5))
    chain = LLMProviderChain(
        [
            _candidate("nvidia", primary, requests_per_minute=40),
            _candidate("groq", fallback),
        ],
        rate_limiter=limiter,
    )

    with pytest.raises(LLMProviderChainError) as exc_info:
        chain.extract_structured_data("SAFE PAYLOAD", {})

    assert exc_info.value.error_category == "rate_limit"
    assert exc_info.value.retryable is True
    assert primary.calls == 0
    assert fallback.calls == 0
    assert chain.last_attempts[0]["status"] == "rate_limited"
    assert chain.last_attempts[0]["retry_after_seconds"] == 12.5


def test_provider_chain_skips_entries_without_provider_credentials(monkeypatch) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("CEREBRAS_API_KEY", raising=False)
    monkeypatch.setenv("GROQ_API_KEY", "groq-key")
    entries = [
        LlmJudgeProviderRuntimeConfig(
            name="nvidia",
            provider="nvidia",
            api_key_env="NVIDIA_API_KEY",
            model="nvidia-model",
        ),
        LlmJudgeProviderRuntimeConfig(
            name="groq",
            provider="groq",
            api_key_env="GROQ_API_KEY",
            model="groq-model",
        ),
    ]
    config = type("Config", (), {"providers": entries})()

    configured = configured_provider_entries(config)

    assert [entry.name for entry in configured] == ["groq"]
    assert configured[0].api_key == "groq-key"


@pytest.mark.parametrize(
    "status_code,message,expected",
    [
        (429, "tokens per minute", "rate_limit"),
        (500, "server error", "server_error"),
        (413, "request too large", "input_too_large"),
        (404, "model not found", "unsupported_model"),
        (400, "bad request", "invalid_request"),
    ],
)
def test_classify_llm_provider_error_uses_bounded_categories(
    status_code: int,
    message: str,
    expected: str,
) -> None:
    error = RuntimeError(message)
    error.status_code = status_code

    assert classify_llm_provider_error(error) == expected
