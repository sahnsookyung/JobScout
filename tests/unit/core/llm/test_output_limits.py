"""Output limits must reject partial data without leaking content or usage."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import json

import pytest

from core.llm.errors import LLMOutputTruncatedError
from core.llm.global_budget import GlobalLlmBudgetUnavailable
from core.llm.openai_service import OpenAIService
from core.llm.provider_chain import (
    LLMProviderCandidate,
    LLMProviderChain,
    LLMProviderChainError,
    classify_llm_provider_error,
)
from core.llm.provider_rate_limiter import ProviderCircuitBreaker
from core.llm.schema_models import EXTRACTION_SCHEMA, RESUME_SCHEMA
from core.llm.system_prompts import REQUIREMENTS_EXTRACTION_SYSTEM_PROMPT, RESUME_EXTRACTION_SYSTEM_PROMPT


@pytest.fixture
def service() -> OpenAIService:
    with patch("core.llm.openai_service.OpenAI"):
        return OpenAIService(api_key="test", max_output_tokens=32768, retry_max_attempts=1)


def response(content: str, finish_reason: str = "stop", total_tokens: int = 4500) -> SimpleNamespace:
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=content), finish_reason=finish_reason)],
        usage=SimpleNamespace(total_tokens=total_tokens),
    )


@pytest.mark.parametrize("content", ['{"partial":', '{"apparently_valid":true}'])
def test_length_response_never_returns_partial_data(service: OpenAIService, caplog, content: str) -> None:
    service.client.chat.completions.create.return_value = response(content, "length")
    with patch("core.llm.openai_service.reconcile_current_global_llm_request_usage") as reconcile:
        with pytest.raises(LLMOutputTruncatedError, match="finish_reason=length"):
            service.extract_structured_data("PRIVATE SOURCE", EXTRACTION_SCHEMA)
    reconcile.assert_called_once_with(4500)
    assert service.last_usage == {"total_tokens": 4500}
    assert service.client.chat.completions.create.call_count == 1
    assert "finish_reason=length" in caplog.text
    assert content not in caplog.text
    assert "PRIVATE SOURCE" not in caplog.text


def test_invalid_json_records_usage_before_parsing(service: OpenAIService, caplog) -> None:
    service.client.chat.completions.create.return_value = response("PRIVATE INVALID JSON")
    with patch("core.llm.openai_service.reconcile_current_global_llm_request_usage") as reconcile:
        with pytest.raises(json.JSONDecodeError):
            service.extract_structured_data("PRIVATE SOURCE", EXTRACTION_SCHEMA)
    reconcile.assert_called_once_with(4500)
    assert "finish_reason=stop" in caplog.text
    assert "PRIVATE" not in caplog.text


def test_failed_new_attempt_does_not_reuse_previous_usage(service: OpenAIService) -> None:
    service.client.chat.completions.create.return_value = response('{"ok":true}')
    service.extract_structured_data("first", EXTRACTION_SCHEMA)
    service.client.chat.completions.create.side_effect = RuntimeError("failed before response")
    with pytest.raises(RuntimeError):
        service.extract_structured_data("second", EXTRACTION_SCHEMA)
    assert service.last_usage is None


@pytest.mark.parametrize("operation,schema,prompt,message", [
    ("extract_resume_data", RESUME_SCHEMA, RESUME_EXTRACTION_SYSTEM_PROMPT,
     "Extract the structured resume data following the schema.\n\nResume:\nexample"),
    ("extract_requirements_data", EXTRACTION_SCHEMA, REQUIREMENTS_EXTRACTION_SYSTEM_PROMPT,
     "<JOB_DESCRIPTION>\nexample\n</JOB_DESCRIPTION>\n\nExtract qualification requirements and the job offerings profile."),
])
def test_budget_estimate_includes_real_default_prompts(
    service: OpenAIService, operation: str, schema: dict, prompt: str, message: str,
) -> None:
    estimate = service.estimate_budget_tokens(operation, "example")
    explicit = service.estimate_budget_tokens(
        "extract_structured_data", "example", schema, system_prompt=prompt, user_message=message,
    )
    assert estimate == explicit
    assert estimate > 32768 + len(prompt + message) // 4


def test_budget_estimate_uses_cap_and_supplied_message_without_duplicate_text(service: OpenAIService) -> None:
    first = service.estimate_budget_tokens(
        "extract_structured_data", "ignored text", EXTRACTION_SCHEMA, user_message="actual prompt",
    )
    service.max_output_tokens = 4096
    second = service.estimate_budget_tokens(
        "extract_structured_data", "ignored text" * 1000, EXTRACTION_SCHEMA, user_message="actual prompt",
    )
    assert first - second == 32768 - 4096


@pytest.mark.parametrize("cap", [None, 0, -1, True])
def test_budgeted_extraction_requires_finite_cap(service: OpenAIService, cap) -> None:
    service.max_output_tokens = cap
    with pytest.raises(GlobalLlmBudgetUnavailable, match="finite output"):
        service.estimate_budget_tokens("extract_requirements_data", "text")


@pytest.mark.parametrize("allow_fallback", [False, True])
def test_truncation_fallback_requires_invalid_output_opt_in(allow_fallback: bool) -> None:
    primary = MagicMock()
    primary.extract_structured_data.side_effect = LLMOutputTruncatedError("Output limit reached")
    fallback = MagicMock()
    fallback.extract_structured_data.return_value = {"ok": True}
    candidate = LLMProviderCandidate("nvidia", "nvidia", "model", primary)
    chain = LLMProviderChain(
        [replace(candidate, fallback_on_invalid_output=allow_fallback),
         LLMProviderCandidate("cerebras", "cerebras", "model", fallback)],
        circuit_breaker=MagicMock(spec=ProviderCircuitBreaker), rate_limiter=MagicMock(),
    )
    if allow_fallback:
        assert chain.extract_structured_data("text", {}) == {"ok": True}
        fallback.extract_structured_data.assert_called_once()
    else:
        with pytest.raises(LLMProviderChainError) as caught:
            chain.extract_structured_data("text", {})
        assert caught.value.error_category == "output_truncated"
        assert caught.value.retryable is False
        fallback.extract_structured_data.assert_not_called()
    assert chain.last_attempts[0]["error_category"] == "output_truncated"
    chain._circuit_breaker.record_failure.assert_not_called()
    assert classify_llm_provider_error(LLMOutputTruncatedError("limit")) == "output_truncated"


def test_judge_preserves_truncation_error_category() -> None:
    from core.llm_evaluation import MatchLlmEvaluationService

    error = LLMOutputTruncatedError("limit")
    assert MatchLlmEvaluationService._provider_error_code(error) == "llm_judge_output_truncated"
    assert MatchLlmEvaluationService._provider_error_retryable(error) is False


def test_provider_health_preserves_truncation_without_opening_circuit() -> None:
    from core.config_loader import LlmJudgeProviderRuntimeConfig
    from core.llm.provider_health import _run_entry_canary

    provider = MagicMock()
    provider.extract_structured_data.side_effect = LLMOutputTruncatedError("Output limit reached")
    circuit = MagicMock(spec=ProviderCircuitBreaker)
    circuit.status.return_value = {}
    entry = LlmJudgeProviderRuntimeConfig(provider="nvidia", api_key="test", model="test-model")
    with patch("core.llm.provider_health.build_llm_provider", return_value=provider), patch(
        "core.llm.provider_health.record_llm_judge_provider_canary"
    ) as metric:
        result = _run_entry_canary(entry, circuit_breaker=circuit, rate_limiter=MagicMock())
    assert result["status"] == "failed"
    assert result["error_category"] == "output_truncated"
    assert result["retryable"] is False
    circuit.record_failure.assert_not_called()
    metric.assert_called_once_with("nvidia", "failed", "output_truncated")


def test_global_budget_disables_hidden_sdk_retries(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    with patch("core.llm.openai_service.OPENAI_CLIENT_MAX_RETRIES", 3), patch(
        "core.llm.openai_service.OpenAI"
    ) as client:
        OpenAIService(api_key="test", embedding_api_key="embedding-test")
    assert client.call_count == 2
    assert all(call.kwargs["max_retries"] == 0 for call in client.call_args_list)


def test_zero_reported_usage_is_not_refunded(service: OpenAIService) -> None:
    service.client.chat.completions.create.return_value = response('{"ok":true}', total_tokens=0)
    with patch("core.llm.openai_service.reconcile_current_global_llm_request_usage") as reconcile:
        assert service.extract_structured_data("text", EXTRACTION_SCHEMA) == {"ok": True}
    assert service.last_usage is None
    reconcile.assert_not_called()


def test_direct_service_call_cannot_bypass_token_reservations(service: OpenAIService, monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    with pytest.raises(GlobalLlmBudgetUnavailable, match="BudgetedLLMProvider"):
        service.extract_structured_data("text", EXTRACTION_SCHEMA)
    service.client.chat.completions.create.assert_not_called()
