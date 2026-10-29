"""NVIDIA model failover preserves account limits and private-data routing."""

from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError

from core.config_loader import JobExtractionRoutingConfig, LlmConfig, load_config
from core.llm.interfaces import LLMProvider
from core.llm.job_extraction_provider import JobExtractionProvider, build_job_extraction_provider
from core.llm.provider_rate_limiter import ProviderCircuitBreaker, ProviderRateLimitExceeded
from core.llm.schema_models import JobExtraction
from tests.unit.core.llm.test_provider_rate_limiter import _FakeCircuitRedis


def _result() -> Dict[str, Any]:
    return {
        name: [] if name in {"tech_stack", "requirements", "benefits"} else
        "Test role" if name in {"thought_process", "job_summary"} else None
        for name in JobExtraction.model_fields
    }


def _build(
    *, circuit: Optional[ProviderCircuitBreaker] = None,
) -> Tuple[JobExtractionProvider, List[Mock], Mock]:
    models = [Mock(spec=LLMProvider) for _ in range(3)]
    for model in models:
        model.extract_requirements_data.return_value = _result()
    default = Mock(spec=LLMProvider)
    config = LlmConfig(
        api_key="etl-test-key", extraction_model="etl-model",
        job_routing={
            "enabled": True, "nvidia_api_key": "nvidia-test-key",
            "nvidia_model": "primary", "nvidia_fallback_models": ["alternate"],
        },
    )
    with patch("core.llm.job_extraction_provider.build_llm_provider", side_effect=models):
        provider = build_job_extraction_provider(config, default)
    provider.chain._rate_limiter = Mock()
    provider.chain._circuit_breaker = circuit or Mock(spec=ProviderCircuitBreaker)
    return provider, models, default


def test_timeout_tries_alternate_nvidia_before_etl() -> None:
    provider, (primary, alternate, etl), _ = _build()
    primary.extract_requirements_data.side_effect = TimeoutError("timed out")

    assert provider.extract_requirements_data("Public job") == _result()

    alternate.extract_requirements_data.assert_called_once_with("Public job")
    etl.extract_requirements_data.assert_not_called()
    assert provider.extraction_model == "alternate"
    assert [a["model"] for a in provider.chain.last_attempts] == ["primary", "alternate"]


def test_model_circuits_are_independent() -> None:
    redis = _FakeCircuitRedis()
    circuit = ProviderCircuitBreaker(client_factory=lambda: redis)
    circuit.defer("nvidia-extraction", model="primary", seconds=120)
    provider, (primary, alternate, etl), _ = _build(circuit=circuit)

    provider.extract_requirements_data("Public job")

    primary.extract_requirements_data.assert_not_called()
    alternate.extract_requirements_data.assert_called_once()
    etl.extract_requirements_data.assert_not_called()
    assert provider.chain.last_attempts[0]["status"] == "circuit_open"


def test_alternate_models_share_the_account_rate_limit() -> None:
    provider, (primary, alternate, etl), _ = _build()
    limiter = provider.chain._rate_limiter
    limiter.wait_for_slot.side_effect = ProviderRateLimitExceeded("nvidia-extraction", 20)

    provider.extract_requirements_data("Public job")

    primary.extract_requirements_data.assert_not_called()
    alternate.extract_requirements_data.assert_not_called()
    etl.extract_requirements_data.assert_called_once()
    assert len(limiter.wait_for_slot.call_args_list) == 2
    assert all(call.kwargs == {
        "provider_name": "nvidia-extraction", "requests_per_minute": 10,
        "max_wait_seconds": 0,
    } for call in limiter.wait_for_slot.call_args_list)


def test_private_resume_keeps_default_provider() -> None:
    provider, models, default = _build()

    provider.extract_resume_data("Private resume")

    default.extract_resume_data.assert_called_once_with("Private resume")
    for model in models:
        model.extract_resume_data.assert_not_called()
        model.extract_requirements_data.assert_not_called()


def test_model_list_environment_is_normalized(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NVIDIA_EXTRACTION_FALLBACK_MODELS", " alternate-a,alternate-b, alternate-a ")
    routing = load_config().etl.llm.job_routing
    assert routing.nvidia_fallback_models == ["alternate-a", "alternate-b"]


@pytest.mark.parametrize("models", [["a", "b", "c"], [""], [123], ["x" * 201]])
def test_model_list_rejects_invalid_or_unbounded_config(models: List[Any]) -> None:
    with pytest.raises(ValidationError):
        JobExtractionRoutingConfig(nvidia_fallback_models=models)


def test_primary_model_is_not_retried_as_its_own_fallback() -> None:
    config = LlmConfig(api_key="etl-test-key", job_routing={
        "enabled": True, "nvidia_api_key": "nvidia-test-key",
        "nvidia_model": "primary", "nvidia_fallback_models": ["primary", "alternate"],
    })
    with patch("core.llm.job_extraction_provider.build_llm_provider") as build:
        provider = build_job_extraction_provider(config, Mock())
    assert [c.model for c in provider.chain._candidates] == ["primary", "alternate", "gpt-4o-mini"]
    assert build.call_count == 3
