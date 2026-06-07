import pytest

from core.config_loader import (
    LlmConfig,
    LlmJudgeRuntimeConfig,
    PreferenceModelConfig,
    SemanticFitLlmConfig,
)
from core.llm.openai_service import OpenAIService
from core.llm.provider_factory import (
    RuntimeLLMConfig,
    build_llm_provider,
    runtime_llm_config_from_etl,
    runtime_llm_config_from_fit,
    runtime_llm_config_from_match_judge,
    runtime_llm_config_from_preference,
)


def test_runtime_llm_config_from_etl_maps_embedding_fields():
    config = LlmConfig(
        provider="openai_compatible",
        base_url="https://etl.example/v1",
        api_key="etl-key",
        extraction_headers={"X-ETL": "1"},
        extraction_model="gpt-etl",
        extraction_temperature=0.3,
        embedding_model="embed-model",
        embedding_dimensions=768,
        embedding_base_url="https://embed.example/v1",
        embedding_api_key="embed-key",
        embedding_headers={"X-Embed": "1"},
    )

    runtime_config = runtime_llm_config_from_etl(config)

    assert runtime_config.provider == "openai_compatible"
    assert runtime_config.model == "gpt-etl"
    assert runtime_config.embedding_model == "embed-model"
    assert runtime_config.embedding_dimensions == 768
    assert runtime_config.headers == {"X-ETL": "1"}
    assert runtime_config.embedding_headers == {"X-Embed": "1"}


def test_runtime_llm_config_from_preference_maps_timeout_and_embeddings():
    config = PreferenceModelConfig(
        provider="openai_compatible",
        base_url="https://preferences.example/v1",
        api_key="preferences-key",
        model="gpt-preferences",
        timeout_seconds=45,
        embedding_model="embed-model",
        embedding_dimensions=512,
    )

    runtime_config = runtime_llm_config_from_preference(config)

    assert runtime_config.model == "gpt-preferences"
    assert runtime_config.timeout_seconds == 45
    assert runtime_config.embedding_model == "embed-model"
    assert runtime_config.embedding_dimensions == 512


def test_runtime_llm_config_from_fit_maps_llm_fields():
    config = SemanticFitLlmConfig(
        enabled=True,
        provider="openai_compatible",
        base_url="https://fit.example/v1",
        api_key="fit-key",
        model="gpt-fit",
        temperature=0.1,
        timeout_seconds=12,
    )

    runtime_config = runtime_llm_config_from_fit(config)

    assert runtime_config.provider == "openai_compatible"
    assert runtime_config.model == "gpt-fit"
    assert runtime_config.temperature == 0.1
    assert runtime_config.timeout_seconds == 12


def test_runtime_llm_config_from_match_judge_maps_groq_fields():
    config = LlmJudgeRuntimeConfig(
        provider="groq",
        base_url="https://api.groq.com/openai/v1",
        api_key="groq-key",
        model="openai/gpt-oss-20b",
        temperature=0.1,
        timeout_seconds=18,
        structured_output_mode="auto",
    )

    runtime_config = runtime_llm_config_from_match_judge(config)

    assert runtime_config.provider == "groq"
    assert runtime_config.base_url == "https://api.groq.com/openai/v1"
    assert runtime_config.api_key == "groq-key"
    assert runtime_config.model == "openai/gpt-oss-20b"
    assert runtime_config.timeout_seconds == 18
    assert runtime_config.structured_output_mode == "auto"


def test_build_llm_provider_constructs_openai_compatible_service():
    provider = build_llm_provider(
        RuntimeLLMConfig(
            provider="openai_compatible",
            base_url="https://llm.example/v1",
            api_key="llm-key",
            model="gpt-runtime",
            temperature=0.2,
            timeout_seconds=12,
            structured_output_mode="auto",
            embedding_model="embed-model",
            embedding_dimensions=1024,
        )
    )

    assert isinstance(provider, OpenAIService)
    assert provider.extraction_model == "gpt-runtime"
    assert provider.embedding_model == "embed-model"
    assert provider.embedding_dimensions == 1024
    assert provider.structured_output_mode == "auto"


def test_build_llm_provider_constructs_groq_alias_service():
    provider = build_llm_provider(
        RuntimeLLMConfig(
            provider="groq",
            api_key="groq-key",
            model="llama-3.1-8b-instant",
            structured_output_mode="auto",
        )
    )

    assert isinstance(provider, OpenAIService)
    assert str(provider.client.base_url).rstrip("/") == "https://api.groq.com/openai/v1"
    assert provider.extraction_model == "llama-3.1-8b-instant"


def test_build_llm_provider_requires_model():
    with pytest.raises(RuntimeError, match="requires a model"):
        build_llm_provider(
            RuntimeLLMConfig(
                provider="openai_compatible",
                base_url="https://llm.example/v1",
                api_key="llm-key",
                model=" ",
            )
        )
