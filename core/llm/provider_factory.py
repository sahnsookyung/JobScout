from __future__ import annotations

from typing import Dict, Literal, Optional

from pydantic import BaseModel

from core.config_loader import (
    CEREBRAS_OPENAI_COMPATIBLE_BASE_URL,
    GROQ_OPENAI_COMPATIBLE_BASE_URL,
    LlmConfig,
    LlmJudgeRuntimeConfig,
    PreferenceModelConfig,
    SemanticFitLlmConfig,
)
from core.llm.interfaces import LLMProvider
from core.llm.openai_service import OpenAIService


class RuntimeLLMConfig(BaseModel):
    provider: Literal["openai_compatible", "groq", "cerebras"] = "openai_compatible"
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    model: Optional[str] = None
    temperature: float = 0.0
    timeout_seconds: Optional[int] = None
    structured_output_mode: Optional[Literal["auto", "json_schema", "json_object"]] = None
    embedding_model: Optional[str] = None
    embedding_dimensions: Optional[int] = None
    embedding_base_url: Optional[str] = None
    embedding_api_key: Optional[str] = None
    embedding_api_secret: Optional[str] = None
    embedding_headers: Optional[Dict[str, str]] = None


def runtime_llm_config_from_etl(config: LlmConfig) -> RuntimeLLMConfig:
    return RuntimeLLMConfig(
        provider=config.provider,
        base_url=config.base_url,
        api_key=config.api_key,
        api_secret=config.api_secret,
        headers=config.extraction_headers,
        model=config.extraction_model,
        temperature=config.extraction_temperature,
        structured_output_mode=config.structured_output_mode,
        embedding_model=config.embedding_model,
        embedding_dimensions=config.embedding_dimensions,
        embedding_base_url=config.embedding_base_url,
        embedding_api_key=config.embedding_api_key,
        embedding_api_secret=config.embedding_api_secret,
        embedding_headers=config.embedding_headers,
    )


def runtime_llm_config_from_preference(config: PreferenceModelConfig) -> RuntimeLLMConfig:
    return RuntimeLLMConfig(
        provider=config.provider,
        base_url=config.base_url,
        api_key=config.api_key,
        api_secret=config.api_secret,
        headers=config.headers,
        model=config.model,
        temperature=config.temperature,
        timeout_seconds=config.timeout_seconds,
        structured_output_mode=config.structured_output_mode,
        embedding_model=config.embedding_model,
        embedding_dimensions=config.embedding_dimensions,
        embedding_base_url=config.embedding_base_url,
        embedding_api_key=config.embedding_api_key,
        embedding_api_secret=config.embedding_api_secret,
        embedding_headers=config.embedding_headers,
    )


def runtime_llm_config_from_fit(config: SemanticFitLlmConfig) -> RuntimeLLMConfig:
    return RuntimeLLMConfig(
        provider=config.provider,
        base_url=config.base_url,
        api_key=config.api_key,
        api_secret=config.api_secret,
        headers=config.headers,
        model=config.model,
        temperature=config.temperature,
        timeout_seconds=config.timeout_seconds,
    )


def runtime_llm_config_from_match_judge(config: LlmJudgeRuntimeConfig) -> RuntimeLLMConfig:
    return RuntimeLLMConfig(
        provider=config.provider,
        base_url=config.base_url,
        api_key=config.api_key,
        api_secret=config.api_secret,
        headers=config.headers,
        model=config.model,
        temperature=config.temperature,
        timeout_seconds=config.timeout_seconds,
        structured_output_mode=config.structured_output_mode,
    )


def _normalize_chat_provider(provider: str) -> str:
    if provider in {"groq", "cerebras"}:
        return "openai_compatible"
    return provider


def _normalize_base_url(config: RuntimeLLMConfig) -> Optional[str]:
    if config.provider == "groq" and not str(config.base_url or "").strip():
        return GROQ_OPENAI_COMPATIBLE_BASE_URL
    if config.provider == "cerebras" and not str(config.base_url or "").strip():
        return CEREBRAS_OPENAI_COMPATIBLE_BASE_URL
    return config.base_url


def _normalize_structured_output_mode(
    config: RuntimeLLMConfig,
) -> Optional[Literal["auto", "json_schema", "json_object"]]:
    if config.provider == "cerebras" and config.structured_output_mode in (None, "auto"):
        return "json_object"
    return config.structured_output_mode


def build_llm_provider(config: RuntimeLLMConfig) -> LLMProvider:
    provider = _normalize_chat_provider(config.provider)
    if provider != "openai_compatible":
        raise RuntimeError(
            f"Unsupported runtime LLM provider '{config.provider}'. "
            "Only 'openai_compatible', 'groq', and 'cerebras' are supported at runtime."
        )
    if not str(config.model or "").strip():
        raise RuntimeError(
            "Runtime LLM provider configuration requires a model for "
            f"provider='{config.provider}'."
        )

    model_config = {
        "extraction_model": config.model,
        "embedding_model": config.embedding_model,
        "embedding_dimensions": config.embedding_dimensions,
        "extraction_temperature": config.temperature,
    }
    return OpenAIService(
        base_url=_normalize_base_url(config),
        api_key=config.api_key,
        api_secret=config.api_secret,
        model_config=model_config,
        extraction_headers=config.headers,
        timeout_seconds=config.timeout_seconds,
        structured_output_mode=_normalize_structured_output_mode(config),
        embedding_base_url=config.embedding_base_url,
        embedding_api_key=config.embedding_api_key,
        embedding_api_secret=config.embedding_api_secret,
        embedding_headers=config.embedding_headers,
    )
