from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from core.app_context import _current_environment, _ensure_fake_ai_allowed, _fake_ai_enabled
from core.config_loader import PreferenceModelConfig
from core.llm.fake_service import FakeLLMService
from core.llm.interfaces import LLMProvider
from core.llm.openai_service import OpenAIService

logger = logging.getLogger(__name__)

PreferenceMode = Literal["semantic_rerank", "llm_judge"]
PREFERENCE_PROFILE_VERSION = "2026-04-01.v1"

PREFERENCE_PARSER_SYSTEM_PROMPT = """
You normalize candidate job preferences into a strict schema.

Rules:
- Use only the user's stated preferences.
- Do not invent hard constraints or qualifications.
- Focus on soft preferences only.
- Normalize into concise labels.
- Leave arrays empty when the user did not express that category.
- Confidence values must be between 0 and 1.
"""


class WeightedPreference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: str
    weight: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)


class PreferenceProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_text: str
    parse_version: str = Field(default=PREFERENCE_PROFILE_VERSION)
    parser_confidence: float = Field(ge=0.0, le=1.0)
    work_style: List[WeightedPreference] = Field(default_factory=list)
    team_culture: List[WeightedPreference] = Field(default_factory=list)
    tech_stack: List[WeightedPreference] = Field(default_factory=list)
    mission_domain: List[WeightedPreference] = Field(default_factory=list)
    growth_preferences: List[WeightedPreference] = Field(default_factory=list)
    negative_preferences: List[WeightedPreference] = Field(default_factory=list)


PREFERENCE_PROFILE_SCHEMA = {
    "name": "preference_profile_schema",
    "strict": True,
    "schema": PreferenceProfile.model_json_schema(),
}


class PreferenceParser(ABC):
    @abstractmethod
    def parse(self, text: str) -> Optional[PreferenceProfile]:
        """Parse free-text user preferences into a normalized profile."""


class PreferenceSemanticReranker(ABC):
    @abstractmethod
    def rerank(self, *args: Any, **kwargs: Any) -> Any:
        """Rerank a fit-qualified shortlist using a semantic preference signal."""


class PreferenceJudge(ABC):
    @abstractmethod
    def judge(self, *args: Any, **kwargs: Any) -> Any:
        """Advanced LLM-based judge for preference-aware ordering."""


class LLMPreferenceParser(PreferenceParser):
    """Parse free-text soft preferences using an independently configured LLM."""

    def __init__(self, llm: LLMProvider):
        self.llm = llm

    def parse(self, text: str) -> Optional[PreferenceProfile]:
        normalized = text.strip()
        if not normalized:
            return None

        data = self.llm.extract_structured_data(
            normalized,
            PREFERENCE_PROFILE_SCHEMA,
            system_prompt=PREFERENCE_PARSER_SYSTEM_PROMPT,
            user_message=f"Normalize this candidate preference text.\n\n{normalized}",
        )
        if not isinstance(data, dict):
            return None
        if not data.get("raw_text"):
            data["raw_text"] = normalized
        if not data.get("parse_version"):
            data["parse_version"] = PREFERENCE_PROFILE_VERSION
        return PreferenceProfile.model_validate(data)


def build_preference_llm(config: PreferenceModelConfig) -> Optional[LLMProvider]:
    if not config.enabled:
        return None

    _ensure_fake_ai_allowed()
    if _fake_ai_enabled():
        return FakeLLMService(embedding_dimensions=1024)

    if not config.model:
        logger.info(
            "Preference model disabled in %s because no model is configured",
            _current_environment(),
        )
        return None

    model_config = {
        "extraction_model": config.model,
        "embedding_model": config.embedding_model,
        "embedding_dimensions": config.embedding_dimensions,
        "extraction_temperature": config.temperature,
    }
    return OpenAIService(
        base_url=config.base_url,
        api_key=config.api_key,
        api_secret=config.api_secret,
        model_config=model_config,
        extraction_headers=config.headers,
        embedding_base_url=config.embedding_base_url,
        embedding_api_key=config.embedding_api_key,
        embedding_api_secret=config.embedding_api_secret,
        embedding_headers=config.embedding_headers,
    )


def summarize_preference_profile(
    profile: Optional[PreferenceProfile],
    raw_text: str,
    *,
    max_length: int = 160,
) -> str:
    if profile is not None:
        labels: List[str] = []
        for field_name in (
            "work_style",
            "team_culture",
            "tech_stack",
            "mission_domain",
            "growth_preferences",
            "negative_preferences",
        ):
            items = getattr(profile, field_name)
            labels.extend(item.label for item in items[:2] if item.label)
        deduped: List[str] = []
        seen: set[str] = set()
        for label in labels:
            lowered = label.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            deduped.append(label)
            if len(deduped) >= 4:
                break
        if deduped:
            return ", ".join(deduped)

    trimmed = " ".join(raw_text.split())
    if len(trimmed) <= max_length:
        return trimmed
    return f"{trimmed[: max_length - 1].rstrip()}…"
