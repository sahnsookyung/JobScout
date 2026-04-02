from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Literal, Optional

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
""".strip()

PREFERENCE_RERANK_SYSTEM_PROMPT = """
You score how well each shortlisted job matches a candidate's soft preferences.

Rules:
- Use fit-qualified jobs only as candidates; do not reason about eligibility.
- Score soft preference alignment from 0 to 1.
- Return short user-safe explanations and terse reason codes.
- Prefer deterministic, schema-following output.
""".strip()

PREFERENCE_JUDGE_SYSTEM_PROMPT = """
You act as an advanced preference judge for already fit-qualified jobs.

Rules:
- Evaluate only soft preference alignment.
- Keep scores between 0 and 1.
- Use concise reason codes and short user-safe explanations.
- Do not invent hard constraints or candidate qualifications.
""".strip()


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


class PreferenceJobPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str
    title: str = ""
    company: str = ""
    location_text: str = ""
    work_mode: str = ""
    employment_type: str = ""
    summary: str = ""
    company_description: str = ""
    skills: List[str] = Field(default_factory=list)


class PreferenceAssessment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str
    preference_score: float = Field(ge=0.0, le=1.0)
    preference_confidence: float = Field(ge=0.0, le=1.0)
    preference_reason_codes: List[str] = Field(default_factory=list)
    preference_explanation: str = ""


class PreferenceRerankResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    results: List[PreferenceAssessment] = Field(default_factory=list)


PREFERENCE_PROFILE_SCHEMA = {
    "name": "preference_profile_schema",
    "strict": True,
    "schema": PreferenceProfile.model_json_schema(),
}

PREFERENCE_SEMANTIC_RERANK_SCHEMA = {
    "name": "preference_semantic_rerank_v1",
    "strict": True,
    "schema": PreferenceRerankResponse.model_json_schema(),
}

PREFERENCE_LLM_JUDGE_SCHEMA = {
    "name": "preference_llm_judge_v1",
    "strict": True,
    "schema": PreferenceRerankResponse.model_json_schema(),
}


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _normalize_skills(value: Any) -> List[str]:
    if isinstance(value, list):
        return [_normalize_text(item) for item in value if _normalize_text(item)]
    if not value:
        return []
    skills = []
    for token in str(value).replace(";", ",").split(","):
        normalized = _normalize_text(token)
        if normalized:
            skills.append(normalized)
    return skills


def _job_work_mode(job: Any) -> str:
    work_from_home_type = _normalize_text(getattr(job, "work_from_home_type", "")).lower()
    location_text = _normalize_text(getattr(job, "location_text", "")).lower()
    if getattr(job, "is_remote", None) is True or "remote" in work_from_home_type:
        return "remote"
    if "hybrid" in work_from_home_type or "hybrid" in location_text:
        return "hybrid"
    return "onsite"


def _job_summary(job: Any) -> str:
    raw_payload = getattr(job, "raw_payload", {}) or {}
    ai_summary = raw_payload.get("ai_job_summary") if isinstance(raw_payload, dict) else ""
    return _normalize_text(
        getattr(job, "canonical_job_summary", None)
        or ai_summary
        or getattr(job, "description", None)
        or ""
    )


def serialize_job_for_preference(job: Any) -> PreferenceJobPayload:
    return PreferenceJobPayload(
        job_id=str(getattr(job, "id")),
        title=_normalize_text(getattr(job, "title", "")),
        company=_normalize_text(getattr(job, "company", "")),
        location_text=_normalize_text(getattr(job, "location_text", "")),
        work_mode=_job_work_mode(job),
        employment_type=_normalize_text(getattr(job, "job_type", "")),
        summary=_job_summary(job),
        company_description=_normalize_text(getattr(job, "company_description", "")),
        skills=_normalize_skills(getattr(job, "skills_raw", "")),
    )


class PreferenceParser(ABC):
    @abstractmethod
    def parse(self, text: str) -> Optional[PreferenceProfile]:
        """Parse free-text user preferences into a normalized profile."""


class PreferenceSemanticReranker(ABC):
    @abstractmethod
    def rerank(
        self,
        profile: PreferenceProfile,
        jobs: List[PreferenceJobPayload],
    ) -> List[PreferenceAssessment]:
        """Rerank a fit-qualified shortlist using a semantic preference signal."""


class PreferenceJudge(ABC):
    @abstractmethod
    def judge(
        self,
        profile: PreferenceProfile,
        jobs: List[PreferenceJobPayload],
    ) -> List[PreferenceAssessment]:
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


class _BaseLLMPreferenceScorer:
    def __init__(
        self,
        llm: LLMProvider,
        *,
        schema_spec: Dict[str, Any],
        system_prompt: str,
        scorer_name: str,
    ):
        self.llm = llm
        self.schema_spec = schema_spec
        self.system_prompt = system_prompt
        self.scorer_name = scorer_name

    def _score(
        self,
        profile: PreferenceProfile,
        jobs: List[PreferenceJobPayload],
    ) -> List[PreferenceAssessment]:
        if not jobs:
            return []

        payload = {
            "profile": profile.model_dump(mode="json"),
            "jobs": [job.model_dump(mode="json") for job in jobs],
            "mode": self.scorer_name,
        }
        data = self.llm.extract_structured_data(
            json.dumps(payload),
            self.schema_spec,
            system_prompt=self.system_prompt,
            user_message="Score these fit-qualified jobs against the candidate's soft preferences.",
        )
        if not isinstance(data, dict):
            return []
        response = PreferenceRerankResponse.model_validate(data)
        return response.results


class LLMPreferenceSemanticReranker(_BaseLLMPreferenceScorer, PreferenceSemanticReranker):
    def __init__(self, llm: LLMProvider):
        super().__init__(
            llm,
            schema_spec=PREFERENCE_SEMANTIC_RERANK_SCHEMA,
            system_prompt=PREFERENCE_RERANK_SYSTEM_PROMPT,
            scorer_name="semantic_rerank",
        )

    def rerank(
        self,
        profile: PreferenceProfile,
        jobs: List[PreferenceJobPayload],
    ) -> List[PreferenceAssessment]:
        return self._score(profile, jobs)


class LLMPreferenceJudge(_BaseLLMPreferenceScorer, PreferenceJudge):
    def __init__(self, llm: LLMProvider):
        super().__init__(
            llm,
            schema_spec=PREFERENCE_LLM_JUDGE_SCHEMA,
            system_prompt=PREFERENCE_JUDGE_SYSTEM_PROMPT,
            scorer_name="llm_judge",
        )

    def judge(
        self,
        profile: PreferenceProfile,
        jobs: List[PreferenceJobPayload],
    ) -> List[PreferenceAssessment]:
        return self._score(profile, jobs)


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


def build_preference_parser(config: PreferenceModelConfig) -> Optional[PreferenceParser]:
    llm = build_preference_llm(config)
    if llm is None:
        return None
    return LLMPreferenceParser(llm)


def build_preference_semantic_reranker(
    config: PreferenceModelConfig,
) -> Optional[PreferenceSemanticReranker]:
    llm = build_preference_llm(config)
    if llm is None:
        return None
    return LLMPreferenceSemanticReranker(llm)


def build_preference_judge(config: PreferenceModelConfig) -> Optional[PreferenceJudge]:
    llm = build_preference_llm(config)
    if llm is None:
        return None
    return LLMPreferenceJudge(llm)


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
