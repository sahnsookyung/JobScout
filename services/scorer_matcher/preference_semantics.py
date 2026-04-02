from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from core.config_loader import PreferenceModelConfig
from core.llm.interfaces import LLMProvider
from core.llm.provider_factory import build_llm_provider, runtime_llm_config_from_preference

logger = logging.getLogger(__name__)

PreferenceMode = Literal["semantic_rerank", "llm_judge"]
PREFERENCE_PROFILE_VERSION = "2026-04-01.v1"
APPROX_CHARS_PER_TOKEN = 4
MIN_PREFERENCE_PAYLOAD_CHARS = 1200
MAX_PREFERENCE_TITLE_CHARS = 160
MAX_PREFERENCE_COMPANY_CHARS = 160
MAX_PREFERENCE_LOCATION_CHARS = 160
MAX_PREFERENCE_WORK_MODE_CHARS = 32
MAX_PREFERENCE_EMPLOYMENT_TYPE_CHARS = 64
MAX_PREFERENCE_SUMMARY_CHARS = 1800
MAX_PREFERENCE_COMPANY_DESCRIPTION_CHARS = 1200
MAX_PREFERENCE_LIST_ITEM_CHARS = 280
MAX_PREFERENCE_LIST_ITEMS = 8

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
    requirements: List[str] = Field(default_factory=list)
    benefits: List[str] = Field(default_factory=list)


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


def _truncate_text(value: Any, max_chars: int) -> str:
    normalized = _normalize_text(value)
    if len(normalized) <= max_chars:
        return normalized
    if max_chars <= 1:
        return normalized[:max_chars]
    return f"{normalized[: max_chars - 1].rstrip()}…"


def _normalize_job_text_list(
    value: Any,
    *,
    item_max_chars: int = MAX_PREFERENCE_LIST_ITEM_CHARS,
    max_items: int = MAX_PREFERENCE_LIST_ITEMS,
) -> List[str]:
    if not value:
        return []

    raw_items = value if isinstance(value, list) else [value]
    normalized: List[str] = []
    for item in raw_items:
        text = ""
        if hasattr(item, "text"):
            text = getattr(item, "text", "")
        elif isinstance(item, dict):
            text = item.get("text") or item.get("label") or item.get("name") or ""
        else:
            text = str(item)
        truncated = _truncate_text(text, item_max_chars)
        if truncated:
            normalized.append(truncated)
        if len(normalized) >= max_items:
            break
    return normalized


def _truncate_text_list(values: List[str], *, max_chars: int, max_items: int) -> List[str]:
    truncated: List[str] = []
    for value in values[:max_items]:
        text = _truncate_text(value, max_chars)
        if text:
            truncated.append(text)
    return truncated


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
        title=_truncate_text(getattr(job, "title", ""), MAX_PREFERENCE_TITLE_CHARS),
        company=_truncate_text(getattr(job, "company", ""), MAX_PREFERENCE_COMPANY_CHARS),
        location_text=_truncate_text(
            getattr(job, "location_text", ""),
            MAX_PREFERENCE_LOCATION_CHARS,
        ),
        work_mode=_truncate_text(_job_work_mode(job), MAX_PREFERENCE_WORK_MODE_CHARS),
        employment_type=_truncate_text(
            getattr(job, "job_type", ""),
            MAX_PREFERENCE_EMPLOYMENT_TYPE_CHARS,
        ),
        summary=_truncate_text(_job_summary(job), MAX_PREFERENCE_SUMMARY_CHARS),
        company_description=_truncate_text(
            getattr(job, "company_description", ""),
            MAX_PREFERENCE_COMPANY_DESCRIPTION_CHARS,
        ),
        skills=_normalize_skills(getattr(job, "skills_raw", "")),
        requirements=_normalize_job_text_list(getattr(job, "requirements", []) or []),
        benefits=_normalize_job_text_list(getattr(job, "benefits", []) or []),
    )


def _payload_char_budget(max_input_tokens: int) -> int:
    return max(MIN_PREFERENCE_PAYLOAD_CHARS, int(max_input_tokens) * APPROX_CHARS_PER_TOKEN)


def _truncate_preference_profile(
    profile: PreferenceProfile,
    *,
    max_input_tokens: int,
) -> PreferenceProfile:
    budget_chars = _payload_char_budget(max_input_tokens)
    label_budget = min(120, max(48, int(budget_chars * 0.03)))
    raw_text_budget = min(800, max(180, int(budget_chars * 0.18)))

    profile_data = profile.model_dump(mode="json")
    profile_data["raw_text"] = _truncate_text(profile_data.get("raw_text", ""), raw_text_budget)

    for field_name in (
        "work_style",
        "team_culture",
        "tech_stack",
        "mission_domain",
        "growth_preferences",
        "negative_preferences",
    ):
        items = []
        for item in list(profile_data.get(field_name, []) or [])[:6]:
            mutated = dict(item)
            mutated["label"] = _truncate_text(mutated.get("label", ""), label_budget)
            items.append(mutated)
        profile_data[field_name] = items

    truncated = PreferenceProfile.model_validate(profile_data)
    if _score_payload_char_size(truncated, [], scorer_name="profile_only") > budget_chars:
        raise ValueError("Preference profile exceeds configured max_input_tokens")
    return truncated


def _truncate_job_payload(
    payload: PreferenceJobPayload,
    *,
    max_input_tokens: int,
) -> PreferenceJobPayload:
    budget_chars = _payload_char_budget(max_input_tokens)
    summary_budget = min(MAX_PREFERENCE_SUMMARY_CHARS, max(500, int(budget_chars * 0.35)))
    company_budget = min(
        MAX_PREFERENCE_COMPANY_DESCRIPTION_CHARS,
        max(240, int(budget_chars * 0.18)),
    )
    list_item_budget = min(MAX_PREFERENCE_LIST_ITEM_CHARS, max(120, int(budget_chars * 0.08)))

    return payload.model_copy(
        update={
            "summary": _truncate_text(payload.summary, summary_budget),
            "company_description": _truncate_text(payload.company_description, company_budget),
            "skills": _truncate_text_list(
                payload.skills,
                max_chars=80,
                max_items=MAX_PREFERENCE_LIST_ITEMS,
            ),
            "requirements": _truncate_text_list(
                payload.requirements,
                max_chars=list_item_budget,
                max_items=MAX_PREFERENCE_LIST_ITEMS,
            ),
            "benefits": _truncate_text_list(
                payload.benefits,
                max_chars=list_item_budget,
                max_items=MAX_PREFERENCE_LIST_ITEMS,
            ),
        }
    )


def _fit_single_job_payload_to_budget(
    profile: PreferenceProfile,
    payload: PreferenceJobPayload,
    *,
    scorer_name: str,
    budget_chars: int,
) -> PreferenceJobPayload:
    candidate = payload
    if _score_payload_char_size(profile, [candidate], scorer_name=scorer_name) <= budget_chars:
        return candidate

    shrinking_specs = (
        {"summary": 800, "company_description": 400, "list_chars": 160, "list_items": 6, "skill_chars": 60, "skill_items": 8},
        {"summary": 480, "company_description": 220, "list_chars": 120, "list_items": 4, "skill_chars": 48, "skill_items": 6},
        {"title": 80, "company": 80, "location_text": 80, "summary": 280, "company_description": 120, "list_chars": 80, "list_items": 2, "skill_chars": 32, "skill_items": 4},
        {"title": 64, "company": 64, "location_text": 64, "summary": 160, "company_description": 0, "list_chars": 0, "list_items": 0, "skill_chars": 24, "skill_items": 2},
    )

    for spec in shrinking_specs:
        update: Dict[str, Any] = {
            "summary": _truncate_text(candidate.summary, spec["summary"]),
            "company_description": (
                ""
                if spec["company_description"] <= 0
                else _truncate_text(candidate.company_description, spec["company_description"])
            ),
            "requirements": _truncate_text_list(
                candidate.requirements,
                max_chars=spec["list_chars"],
                max_items=spec["list_items"],
            ),
            "benefits": _truncate_text_list(
                candidate.benefits,
                max_chars=spec["list_chars"],
                max_items=spec["list_items"],
            ),
            "skills": _truncate_text_list(
                candidate.skills,
                max_chars=spec["skill_chars"],
                max_items=spec["skill_items"],
            ),
        }
        if "title" in spec:
            update["title"] = _truncate_text(candidate.title, spec["title"])
            update["company"] = _truncate_text(candidate.company, spec["company"])
            update["location_text"] = _truncate_text(
                candidate.location_text,
                spec["location_text"],
            )

        candidate = candidate.model_copy(update=update)
        if _score_payload_char_size(profile, [candidate], scorer_name=scorer_name) <= budget_chars:
            return candidate

    raise ValueError("Preference job payload exceeds configured max_input_tokens")


def _score_payload_char_size(
    profile: PreferenceProfile,
    jobs: List[PreferenceJobPayload],
    *,
    scorer_name: str,
) -> int:
    payload = {
        "profile": profile.model_dump(mode="json"),
        "jobs": [job.model_dump(mode="json") for job in jobs],
        "mode": scorer_name,
    }
    return len(json.dumps(payload, separators=(",", ":")))


def _chunk_jobs_for_budget(
    profile: PreferenceProfile,
    jobs: List[PreferenceJobPayload],
    *,
    scorer_name: str,
    max_input_tokens: int,
) -> List[List[PreferenceJobPayload]]:
    if not jobs:
        return []

    budget_chars = _payload_char_budget(max_input_tokens)
    base_size = _score_payload_char_size(profile, [], scorer_name=scorer_name)
    chunks: List[List[PreferenceJobPayload]] = []
    current: List[PreferenceJobPayload] = []

    for job in jobs:
        truncated_job = _truncate_job_payload(job, max_input_tokens=max_input_tokens)
        if _score_payload_char_size(profile, [truncated_job], scorer_name=scorer_name) > budget_chars:
            truncated_job = _fit_single_job_payload_to_budget(
                profile,
                truncated_job,
                scorer_name=scorer_name,
                budget_chars=budget_chars,
            )
        next_size = _score_payload_char_size(
            profile,
            current + [truncated_job],
            scorer_name=scorer_name,
        )
        if current and next_size > budget_chars:
            chunks.append(current)
            current = [truncated_job]
            continue
        current.append(truncated_job)

    if current:
        chunks.append(current)
    return chunks


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
        max_input_tokens: int,
    ):
        self.llm = llm
        self.schema_spec = schema_spec
        self.system_prompt = system_prompt
        self.scorer_name = scorer_name
        self.max_input_tokens = max_input_tokens

    def _score(
        self,
        profile: PreferenceProfile,
        jobs: List[PreferenceJobPayload],
    ) -> List[PreferenceAssessment]:
        if not jobs:
            return []

        prepared_profile = _truncate_preference_profile(
            profile,
            max_input_tokens=self.max_input_tokens,
        )
        results: List[PreferenceAssessment] = []
        for chunk in _chunk_jobs_for_budget(
            prepared_profile,
            jobs,
            scorer_name=self.scorer_name,
            max_input_tokens=self.max_input_tokens,
        ):
            payload = {
                "profile": prepared_profile.model_dump(mode="json"),
                "jobs": [job.model_dump(mode="json") for job in chunk],
                "mode": self.scorer_name,
            }
            data = self.llm.extract_structured_data(
                json.dumps(payload),
                self.schema_spec,
                system_prompt=self.system_prompt,
                user_message="Score these fit-qualified jobs against the candidate's soft preferences.",
            )
            if not isinstance(data, dict):
                continue
            response = PreferenceRerankResponse.model_validate(data)
            results.extend(response.results)
        return results


class LLMPreferenceSemanticReranker(_BaseLLMPreferenceScorer, PreferenceSemanticReranker):
    def __init__(self, llm: LLMProvider, *, max_input_tokens: int):
        super().__init__(
            llm,
            schema_spec=PREFERENCE_SEMANTIC_RERANK_SCHEMA,
            system_prompt=PREFERENCE_RERANK_SYSTEM_PROMPT,
            scorer_name="semantic_rerank",
            max_input_tokens=max_input_tokens,
        )

    def rerank(
        self,
        profile: PreferenceProfile,
        jobs: List[PreferenceJobPayload],
    ) -> List[PreferenceAssessment]:
        return self._score(profile, jobs)


class LLMPreferenceJudge(_BaseLLMPreferenceScorer, PreferenceJudge):
    def __init__(self, llm: LLMProvider, *, max_input_tokens: int):
        super().__init__(
            llm,
            schema_spec=PREFERENCE_LLM_JUDGE_SCHEMA,
            system_prompt=PREFERENCE_JUDGE_SYSTEM_PROMPT,
            scorer_name="llm_judge",
            max_input_tokens=max_input_tokens,
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

    if not config.model:
        logger.info("Preference model disabled because no model is configured")
        return None

    return build_llm_provider(runtime_llm_config_from_preference(config))


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
    return LLMPreferenceSemanticReranker(llm, max_input_tokens=config.max_input_tokens)


def build_preference_judge(config: PreferenceModelConfig) -> Optional[PreferenceJudge]:
    llm = build_preference_llm(config)
    if llm is None:
        return None
    return LLMPreferenceJudge(llm, max_input_tokens=config.max_input_tokens)


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
