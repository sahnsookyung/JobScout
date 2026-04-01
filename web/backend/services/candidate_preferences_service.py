"""Candidate preference settings service for the web application."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

from sqlalchemy.orm import Session

from database.repository import JobRepository
from services.scorer_matcher.preference_semantics import (
    LLMPreferenceParser,
    build_preference_llm,
    summarize_preference_profile,
)
from web.backend.config import get_config

VALID_REMOTE_MODES = {"any", "remote", "hybrid", "onsite"}
VALID_PREFERENCE_MODES = {"semantic_rerank", "llm_judge"}


def _normalize_string_list(values: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    seen: set[str] = set()
    for value in values:
        item = value.strip()
        if not item:
            continue
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(item)
    return normalized


class CandidatePreferencesService:
    """Resolve and persist per-user candidate preferences."""

    def __init__(self, db: Session):
        self.db = db
        self.repo = JobRepository(db)
        self.config = get_config()

    def get_preferences(self, user) -> Dict[str, Any]:
        preferences = self.repo.candidate_preferences.get_or_create_preferences(user.id)
        self.db.refresh(preferences)
        return self._to_response(preferences)

    def update_preferences(self, user, payload: Dict[str, Any]) -> Dict[str, Any]:
        preferences = self.repo.candidate_preferences.get_or_create_preferences(user.id)

        remote_mode = payload["remote_mode"].lower()
        if remote_mode not in VALID_REMOTE_MODES:
            remote_mode = "any"

        preferences.remote_mode = remote_mode
        preferences.target_locations = _normalize_string_list(payload.get("target_locations", []))
        preferences.visa_sponsorship_required = bool(payload.get("visa_sponsorship_required", False))
        preferences.salary_min = payload.get("salary_min")
        preferences.employment_types = _normalize_string_list(payload.get("employment_types", []))
        preferences.soft_preferences = payload.get("soft_preferences", "").strip()
        preferences.preference_mode = self._resolve_requested_mode(payload.get("preference_mode"))
        profile = self._parse_soft_preferences(preferences.soft_preferences)
        preferences.preference_profile = profile.model_dump() if profile is not None else None
        preferences.soft_preference_summary = summarize_preference_profile(
            profile,
            preferences.soft_preferences,
        ) if preferences.soft_preferences else None
        preferences.revision = int(preferences.revision or 0) + 1

        self.db.commit()
        self.db.refresh(preferences)
        return self._to_response(preferences)

    def _to_response(self, preferences) -> Dict[str, Any]:
        allowed_modes = self._allowed_modes()
        stored_mode = getattr(preferences, "preference_mode", None) or self.config.preferences.default_mode
        effective_mode = stored_mode if stored_mode in allowed_modes else self.config.preferences.default_mode
        return {
            "remote_mode": preferences.remote_mode,
            "target_locations": list(preferences.target_locations or []),
            "visa_sponsorship_required": bool(preferences.visa_sponsorship_required),
            "salary_min": preferences.salary_min,
            "employment_types": list(preferences.employment_types or []),
            "soft_preferences": preferences.soft_preferences or "",
            "soft_preference_summary": getattr(preferences, "soft_preference_summary", None),
            "preference_mode": stored_mode,
            "allowed_preference_modes": allowed_modes,
            "effective_preference_mode": effective_mode,
            "revision": int(preferences.revision or 0),
        }

    def _allowed_modes(self) -> List[str]:
        configured = list(self.config.preferences.allowed_modes or [])
        normalized = [mode for mode in configured if mode in VALID_PREFERENCE_MODES]
        if not normalized:
            return [self.config.preferences.default_mode]
        return normalized

    def _resolve_requested_mode(self, requested_mode: Any) -> str:
        normalized = str(requested_mode or self.config.preferences.default_mode).strip().lower()
        if normalized not in VALID_PREFERENCE_MODES:
            normalized = self.config.preferences.default_mode

        allowed_modes = self._allowed_modes()
        if normalized not in allowed_modes:
            return self.config.preferences.default_mode
        return normalized

    def _parse_soft_preferences(self, text: str):
        if not text.strip():
            return None

        parser_config = self.config.preferences.parser
        llm = build_preference_llm(parser_config)
        if llm is None:
            return None

        try:
            parser = LLMPreferenceParser(llm)
            return parser.parse(text)
        except Exception:
            return None
