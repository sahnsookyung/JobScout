"""Candidate preference settings service for the web application."""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List

from sqlalchemy.orm import Session

from services.scorer_matcher.preference_semantics import (
    build_preference_parser,
    summarize_preference_profile,
)
from database.repository import JobRepository
from web.backend.config import get_config

logger = logging.getLogger(__name__)

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
        preferences.preference_rerank_top_n = self._resolve_requested_top_n(
            payload.get("preference_rerank_top_n")
        )
        # Parse eagerly so the profile is persisted alongside preferences. This is an
        # intentional sync LLM call: preferences are set infrequently and the parsed
        # profile is needed at match time. Failures are swallowed — the scorer-matcher
        # will re-parse on demand if preference_profile is None.
        profile = self._parse_preference_profile(preferences.soft_preferences)
        preferences.preference_profile = profile.model_dump(mode="json") if profile else None
        preferences.soft_preference_summary = (
            summarize_preference_profile(profile, preferences.soft_preferences)
            if preferences.soft_preferences
            else None
        )
        preferences.revision = int(preferences.revision or 0) + 1

        self.db.commit()
        self.db.refresh(preferences)
        return self._to_response(preferences)

    def _to_response(self, preferences) -> Dict[str, Any]:
        allowed_modes = self._allowed_modes()
        stored_mode = getattr(preferences, "preference_mode", None) or self.config.preferences.default_mode
        effective_mode = self._resolve_effective_mode(stored_mode, allowed_modes)
        bounds = self._top_n_bounds()
        stored_top_n = getattr(preferences, "preference_rerank_top_n", None)
        return {
            "remote_mode": preferences.remote_mode,
            "target_locations": list(preferences.target_locations or []),
            "visa_sponsorship_required": bool(preferences.visa_sponsorship_required),
            "salary_min": preferences.salary_min,
            "employment_types": list(preferences.employment_types or []),
            "soft_preferences": preferences.soft_preferences or "",
            "soft_preference_summary": getattr(preferences, "soft_preference_summary", None),
            "preference_mode": stored_mode,
            "preference_rerank_top_n": stored_top_n,
            "effective_preference_rerank_top_n": self._resolve_top_n(stored_top_n, bounds),
            "preference_rerank_top_n_bounds": bounds,
            "allowed_preference_modes": allowed_modes,
            "effective_preference_mode": effective_mode,
            "revision": int(preferences.revision or 0),
        }

    def _allowed_modes(self) -> List[str]:
        return self.config.preferences.allowed_modes_normalized()

    def _resolve_effective_mode(self, requested_mode: Any, allowed_modes: List[str]) -> str:
        normalized = str(requested_mode or self.config.preferences.default_mode).strip().lower()
        if normalized not in VALID_PREFERENCE_MODES:
            normalized = self.config.preferences.default_mode
        if normalized in allowed_modes:
            return normalized
        if self.config.preferences.default_mode in allowed_modes:
            logger.warning(
                "Requested mode %r not allowed; falling back to configured default %r",
                normalized,
                self.config.preferences.default_mode,
            )
            return self.config.preferences.default_mode
        logger.warning(
            "Neither requested mode %r nor default_mode %r is in allowed_modes %r; "
            "using first allowed mode %r. Check preferences.allowed_modes config.",
            normalized,
            self.config.preferences.default_mode,
            allowed_modes,
            allowed_modes[0],
        )
        return allowed_modes[0]

    def _resolve_requested_mode(self, requested_mode: Any) -> str:
        allowed_modes = self._allowed_modes()
        return self._resolve_effective_mode(requested_mode, allowed_modes)

    def _top_n_bounds(self) -> Dict[str, int]:
        resolver = getattr(self.config.preferences, "preference_rerank_top_n_bounds", None)
        if callable(resolver):
            return resolver()
        semantic_reranker = getattr(self.config.preferences, "semantic_reranker", None)
        min_value = max(1, int(getattr(semantic_reranker, "top_n_min", 1) or 1))
        max_value = max(min_value, int(getattr(semantic_reranker, "top_n_max", 100) or 100))
        default_value = int(getattr(semantic_reranker, "top_n_default", 25) or 25)
        default_value = max(min_value, min(max_value, default_value))
        return {"min": min_value, "max": max_value, "default": default_value}

    def _resolve_top_n(self, requested_top_n: Any, bounds: Dict[str, int]) -> int:
        try:
            value = int(requested_top_n) if requested_top_n is not None else bounds["default"]
        except (TypeError, ValueError):
            value = bounds["default"]
        return max(bounds["min"], min(bounds["max"], value))

    def _resolve_requested_top_n(self, requested_top_n: Any) -> int | None:
        if requested_top_n is None:
            return None
        return self._resolve_top_n(requested_top_n, self._top_n_bounds())

    def _parse_preference_profile(self, raw_text: str):
        if not raw_text.strip():
            return None

        matching = getattr(self.config, "matching", None)
        llm_judge = getattr(matching, "llm_judge", None)
        parser = build_preference_parser(
            self.config.preferences.parser,
            provider_route=getattr(llm_judge, "runtime", None),
        )
        if parser is None:
            return None
        try:
            return parser.parse(raw_text)
        except Exception:
            logger.warning("Preference parsing failed during preference update", exc_info=True)
            return None
