"""Helpers for applying candidate preferences during matching."""

from __future__ import annotations

import logging
import math
import re
from typing import Any, Dict, List, Optional

from core.config_loader import PreferencesConfig
from core.preference_semantics import (
    PreferenceAssessment,
    PreferenceProfile,
    build_preference_judge,
    build_preference_parser,
    build_preference_semantic_reranker,
    serialize_job_for_preference,
)

logger = logging.getLogger(__name__)

POSITIVE_VISA_PATTERNS = (
    re.compile(r"\bvisa sponsorship\b"),
    re.compile(r"\bsponsor(?:ship|ing)?\b"),
    re.compile(r"\bwork authorization support\b"),
    re.compile(r"\brelocation assistance\b"),
)
NEGATIVE_VISA_PATTERNS = (
    re.compile(r"\bno visa sponsorship\b"),
    re.compile(r"\bunable to sponsor\b"),
    re.compile(r"\bwithout sponsorship\b"),
    re.compile(r"\bmust be authorized to work\b"),
)


def _normalize_text(value: Any) -> str:
    """Collapse arbitrary values into a lowercase comparable string."""
    if value is None:
        return ""
    return " ".join(str(value).strip().lower().split())


def _fit_band(match: Any) -> int:
    return int(math.floor(float(getattr(match, "fit_score", 0.0) or 0.0) / 5.0))


def _preference_sort_key(match: Any) -> tuple[float, float, float, float, str]:
    fit_components = dict(getattr(match, "fit_components", {}) or {})
    return (
        -float(_fit_band(match)),
        -float(fit_components.get("preference_score", 0.0) or 0.0),
        -float(getattr(match, "fit_score", 0.0) or 0.0),
        -float(getattr(match, "job_similarity", 0.0) or 0.0),
        str(getattr(getattr(match, "job", None), "id", "")),
    )


def _bounded_preference_overall_score(match: Any, preference_score: float) -> float:
    fit_score = float(getattr(match, "fit_score", 0.0) or 0.0)
    band_floor = math.floor(fit_score / 5.0) * 5.0
    fit_within_band = max(0.0, fit_score - band_floor)
    similarity = float(getattr(match, "job_similarity", 0.0) or 0.0)
    within_band = min(
        4.99,
        (float(preference_score) * 4.0)
        + ((fit_within_band / 5.0) * 0.9)
        + (similarity * 0.09),
    )
    return round(min(100.0, band_floor + within_band), 2)


def load_candidate_preferences(repo, owner_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """Load the current user's candidate preferences without creating defaults."""
    if not owner_id:
        return None

    preferences = repo.candidate_preferences.get_preferences(owner_id)
    if preferences is None:
        return None

    return {
        "remote_mode": _normalize_text(getattr(preferences, "remote_mode", "")) or "any",
        "target_locations": list(getattr(preferences, "target_locations", []) or []),
        "visa_sponsorship_required": bool(
            getattr(preferences, "visa_sponsorship_required", False)
        ),
        "salary_min": getattr(preferences, "salary_min", None),
        "employment_types": list(getattr(preferences, "employment_types", []) or []),
        "soft_preferences": getattr(preferences, "soft_preferences", "") or "",
        "soft_preference_summary": getattr(preferences, "soft_preference_summary", None),
        "preference_mode": getattr(preferences, "preference_mode", "semantic_rerank")
        or "semantic_rerank",
        "preference_profile": getattr(preferences, "preference_profile", None),
        "revision": int(getattr(preferences, "revision", 0) or 0),
    }


def _job_work_mode(job) -> str:
    """Infer the job's work arrangement from structured metadata."""
    work_from_home_type = _normalize_text(getattr(job, "work_from_home_type", ""))
    location_text = _normalize_text(getattr(job, "location_text", ""))

    if getattr(job, "is_remote", None) is True or "remote" in work_from_home_type:
        return "remote"
    if "hybrid" in work_from_home_type or "hybrid" in location_text:
        return "hybrid"
    return "onsite"


def _job_matches_remote_mode(job, remote_mode: str) -> bool:
    """Return whether the job satisfies the requested work arrangement."""
    if remote_mode == "any":
        return True

    job_mode = _job_work_mode(job)
    if remote_mode == "remote":
        return job_mode == "remote"
    if remote_mode == "hybrid":
        return job_mode in {"remote", "hybrid"}
    if remote_mode == "onsite":
        return job_mode in {"hybrid", "onsite"}
    return True


def _job_matches_locations(job, target_locations: List[str]) -> bool:
    """Match target locations using a simple normalized substring comparison."""
    if not target_locations:
        return True

    normalized_targets = [_normalize_text(location) for location in target_locations]
    location_text = _normalize_text(getattr(job, "location_text", ""))
    if not location_text:
        return bool(getattr(job, "is_remote", None) is True) and any(
            "remote" in location for location in normalized_targets
        )

    return any(
        target and (target in location_text or location_text in target)
        for target in normalized_targets
    )


def _job_meets_salary_floor(job, salary_min: Optional[int]) -> bool:
    """Treat known salary mismatches as hard rejects while keeping unknowns eligible."""
    if salary_min is None:
        return True

    known_salaries = [
        value
        for value in (getattr(job, "salary_max", None), getattr(job, "salary_min", None))
        if value is not None
    ]
    if not known_salaries:
        return True

    try:
        requested_floor = float(salary_min)
    except (TypeError, ValueError):
        return True

    return any(float(value) >= requested_floor for value in known_salaries)


def _job_matches_employment_types(job, employment_types: List[str]) -> bool:
    """Match job type against requested employment types when the metadata is known."""
    if not employment_types:
        return True

    job_type = _normalize_text(getattr(job, "job_type", ""))
    if not job_type:
        return True

    normalized_types = [_normalize_text(value) for value in employment_types]
    return any(
        employment_type and (employment_type in job_type or job_type in employment_type)
        for employment_type in normalized_types
    )


def _job_supports_visa(job) -> bool:
    """Use structured metadata first, then conservative text hints, for visa support."""
    raw_payload = getattr(job, "raw_payload", {}) or {}
    if isinstance(raw_payload, dict):
        direct_flag = raw_payload.get("visa_sponsorship_available")
        if isinstance(direct_flag, bool):
            return direct_flag
        payload_text = raw_payload.get("visa_sponsorship", "")
    else:
        payload_text = ""

    haystack = _normalize_text(
        " ".join(
            filter(
                None,
                [
                    payload_text,
                    getattr(job, "description", None),
                    getattr(job, "company_description", None),
                ],
            )
        )
    )
    if not haystack:
        return False
    if any(pattern.search(haystack) for pattern in NEGATIVE_VISA_PATTERNS):
        return False
    return any(pattern.search(haystack) for pattern in POSITIVE_VISA_PATTERNS)


def _matches_candidate_preferences(preliminary, preferences: Dict[str, Any]) -> bool:
    """Return whether a preliminary match passes all configured hard filters."""
    job = preliminary.job

    if not _job_matches_remote_mode(job, preferences["remote_mode"]):
        return False
    if not _job_matches_locations(job, preferences["target_locations"]):
        return False
    if preferences["visa_sponsorship_required"] and not _job_supports_visa(job):
        return False
    if not _job_meets_salary_floor(job, preferences["salary_min"]):
        return False
    if not _job_matches_employment_types(job, preferences["employment_types"]):
        return False
    return True


def apply_candidate_preference_filters(preliminary_matches, preferences: Optional[Dict[str, Any]]):
    """Apply candidate hard filters before the scoring stage."""
    if not preferences:
        return preliminary_matches

    filtered_matches = [
        preliminary
        for preliminary in preliminary_matches
        if _matches_candidate_preferences(preliminary, preferences)
    ]
    logger.info(
        "Candidate preference filters kept %d/%d preliminary matches",
        len(filtered_matches),
        len(preliminary_matches),
    )
    return filtered_matches


def _stored_preference_profile(preferences: Dict[str, Any]) -> Optional[PreferenceProfile]:
    raw_profile = preferences.get("preference_profile")
    if not raw_profile:
        return None
    try:
        return PreferenceProfile.model_validate(raw_profile)
    except Exception:
        logger.warning("Ignoring invalid stored preference profile", exc_info=True)
        return None


def _resolve_preference_profile(
    preferences: Dict[str, Any],
    config: PreferencesConfig,
) -> Optional[PreferenceProfile]:
    stored = _stored_preference_profile(preferences)
    if stored is not None:
        return stored

    raw_text = str(preferences.get("soft_preferences") or "").strip()
    if not raw_text:
        return None

    parser = build_preference_parser(config.parser)
    if parser is None:
        return None
    try:
        return parser.parse(raw_text)
    except Exception:
        logger.warning("Preference parsing failed during matching", exc_info=True)
        return None


def _allowed_preference_modes(config: PreferencesConfig) -> List[str]:
    normalized = [
        mode
        for mode in (
            str(item).strip().lower()
            for item in (getattr(config, "allowed_modes", None) or [])
        )
        if mode in {"semantic_rerank", "llm_judge"}
    ]
    if normalized:
        return list(dict.fromkeys(normalized))
    return [getattr(config, "default_mode", "semantic_rerank")]


def _resolve_requested_mode(
    requested_mode: Any,
    config: PreferencesConfig,
) -> tuple[str, str]:
    requested = str(requested_mode or config.default_mode).strip().lower()
    if requested not in {"semantic_rerank", "llm_judge"}:
        requested = config.default_mode

    allowed_modes = _allowed_preference_modes(config)
    effective = requested if requested in allowed_modes else config.default_mode
    if effective not in allowed_modes:
        effective = allowed_modes[0]
    return requested, effective


def _fit_only_fallback(
    scored_matches,
    *,
    requested_mode: str,
    effective_mode: str,
    reason: str,
):
    for match in scored_matches:
        fit_components = dict(getattr(match, "fit_components", {}) or {})
        fit_components.update(
            {
                "preference_score": 0.0,
                "preference_confidence": 0.0,
                "preference_reason_codes": ["fallback_fit_only"],
                "preference_explanation": "Preference reranking unavailable for this run.",
                "preference_mode_requested": requested_mode,
                "preference_mode_used": effective_mode,
                "preference_fallback_reason": reason,
            }
        )
        match.fit_components = fit_components
    return scored_matches


def _assessments_by_job_id(
    assessments: List[PreferenceAssessment],
) -> Dict[str, PreferenceAssessment]:
    return {assessment.job_id: assessment for assessment in assessments}


def _apply_assessments(
    scored_matches,
    assessments: List[PreferenceAssessment],
    *,
    requested_mode: str,
    effective_mode: str,
):
    by_job_id = _assessments_by_job_id(assessments)
    for match in scored_matches:
        assessment = by_job_id.get(str(getattr(match.job, "id")))
        fit_components = dict(getattr(match, "fit_components", {}) or {})
        if assessment is None:
            preference_score = 0.0
            preference_confidence = 0.0
            reason_codes = ["no_preference_signal"]
            explanation = "The job description did not strongly reflect the saved soft preferences."
        else:
            preference_score = float(assessment.preference_score)
            preference_confidence = float(assessment.preference_confidence)
            reason_codes = list(assessment.preference_reason_codes or [])
            explanation = assessment.preference_explanation

        fit_components.update(
            {
                "preference_score": preference_score,
                "preference_confidence": preference_confidence,
                "preference_reason_codes": reason_codes,
                "preference_explanation": explanation,
                "preference_mode_requested": requested_mode,
                "preference_mode_used": effective_mode,
            }
        )
        match.fit_components = fit_components
        match.overall_score = _bounded_preference_overall_score(match, preference_score)

    scored_matches.sort(key=_preference_sort_key)
    return scored_matches


def apply_preference_semantic_reranking(
    scored_matches,
    preferences: Optional[Dict[str, Any]],
    *,
    config: PreferencesConfig,
):
    """Apply semantic preference reranking after fit-qualified scoring."""
    if not preferences:
        return scored_matches

    soft_preferences = str(preferences.get("soft_preferences") or "").strip()
    if not soft_preferences:
        return scored_matches

    requested_mode, effective_mode = _resolve_requested_mode(
        preferences.get("preference_mode"),
        config,
    )
    profile = _resolve_preference_profile(preferences, config)
    if profile is None:
        return _fit_only_fallback(
            scored_matches,
            requested_mode=requested_mode,
            effective_mode=effective_mode,
            reason="preference_profile_unavailable",
        )

    job_payloads = [serialize_job_for_preference(match.job) for match in scored_matches]

    try:
        if effective_mode == "llm_judge":
            judge = build_preference_judge(config.llm_judge)
            if judge is None:
                return _fit_only_fallback(
                    scored_matches,
                    requested_mode=requested_mode,
                    effective_mode=effective_mode,
                    reason="preference_judge_unavailable",
                )
            assessments = judge.judge(profile, job_payloads)
        else:
            reranker = build_preference_semantic_reranker(config.semantic_reranker)
            if reranker is None:
                return _fit_only_fallback(
                    scored_matches,
                    requested_mode=requested_mode,
                    effective_mode=effective_mode,
                    reason="preference_reranker_unavailable",
                )
            assessments = reranker.rerank(profile, job_payloads)
    except Exception:
        logger.warning("Preference reranking failed; degrading to fit-only ordering", exc_info=True)
        return _fit_only_fallback(
            scored_matches,
            requested_mode=requested_mode,
            effective_mode=effective_mode,
            reason="preference_reranking_failed",
        )

    return _apply_assessments(
        scored_matches,
        assessments,
        requested_mode=requested_mode,
        effective_mode=effective_mode,
    )
