"""Helpers for applying candidate preferences during matching."""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

SOFT_PREFERENCE_TOKEN_PATTERN = re.compile(r"[a-z0-9]+")
SOFT_PREFERENCE_STOPWORDS = {
    "about",
    "after",
    "among",
    "and",
    "are",
    "for",
    "from",
    "have",
    "into",
    "next",
    "role",
    "that",
    "the",
    "their",
    "them",
    "then",
    "they",
    "this",
    "want",
    "with",
    "your",
}
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
        "preference_mode": getattr(preferences, "preference_mode", "semantic_rerank") or "semantic_rerank",
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


def _tokenize_soft_preferences(text: str) -> set[str]:
    """Tokenize soft preference text into a small, stable signal vocabulary."""
    return {
        token
        for token in SOFT_PREFERENCE_TOKEN_PATTERN.findall(_normalize_text(text))
        if len(token) >= 4 and token not in SOFT_PREFERENCE_STOPWORDS
    }


def _job_preference_tokens(job) -> set[str]:
    """Collect lexical signals from the job record for soft-preference reranking."""
    raw_payload = getattr(job, "raw_payload", {}) or {}
    ai_summary = raw_payload.get("ai_job_summary") if isinstance(raw_payload, dict) else ""
    canonical_summary = getattr(job, "canonical_job_summary", None)
    job_text = " ".join(
        filter(
            None,
            [
                getattr(job, "title", None),
                getattr(job, "company", None),
                getattr(job, "description", None),
                getattr(job, "company_description", None),
                getattr(job, "skills_raw", None),
                getattr(job, "job_type", None),
                getattr(job, "location_text", None),
                getattr(job, "work_from_home_type", None),
                canonical_summary,
                ai_summary,
            ],
        )
    )
    return _tokenize_soft_preferences(job_text)


def apply_soft_preference_reranking(scored_matches, preferences: Optional[Dict[str, Any]]):
    """Apply a bounded lexical rerank so soft preferences can influence close calls."""
    if not preferences:
        return scored_matches

    preference_tokens = _tokenize_soft_preferences(preferences["soft_preferences"])
    if not preference_tokens:
        return scored_matches

    for match in scored_matches:
        overlap = preference_tokens & _job_preference_tokens(match.job)
        if not overlap:
            continue

        bonus = round(min(5.0, 5.0 * (len(overlap) / len(preference_tokens))), 2)
        match.overall_score = min(100.0, round(match.overall_score + bonus, 2))

        fit_components = dict(match.fit_components or {})
        fit_components["soft_preference_bonus"] = bonus
        fit_components["soft_preference_overlap"] = sorted(overlap)[:8]
        match.fit_components = fit_components

    scored_matches.sort(key=lambda match: match.overall_score, reverse=True)
    return scored_matches
