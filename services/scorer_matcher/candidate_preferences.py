"""Helpers for applying candidate preferences during matching."""

from __future__ import annotations

import logging
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from core.config_loader import LlmJudgeRuntimeConfig, PreferencesConfig
from core.llm.schema_models import JOB_OFFERINGS_PROFILE_VERSION
from core.metrics import record_preference_status
from services.scorer_matcher.preference_semantics import (
    PreferenceAssessment,
    PreferenceProfile,
    job_work_mode,
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

@dataclass(frozen=True)
class PreferenceStatus:
    """Run-level status for preference reranking."""

    applied: bool
    reason: Optional[str] = None
    requested_mode: Optional[str] = None
    effective_mode: Optional[str] = None
    effective_top_n: Optional[int] = None
    judged_count: Optional[int] = None
    eligible_count: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"applied": self.applied}
        if self.reason:
            payload["reason"] = self.reason
        if self.requested_mode:
            payload["requested_mode"] = self.requested_mode
        if self.effective_mode:
            payload["effective_mode"] = self.effective_mode
        if self.effective_top_n is not None:
            payload["effective_top_n"] = self.effective_top_n
        if self.judged_count is not None:
            payload["judged_count"] = self.judged_count
        if self.eligible_count is not None:
            payload["eligible_count"] = self.eligible_count
        return payload

@dataclass(frozen=True)
class PreferenceRerankResult:
    """Backward-compatible preference rerank result."""

    matches: List[Any]
    status: PreferenceStatus

    def __iter__(self):
        return iter(self.matches)

    def __getitem__(self, index):
        return self.matches[index]

    def __len__(self) -> int:
        return len(self.matches)


@dataclass(frozen=True)
class OfferingsProfileLoadResult:
    profiles: Dict[str, Any]
    failed: bool = False


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
        "soft_preference_summary": getattr(preferences, "soft_preference_summary", None),
        "preference_mode": getattr(preferences, "preference_mode", "semantic_rerank")
        or "semantic_rerank",
        "preference_rerank_top_n": getattr(preferences, "preference_rerank_top_n", None),
        "preference_profile": getattr(preferences, "preference_profile", None),
        "revision": int(getattr(preferences, "revision", 0) or 0),
    }



def _job_matches_remote_mode(job, remote_mode: str) -> bool:
    """Return whether the job satisfies the requested work arrangement."""
    if remote_mode == "any":
        return True

    job_mode = job_work_mode(job)
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


def job_matches_candidate_preferences(job, preferences: Dict[str, Any]) -> bool:
    """Return whether a job passes all configured candidate hard filters."""
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


def _matches_candidate_preferences(preliminary, preferences: Dict[str, Any]) -> bool:
    """Return whether a preliminary match passes all configured hard filters."""
    return job_matches_candidate_preferences(preliminary.job, preferences)


def apply_candidate_preference_filters(preliminary_matches, preferences: Optional[Dict[str, Any]]):
    """Apply candidate hard filters before the scoring stage."""
    if not preferences:
        return preliminary_matches

    filtered_matches = [
        preliminary
        for preliminary in preliminary_matches
        if _matches_candidate_preferences(preliminary, preferences)
    ]
    return filtered_matches


def _stored_preference_profile(preferences: Dict[str, Any]) -> Optional[PreferenceProfile]:
    raw_profile = preferences.get("preference_profile")
    if not raw_profile:
        return None
    try:
        return PreferenceProfile.model_validate(raw_profile)
    except Exception:
        logger.info(
            "Stored preference profile failed validation and will be re-parsed from soft_preferences text. "
            "Re-saving preferences will update the stored profile.",
            exc_info=True,
        )
        return None


def _resolve_preference_profile(
    preferences: Dict[str, Any],
    config: PreferencesConfig,
    provider_route: Optional[LlmJudgeRuntimeConfig] = None,
) -> Optional[PreferenceProfile]:
    stored = _stored_preference_profile(preferences)
    if stored is not None:
        return stored

    raw_text = str(preferences.get("soft_preferences") or "").strip()
    if not raw_text:
        return None

    parser = build_preference_parser(
        config.parser,
        provider_route=provider_route,
    )
    if parser is None:
        return None
    try:
        return parser.parse(raw_text)
    except Exception:
        logger.warning("Preference parsing failed during matching", exc_info=True)
        return None


def _allowed_preference_modes(config: PreferencesConfig) -> List[str]:
    return config.allowed_modes_normalized()


_KNOWN_PREFERENCE_MODES: frozenset[str] = frozenset(
    {"semantic_rerank", "llm_judge", "fit_only", "default"}
)


def _safe_mode(value: str) -> str:
    """Return value only if it is a known preference mode, else 'other'.

    Breaks the taint chain from user-supplied config into log sinks so that
    static analyzers do not flag downstream logging as sensitive.
    """
    return value if value in _KNOWN_PREFERENCE_MODES else "other"


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
        logger.warning(
            "Preference default_mode '%s' is not in allowed_modes %s; falling back to '%s'",
            config.default_mode,
            allowed_modes,
            allowed_modes[0],
        )
        effective = allowed_modes[0]
    return requested, effective


def _fit_only_fallback(
    scored_matches,
    *,
    requested_mode: str,
    effective_mode: str,
    reason: str,
    effective_top_n: Optional[int] = None,
):
    public_reason = _public_preference_reason(reason)
    for match in scored_matches:
        preference_components = dict(getattr(match, "preference_components", {}) or {})
        preference_components.update(
            {
                "preference_reason_codes": [public_reason],
                "preference_explanation": "Preference reranking unavailable for this run.",
                "preference_mode_requested": requested_mode,
                "preference_mode_effective": effective_mode,
                "preference_mode_used": "fit_only_fallback",
                "preference_fallback_reason": public_reason,
                "preference_status": public_reason,
            }
        )
        if effective_top_n is not None:
            preference_components["preference_rerank_top_n"] = effective_top_n
        match.preference_components = preference_components
        match.preference_score = None  # NULL = evaluator did not run
    return PreferenceRerankResult(
        matches=scored_matches,
        status=PreferenceStatus(
            applied=False,
            reason=public_reason,
            requested_mode=requested_mode,
            effective_mode=effective_mode,
            effective_top_n=effective_top_n,
        ),
    )


def _assessments_by_job_id(
    assessments: List[PreferenceAssessment],
) -> Dict[str, PreferenceAssessment]:
    return {assessment.job_id: assessment for assessment in assessments}


def _public_preference_reason(reason: Optional[str]) -> str:
    if not reason:
        return "disabled"
    if reason in {
        "preference_profile_unavailable",
        "preference_reranker_unavailable",
        "preference_judge_unavailable",
    }:
        return "preference_scorer_unavailable"
    if reason == "job_offerings_lookup_failed":
        return "preference_scorer_failed"
    if reason in {"invalid_llm_output", "missing_preference_assessment"}:
        return "invalid_llm_output"
    if reason in {"missing_job_offerings", "job_offerings_unavailable"}:
        return "missing_job_offerings"
    if reason == "outside_preference_window":
        return "outside_preference_window"
    if reason.startswith("preference_reranking_failed") or reason.startswith("runtime_error:"):
        return "preference_scorer_failed"
    if reason == "disabled":
        return "disabled"
    return reason


def _profile_hash(profile: PreferenceProfile) -> str:
    data = json.dumps(profile.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()[:32]


def _resolve_effective_top_n(config: PreferencesConfig, preferences: Dict[str, Any]) -> int:
    requested = preferences.get("preference_rerank_top_n")
    resolver = getattr(config, "resolve_preference_rerank_top_n", None)
    if callable(resolver):
        return resolver(requested)
    try:
        return max(1, int(requested or 25))
    except (TypeError, ValueError):
        return 25


def _match_fit_sort_key(match: Any) -> tuple[float, float, str]:
    fit_score = float(getattr(match, "fit_score", 0.0) or 0.0)
    job_similarity = float(getattr(match, "job_similarity", 0.0) or 0.0)
    return (fit_score, job_similarity, str(getattr(getattr(match, "job", None), "id", "")))


def _top_n_window(scored_matches, top_n: int) -> List[Any]:
    return sorted(scored_matches, key=_match_fit_sort_key, reverse=True)[:top_n]


def _load_offerings_profiles(repo: Any, matches: List[Any]) -> OfferingsProfileLoadResult:
    if repo is None or not hasattr(repo, "get_job_offerings_profiles_by_job_ids"):
        return OfferingsProfileLoadResult({})
    job_ids = [
        getattr(match.job, "id")
        for match in matches
        if getattr(getattr(match, "job", None), "id", None) is not None
    ]
    try:
        return OfferingsProfileLoadResult(
            repo.get_job_offerings_profiles_by_job_ids(job_ids)
        )
    except Exception:
        logger.warning("Loading cached job offerings failed", exc_info=True)
        return OfferingsProfileLoadResult({}, failed=True)


def _job_description_hash(job: Any) -> Optional[str]:
    for attribute in ("description_hash", "content_hash"):
        value = getattr(job, attribute, None)
        if value:
            return str(value)
    return None


def _is_offerings_profile_fresh(repo: Any, profile: Any, job: Any) -> bool:
    if profile is None:
        return False

    source_hash = _job_description_hash(job)
    freshness_check = getattr(repo, "is_job_offerings_profile_fresh", None)
    if callable(freshness_check):
        try:
            return bool(
                freshness_check(
                    profile,
                    source_description_hash=source_hash,
                    profile_schema_version=JOB_OFFERINGS_PROFILE_VERSION,
                )
            )
        except Exception:
            logger.warning("Checking cached job offerings freshness failed", exc_info=True)
            return False

    return (
        str(getattr(profile, "source_description_hash", "") or "") == str(source_hash or "")
        and int(getattr(profile, "profile_schema_version", 0) or 0)
        == JOB_OFFERINGS_PROFILE_VERSION
    )


def _preference_metadata(
    *,
    preferences: Dict[str, Any],
    profile_hash: str,
    effective_top_n: int,
    offerings_profile: Any = None,
) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {
        "preference_revision": int(preferences.get("revision") or 0),
        "preference_profile_hash": profile_hash,
        "preference_rerank_top_n": effective_top_n,
    }
    if offerings_profile is not None:
        metadata["offerings_profile_schema_version"] = int(
            getattr(offerings_profile, "profile_schema_version", 0) or 0
        )
        metadata["offerings_source_description_hash"] = getattr(
            offerings_profile,
            "source_description_hash",
            None,
        )
    return metadata


def _mark_preference_skipped(
    match: Any,
    *,
    reason: str,
    explanation: str,
    requested_mode: str,
    effective_mode: str,
    metadata: Dict[str, Any],
) -> None:
    preference_components = dict(getattr(match, "preference_components", {}) or {})
    public_reason = _public_preference_reason(reason)
    preference_components.update(
        {
            "preference_confidence": None,
            "preference_reason_codes": [public_reason],
            "preference_explanation": explanation,
            "preference_mode_requested": requested_mode,
            "preference_mode_effective": effective_mode,
            "preference_mode_used": effective_mode,
            "preference_status": public_reason,
            **metadata,
        }
    )
    match.preference_components = preference_components
    match.preference_score = None


def _mark_outside_window(
    scored_matches,
    *,
    window_job_ids: Set[str],
    requested_mode: str,
    effective_mode: str,
    metadata: Dict[str, Any],
) -> None:
    for match in scored_matches:
        if str(getattr(match.job, "id")) in window_job_ids:
            continue
        _mark_preference_skipped(
            match,
            reason="outside_preference_window",
            explanation="Outside preference judging window.",
            requested_mode=requested_mode,
            effective_mode=effective_mode,
            metadata=metadata,
        )


def _apply_assessments(
    scored_matches,
    assessments: List[PreferenceAssessment],
    *,
    requested_mode: str,
    effective_mode: str,
    target_job_ids: Optional[Set[str]] = None,
    metadata_by_job_id: Optional[Dict[str, Dict[str, Any]]] = None,
    effective_top_n: Optional[int] = None,
    judged_count: Optional[int] = None,
    eligible_count: Optional[int] = None,
):
    by_job_id = _assessments_by_job_id(assessments)
    for match in scored_matches:
        job_id = str(getattr(match.job, "id"))
        if target_job_ids is not None and job_id not in target_job_ids:
            continue
        assessment = by_job_id.get(job_id)
        preference_components = dict(getattr(match, "preference_components", {}) or {})
        if assessment is None:
            preference_score = None
            preference_confidence = None
            reason_codes = ["invalid_llm_output"]
            explanation = "Preference scorer returned invalid output."
            status = "invalid_llm_output"
        else:
            preference_score = float(assessment.preference_score)
            preference_confidence = float(assessment.preference_confidence)
            reason_codes = list(assessment.preference_reason_codes or [])
            explanation = assessment.preference_explanation
            status = "applied"

        preference_components.update(
            {
                "preference_confidence": preference_confidence,
                "preference_reason_codes": reason_codes,
                "preference_explanation": explanation,
                "preference_mode_requested": requested_mode,
                "preference_mode_effective": effective_mode,
                "preference_mode_used": effective_mode,
                "preference_status": status,
                **((metadata_by_job_id or {}).get(job_id, {})),
            }
        )
        match.preference_components = preference_components
        match.preference_score = preference_score  # 0-100; None = not evaluated
    return PreferenceRerankResult(
        matches=scored_matches,
        status=PreferenceStatus(
            applied=True,
            requested_mode=requested_mode,
            effective_mode=effective_mode,
            effective_top_n=effective_top_n,
            judged_count=judged_count,
            eligible_count=eligible_count,
        ),
    )


def apply_preference_semantic_reranking(
    scored_matches,
    preferences: Optional[Dict[str, Any]],
    *,
    config: PreferencesConfig,
    repo: Any = None,
    provider_route: Optional[LlmJudgeRuntimeConfig] = None,
):
    """Apply semantic preference reranking after fit-qualified scoring."""
    result = _apply_preference_semantic_reranking(
        scored_matches,
        preferences,
        config=config,
        repo=repo,
        provider_route=provider_route,
    )
    record_preference_status(result.status.applied, result.status.reason)
    return result


def _apply_preference_semantic_reranking(
    scored_matches,
    preferences: Optional[Dict[str, Any]],
    *,
    config: PreferencesConfig,
    repo: Any = None,
    provider_route: Optional[LlmJudgeRuntimeConfig] = None,
):
    if not preferences:
        return PreferenceRerankResult(
            matches=scored_matches,
            status=PreferenceStatus(applied=False, reason="unconfigured"),
        )

    soft_preferences = str(preferences.get("soft_preferences") or "").strip()
    if not soft_preferences:
        return PreferenceRerankResult(
            matches=scored_matches,
            status=PreferenceStatus(applied=False, reason="disabled"),
        )

    requested_mode, effective_mode = _resolve_requested_mode(
        preferences.get("preference_mode"),
        config,
    )
    effective_top_n = _resolve_effective_top_n(config, preferences)
    profile = _resolve_preference_profile(
        preferences,
        config,
        provider_route,
    )
    if profile is None:
        return _fit_only_fallback(
            scored_matches,
            requested_mode=requested_mode,
            effective_mode=effective_mode,
            reason="preference_profile_unavailable",
            effective_top_n=effective_top_n,
        )

    profile_hash = _profile_hash(profile)
    base_metadata = _preference_metadata(
        preferences=preferences,
        profile_hash=profile_hash,
        effective_top_n=effective_top_n,
    )
    window_matches = _top_n_window(scored_matches, effective_top_n)
    window_job_ids = {str(getattr(match.job, "id")) for match in window_matches}
    _mark_outside_window(
        scored_matches,
        window_job_ids=window_job_ids,
        requested_mode=requested_mode,
        effective_mode=effective_mode,
        metadata=base_metadata,
    )

    offerings_load = _load_offerings_profiles(repo, window_matches)
    if offerings_load.failed:
        return _fit_only_fallback(
            scored_matches,
            requested_mode=requested_mode,
            effective_mode=effective_mode,
            reason="job_offerings_lookup_failed",
            effective_top_n=effective_top_n,
        )

    offerings_by_job_id = offerings_load.profiles
    require_cached_offerings = repo is not None
    job_payloads = []
    judged_job_ids: Set[str] = set()
    metadata_by_job_id: Dict[str, Dict[str, Any]] = {}

    for match in window_matches:
        job_id = str(getattr(match.job, "id"))
        offerings_profile = offerings_by_job_id.get(job_id)
        if require_cached_offerings and not _is_offerings_profile_fresh(
            repo,
            offerings_profile,
            match.job,
        ):
            _mark_preference_skipped(
                match,
                reason="missing_job_offerings",
                explanation="Waiting for job offering extraction.",
                requested_mode=requested_mode,
                effective_mode=effective_mode,
                metadata=base_metadata,
            )
            continue

        metadata = _preference_metadata(
            preferences=preferences,
            profile_hash=profile_hash,
            effective_top_n=effective_top_n,
            offerings_profile=offerings_profile,
        )
        metadata_by_job_id[job_id] = metadata
        job_payloads.append(
            serialize_job_for_preference(
                match.job,
                offerings_profile=(
                    getattr(offerings_profile, "profile_json", None)
                    if offerings_profile is not None
                    else None
                ),
                offerings_profile_schema_version=metadata.get(
                    "offerings_profile_schema_version"
                ),
                offerings_source_description_hash=metadata.get(
                    "offerings_source_description_hash"
                ),
            )
        )
        judged_job_ids.add(job_id)

    if window_matches and not job_payloads:
        return PreferenceRerankResult(
            matches=scored_matches,
            status=PreferenceStatus(
                applied=False,
                reason="job_offerings_unavailable",
                requested_mode=requested_mode,
                effective_mode=effective_mode,
                effective_top_n=effective_top_n,
                judged_count=0,
                eligible_count=len(window_matches),
            ),
        )

    try:
        if effective_mode == "llm_judge":
            judge = build_preference_judge(
                config.llm_judge,
                provider_route=provider_route,
            )
            if judge is None:
                return _fit_only_fallback(
                    scored_matches,
                    requested_mode=requested_mode,
                    effective_mode=effective_mode,
                    reason="preference_judge_unavailable",
                    effective_top_n=effective_top_n,
                )
            assessments = judge.judge(profile, job_payloads)
        else:
            reranker = build_preference_semantic_reranker(
                config,
                provider_route=provider_route,
            )
            if reranker is None:
                return _fit_only_fallback(
                    scored_matches,
                    requested_mode=requested_mode,
                    effective_mode=effective_mode,
                    reason="preference_reranker_unavailable",
                    effective_top_n=effective_top_n,
                )
            assessments = reranker.rerank(profile, job_payloads)
    except Exception as exc:  # noqa: BLE001 - we record the exception class in the reason
        reason = f"runtime_error:{type(exc).__name__}"
        logger.warning(
            "Preference reranking failed (%s); degrading to fit-only ordering",
            reason,
            exc_info=True,
        )
        return _fit_only_fallback(
            scored_matches,
            requested_mode=requested_mode,
            effective_mode=effective_mode,
            reason=reason,
            effective_top_n=effective_top_n,
        )

    if job_payloads and not any(
        assessment.job_id in judged_job_ids for assessment in assessments
    ):
        for match in window_matches:
            if str(getattr(match.job, "id")) not in judged_job_ids:
                continue
            _mark_preference_skipped(
                match,
                reason="invalid_llm_output",
                explanation="Preference scorer returned invalid output.",
                requested_mode=requested_mode,
                effective_mode=effective_mode,
                metadata=metadata_by_job_id.get(str(getattr(match.job, "id")), base_metadata),
            )
        return PreferenceRerankResult(
            matches=scored_matches,
            status=PreferenceStatus(
                applied=False,
                reason="invalid_llm_output",
                requested_mode=requested_mode,
                effective_mode=effective_mode,
                effective_top_n=effective_top_n,
                judged_count=0,
                eligible_count=len(judged_job_ids),
            ),
        )

    logger.info(
        "Preference reranking applied: effective_judge_mode=%s judged=%d matches=%d",
        effective_mode == "llm_judge",
        len(judged_job_ids),
        len(scored_matches),
    )
    return _apply_assessments(
        scored_matches,
        assessments,
        requested_mode=requested_mode,
        effective_mode=effective_mode,
        target_job_ids=judged_job_ids,
        metadata_by_job_id=metadata_by_job_id,
        effective_top_n=effective_top_n,
        judged_count=len(judged_job_ids),
        eligible_count=len(window_matches),
    )
