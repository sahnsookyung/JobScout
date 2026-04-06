"""Ranking engine — applies a declared mode to a bounded candidate pool.

Three modes:
  preference_first — sort by (pref DESC, fit DESC, sim DESC, stable_id)
  fit_first        — sort by (fit DESC, pref DESC, sim DESC, stable_id)
  balanced         — sort by (w_pref*pref + w_fit*fit DESC, sim DESC, stable_id)

NULL semantics for preference_score:
  NULL means "evaluator did not run / failed" (distinct from 0.0 = "scored poor").
  In preference_first and fit_first: NULL sorts after all non-NULL values (including 0.0).
  In balanced: NULL is treated as 0.0 in the blend; the substitution is recorded in
    RankingExplanation.missing_scores so callers can distinguish the two cases.

Normalisation:
  fit_score is stored as 0–100; divided by 100.0 before comparison.
  preference_score and job_similarity are already 0–1.

Aggregate logging:
  One DEBUG line per call reporting how many matches had NULL preference_score.
  Elevated to WARNING only when the missing ratio exceeds 50 % (systemic issue).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional

from core.ranking.explainability import RankingExplanation
from core.ranking.policy import RankingConfig

logger = logging.getLogger(__name__)


class RankingMode(str, Enum):
    PREFERENCE_FIRST = "preference_first"
    FIT_FIRST = "fit_first"
    BALANCED = "balanced"


@dataclass
class RankingContext:
    """Inputs for a single rank_matches() call."""
    mode: RankingMode
    config: RankingConfig


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def rank_matches(matches: List[Any], ctx: RankingContext) -> List[Any]:
    """Sort *matches* in-place by the declared mode.

    Expects the FULL (unbounded) candidate pool retrieved from the DB.
    top_k truncation must happen AFTER this function returns.

    Each element must expose (via attribute access):
        .fit_score        float | None  — 0–100 scale
        .job_similarity   float | None  — 0–1 scale
        .preference_score float | None  — 0–1, or None if not evaluated
        .id               str | UUID    — used when stable_tie_break_key="match_id" (default)
        .job_id           str | UUID    — used when stable_tie_break_key="job_id"

    Attaches a RankingExplanation instance to match.ranking_explanation.
    Returns the same list (sorted in-place).
    """
    if not matches:
        return matches

    mode = ctx.mode
    config = ctx.config
    null_count = 0

    keyed: list[tuple[tuple, Any]] = []
    for match in matches:
        pref, fit, sim, missing = _resolve_scores(match)
        if pref is None:
            null_count += 1
        stable = _stable_key(match, config)
        sort_key = _build_sort_key(pref, fit, sim, stable, mode, config)
        _attach_explanation(match, pref, fit, sim, mode, config, missing)
        keyed.append((sort_key, match))

    keyed.sort(key=lambda x: x[0])
    matches[:] = [m for _, m in keyed]

    _log_missing(null_count, len(matches))
    return matches


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_scores(match: Any) -> tuple[Optional[float], float, float, list[str]]:
    """Return (preference_01, fit_01, sim_01, missing_fields).

    preference_score: returned as-is (None preserved).
    fit_score: divided by 100.0; defaults to 0.0 if None.
    job_similarity: defaults to 0.0 if None.
    """
    missing: list[str] = []

    # preference_score — None means not evaluated, not "poor match"
    pref = getattr(match, "preference_score", None)
    if pref is not None:
        pref = max(0.0, min(1.0, float(pref)))
    else:
        missing.append("preference_score")

    # fit_score — 0–100 in DB; normalise to 0–1
    raw_fit = getattr(match, "fit_score", None)
    if raw_fit is None:
        fit = 0.0
        missing.append("fit_score")
    else:
        fit = max(0.0, min(1.0, float(raw_fit) / 100.0))

    # similarity — already 0–1
    raw_sim = getattr(match, "job_similarity", None)
    if raw_sim is None:
        sim = 0.0
    else:
        sim = max(0.0, min(1.0, float(raw_sim)))

    return pref, fit, sim, missing


def _stable_key(match: Any, config: RankingConfig) -> str:
    """Deterministic tie-break key selected by config.stable_tie_break_key.

    "match_id" (default): reads match.id
    "job_id": reads match.job_id, falls back to match.id
    Falls back to Python id() only when the chosen attribute is absent.
    """
    if config.stable_tie_break_key == "job_id":
        return str(getattr(match, "job_id", getattr(match, "id", id(match))))
    return str(getattr(match, "id", id(match)))


def _build_sort_key(
    pref: Optional[float],
    fit: float,
    sim: float,
    stable: str,
    mode: RankingMode,
    config: RankingConfig,
) -> tuple:
    if mode == RankingMode.PREFERENCE_FIRST:
        # NULL sorts last: (True, ...) > (False, ...) in ascending tuple comparison
        return (pref is None, -(pref or 0.0), -fit, -sim, stable)

    if mode == RankingMode.FIT_FIRST:
        return (-fit, pref is None, -(pref or 0.0), -sim, stable)

    # balanced
    pref_for_blend = pref if pref is not None else 0.0
    primary = config.balanced_w_pref * pref_for_blend + config.balanced_w_fit * fit
    return (-primary, -sim, stable)


def _attach_explanation(
    match: Any,
    pref: Optional[float],
    fit: float,
    sim: float,
    mode: RankingMode,
    config: RankingConfig,
    missing: list[str],
) -> None:
    balanced_primary: Optional[float] = None
    if mode == RankingMode.BALANCED:
        pref_for_blend = pref if pref is not None else 0.0
        balanced_primary = config.balanced_w_pref * pref_for_blend + config.balanced_w_fit * fit

    if "preference_score" in missing:
        code = "preference_unavailable"
    elif mode == RankingMode.PREFERENCE_FIRST:
        code = "preference_first"
    elif mode == RankingMode.FIT_FIRST:
        code = "fit_first"
    else:
        code = "balanced_blend"

    match.ranking_explanation = RankingExplanation(
        ranking_mode_used=mode.value,
        config_version=config.config_version,
        preference_score=pref,
        fit_score=fit,
        similarity_score=sim,
        balanced_primary_score=balanced_primary,
        dominant_reason_code=code,
        explanation_label=config.label_for_mode(mode.value),
        missing_scores=missing,
    )


def _log_missing(null_count: int, total: int) -> None:
    if null_count == 0 or total == 0:
        return
    ratio = null_count / total
    msg = "rank_matches: %d/%d matches had NULL preference_score"
    if ratio > 0.5:
        logger.warning(msg, null_count, total)
    else:
        logger.debug(msg, null_count, total)
