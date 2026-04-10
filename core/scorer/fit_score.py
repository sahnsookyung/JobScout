#!/usr/bin/env python3
"""Fit Score (Can-do-the-job) v1.1.

Key behavior:
- Required coverage: quality-weighted (similarity threshold) and depends on covered/total.
- Missing required: explicit detractor (hybrid ratio + per-item), plus any external fit_penalties.
- Defensive: clamps similarity inputs, clamps threshold, sanitizes misconfigured weights/penalties.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

from core.config_loader import ScorerConfig
from core.scorer.coverage import (
    calculate_requirement_coverage,
    clamp as _clamp,
    clamp01 as _clamp01,
)

logger = logging.getLogger(__name__)

DEFAULT_REQ_SIMILARITY_THRESHOLD = 0.6
DEFAULT_SIMILARITY_CLAMP = True

DEFAULT_WEIGHT_REQUIRED = 0.60
DEFAULT_JOB_SIMILARITY_WEIGHT = 0.325

DEFAULT_MISSING_REQUIRED_PENALTY_MAX = 0.0
DEFAULT_PER_MISSING_REQUIRED_PENALTY = 0.0
DEFAULT_MISSING_REQUIRED_PENALTY_CAP = 70.0

DEFAULT_ENABLE_EXPLICIT_MISSING_REQUIRED_PENALTY = True


def _warn_correct(name: str, old: Any, new: Any) -> None:
    if old != new:
        logger.warning("Corrected %s from %r to %r", name, old, new)


def _cfg_float(config: ScorerConfig, name: str, default: float) -> float:
    raw = getattr(config, name, default)
    try:
        return float(raw)
    except Exception:
        logger.warning("Invalid config %s=%r; using default=%r", name, raw, default)
        return float(default)


def _cfg_bool(config: ScorerConfig, name: str, default: bool) -> bool:
    raw = getattr(config, name, default)
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)
    if isinstance(raw, str):
        value = raw.strip().lower()
        if value in ("true", "1", "yes", "y", "on"):
            return True
        if value in ("false", "0", "no", "n", "off"):
            return False
    logger.warning("Invalid config %s=%r; using default=%r", name, raw, default)
    return default


def _nonneg(name: str, value: float) -> float:
    normalized = max(0.0, float(value))
    _warn_correct(name, value, normalized)
    return normalized


def calculate_fit_score(
    job_similarity: float,
    matched_requirements: List[Any],
    missing_requirements: List[Any],
    fit_penalties: float,
    config: ScorerConfig,
) -> Tuple[float, Dict[str, Any]]:
    threshold = _cfg_float(config, "req_similarity_threshold", DEFAULT_REQ_SIMILARITY_THRESHOLD)
    clamp_similarity = _cfg_bool(config, "similarity_clamp", DEFAULT_SIMILARITY_CLAMP)

    w_req = _cfg_float(config, "weight_required", DEFAULT_WEIGHT_REQUIRED)
    w_sim = _cfg_float(config, "job_similarity_weight", DEFAULT_JOB_SIMILARITY_WEIGHT)

    missing_required_penalty_max = _cfg_float(
        config,
        "missing_required_penalty_max",
        DEFAULT_MISSING_REQUIRED_PENALTY_MAX,
    )
    per_missing_required_penalty = _cfg_float(
        config,
        "per_missing_required_penalty",
        DEFAULT_PER_MISSING_REQUIRED_PENALTY,
    )
    missing_required_penalty_cap = _cfg_float(
        config,
        "missing_required_penalty_cap",
        DEFAULT_MISSING_REQUIRED_PENALTY_CAP,
    )

    enable_missing_required_penalty = _cfg_bool(
        config,
        "enable_explicit_missing_required_penalty",
        DEFAULT_ENABLE_EXPLICIT_MISSING_REQUIRED_PENALTY,
    )

    threshold0 = threshold
    threshold = _clamp01(threshold)
    _warn_correct("req_similarity_threshold", threshold0, threshold)

    w_req = _nonneg("weight_required", w_req)
    w_sim = _nonneg("job_similarity_weight", w_sim)

    missing_required_penalty_max = _nonneg(
        "missing_required_penalty_max",
        missing_required_penalty_max,
    )
    per_missing_required_penalty = _nonneg(
        "per_missing_required_penalty",
        per_missing_required_penalty,
    )
    missing_required_penalty_cap = _nonneg(
        "missing_required_penalty_cap",
        missing_required_penalty_cap,
    )

    try:
        job_similarity_f = float(job_similarity)
    except Exception:
        logger.warning("Invalid job_similarity=%r; defaulting to 0.0", job_similarity)
        job_similarity_f = 0.0
    if clamp_similarity:
        original_similarity = job_similarity_f
        job_similarity_f = _clamp01(job_similarity_f)
        _warn_correct("job_similarity", original_similarity, job_similarity_f)

    req_stats = calculate_requirement_coverage(
        matched_requirements,
        missing_requirements,
        req_type="required",
        threshold=threshold,
        clamp_similarity=clamp_similarity,
    )
    required_coverage = req_stats["coverage"]
    total_required_weight = req_stats["total_weight"]

    denom = w_req + w_sim
    if denom <= 0:
        core = 0.0
    else:
        core = (w_req * required_coverage + w_sim * job_similarity_f) / denom

    missing_required_count = int(req_stats["missing_count"])
    missing_required_weight = req_stats["missing_weight"]
    missing_required_ratio = req_stats["missing_ratio"]

    if enable_missing_required_penalty:
        missing_required_penalty = (
            missing_required_ratio * missing_required_penalty_max
            + missing_required_count * per_missing_required_penalty
        )
        if missing_required_penalty_cap > 0:
            missing_required_penalty = min(
                missing_required_penalty,
                missing_required_penalty_cap,
            )
    else:
        missing_required_penalty = 0.0

    try:
        fit_penalties_f = float(fit_penalties)
    except Exception:
        logger.warning("Invalid fit_penalties=%r; defaulting to 0.0", fit_penalties)
        fit_penalties_f = 0.0

    raw_score = 100.0 * core - missing_required_penalty - fit_penalties_f
    fit_score = _clamp(raw_score, 0.0, 100.0)

    components: Dict[str, Any] = {
        "job_similarity": job_similarity_f,
        "threshold": threshold,
        "similarity_clamp": clamp_similarity,
        "required_coverage": required_coverage,
        "required_quality_sum": req_stats["quality_sum"],
        "required_covered_weight": req_stats["covered_weight"],
        "total_required_weight": total_required_weight,
        "w_req": w_req,
        "w_sim": w_sim,
        "core": core,
        "enable_explicit_missing_required_penalty": enable_missing_required_penalty,
        "missing_required_count": missing_required_count,
        "missing_required_weight": missing_required_weight,
        "missing_required_ratio": missing_required_ratio,
        "missing_required_penalty_max": missing_required_penalty_max,
        "per_missing_required_penalty": per_missing_required_penalty,
        "missing_required_penalty_cap": missing_required_penalty_cap,
        "missing_required_penalty": missing_required_penalty,
        "fit_penalties": fit_penalties_f,
        "raw_score": raw_score,
        "fit_score": fit_score,
    }

    logger.debug(
        "Fit score %.1f (core=%.3f, miss_req_pen=%.1f, fit_pen=%.1f)",
        fit_score,
        core,
        missing_required_penalty,
        fit_penalties_f,
    )

    return fit_score, components
