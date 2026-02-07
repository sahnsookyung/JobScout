#!/usr/bin/env python3
"""
Fit Score (Can-do-the-job) v1.1

Key behavior:
- Required coverage: quality-weighted (similarity threshold) and depends on covered/total.
- Preferred coverage: positive-only bonus, never a detractor.
- Missing required: explicit detractor (hybrid ratio + per-item), plus any external fit_penalties.
- Defensive: clamps similarity inputs, clamps threshold, sanitizes misconfigured weights/penalties.
- Supports missing items provided either as "match-like" or "requirement-like" objects.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Tuple, List, Optional, Protocol, Literal, runtime_checkable
import logging

from core.config_loader import ScorerConfig

logger = logging.getLogger(__name__)

# ----------------------------
# Tunable defaults (v1.1)
# ----------------------------
# Similarity handling
DEFAULT_REQ_SIMILARITY_THRESHOLD = 0.55  # in [0,1]
DEFAULT_SIMILARITY_CLAMP = True          # clamp all similarities + job_similarity to [0,1]

# Core weights (unitless, must be >= 0)
DEFAULT_WEIGHT_REQUIRED = 0.60
DEFAULT_JOB_SIMILARITY_WEIGHT = 0.30

# Preferred bonus (fraction of 1; later multiplied by 100 into "points")
DEFAULT_PREFERRED_BONUS_MAX_FRACTION = 0.08  # up to +8 points

# Missing required explicit penalty (in "points")
DEFAULT_MISSING_REQUIRED_PENALTY_MAX = 40.0  # ratio-based component
DEFAULT_PER_MISSING_REQUIRED_PENALTY = 0.0   # per missing required (count-based)
DEFAULT_MISSING_REQUIRED_PENALTY_CAP = 70.0  # 0 disables cap

# Optional behavior toggles
DEFAULT_ENABLE_EXPLICIT_MISSING_REQUIRED_PENALTY = True


# ----------------------------
# Protocols (typing)
# ----------------------------
ReqType = Literal["required", "preferred"]

@runtime_checkable
class RequirementProto(Protocol):
    req_type: ReqType
    weight: float  # optional in impl; we’ll default to 1.0 if missing

@runtime_checkable
class MatchProto(Protocol):
    similarity: float
    requirement: RequirementProto


@dataclass(frozen=True)
class _AdaptedRequirement:
    req_type: ReqType
    weight: float = 1.0

@dataclass(frozen=True)
class _AdaptedMatch:
    similarity: float
    requirement: _AdaptedRequirement


# ----------------------------
# Helpers
# ----------------------------
def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _clamp01(x: float) -> float:
    return _clamp(x, 0.0, 1.0)

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
        v = raw.strip().lower()
        if v in ("true", "1", "yes", "y", "on"):
            return True
        if v in ("false", "0", "no", "n", "off"):
            return False
    logger.warning("Invalid config %s=%r; using default=%r", name, raw, default)
    return default

def _nonneg(name: str, x: float) -> float:
    y = max(0.0, float(x))
    _warn_correct(name, x, y)
    return y

def _as_requirement(obj: Any) -> _AdaptedRequirement:
    # Supports both requirement-like objects and match-like objects.
    # If obj has .requirement, we treat obj.requirement as the requirement.
    req = getattr(obj, "requirement", obj)

    req_type = getattr(req, "req_type", None)
    if req_type not in ("required", "preferred"):
        raise TypeError(f"Requirement missing/invalid req_type: {req_type!r}")

    weight = getattr(req, "weight", 1.0)
    try:
        weight_f = float(weight)
    except Exception:
        logger.warning("Invalid requirement.weight=%r; defaulting to 1.0", weight)
        weight_f = 1.0

    # Negative weights cause bizarre incentives; clamp to 0.
    weight_f = max(0.0, weight_f)
    return _AdaptedRequirement(req_type=req_type, weight=weight_f)

def _as_match(obj: Any, *, default_similarity: float = 0.0) -> _AdaptedMatch:
    # If it's already match-like, use its similarity; else treat it as requirement-like with default similarity.
    if hasattr(obj, "similarity"):
        sim = getattr(obj, "similarity", default_similarity)
    else:
        sim = default_similarity

    try:
        sim_f = float(sim)
    except Exception:
        logger.warning("Invalid similarity=%r; defaulting to %r", sim, default_similarity)
        sim_f = float(default_similarity)

    return _AdaptedMatch(similarity=sim_f, requirement=_as_requirement(obj))

def _scaled_quality(sim: float, threshold: float, clamp_similarity: bool) -> float:
    # Clamp first to avoid weird math if sim/threshold are out of range.
    if clamp_similarity:
        sim = _clamp01(sim)
    threshold = _clamp01(threshold)

    if sim < threshold:
        return 0.0

    # threshold == 1 means only perfect matches get credit.
    if threshold >= 1.0:
        return 1.0 if sim >= 1.0 else 0.0

    return (sim - threshold) / (1.0 - threshold)  # in [0,1]

def _quality_weighted_coverage(matches: List[_AdaptedMatch], total_weight: float, threshold: float, clamp_similarity: bool) -> Dict[str, float]:
    if total_weight <= 0:
        return {"coverage": 0.0, "quality_sum": 0.0, "covered_weight": 0.0}

    quality_sum = 0.0
    covered_weight = 0.0

    for m in matches:
        w = m.requirement.weight
        q = _scaled_quality(m.similarity, threshold, clamp_similarity)
        quality_sum += w * q

        sim_for_covered = _clamp01(m.similarity) if clamp_similarity else m.similarity
        if sim_for_covered >= _clamp01(threshold):
            covered_weight += w

    return {
        "coverage": quality_sum / total_weight,
        "quality_sum": quality_sum,
        "covered_weight": covered_weight,
    }


# ----------------------------
# Main
# ----------------------------
def calculate_fit_score(
    job_similarity: float,
    matched_requirements: List[Any],
    missing_requirements: List[Any],
    fit_penalties: float,
    config: ScorerConfig
) -> Tuple[float, Dict[str, Any]]:

    # --- Load config with defaults (centralized) ---
    threshold = _cfg_float(config, "req_similarity_threshold", DEFAULT_REQ_SIMILARITY_THRESHOLD)
    clamp_similarity = _cfg_bool(config, "similarity_clamp", DEFAULT_SIMILARITY_CLAMP)

    w_req = _cfg_float(config, "weight_required", DEFAULT_WEIGHT_REQUIRED)
    w_sim = _cfg_float(config, "job_similarity_weight", DEFAULT_JOB_SIMILARITY_WEIGHT)
    preferred_bonus_max_fraction = _cfg_float(config, "preferred_bonus_max_fraction", DEFAULT_PREFERRED_BONUS_MAX_FRACTION)

    missing_required_penalty_max = _cfg_float(config, "missing_required_penalty_max", DEFAULT_MISSING_REQUIRED_PENALTY_MAX)
    per_missing_required_penalty = _cfg_float(config, "per_missing_required_penalty", DEFAULT_PER_MISSING_REQUIRED_PENALTY)
    missing_required_penalty_cap = _cfg_float(config, "missing_required_penalty_cap", DEFAULT_MISSING_REQUIRED_PENALTY_CAP)

    enable_missing_required_penalty = _cfg_bool(
        config,
        "enable_explicit_missing_required_penalty",
        DEFAULT_ENABLE_EXPLICIT_MISSING_REQUIRED_PENALTY
    )

    # --- Sanitize ranges / signs (warn if corrected) ---
    threshold0 = threshold
    threshold = _clamp01(threshold)
    _warn_correct("req_similarity_threshold", threshold0, threshold)

    w_req = _nonneg("weight_required", w_req)
    w_sim = _nonneg("job_similarity_weight", w_sim)
    preferred_bonus_max_fraction0 = preferred_bonus_max_fraction
    preferred_bonus_max_fraction = _nonneg("preferred_bonus_max_fraction", preferred_bonus_max_fraction)
    _warn_correct("preferred_bonus_max_fraction", preferred_bonus_max_fraction0, preferred_bonus_max_fraction)

    missing_required_penalty_max = _nonneg("missing_required_penalty_max", missing_required_penalty_max)
    per_missing_required_penalty = _nonneg("per_missing_required_penalty", per_missing_required_penalty)
    missing_required_penalty_cap = _nonneg("missing_required_penalty_cap", missing_required_penalty_cap)

    try:
        job_similarity_f = float(job_similarity)
    except Exception:
        logger.warning("Invalid job_similarity=%r; defaulting to 0.0", job_similarity)
        job_similarity_f = 0.0
    if clamp_similarity:
        js0 = job_similarity_f
        job_similarity_f = _clamp01(job_similarity_f)
        _warn_correct("job_similarity", js0, job_similarity_f)

    # --- Adapt inputs (supports requirement-like in missing list) ---
    adapted_matched = [_as_match(m) for m in matched_requirements]
    adapted_missing = [_as_match(m, default_similarity=0.0) for m in missing_requirements]

    matched_req = [m for m in adapted_matched if m.requirement.req_type == "required"]
    matched_pref = [m for m in adapted_matched if m.requirement.req_type == "preferred"]

    missing_req = [m for m in adapted_missing if m.requirement.req_type == "required"]
    missing_pref = [m for m in adapted_missing if m.requirement.req_type == "preferred"]

    # --- Totals (weight-aware; denominator includes missing, so missing reduces coverage) ---
    total_required_weight = sum(m.requirement.weight for m in matched_req) + sum(m.requirement.weight for m in missing_req)
    total_preferred_weight = sum(m.requirement.weight for m in matched_pref) + sum(m.requirement.weight for m in missing_pref)

    req_stats = _quality_weighted_coverage(matched_req, total_required_weight, threshold, clamp_similarity)
    pref_stats = _quality_weighted_coverage(matched_pref, total_preferred_weight, threshold, clamp_similarity)

    required_coverage = req_stats["coverage"]    # 0..1
    preferred_coverage = pref_stats["coverage"]  # 0..1

    # --- Core (normalize) ---
    denom = (w_req + w_sim)
    if denom <= 0:
        # Degenerate config: no signal, return purely penalty-driven score.
        core = 0.0
    else:
        core = (w_req * required_coverage + w_sim * job_similarity_f) / denom

    # --- Preferred bonus (positive-only) ---
    preferred_bonus_fraction = preferred_bonus_max_fraction * preferred_coverage  # 0..preferred_bonus_max_fraction

    # --- Missing required explicit penalty (optional) ---
    missing_required_count = len(missing_req)
    missing_required_weight = sum(m.requirement.weight for m in missing_req)
    missing_required_ratio = (missing_required_weight / total_required_weight) if total_required_weight > 0 else 0.0

    if enable_missing_required_penalty:
        missing_required_penalty = (
            missing_required_ratio * missing_required_penalty_max
            + missing_required_count * per_missing_required_penalty
        )
        if missing_required_penalty_cap > 0:
            missing_required_penalty = min(missing_required_penalty, missing_required_penalty_cap)
    else:
        missing_required_penalty = 0.0

    # --- Final (points) ---
    try:
        fit_penalties_f = float(fit_penalties)
    except Exception:
        logger.warning("Invalid fit_penalties=%r; defaulting to 0.0", fit_penalties)
        fit_penalties_f = 0.0

    raw_score = 100.0 * (core + preferred_bonus_fraction) - missing_required_penalty - fit_penalties_f
    fit_score = _clamp(raw_score, 0.0, 100.0)

    components: Dict[str, Any] = {
        # Inputs (sanitized)
        "job_similarity": job_similarity_f,
        "threshold": threshold,
        "similarity_clamp": clamp_similarity,

        # Coverage
        "required_coverage": required_coverage,
        "required_quality_sum": req_stats["quality_sum"],
        "required_covered_weight": req_stats["covered_weight"],
        "total_required_weight": total_required_weight,

        "preferred_coverage": preferred_coverage,
        "preferred_quality_sum": pref_stats["quality_sum"],
        "preferred_covered_weight": pref_stats["covered_weight"],
        "total_preferred_weight": total_preferred_weight,

        # Weights
        "w_req": w_req,
        "w_sim": w_sim,
        "core": core,

        # Preferred bonus (fractions + implied points)
        "preferred_bonus_max_fraction": preferred_bonus_max_fraction,
        "preferred_bonus_fraction": preferred_bonus_fraction,
        "preferred_bonus_points": 100.0 * preferred_bonus_fraction,

        # Missing required
        "enable_explicit_missing_required_penalty": enable_missing_required_penalty,
        "missing_required_count": missing_required_count,
        "missing_required_weight": missing_required_weight,
        "missing_required_ratio": missing_required_ratio,
        "missing_required_penalty_max": missing_required_penalty_max,
        "per_missing_required_penalty": per_missing_required_penalty,
        "missing_required_penalty_cap": missing_required_penalty_cap,
        "missing_required_penalty": missing_required_penalty,

        # External penalties
        "fit_penalties": fit_penalties_f,

        # Outputs
        "raw_score": raw_score,
        "fit_score": fit_score,
    }

    logger.debug(
        "Fit score %.1f (core=%.3f, pref_bonus_pts=%.1f, miss_req_pen=%.1f, fit_pen=%.1f)",
        fit_score, core, components["preferred_bonus_points"], missing_required_penalty, fit_penalties_f
    )

    return fit_score, components
