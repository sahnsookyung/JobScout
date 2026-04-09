"""Shared requirement coverage helpers for scorer diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, Dict, List, Literal, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

ReqType = Literal["required", "preferred"]


@runtime_checkable
class RequirementProto(Protocol):
    req_type: ReqType
    weight: float


@runtime_checkable
class MatchProto(Protocol):
    similarity: float
    requirement: RequirementProto


@dataclass(frozen=True)
class AdaptedRequirement:
    req_type: ReqType
    weight: float = 1.0


@dataclass(frozen=True)
class AdaptedMatch:
    similarity: float
    requirement: AdaptedRequirement


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def clamp01(value: float) -> float:
    return clamp(value, 0.0, 1.0)


def as_requirement(obj: Any) -> AdaptedRequirement:
    req = getattr(obj, "requirement", obj)
    req_type = getattr(req, "req_type", None)
    if req_type not in ("required", "preferred"):
        logger.debug("Skipping unscored req_type=%r in requirement coverage", req_type)
        return AdaptedRequirement(req_type=req_type, weight=0.0)  # type: ignore[arg-type]

    weight = getattr(req, "weight", 1.0)
    try:
        weight_f = float(weight)
    except Exception:
        logger.warning("Invalid requirement.weight=%r; defaulting to 1.0", weight)
        weight_f = 1.0

    return AdaptedRequirement(req_type=req_type, weight=max(0.0, weight_f))


def as_match(obj: Any, *, default_similarity: float = 0.0) -> AdaptedMatch:
    sim = getattr(obj, "similarity", default_similarity)
    try:
        sim_f = float(sim)
    except Exception:
        logger.warning("Invalid similarity=%r; defaulting to %r", sim, default_similarity)
        sim_f = float(default_similarity)

    return AdaptedMatch(similarity=sim_f, requirement=as_requirement(obj))


def _scaled_quality(similarity: float, threshold: float, clamp_similarity: bool) -> float:
    sim = clamp01(similarity) if clamp_similarity else similarity
    threshold = clamp01(threshold)
    if sim < threshold:
        return 0.0
    return sim


def _quality_weighted_coverage(
    matches: List[AdaptedMatch],
    total_weight: float,
    threshold: float,
    clamp_similarity: bool,
) -> Dict[str, float]:
    if total_weight <= 0:
        return {"coverage": 0.0, "quality_sum": 0.0, "covered_weight": 0.0}

    quality_sum = 0.0
    covered_weight = 0.0
    normalized_threshold = clamp01(threshold)
    for match in matches:
        weight = match.requirement.weight
        quality = _scaled_quality(match.similarity, normalized_threshold, clamp_similarity)
        quality_sum += weight * quality

        similarity = clamp01(match.similarity) if clamp_similarity else match.similarity
        if similarity >= normalized_threshold:
            covered_weight += weight

    return {
        "coverage": quality_sum / total_weight,
        "quality_sum": quality_sum,
        "covered_weight": covered_weight,
    }


def calculate_requirement_coverage(
    matched_requirements: List[Any],
    missing_requirements: List[Any],
    *,
    req_type: ReqType,
    threshold: float,
    clamp_similarity: bool,
) -> Dict[str, float]:
    """Calculate coverage diagnostics for one requirement type."""
    adapted_matched = [as_match(item) for item in matched_requirements]
    adapted_missing = [as_match(item, default_similarity=0.0) for item in missing_requirements]

    matched = [item for item in adapted_matched if item.requirement.req_type == req_type]
    missing = [item for item in adapted_missing if item.requirement.req_type == req_type]
    total_weight = sum(item.requirement.weight for item in matched + missing)
    stats = _quality_weighted_coverage(matched, total_weight, threshold, clamp_similarity)
    missing_weight = sum(item.requirement.weight for item in missing)
    missing_ratio = (missing_weight / total_weight) if total_weight > 0 else 0.0

    return {
        **stats,
        "total_weight": total_weight,
        "matched_count": float(len(matched)),
        "missing_count": float(len(missing)),
        "missing_weight": missing_weight,
        "missing_ratio": missing_ratio,
    }
