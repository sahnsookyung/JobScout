#!/usr/bin/env python3
"""
Fit Score Calculation - "Can do the job" score.

Fit score reflects ability to perform the job based on:
- Requirements coverage (required + preferred)
- Job similarity (resume embedding vs job summary embedding)
- Capability penalties (missing skills, seniority mismatch, etc.)
"""

from typing import Dict, Any, Tuple
import logging

from core.config_loader import ScorerConfig

logger = logging.getLogger(__name__)


def calculate_fit_score(
    job_similarity: float,
    required_coverage: float,
    preferred_coverage: float,
    fit_penalties: float,
    config: ScorerConfig
) -> Tuple[float, Dict[str, Any]]:
    """
    Calculate Fit score: "Can do the job"

    Formula:
        blended = w_sim * job_similarity + w_req * required_coverage + w_pref * preferred_coverage
        fit_score = clamp(0, 100, 100 * blended - fit_penalties)

    Args:
        job_similarity: Cosine similarity between resume and job summary (0.0-1.0)
        required_coverage: Fraction of required requirements covered (0.0-1.0)
        preferred_coverage: Fraction of preferred requirements covered (0.0-1.0)
        fit_penalties: Total capability penalty points
        config: ScorerConfig with weights

    Returns:
        Tuple of (fit_score, components_dict)
    """
    blended = (
        config.weight_required * required_coverage +
        config.weight_preferred * preferred_coverage +
        getattr(config, 'job_similarity_weight', 0.3) * job_similarity
    )

    raw_score = 100.0 * blended - fit_penalties
    fit_score = max(0.0, min(100.0, raw_score))

    components = {
        'job_similarity': job_similarity,
        'required_coverage': required_coverage,
        'preferred_coverage': preferred_coverage,
        'job_similarity_weight': getattr(config, 'job_similarity_weight', 0.3),
        'required_coverage_weight': config.weight_required,
        'preferred_coverage_weight': config.weight_preferred,
        'blended': blended,
        'fit_penalties': fit_penalties,
        'raw_score': raw_score,
        'fit_score': fit_score
    }

    logger.debug(f"Fit score: {fit_score:.1f}, blended={blended:.3f}, penalties={fit_penalties:.1f}")

    return fit_score, components
