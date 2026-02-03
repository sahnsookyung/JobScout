#!/usr/bin/env python3
"""
Scoring Modes - Discovery and Strict mode scoring formulas.

Implements FR-6.1 (Discovery Mode) and FR-6.2 (Strict Mode) scoring formulas.
"""

from typing import Optional, Dict, Any, Tuple
import logging

from core.matcher import PreferencesAlignmentScore

logger = logging.getLogger(__name__)


def calculate_discovery_score(
    job_similarity: float,
    required_coverage: float,
    preferred_coverage: float,
    preferences_alignment: Optional[PreferencesAlignmentScore],
    soft_penalties: float,
    ranking_config: Any
) -> Tuple[float, Dict[str, Any]]:
    """
    Calculate score using discovery mode formula (FR-6.1).

    Discovery mode prioritizes exploration while preventing weak fits from dominating.

    Formula:
    - preferences_alignment_default_score = 0.5 if preferences absent
    - required_coverage_factor = floor + (1 - floor) * (required_coverage ^ power)
    - blended_score = w_sim*job_similarity + w_prefcov*preferred_coverage + w_prefalign*pref_align
    - overall_score = clamp(0, 100, 100 * required_coverage_factor * blended_score - soft_penalties)

    Args:
        job_similarity: Job-level cosine similarity (0.0-1.0)
        required_coverage: Fraction of required requirements covered (0.0-1.0)
        preferred_coverage: Fraction of preferred requirements covered (0.0-1.0)
        preferences_alignment: Preferences alignment score object
        soft_penalties: Total penalty points
        ranking_config: DiscoveryModeConfig instance

    Returns:
        Tuple of (overall_score, score_components_dict)
    """
    cfg = ranking_config

    # Preferences alignment score with default
    if preferences_alignment:
        pref_align_score = preferences_alignment.overall_score
    else:
        pref_align_score = 0.5

    # Required coverage factor (non-linear dampening)
    req_factor = (
        cfg.required_coverage_factor_floor +
        (1.0 - cfg.required_coverage_factor_floor) *
        (required_coverage ** cfg.required_coverage_factor_power)
    )

    # Blended relevance score (weighted combination)
    blended_score = (
        cfg.job_similarity_weight * job_similarity +
        cfg.preferred_coverage_weight * preferred_coverage +
        cfg.preferences_alignment_weight * pref_align_score
    )

    # Apply penalties with multiplier
    adjusted_penalties = soft_penalties * cfg.penalties_multiplier

    # Final score calculation
    raw_score = 100.0 * req_factor * blended_score - adjusted_penalties
    overall_score = max(0.0, min(100.0, raw_score))

    # Return score with component breakdown for explainability
    score_components = {
        'mode': 'discovery',
        'job_similarity': job_similarity,
        'required_coverage': required_coverage,
        'preferred_coverage': preferred_coverage,
        'preferences_alignment_score': pref_align_score,
        'required_coverage_factor': req_factor,
        'blended_score': blended_score,
        'soft_penalties': soft_penalties,
        'penalties_multiplier': cfg.penalties_multiplier,
        'adjusted_penalties': adjusted_penalties,
        'raw_score': raw_score,
        'overall_score': overall_score
    }

    return overall_score, score_components


def calculate_strict_score(
    job_similarity: float,
    required_coverage: float,
    preferred_coverage: float,
    preferences_alignment: Optional[PreferencesAlignmentScore],
    penalties: float,
    ranking_config: Any
) -> Tuple[float, Dict[str, Any]]:
    """
    Calculate score using strict mode formula (FR-6.2).

    Strict mode strongly prefers high required coverage and enforces hard constraints.

    Formula:
    - If required_coverage < required_coverage_minimum:
      - If low_fit_policy == reject: overall_score = 0
      - If low_fit_policy == cap: overall_score = min(calculated_score, low_fit_score_cap)
    - Otherwise:
      - required_coverage_factor = required_coverage ^ power
      - blended_score = w_req*required_coverage + w_sim*job_similarity + w_prefcov*preferred_coverage + w_prefalign*pref_align
      - overall_score = clamp(0, 100, 100 * req_factor * blended_score - adjusted_penalties)

    where req_factor = required_coverage ^ required_coverage_emphasis_power
    and adjusted_penalties = penalties * penalties_multiplier

    Args:
        job_similarity: Job-level cosine similarity (0.0-1.0)
        required_coverage: Fraction of required requirements covered (0.0-1.0)
        preferred_coverage: Fraction of preferred requirements covered (0.0-1.0)
        preferences_alignment: Preferences alignment score object
        penalties: Total penalty points
        ranking_config: StrictModeConfig instance

    Returns:
        Tuple of (overall_score, score_components_dict)
    """
    cfg = ranking_config

    # Preferences alignment score with default
    if preferences_alignment:
        pref_align_score = preferences_alignment.overall_score
    else:
        pref_align_score = 0.5

    # Check coverage gate
    coverage_gate_triggered = required_coverage < cfg.required_coverage_minimum
    gate_action = None

    if coverage_gate_triggered:
        gate_action = cfg.low_fit_policy
        if cfg.low_fit_policy == "reject":
            # Reject low-coverage matches entirely
            score_components = {
                'mode': 'strict',
                'job_similarity': job_similarity,
                'required_coverage': required_coverage,
                'preferred_coverage': preferred_coverage,
                'preferences_alignment_score': pref_align_score,
                'coverage_gate_triggered': True,
                'gate_action': 'reject',
                'required_coverage_minimum': cfg.required_coverage_minimum,
                'overall_score': 0.0
            }
            return 0.0, score_components
        elif cfg.low_fit_policy not in ("reject", "cap"):
            logger.warning(
                f"Unknown low_fit_policy '{cfg.low_fit_policy}', defaulting to 'reject'"
            )
            score_components = {
                'mode': 'strict',
                'job_similarity': job_similarity,
                'required_coverage': required_coverage,
                'preferred_coverage': preferred_coverage,
                'preferences_alignment_score': pref_align_score,
                'coverage_gate_triggered': True,
                'gate_action': 'reject',
                'required_coverage_minimum': cfg.required_coverage_minimum,
                'overall_score': 0.0
            }
            return 0.0, score_components

    # Required coverage factor (exponential emphasis)
    req_factor = required_coverage ** cfg.required_coverage_emphasis_power

    # Blended relevance score (weighted combination with required coverage prominently)
    blended_score = (
        cfg.required_coverage_weight * required_coverage +
        cfg.job_similarity_weight * job_similarity +
        cfg.preferred_coverage_weight * preferred_coverage +
        cfg.preferences_alignment_weight * pref_align_score
    )

    # Apply penalties with multiplier (strict mode penalizes more)
    adjusted_penalties = penalties * cfg.penalties_multiplier

    # Final score calculation
    raw_score = 100.0 * req_factor * blended_score - adjusted_penalties
    overall_score = max(0.0, min(100.0, raw_score))

    # Apply low-fit cap if triggered and policy is "cap"
    if coverage_gate_triggered and cfg.low_fit_policy == "cap":
        overall_score = min(overall_score, cfg.low_fit_score_cap)

    # Return score with component breakdown for explainability
    score_components = {
        'mode': 'strict',
        'job_similarity': job_similarity,
        'required_coverage': required_coverage,
        'preferred_coverage': preferred_coverage,
        'preferences_alignment_score': pref_align_score,
        'required_coverage_factor': req_factor,
        'blended_score': blended_score,
        'penalties': penalties,
        'penalties_multiplier': cfg.penalties_multiplier,
        'adjusted_penalties': adjusted_penalties,
        'coverage_gate_triggered': coverage_gate_triggered,
        'gate_action': gate_action,
        'required_coverage_minimum': cfg.required_coverage_minimum,
        'raw_score': raw_score,
        'overall_score': overall_score
    }

    return overall_score, score_components
