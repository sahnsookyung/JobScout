#!/usr/bin/env python3
"""
Penalty Calculations - Calculate penalties for various mismatches.

Penalty functions:
- calculate_fit_penalties: Capability-related penalties (missing skills, seniority, compensation, experience)
- calculate_want_penalties: Preference-related penalties (currently unused - structured prefs are display-time filters)
- calculate_penalties: Legacy function for backward compatibility

NOTE: This module contains pure calculation logic. DB access should happen in the calling layer
and pass pre-fetched data via parameters.
"""

from typing import List, Dict, Any, Optional, Tuple
import logging
import re

from database.models import JobPost
from core.config_loader import ScorerConfig
from core.matcher import RequirementMatchResult

logger = logging.getLogger(__name__)

_YEARS_PATTERNS = [
    r'(\d+)\+?\s*(?:years?|yrs?|exp|experience)',
    r'(\d+)\s*-\s*(?:years?|yrs?|exp|experience)',
    r'over\s+(\d+)\s*years'
]

_YEARS_KEYWORDS = ['years', 'year', 'experience', 'exp', 'yrs', 'yr']


def _calculate_experience_penalty(
    matched_requirements: List[RequirementMatchResult],
    experience_sections: Optional[List[Dict[str, Any]]],
    config: ScorerConfig,
    penalized_requirements: set
) -> Tuple[float, List[Dict[str, Any]]]:
    """
    Calculate experience mismatch penalty by comparing required years to resume experience.

    Uses two strategies:
    1. Extract years_value from source_data if available
    2. Parse source_text with regex patterns as fallback

    Args:
        matched_requirements: List of matched requirements
        experience_sections: Pre-fetched experience sections
        config: ScorerConfig with penalty settings
        penalized_requirements: Set of already-penalized requirement IDs

    Returns:
        Tuple of (total_penalty, penalty_details_list)
    """
    penalty = 0.0
    penalty_details = []

    if not experience_sections:
        return penalty, penalty_details

    for req in matched_requirements:
        if not req.evidence or not req.is_covered:
            continue

        req_row = getattr(req, 'requirement_row', None)
        unit = getattr(req_row, 'unit', None) if req_row else None
        req_years = getattr(unit, 'min_years', None) if unit else None

        if not req_years:
            continue

        best_exp_years = 0.0
        best_exp_source = ""

        for exp_section in experience_sections:
            if not exp_section.get('has_embedding', False):
                continue

            source_data = exp_section.get('source_data', {})
            exp_years_from_data = source_data.get('years_value')

            if exp_years_from_data is not None and exp_years_from_data > best_exp_years:
                best_exp_years = exp_years_from_data
                best_exp_source = exp_section.get('source_text', '')

            if best_exp_years == 0.0:
                for pattern in _YEARS_PATTERNS:
                    match = re.search(pattern, exp_section.get('source_text', '').lower())
                    if match:
                        extracted_years = float(match.group(1))
                        if extracted_years > best_exp_years:
                            best_exp_years = extracted_years
                            best_exp_source = exp_section.get('source_text', '')
                        break

        if req_years > best_exp_years and req.requirement.id not in penalized_requirements:
            shortfall = req_years - best_exp_years
            penalty_amount = min(
                shortfall * config.penalty_experience_shortfall,
                config.penalty_experience_shortfall * 3
            )
            penalty += penalty_amount
            penalty_details.append({
                'type': 'experience_years_mismatch',
                'amount': penalty_amount,
                'reason': f"Best experience section has {best_exp_years} years, requires {req_years}",
                'requirement_text': req.requirement.text
            })
            penalized_requirements.add(req.requirement.id)

    return penalty, penalty_details


def calculate_fit_penalties(
    job: JobPost,
    matched_requirements: List[RequirementMatchResult],
    missing_requirements: List[RequirementMatchResult],
    config: ScorerConfig,
    candidate_total_years: Optional[float] = None,
    experience_sections: Optional[List[Dict[str, Any]]] = None
) -> Tuple[float, List[Dict[str, Any]]]:
    """
    Calculate capability-related penalties only.

    Includes penalties from:
    - Missing required skills
    - Seniority mismatch
    - Compensation mismatch
    - Experience shortfall

    NOTE: Location, industry, and role mismatches are now display-time hard filters,
    not penalties. They are NOT included in this function.

    Args:
        job: Job post being scored
        matched_requirements: List of matched requirements
        missing_requirements: List of missing requirements
        config: ScorerConfig with penalty settings
        candidate_total_years: Deprecated - kept for API compatibility, not currently used
        experience_sections: Pre-fetched experience sections (list of dicts with source_data, source_text, etc.)

    Returns: (total_penalties, penalty_details)
    """
    penalties = 0.0
    penalty_details = []

    all_reqs = matched_requirements + missing_requirements
    required_total = len([r for r in all_reqs if r.requirement.req_type == 'required'])
    required_covered = len([m for m in matched_requirements if m.requirement.req_type == 'required'])
    missing_required = required_total - required_covered

    if missing_required > 0:
        penalty_amount = missing_required * config.penalty_missing_required
        penalties += penalty_amount
        missing_reqs = [m.requirement.text for m in missing_requirements
                       if m.requirement.req_type == 'required']
        penalty_details.append({
            'type': 'missing_required',
            'amount': penalty_amount,
            'reason': f"{missing_required} required skill(s) not covered",
            'details': missing_reqs[:3]
        })

    if config.target_seniority and job.job_level:
        job_level = (job.job_level or '').lower()
        target = config.target_seniority.lower()

        seniority_mismatch = False
        if target == 'junior' and ('senior' in job_level or 'lead' in job_level):
            seniority_mismatch = True
        elif target == 'senior' and ('junior' in job_level or 'entry' in job_level):
            seniority_mismatch = True

        if seniority_mismatch:
            penalties += config.penalty_seniority_mismatch
            penalty_details.append({
                'type': 'seniority_mismatch',
                'amount': config.penalty_seniority_mismatch,
                'reason': "Seniority level mismatch",
                'details': f"Job level: {job.job_level}, Target: {config.target_seniority}"
            })

    penalized_requirements = set()
    experience_mismatch_penalty, experience_mismatch_details = _calculate_experience_penalty(
        matched_requirements, experience_sections, config, penalized_requirements
    )

    if experience_mismatch_penalty > 0:
        penalties += experience_mismatch_penalty
        penalty_details.extend(experience_mismatch_details)

    if config.min_salary and job.salary_max:
        try:
            job_salary = float(job.salary_max)
            if job_salary < config.min_salary:
                penalties += config.penalty_compensation_mismatch
                penalty_details.append({
                    'type': 'compensation_mismatch',
                    'amount': config.penalty_compensation_mismatch,
                    'reason': "Salary below minimum requirement",
                    'details': f"Job max: {job.salary_max}, User min: {config.min_salary}"
                })
        except (ValueError, TypeError):
            pass

    return penalties, penalty_details


def calculate_penalties(
    job: JobPost,
    matched_requirements: List[RequirementMatchResult],
    missing_requirements: List[RequirementMatchResult],
    config: ScorerConfig,
    candidate_total_years: Optional[float] = None,
    experience_sections: Optional[List[Dict[str, Any]]] = None
) -> Tuple[float, List[Dict[str, Any]]]:
    """
    Calculate total penalties from capability mismatches.
    
    Args:
        job: Job post being scored
        matched_requirements: List of matched requirements
        missing_requirements: List of missing requirements
        config: ScorerConfig with penalty settings
        candidate_total_years: Pre-fetched total years of experience
        experience_sections: Pre-fetched experience sections (list of dicts)
    """
    penalties, penalty_details = calculate_fit_penalties(
        job=job,
        matched_requirements=matched_requirements,
        missing_requirements=missing_requirements,
        config=config,
        candidate_total_years=candidate_total_years,
        experience_sections=experience_sections
    )

    if config.wants_remote and not job.is_remote:
        penalties += config.penalty_location_mismatch
        penalty_details.append({
            'type': 'location_mismatch',
            'amount': config.penalty_location_mismatch,
            'reason': "Job is not remote (user preference: remote)",
            'details': f"Job location: {job.location_text}, remote={job.is_remote}"
        })

    return penalties, penalty_details
