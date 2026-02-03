#!/usr/bin/env python3
"""
Penalty Calculations - Calculate penalties for various mismatches.

Penalty functions:
- calculate_fit_penalties: Capability-related penalties (missing skills, seniority, compensation, experience)
- calculate_want_penalties: Preference-related penalties (currently unused - structured prefs are display-time filters)
- calculate_penalties: Legacy function for backward compatibility
"""

from typing import List, Dict, Any, Optional, Tuple
import logging
import re

from database.models import JobPost
from database.repository import JobRepository
from core.config_loader import ScorerConfig
from core.matcher import RequirementMatchResult, PreferencesAlignmentScore

logger = logging.getLogger(__name__)


def calculate_fit_penalties(
    job: JobPost,
    matched_requirements: List[RequirementMatchResult],
    missing_requirements: List[RequirementMatchResult],
    config: ScorerConfig,
    resume_fingerprint: Optional[str] = None,
    repo: Optional[JobRepository] = None
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
        resume_fingerprint: Optional resume fingerprint for experience section matching
        repo: Optional JobRepository for database access

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

    experience_mismatch_penalty = 0.0
    experience_mismatch_details = []
    penalized_requirements = set()

    if resume_fingerprint and repo:
        from database.models import ResumeSectionEmbedding
        from sqlalchemy import select

        stmt = select(ResumeSectionEmbedding).where(
            ResumeSectionEmbedding.resume_fingerprint == resume_fingerprint,
            ResumeSectionEmbedding.section_type == 'experience'
        )
        experience_sections = repo.db.execute(stmt).scalars().all()

        for req in matched_requirements:
            if not req.evidence or not req.is_covered:
                continue

            req_row = getattr(req, 'requirement_row', None)
            unit = getattr(req_row, 'unit', None) if req_row else None
            req_years = getattr(unit, 'min_years', None) if unit else None

            if req_years and experience_sections:
                best_exp_years = 0.0
                for exp_section in experience_sections:
                    if exp_section.embedding:
                        exp_years = exp_section.source_data.get('years_value', 0.0) if 'years_value' in exp_section.source_data else 0.0
                        best_exp_years = max(best_exp_years, exp_years)

                if req_years > best_exp_years and req.requirement.id not in penalized_requirements:
                    shortfall = req_years - best_exp_years
                    penalty_amount = min(
                        shortfall * config.penalty_experience_shortfall,
                        config.penalty_experience_shortfall * 3
                    )
                    experience_mismatch_penalty += penalty_amount
                    experience_mismatch_details.append({
                        'type': 'experience_years_mismatch',
                        'amount': penalty_amount,
                        'reason': f"Best experience section has {best_exp_years} years, requires {req_years}",
                        'requirement_text': req.requirement.text
                    })
                    penalized_requirements.add(req.requirement.id)

    if experience_mismatch_penalty > 0:
        penalties += experience_mismatch_penalty
        for detail in experience_mismatch_details:
            penalty_details.append(detail)

    experience_mismatch_penalty2 = 0.0
    experience_mismatch_details2 = []

    if resume_fingerprint and repo:
        from database.models import ResumeSectionEmbedding
        from sqlalchemy import select

        stmt = select(ResumeSectionEmbedding).where(
            ResumeSectionEmbedding.resume_fingerprint == resume_fingerprint,
            ResumeSectionEmbedding.section_type == 'experience'
        )
        experience_sections2 = repo.db.execute(stmt).scalars().all()

        for req in matched_requirements:
            req_text_lower = req.requirement.text.lower() if req.requirement.text else ''

            years_keywords = ['years', 'year', 'experience', 'exp', 'yrs', 'yr']
            has_years_keyword = any(keyword in req_text_lower for keyword in years_keywords)

            if has_years_keyword and experience_sections2:
                section_scores = []
                for exp_section in experience_sections2:
                    if exp_section.embedding:
                        section_scores.append({
                            'similarity': 0.5,
                            'section_type': exp_section.section_type,
                            'section_index': exp_section.section_index,
                            'source_text': exp_section.source_text
                        })

                if section_scores:
                    best_match = max(section_scores, key=lambda x: x['similarity'])

                    best_exp_years = None
                    best_exp_source = best_match['source_text']

                    years_patterns = [
                        r'(\d+)\+?\s*(?:years?|yrs?|exp|experience)',
                        r'(\d+)\s*-\s*(?:years?|yrs?|exp|experience)',
                        r'over\s+(\d+)\s*years'
                    ]

                    for pattern in years_patterns:
                        match = re.search(pattern, best_exp_source.lower())
                        if match:
                            best_exp_years = float(match.group(1))
                            break

                    req_row = getattr(req, 'requirement_row', None)
                    unit = getattr(req_row, 'unit', None) if req_row else None
                    req_years = getattr(unit, 'min_years', None) if unit else None

                    if req_years and best_exp_years and req.requirement.id not in penalized_requirements:
                        shortfall = req_years - best_exp_years
                        if shortfall > 0:
                            penalty_amount = min(
                                shortfall * config.penalty_experience_shortfall,
                                config.penalty_experience_shortfall * 3
                            )
                            experience_mismatch_penalty2 += penalty_amount
                            experience_mismatch_details2.append({
                                'type': 'experience_years_mismatch',
                                'amount': penalty_amount,
                                'reason': f"Best exp section has {best_exp_years} years, requires {req_years}",
                                'best_section': best_match['source_text'],
                                'best_section_similarity': best_match['similarity'],
                                'requirement_text': req.requirement.text
                            })
                            penalized_requirements.add(req.requirement.id)

    if experience_mismatch_penalty2 > 0:
        penalties += experience_mismatch_penalty2
        for detail in experience_mismatch_details2:
            penalty_details.append(detail)

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


def calculate_want_penalties(
    job: JobPost,
    config: ScorerConfig,
    preferences_alignment: Optional[PreferencesAlignmentScore] = None
) -> Tuple[float, List[Dict[str, Any]]]:
    """
    Calculate preference-related penalties.

    NOTE: Currently returns empty. Structured preferences (location, industry, role)
    are used as DISPLAY-TIME HARD FILTERS, not penalties. This function exists
    for future extensibility if we want preference mismatches to contribute to Want score.

    Args:
        job: Job post being scored
        config: ScorerConfig with penalty settings
        preferences_alignment: Optional preferences alignment score

    Returns: (total_penalties, penalty_details)
    """
    return 0.0, []


def calculate_penalties(
    job: JobPost,
    required_coverage: float,
    matched_requirements: List[RequirementMatchResult],
    missing_requirements: List[RequirementMatchResult],
    config: ScorerConfig,
    preferences_alignment: Optional[PreferencesAlignmentScore] = None,
    resume_fingerprint: Optional[str] = None,
    repo: Optional[JobRepository] = None
) -> Tuple[float, List[Dict[str, Any]]]:
    """
    Legacy function for backward compatibility.

    Calculates total penalties including:
    - Capability penalties from calculate_fit_penalties
    - Location mismatch penalty (kept for backward compatibility)
    - Industry/role penalties (kept for backward compatibility)
    """
    penalties, penalty_details = calculate_fit_penalties(
        job=job,
        matched_requirements=matched_requirements,
        missing_requirements=missing_requirements,
        config=config,
        resume_fingerprint=resume_fingerprint,
        repo=repo
    )

    if preferences_alignment:
        if preferences_alignment.location_match < 0.5:
            penalties += config.penalty_location_mismatch
            penalty_details.append({
                'type': 'location_mismatch',
                'amount': config.penalty_location_mismatch,
                'reason': "Poor location match",
                'details': preferences_alignment.details.get('location', {})
            })

        if preferences_alignment.industry_match == 0.0:
            penalty_amount = 10.0
            penalties += penalty_amount
            penalty_details.append({
                'type': 'industry_mismatch',
                'amount': penalty_amount,
                'reason': "Job in avoided industry",
                'details': preferences_alignment.details.get('industry', {})
            })

        if preferences_alignment.role_match == 0.0:
            penalty_amount = 10.0
            penalties += penalty_amount
            penalty_details.append({
                'type': 'role_mismatch',
                'amount': penalty_amount,
                'reason': "Job title matches avoided role",
                'details': preferences_alignment.details.get('role', {})
            })
    elif config.wants_remote and not job.is_remote:
        penalties += config.penalty_location_mismatch
        penalty_details.append({
            'type': 'location_mismatch',
            'amount': config.penalty_location_mismatch,
            'reason': "Job is not remote (user preference: remote)",
            'details': f"Job location: {job.location_text}, remote={job.is_remote}"
        })

    return penalties, penalty_details
