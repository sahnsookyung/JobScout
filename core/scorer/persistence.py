#!/usr/bin/env python3
"""
Persistence Operations - Database operations for scored matches.

Handles saving scored matches to the database, including creating
or updating JobMatch records and their associated JobMatchRequirement records.

Supports both ScoredJobMatch ORM objects and MatchResultDTO data transfer objects.
"""

import logging
from typing import Union

from sqlalchemy import select, delete, func

from database.models import JobMatch, JobMatchRequirement
from core.scorer.models import ScoredJobMatch
from core.matcher.dto import MatchResultDTO

logger = logging.getLogger(__name__)


def _to_float(value):
    """Convert value to native Python float for database compatibility."""
    if value is None:
        return 0.0
    return float(value)


def _to_native_types(obj):
    """Recursively convert numpy types to native Python types for JSON serialization."""
    if obj is None:
        return None
    if hasattr(obj, 'tolist'):  # numpy array or matrix (check before scalars)
        return obj.tolist()
    if hasattr(obj, 'item'):  # numpy scalar (float32, int64, etc.)
        return obj.item()
    if isinstance(obj, dict):
        return {k: _to_native_types(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_native_types(item) for item in obj]
    return obj


def _extract_job_data(scored_match: Union[ScoredJobMatch, MatchResultDTO]):
    """Extract job data from either ORM object or DTO."""
    if isinstance(scored_match, MatchResultDTO):
        # DTO case - job is JobMatchDTO
        job = scored_match.job
        return {
            'id': str(job.id),
            'content_hash': job.content_hash,
        }
    else:
        # ORM case
        return {
            'id': str(scored_match.job.id),
            'content_hash': getattr(scored_match.job, 'content_hash', ''),
        }


def _extract_requirement_matches(scored_match: Union[ScoredJobMatch, MatchResultDTO]):
    """Extract requirement matches from either ORM object or DTO."""
    if isinstance(scored_match, MatchResultDTO):
        # DTO case - requirements are RequirementMatchDTO
        matched = []
        missing = []
        for req in scored_match.requirement_matches:
            matched.append({
                'requirement_id': str(req.requirement.id),
                'req_type': req.requirement.req_type,
                'evidence_text': req.evidence.text if req.evidence else "",
                'evidence_section': req.evidence.source_section if req.evidence else None,
                'evidence_tags': req.evidence.tags if req.evidence else {},
                'similarity': req.similarity,
                'is_covered': req.is_covered,
            })
        for req in scored_match.missing_requirements:
            missing.append({
                'requirement_id': str(req.requirement.id),
                'req_type': req.requirement.req_type,
                'similarity': req.similarity,
            })
        return matched, missing
    else:
        # ORM case - requirements are RequirementMatchResult objects
        matched = []
        missing = []
        for req in scored_match.matched_requirements:
            evidence = req.evidence
            matched.append({
                'requirement_id': str(req.requirement.id),
                'req_type': req.requirement.req_type,
                'evidence_text': getattr(evidence, 'text', '') if evidence else "",
                'evidence_section': getattr(evidence, 'source_section', None) if evidence else None,
                'evidence_tags': getattr(evidence, 'tags', {}) if evidence else {},
                'similarity': req.similarity,
                'is_covered': req.is_covered,
            })
        for req in scored_match.missing_requirements:
            missing.append({
                'requirement_id': str(req.requirement.id),
                'req_type': req.requirement.req_type,
                'similarity': req.similarity,
            })
        return matched, missing


def _extract_scores(scored_match: Union[ScoredJobMatch, MatchResultDTO]):
    """Extract score values from either ORM object or DTO."""
    if isinstance(scored_match, MatchResultDTO):
        return {
            'job_similarity': scored_match.job_similarity,
            'fit_score': scored_match.fit_score,
            'want_score': scored_match.want_score,
            'overall_score': scored_match.overall_score,
            'fit_components': scored_match.fit_components,
            'want_components': scored_match.want_components,
            'fit_weight': scored_match.fit_weight,
            'want_weight': scored_match.want_weight,
            'base_score': scored_match.base_score,
            'penalties': scored_match.penalties,
            'penalty_details': scored_match.penalty_details,
            'preferences_boost': scored_match.preferences_boost,
            'jd_required_coverage': scored_match.jd_required_coverage,
            'jd_preferences_coverage': scored_match.jd_preferences_coverage,
            'match_type': scored_match.match_type,
        }
    else:
        return {
            'job_similarity': scored_match.job_similarity,
            'fit_score': scored_match.fit_score,
            'want_score': scored_match.want_score,
            'overall_score': scored_match.overall_score,
            'fit_components': _to_native_types(getattr(scored_match, 'fit_components', {})),
            'want_components': _to_native_types(getattr(scored_match, 'want_components', {})),
            'fit_weight': getattr(scored_match, 'fit_weight', 0.7),
            'want_weight': getattr(scored_match, 'want_weight', 0.3),
            'base_score': scored_match.base_score,
            'penalties': scored_match.penalties,
            'penalty_details': {
                'details': getattr(scored_match, 'penalty_details', []),
                'total': _to_float(scored_match.penalties),
                'preferences_boost': _to_float(getattr(scored_match, 'preferences_boost', 0.0))
            },
            'preferences_boost': getattr(scored_match, 'preferences_boost', 0.0),
            'jd_required_coverage': scored_match.jd_required_coverage,
            'jd_preferences_coverage': scored_match.jd_preferences_coverage,
            'match_type': scored_match.match_type,
        }


def save_match_to_db(
    scored_match: Union[ScoredJobMatch, MatchResultDTO],
    repo,
) -> JobMatch:
    """
    Save scored match to database.

    Creates JobMatch record with associated JobMatchRequirement records.
    Accepts either a ScoredJobMatch ORM object or a MatchResultDTO.

    Args:
        scored_match: ScoredJobMatch instance or MatchResultDTO with all match details
        repo: JobRepository instance for database access

    Returns:
        JobMatch record that was created or updated
    """
    job_data = _extract_job_data(scored_match)
    job_id = job_data['id']
    job_content_hash = job_data['content_hash']
    scores = _extract_scores(scored_match)
    matched_reqs, missing_reqs = _extract_requirement_matches(scored_match)

    existing_stmt = select(JobMatch).where(
        JobMatch.job_post_id == job_id,
        JobMatch.resume_fingerprint == scored_match.resume_fingerprint
    )
    existing = repo.db.execute(existing_stmt).scalar_one_or_none()

    if existing:
        match_record = existing
        match_record.status = 'active'
        match_record.job_similarity = _to_float(scores['job_similarity'])
        match_record.fit_score = _to_float(scores['fit_score'])
        match_record.want_score = _to_float(scores['want_score'])
        match_record.overall_score = _to_float(scores['overall_score'])
        match_record.fit_components = _to_native_types(scores['fit_components'])
        match_record.want_components = _to_native_types(scores['want_components'])
        match_record.fit_weight = scores['fit_weight']
        match_record.want_weight = scores['want_weight']
        match_record.base_score = _to_float(scores['base_score'])
        match_record.penalties = _to_float(scores['penalties'])
        match_record.penalty_details = scores['penalty_details']
        match_record.required_coverage = _to_float(scores['jd_required_coverage'])
        match_record.preferred_coverage = _to_float(scores['jd_preferences_coverage'])
        match_record.total_requirements = len(matched_reqs) + len(missing_reqs)
        match_record.matched_requirements_count = len(matched_reqs)
        match_record.match_type = scores['match_type']
        match_record.job_content_hash = job_content_hash
        match_record.calculated_at = func.now()
    else:
        match_record = JobMatch(
            job_post_id=job_id,
            resume_fingerprint=scored_match.resume_fingerprint,
            job_similarity=_to_float(scores['job_similarity']),
            fit_score=_to_float(scores['fit_score']),
            want_score=_to_float(scores['want_score']),
            overall_score=_to_float(scores['overall_score']),
            fit_components=_to_native_types(scores['fit_components']),
            want_components=_to_native_types(scores['want_components']),
            fit_weight=scores['fit_weight'],
            want_weight=scores['want_weight'],
            base_score=_to_float(scores['base_score']),
            penalties=_to_float(scores['penalties']),
            penalty_details=scores['penalty_details'],
            required_coverage=_to_float(scores['jd_required_coverage']),
            preferred_coverage=_to_float(scores['jd_preferences_coverage']),
            total_requirements=len(matched_reqs) + len(missing_reqs),
            matched_requirements_count=len(matched_reqs),
            match_type=scores['match_type'],
            job_content_hash=job_content_hash,
            notified=False,
            calculated_at=func.now()
        )
        repo.db.add(match_record)

    repo.db.flush()

    if existing:
        repo.db.execute(
            delete(JobMatchRequirement).where(
                JobMatchRequirement.job_match_id == match_record.id
            )
        )

    for req in matched_reqs:
        jmr = JobMatchRequirement(
            job_match_id=match_record.id,
            job_requirement_unit_id=req['requirement_id'],
            evidence_text=req['evidence_text'],
            evidence_section=req['evidence_section'],
            evidence_tags=req['evidence_tags'],
            similarity_score=_to_float(req['similarity']),
            is_covered=req['is_covered'],
            req_type=req['req_type']
        )
        repo.db.add(jmr)

    for req in missing_reqs:
        jmr = JobMatchRequirement(
            job_match_id=match_record.id,
            job_requirement_unit_id=req['requirement_id'],
            evidence_text="",
            evidence_section=None,
            evidence_tags={},
            similarity_score=_to_float(req['similarity']),
            is_covered=False,
            req_type=req['req_type']
        )
        repo.db.add(jmr)

    repo.db.commit()

    logger.info(f"Saved match for job {job_id}: fit={scores['fit_score']:.1f}, want={scores['want_score']:.1f}, overall={scores['overall_score']:.1f}")

    return match_record
