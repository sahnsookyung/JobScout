#!/usr/bin/env python3
"""
Persistence Operations - Database operations for scored matches.

Handles saving scored matches to the database, including creating
or updating JobMatch records and their associated JobMatchRequirement records.
"""

from typing import Optional
import logging

from sqlalchemy import select, delete, func

from database.models import JobMatch, JobMatchRequirement
from core.scorer.models import ScoredJobMatch

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


def save_match_to_db(
    scored_match: ScoredJobMatch,
    repo,
    preferences_file_hash: Optional[str] = None
) -> JobMatch:
    """
    Save scored match to database.

    Creates JobMatch record with associated JobMatchRequirement records.

    Args:
        scored_match: ScoredJobMatch instance with all match details
        repo: JobRepository instance for database access
        preferences_file_hash: Optional hash of preferences file if used

    Returns:
        JobMatch record that was created or updated
    """
    job = scored_match.job

    existing_stmt = select(JobMatch).where(
        JobMatch.job_post_id == job.id,
        JobMatch.resume_fingerprint == scored_match.resume_fingerprint
    )
    existing = repo.db.execute(existing_stmt).scalar_one_or_none()

    if existing:
        match_record = existing
        match_record.status = 'active'
        match_record.job_similarity = _to_float(scored_match.job_similarity)
        match_record.fit_score = _to_float(scored_match.fit_score)
        match_record.want_score = _to_float(scored_match.want_score)
        match_record.overall_score = _to_float(scored_match.overall_score)
        match_record.fit_components = _to_native_types(scored_match.fit_components)
        match_record.want_components = _to_native_types(scored_match.want_components)
        match_record.fit_weight = getattr(scored_match, 'fit_weight', 0.7)
        match_record.want_weight = getattr(scored_match, 'want_weight', 0.3)
        match_record.base_score = _to_float(scored_match.base_score)
        match_record.penalties = _to_float(scored_match.penalties)
        match_record.penalty_details = {
            'details': scored_match.penalty_details,
            'total': _to_float(scored_match.penalties),
            'preferences_boost': _to_float(scored_match.preferences_boost)
        }
        match_record.required_coverage = _to_float(scored_match.jd_required_coverage)
        match_record.preferred_coverage = _to_float(scored_match.jd_preferences_coverage)
        match_record.total_requirements = len(scored_match.matched_requirements) + len(scored_match.missing_requirements)
        match_record.matched_requirements_count = len(scored_match.matched_requirements)
        match_record.match_type = scored_match.match_type
        match_record.preferences_file_hash = preferences_file_hash
        match_record.job_content_hash = job.content_hash
        match_record.calculated_at = func.now()
    else:
        match_record = JobMatch(
            job_post_id=job.id,
            resume_fingerprint=scored_match.resume_fingerprint,
            job_similarity=_to_float(scored_match.job_similarity),
            fit_score=_to_float(scored_match.fit_score),
            want_score=_to_float(scored_match.want_score),
            overall_score=_to_float(scored_match.overall_score),
            fit_components=_to_native_types(scored_match.fit_components),
            want_components=_to_native_types(scored_match.want_components),
            fit_weight=getattr(scored_match, 'fit_weight', 0.7),
            want_weight=getattr(scored_match, 'want_weight', 0.3),
            base_score=_to_float(scored_match.base_score),
            penalties=_to_float(scored_match.penalties),
            penalty_details={
                'details': scored_match.penalty_details,
                'total': _to_float(scored_match.penalties),
                'preferences_boost': _to_float(scored_match.preferences_boost)
            },
            required_coverage=_to_float(scored_match.jd_required_coverage),
            preferred_coverage=_to_float(scored_match.jd_preferences_coverage),
            total_requirements=len(scored_match.matched_requirements) + len(scored_match.missing_requirements),
            matched_requirements_count=len(scored_match.matched_requirements),
            match_type=scored_match.match_type,
            preferences_file_hash=preferences_file_hash,
            job_content_hash=job.content_hash,
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

    for req_match in scored_match.matched_requirements:
        jmr = JobMatchRequirement(
            job_match_id=match_record.id,
            job_requirement_unit_id=req_match.requirement.id,
            evidence_text=req_match.evidence.text if req_match.evidence else "",
            evidence_section=req_match.evidence.source_section if req_match.evidence else None,
            evidence_tags=req_match.evidence.tags if req_match.evidence else {},
            similarity_score=_to_float(req_match.similarity),
            is_covered=req_match.is_covered,
            req_type=req_match.requirement.req_type
        )
        repo.db.add(jmr)
    
    for req_match in scored_match.missing_requirements:
        jmr = JobMatchRequirement(
            job_match_id=match_record.id,
            job_requirement_unit_id=req_match.requirement.id,
            evidence_text="",
            evidence_section=None,
            evidence_tags={},
            similarity_score=_to_float(req_match.similarity),
            is_covered=False,
            req_type=req_match.requirement.req_type
        )
        repo.db.add(jmr)

    repo.db.commit()

    logger.info(f"Saved match for job {job.id}: fit={scored_match.fit_score:.1f}, want={scored_match.want_score:.1f}, overall={scored_match.overall_score:.1f}")

    return match_record
