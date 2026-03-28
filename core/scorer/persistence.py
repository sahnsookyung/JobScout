"""
Persistence Operations - Database operations for scored matches.

Handles saving scored matches to the database, including creating
or updating JobMatch records and their associated JobMatchRequirement records.

Supports both ScoredJobMatch ORM objects and MatchResultDTO data transfer objects.
"""

import logging

from sqlalchemy import select, delete, func

from database.models import JobMatch, JobMatchRequirement
from core.scorer.models import ScoredJobMatch
from core.matcher.dto import MatchResultDTO

logger = logging.getLogger(__name__)

ScoredMatch = ScoredJobMatch | MatchResultDTO


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


def _extract_job_data(scored_match: ScoredMatch):
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


def _extract_requirement_match_data(requirement_match, *, use_getattr: bool):
    evidence = requirement_match.evidence
    if use_getattr:
        evidence_text = getattr(evidence, 'text', '') if evidence else ""
        evidence_section = getattr(evidence, 'source_section', None) if evidence else None
        evidence_tags = getattr(evidence, 'tags', {}) if evidence else {}
    else:
        evidence_text = evidence.text if evidence else ""
        evidence_section = evidence.source_section if evidence else None
        evidence_tags = evidence.tags if evidence else {}

    return {
        'requirement_id': str(requirement_match.requirement.id),
        'req_type': requirement_match.requirement.req_type,
        'evidence_text': evidence_text,
        'evidence_section': evidence_section,
        'evidence_tags': evidence_tags,
        'similarity': requirement_match.similarity,
        'is_covered': requirement_match.is_covered,
    }


def _extract_missing_requirement_data(requirement_match):
    return {
        'requirement_id': str(requirement_match.requirement.id),
        'req_type': requirement_match.requirement.req_type,
        'similarity': requirement_match.similarity,
    }


def _extract_requirement_matches(scored_match: ScoredMatch):
    """Extract requirement matches from either ORM object or DTO."""
    if isinstance(scored_match, MatchResultDTO):
        requirement_matches = scored_match.requirement_matches
        use_getattr = False
    else:
        requirement_matches = scored_match.matched_requirements
        use_getattr = True

    matched = [
        _extract_requirement_match_data(req, use_getattr=use_getattr)
        for req in requirement_matches
    ]
    missing = [
        _extract_missing_requirement_data(req)
        for req in scored_match.missing_requirements
    ]
    return matched, missing


def _extract_scores(scored_match: ScoredMatch):
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
            },
            'jd_required_coverage': scored_match.jd_required_coverage,
            'jd_preferences_coverage': scored_match.jd_preferences_coverage,
            'match_type': scored_match.match_type,
        }


def _build_match_values(scores, matched_reqs, missing_reqs, job_content_hash):
    total_requirements = len(matched_reqs) + len(missing_reqs)
    return {
        'job_similarity': _to_float(scores['job_similarity']),
        'fit_score': _to_float(scores['fit_score']),
        'want_score': _to_float(scores['want_score']),
        'overall_score': _to_float(scores['overall_score']),
        'fit_components': _to_native_types(scores['fit_components']),
        'want_components': _to_native_types(scores['want_components']),
        'fit_weight': scores['fit_weight'],
        'want_weight': scores['want_weight'],
        'base_score': _to_float(scores['base_score']),
        'penalties': _to_float(scores['penalties']),
        'penalty_details': scores['penalty_details'],
        'required_coverage': _to_float(scores['jd_required_coverage']),
        'preferred_coverage': _to_float(scores['jd_preferences_coverage']),
        'total_requirements': total_requirements,
        'matched_requirements_count': len(matched_reqs),
        'match_type': scores['match_type'],
        'job_content_hash': job_content_hash,
        'calculated_at': func.now(),
    }


def _apply_match_values(match_record: JobMatch, values) -> None:
    match_record.job_similarity = values['job_similarity']
    match_record.fit_score = values['fit_score']
    match_record.want_score = values['want_score']
    match_record.overall_score = values['overall_score']
    match_record.fit_components = values['fit_components']
    match_record.want_components = values['want_components']
    match_record.fit_weight = values['fit_weight']
    match_record.want_weight = values['want_weight']
    match_record.base_score = values['base_score']
    match_record.penalties = values['penalties']
    match_record.penalty_details = values['penalty_details']
    match_record.required_coverage = values['required_coverage']
    match_record.preferred_coverage = values['preferred_coverage']
    match_record.total_requirements = values['total_requirements']
    match_record.matched_requirements_count = values['matched_requirements_count']
    match_record.match_type = values['match_type']
    match_record.job_content_hash = values['job_content_hash']
    match_record.calculated_at = values['calculated_at']


def _find_existing_match(repo, job_id: str, resume_fingerprint: str):
    existing_stmt = select(JobMatch).where(
        JobMatch.job_post_id == job_id,
        JobMatch.resume_fingerprint == resume_fingerprint,
    )
    existing = repo.db.execute(existing_stmt).scalar_one_or_none()
    return existing_stmt, existing


def _resolve_hidden_state(repo, job_id: str, existing: JobMatch | None) -> bool:
    if existing:
        return existing.is_hidden

    hidden_stmt = select(JobMatch).where(
        JobMatch.job_post_id == job_id,
        JobMatch.is_hidden.is_(True),
    ).limit(1)
    hidden_match = repo.db.execute(hidden_stmt).scalar_one_or_none()
    return bool(hidden_match)


def _create_match_record(scored_match: ScoredMatch, values, is_hidden: bool) -> JobMatch:
    return JobMatch(
        job_post_id=values['job_post_id'],
        resume_fingerprint=scored_match.resume_fingerprint,
        job_similarity=values['job_similarity'],
        fit_score=values['fit_score'],
        want_score=values['want_score'],
        overall_score=values['overall_score'],
        fit_components=values['fit_components'],
        want_components=values['want_components'],
        fit_weight=values['fit_weight'],
        want_weight=values['want_weight'],
        base_score=values['base_score'],
        penalties=values['penalties'],
        penalty_details=values['penalty_details'],
        required_coverage=values['required_coverage'],
        preferred_coverage=values['preferred_coverage'],
        total_requirements=values['total_requirements'],
        matched_requirements_count=values['matched_requirements_count'],
        match_type=values['match_type'],
        job_content_hash=values['job_content_hash'],
        notified=False,
        is_hidden=is_hidden,
        calculated_at=values['calculated_at'],
    )


def _upsert_match_record(
    repo,
    scored_match: ScoredMatch,
    existing: JobMatch | None,
    values,
    is_hidden: bool,
    is_stale_replacement: bool,
) -> JobMatch:
    if existing and not is_stale_replacement:
        existing.status = 'active'
        _apply_match_values(existing, values)
        return existing

    match_record = _create_match_record(scored_match, values, is_hidden)
    repo.db.add(match_record)
    return match_record


def _flush_match_record(repo, match_record: JobMatch, existing_stmt, job_id: str) -> tuple[JobMatch, bool]:
    from sqlalchemy.exc import IntegrityError

    try:
        repo.db.flush()
        return match_record, False
    except IntegrityError:
        repo.db.rollback()
        logger.warning("Race condition detected for job %s, refetching existing match", job_id)
        existing = repo.db.execute(existing_stmt).scalar_one_or_none()
        if not existing:
            raise
        return existing, True


def _delete_existing_requirements(repo, match_record: JobMatch, should_replace: bool) -> None:
    if not should_replace:
        return

    repo.db.execute(
        delete(JobMatchRequirement).where(
            JobMatchRequirement.job_match_id == match_record.id
        )
    )


def _build_requirement_record(match_record: JobMatch, requirement_data, *, is_missing: bool) -> JobMatchRequirement:
    return JobMatchRequirement(
        job_match_id=match_record.id,
        job_requirement_unit_id=requirement_data['requirement_id'],
        evidence_text="" if is_missing else requirement_data['evidence_text'],
        evidence_section=None if is_missing else requirement_data['evidence_section'],
        evidence_tags={} if is_missing else requirement_data['evidence_tags'],
        similarity_score=_to_float(requirement_data['similarity']),
        is_covered=False if is_missing else requirement_data['is_covered'],
        req_type=requirement_data['req_type'],
    )


def _persist_requirement_matches(repo, match_record: JobMatch, matched_reqs, missing_reqs) -> None:
    for req in matched_reqs:
        repo.db.add(_build_requirement_record(match_record, req, is_missing=False))

    for req in missing_reqs:
        repo.db.add(_build_requirement_record(match_record, req, is_missing=True))


def save_match_to_db(
    scored_match: ScoredMatch,
    repo,
    is_stale_replacement: bool = False,
) -> JobMatch:
    """
    Save scored match to database.

    Creates JobMatch record with associated JobMatchRequirement records.
    Accepts either a ScoredJobMatch ORM object or a MatchResultDTO.

    Args:
        scored_match: ScoredJobMatch instance or MatchResultDTO with all match details
        repo: JobRepository instance for database access
        is_stale_replacement: If True, creates new record instead of updating existing
                              (used when job content changed and old match was marked stale)

    Returns:
        JobMatch record that was created or updated
    """
    job_data = _extract_job_data(scored_match)
    job_id = job_data['id']
    job_content_hash = job_data['content_hash']
    scores = _extract_scores(scored_match)
    matched_reqs, missing_reqs = _extract_requirement_matches(scored_match)
    existing_stmt, existing = _find_existing_match(
        repo,
        job_id,
        scored_match.resume_fingerprint,
    )
    values = _build_match_values(scores, matched_reqs, missing_reqs, job_content_hash)
    values['job_post_id'] = job_id
    is_hidden = _resolve_hidden_state(repo, job_id, existing)
    match_record = _upsert_match_record(
        repo,
        scored_match,
        existing,
        values,
        is_hidden,
        is_stale_replacement,
    )
    match_record, reused_existing_record = _flush_match_record(
        repo,
        match_record,
        existing_stmt,
        job_id,
    )
    should_replace_requirements = not is_stale_replacement and (
        existing is not None or reused_existing_record
    )
    match_record.status = 'active'
    _apply_match_values(match_record, values)
    repo.db.flush()

    _delete_existing_requirements(repo, match_record, should_replace_requirements)
    _persist_requirement_matches(repo, match_record, matched_reqs, missing_reqs)

    repo.db.commit()

    logger.info(f"Saved match for job {job_id}: fit={scores['fit_score']:.1f}, want={scores['want_score']:.1f}, overall={scores['overall_score']:.1f}")

    return match_record
