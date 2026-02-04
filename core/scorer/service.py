#!/usr/bin/env python3
"""
Scoring Service - Stage 2: Rule-based Scoring with Fit/Want scores.

Takes preliminary matches from MatcherService and calculates final scores:
- Fit Score: "Can do the job" (requirements + JD similarity - capability penalties)
- Want Score: "Matches what I want" (facet embeddings vs user wants) - optional
- Overall Score: Fit + optional Want blend

Post-scoring ResultPolicy can be applied to filter and truncate results.

Designed to be microservice-ready and can run independently
of the MatcherService.
"""

from typing import List, Optional, Any, Dict
import logging
import numpy as np
from sqlalchemy import select

from database.repository import JobRepository
from database.models import StructuredResume, ResumeSectionEmbedding
from core.config_loader import ScorerConfig, ResultPolicy
from core.matcher import JobMatchPreliminary

from core.scorer.models import ScoredJobMatch
from core.scorer import coverage
from core.scorer import penalties as penalty_calculations
from core.scorer import fit_score, want_score

logger = logging.getLogger(__name__)


def _prefetch_candidate_data(
    preliminary_matches: List[JobMatchPreliminary],
    db
) -> Dict[str, Dict[str, Any]]:
    """Batch prefetch StructuredResume data for all preliminary matches.

    Executes a single query with WHERE ... IN (...) clause to fetch all
    resume data at once, eliminating N+1 queries.

    Args:
        preliminary_matches: List of preliminary matches containing resume_fingerprints
        db: SQLAlchemy session or connection

    Returns:
        Dict mapping resume_fingerprint -> {'calculated_total_years': Optional[float]}
    """
    fingerprints = {
        pm.resume_fingerprint
        for pm in preliminary_matches
        if pm.resume_fingerprint
    }
    if not fingerprints:
        return {}

    stmt = (
        select(
            StructuredResume.resume_fingerprint,
            StructuredResume.calculated_total_years
        )
        .where(StructuredResume.resume_fingerprint.in_(fingerprints))
    )
    rows = db.execute(stmt).fetchall()

    return {
        fp: {'calculated_total_years': float(years) if years is not None else None}
        for fp, years in rows
    }


def _prefetch_experience_sections(
    preliminary_matches: List[JobMatchPreliminary],
    db
) -> Dict[str, List[Dict[str, Any]]]:
    """Batch prefetch ResumeSectionEmbedding data for all preliminary matches.

    Executes a single query with WHERE ... IN (...) clause to fetch all
    experience sections at once.

    Args:
        preliminary_matches: List of preliminary matches containing resume_fingerprints
        db: SQLAlchemy session or connection

    Returns:
        Dict mapping resume_fingerprint -> List of experience section dicts
    """
    fingerprints = {
        pm.resume_fingerprint
        for pm in preliminary_matches
        if pm.resume_fingerprint
    }
    if not fingerprints:
        return {}

    stmt = (
        select(ResumeSectionEmbedding)
        .where(
            ResumeSectionEmbedding.resume_fingerprint.in_(fingerprints),
            ResumeSectionEmbedding.section_type == 'experience'
        )
    )
    rows = db.execute(stmt).scalars().all()

    result: Dict[str, List[Dict[str, Any]]] = {fp: [] for fp in fingerprints}
    for row in rows:
        result[row.resume_fingerprint].append({
            'source_data': row.source_data,
            'source_text': row.source_text,
            'section_type': row.section_type,
            'section_index': row.section_index,
            'has_embedding': row.embedding is not None,
        })

    return result


def _apply_result_policy(
    results: List[ScoredJobMatch],
    policy: Optional[ResultPolicy]
) -> List[ScoredJobMatch]:
    """Apply ResultPolicy to filter and truncate results.

    Args:
        results: List of scored matches (already sorted by overall_score)
        policy: ResultPolicy to apply, or None for no filtering

    Returns:
        Filtered and truncated results
    """
    if policy is None:
        return results

    filtered = results

    if policy.min_fit > 0:
        filtered = [r for r in filtered if r.fit_score >= policy.min_fit]

    if policy.min_jd_required_coverage is not None:
        filtered = [r for r in filtered if r.jd_required_coverage >= policy.min_jd_required_coverage]

    return filtered[:policy.top_k]


class ScoringService:
    """
    Service for Stage 2: Rule-based Scoring with Preferences.

    Calculates final scores from preliminary matches:
    - Fit Score: "Can do the job"
    - Want Score: "Matches what I want" (optional, requires user wants)
    - Overall Score: Weighted combination (or Fit-only if no wants)

    Supports ResultPolicy for post-scoring filtering and truncation.
    """

    def __init__(
        self,
        repo: JobRepository,
        config: ScorerConfig
    ):
        self.repo = repo
        self.config = config

    def score_preliminary_match(
        self,
        preliminary: JobMatchPreliminary,
        match_type: str = "requirements_only",
        candidate_data: Optional[Dict[str, Any]] = None,
        experience_sections: Optional[List[Dict[str, Any]]] = None,
        user_want_embeddings: Optional[List[np.ndarray]] = None,
        job_facet_embeddings: Optional[Dict[str, np.ndarray]] = None,
        result_policy: Optional[ResultPolicy] = None
    ) -> ScoredJobMatch:
        """Calculate final score from preliminary match.

        Computes Fit score (always) and optional Want score (if embeddings exist).
        Overall score is Fit-only or Fit+Want blend depending on wants presence.

        Args:
            preliminary: Preliminary match from MatcherService
            match_type: Type of match being performed
            candidate_data: Pre-fetched candidate data (calculated_total_years)
            experience_sections: Pre-fetched experience sections for penalties
            user_want_embeddings: Optional user want embeddings for Want score
            job_facet_embeddings: Optional job facet embeddings for Want score
            result_policy: Optional policy to apply (used in batch scoring)

        Returns:
            ScoredJobMatch with fit_score, optional want_score, and overall_score
        """
        job = preliminary.job

        jd_required_coverage, jd_preferences_coverage = coverage.calculate_coverage(
            preliminary.requirement_matches,
            preliminary.missing_requirements
        )

        candidate_total_years = None
        if candidate_data:
            candidate_total_years = candidate_data.get('calculated_total_years')
            if candidate_total_years:
                logger.debug(f"Candidate has {candidate_total_years:.1f} years of experience")

        fit_penalties, penalty_details = penalty_calculations.calculate_fit_penalties(
            job=job,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            config=self.config,
            candidate_total_years=candidate_total_years,
            experience_sections=experience_sections
        )

        fit_score_value, fit_components = fit_score.calculate_fit_score(
            job_similarity=preliminary.job_similarity,
            required_coverage=jd_required_coverage,
            preferred_coverage=jd_preferences_coverage,
            fit_penalties=fit_penalties,
            config=self.config
        )

        want_score_value = None
        want_components = None
        if user_want_embeddings and job_facet_embeddings:
            want_score_value, want_components = want_score.calculate_want_score(
                user_want_embeddings=user_want_embeddings,
                job_facet_embeddings=job_facet_embeddings,
                facet_weights=self.config.facet_weights
            )

        if want_score_value is not None:
            overall_score = min(100.0,
                self.config.fit_weight * fit_score_value + self.config.want_weight * want_score_value
            )
        else:
            overall_score = min(100.0, fit_score_value)

        logger.debug(f"Job {job.id}: fit={fit_score_value:.1f}, want={want_score_value or 'N/A'}, overall={overall_score:.1f}")

        return ScoredJobMatch(
            job=job,
            fit_score=fit_score_value,
            want_score=want_score_value if want_score_value is not None else 0.0,
            overall_score=overall_score,
            fit_components=fit_components,
            want_components=want_components if want_components else {},
            base_score=fit_components.get('blended', 0.0) * 100.0,
            preferences_boost=0.0,
            penalties=fit_penalties,
            jd_required_coverage=jd_required_coverage,
            jd_preferences_coverage=jd_preferences_coverage,
            job_similarity=preliminary.job_similarity,
            penalty_details=penalty_details,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            resume_fingerprint=preliminary.resume_fingerprint,
            match_type=match_type,
            policy_applied=None
        )

    def score_matches(
        self,
        preliminary_matches: List[JobMatchPreliminary],
        result_policy: Optional[ResultPolicy] = None,
        user_want_embeddings: Optional[List[np.ndarray]] = None,
        job_facet_embeddings_map: Optional[Dict[str, Dict[str, np.ndarray]]] = None,
        match_type: str = "requirements_only"
    ) -> List[ScoredJobMatch]:
        """Score multiple preliminary matches with Fit + optional Want scores.

        Performs batch prefetch of candidate data to eliminate N+1 queries.
        Applies ResultPolicy at the end for filtering and truncation.

        Args:
            preliminary_matches: List of preliminary matches
            result_policy: Optional policy for post-scoring filtering/truncation
            user_want_embeddings: Optional user want embeddings for Want score
            job_facet_embeddings_map: Optional map of job_id -> facet embeddings
            match_type: Type of match being performed

        Returns:
            List of ScoredJobMatch sorted by overall_score (highest first),
            filtered and truncated by ResultPolicy if provided
        """
        candidate_data = _prefetch_candidate_data(preliminary_matches, self.repo.db)
        experience_sections = _prefetch_experience_sections(preliminary_matches, self.repo.db)

        scored_matches = []

        for preliminary in preliminary_matches:
            cand_data = candidate_data.get(preliminary.resume_fingerprint)
            exp_sections = experience_sections.get(preliminary.resume_fingerprint) if preliminary.resume_fingerprint else None

            job_facets = {}
            if job_facet_embeddings_map:
                job_id = str(preliminary.job.id)
                job_facets = job_facet_embeddings_map.get(job_id, {})

            scored = self.score_preliminary_match(
                preliminary=preliminary,
                match_type=match_type,
                candidate_data=cand_data,
                experience_sections=exp_sections,
                user_want_embeddings=user_want_embeddings,
                job_facet_embeddings=job_facets if job_facets else None,
                result_policy=None
            )
            scored_matches.append(scored)

        scored_matches.sort(key=lambda x: x.overall_score, reverse=True)

        if result_policy:
            scored_matches = _apply_result_policy(scored_matches, result_policy)
            logger.info(f"Scored {len(scored_matches)} matches, "
                       f"returning top {len(scored_matches)} (policy: min_fit={result_policy.min_fit})")

        return scored_matches
