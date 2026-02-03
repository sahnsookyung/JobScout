#!/usr/bin/env python3
"""
Scoring Service - Stage 2: Rule-based Scoring with Fit/Want scores.

Takes preliminary matches from MatcherService and calculates final scores:
- Fit Score: "Can do the job" (requirements + JD similarity - capability penalties)
- Want Score: "Matches what I want" (facet embeddings vs user wants)
- Overall Score: Weighted combination of Fit and Want

Designed to be microservice-ready and can run independently
of the MatcherService.
"""

from typing import List, Optional, Any, Dict
import logging
import numpy as np
from sqlalchemy import select

from database.repository import JobRepository
from database.models import StructuredResume, JobFacetEmbedding
from core.config_loader import ScorerConfig
from core.matcher import JobMatchPreliminary

from core.scorer.models import ScoredJobMatch
from core.scorer import coverage, scoring_modes, preferences
from core.scorer import penalties as penalty_calculations
from core.scorer import fit_score, want_score

logger = logging.getLogger(__name__)


class ScoringService:
    """
    Service for Stage 2: Rule-based Scoring with Preferences.

    Calculates final scores from preliminary matches:
    - Coverage percentages
    - Preferences alignment boost
    - Weighted base score
    - Penalties for mismatches
    - Final overall score

    Supports configurable ranking modes:
    - Discovery mode: optimize for breadth/recall
    - Strict mode: optimize for precision with coverage gates

    Designed to be independent - can be run as separate microservice.
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
        match_type: str = "requirements_only"
    ) -> ScoredJobMatch:
        """
        Calculate final score from preliminary match.

        Formula: Overall = BaseScore + PreferencesBoost - Penalties
        """
        job = preliminary.job

        # Calculate coverage
        required_coverage, preferred_coverage = coverage.calculate_coverage(
            preliminary.requirement_matches,
            preliminary.missing_requirements
        )

        # Calculate base score
        base_score = coverage.calculate_base_score(required_coverage, preferred_coverage, self.config)

        # Calculate preferences boost (bonus for good matches)
        preferences_boost, boost_details = preferences.calculate_preferences_boost(
            preliminary.preferences_alignment,
            self.config
        )

        # Retrieve candidate's years of experience from structured resume
        candidate_total_years = None
        if preliminary.resume_fingerprint:
            stmt = select(StructuredResume).where(
                StructuredResume.resume_fingerprint == preliminary.resume_fingerprint
            )
            structured_resume = self.repo.db.execute(stmt).scalar_one_or_none()
            if structured_resume and structured_resume.calculated_total_years:
                candidate_total_years = float(structured_resume.calculated_total_years)
                logger.debug(f"Candidate has {candidate_total_years:.1f} years of experience")

        # Calculate penalties (including from preferences and experience shortfall)
        penalties, penalty_details = penalty_calculations.calculate_penalties(
            job,
            required_coverage,
            preliminary.requirement_matches,
            preliminary.missing_requirements,
            self.config,
            preliminary.preferences_alignment,
            resume_fingerprint=preliminary.resume_fingerprint,
            repo=self.repo
        )

        # Final score (never negative)
        overall_score = max(0.0, base_score + preferences_boost - penalties)

        # Log if preferences contributed significantly
        if preferences_boost > 0:
            logger.debug(f"Job {job.id}: +{preferences_boost:.1f} boost from preferences")

        return ScoredJobMatch(
            job=job,
            overall_score=overall_score,
            base_score=base_score,
            preferences_boost=preferences_boost,
            penalties=penalties,
            required_coverage=required_coverage,
            preferred_coverage=preferred_coverage,
            job_similarity=preliminary.job_similarity,
            preferences_alignment=preliminary.preferences_alignment,
            penalty_details=penalty_details + [boost_details] if preferences_boost > 0 else penalty_details,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            resume_fingerprint=preliminary.resume_fingerprint,
            match_type=match_type
        )

    def score_matches(
        self,
        preliminary_matches: List[JobMatchPreliminary],
        match_type: str = "requirements_only"
    ) -> List[ScoredJobMatch]:
        """
        Score multiple preliminary matches.

        Returns sorted by overall score (highest first).
        """
        scored_matches = []

        for preliminary in preliminary_matches:
            scored = self.score_preliminary_match(preliminary, match_type)
            scored_matches.append(scored)

        # Sort by overall score descending
        scored_matches.sort(key=lambda x: x.overall_score, reverse=True)

        return scored_matches

    def score_preliminary_match_with_mode(
        self,
        preliminary: JobMatchPreliminary,
        ranking_config: Any,
        match_type: str = "requirements_only"
    ) -> ScoredJobMatch:
        """
        Calculate final score from preliminary match using mode-specific formula.

        Supports discovery and strict modes with different scoring formulas:
        - Discovery: optimize for breadth/recall (FR-6.1)
        - Strict: optimize for precision with coverage gates (FR-6.2)

        Args:
            preliminary: Preliminary match from MatcherService
            ranking_config: RankingConfig (contains mode and mode-specific settings)
            match_type: Type of match being performed

        Returns:
            ScoredJobMatch with mode-specific score and component breakdown
        """
        job = preliminary.job
        mode = ranking_config.mode

        # Calculate coverage (common to both modes)
        required_coverage, preferred_coverage = coverage.calculate_coverage(
            preliminary.requirement_matches,
            preliminary.missing_requirements
        )

        # Retrieve candidate's years of experience from structured resume
        candidate_total_years = None
        if preliminary.resume_fingerprint:
            stmt = select(StructuredResume).where(
                StructuredResume.resume_fingerprint == preliminary.resume_fingerprint
            )
            structured_resume = self.repo.db.execute(stmt).scalar_one_or_none()
            if structured_resume and structured_resume.calculated_total_years:
                candidate_total_years = float(structured_resume.calculated_total_years)
                logger.debug(f"Candidate has {candidate_total_years:.1f} years of experience")

        # Calculate penalties (with mode-specific handling)
        penalties, penalty_details = penalty_calculations.calculate_penalties(
            job,
            required_coverage,
            preliminary.requirement_matches,
            preliminary.missing_requirements,
            self.config,
            preliminary.preferences_alignment,
            resume_fingerprint=preliminary.resume_fingerprint,
            repo=self.repo
        )

        if mode == "discovery":
            # Discovery mode: optimize for breadth/recall (FR-6.1)
            mode_config = ranking_config.discovery

            # Missing required policy: disabled by default in discovery
            if mode_config.missing_required_policy == "disabled":
                # Remove missing_required penalties to avoid double-counting
                penalties = sum(p['amount'] for p in penalty_details if p['type'] != 'missing_required')
                penalty_details = [p for p in penalty_details if p['type'] != 'missing_required']

            overall_score, score_components = scoring_modes.calculate_discovery_score(
                job_similarity=preliminary.job_similarity,
                required_coverage=required_coverage,
                preferred_coverage=preferred_coverage,
                preferences_alignment=preliminary.preferences_alignment,
                soft_penalties=penalties,
                ranking_config=mode_config
            )

            # Calculate base_score and preferences_boost for backward compatibility
            base_score = score_components.get('blended_score', 0.0) * 100.0
            preferences_boost = 0.0

        else:
            # Strict mode: optimize for precision with coverage gates (FR-6.2)
            mode_config = ranking_config.strict

            overall_score, score_components = scoring_modes.calculate_strict_score(
                job_similarity=preliminary.job_similarity,
                required_coverage=required_coverage,
                preferred_coverage=preferred_coverage,
                preferences_alignment=preliminary.preferences_alignment,
                penalties=penalties,
                ranking_config=mode_config
            )

            # Calculate base_score and preferences_boost for backward compatibility
            base_score = score_components.get('blended_score', 0.0) * 100.0
            preferences_boost = 0.0

        # Log score components for debugging
        logger.debug(f"Job {job.id} ({mode} mode): score={overall_score:.1f}, "
                    f"req_cov={required_coverage:.2f}, sim={preliminary.job_similarity:.2f}")

        return ScoredJobMatch(
            job=job,
            overall_score=overall_score,
            base_score=base_score,
            preferences_boost=preferences_boost,
            penalties=penalties,
            required_coverage=required_coverage,
            preferred_coverage=preferred_coverage,
            job_similarity=preliminary.job_similarity,
            preferences_alignment=preliminary.preferences_alignment,
            penalty_details=penalty_details,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            resume_fingerprint=preliminary.resume_fingerprint,
            match_type=match_type,
            ranking_mode=mode,
            score_components=score_components
        )

    def score_matches_with_mode(
        self,
        preliminary_matches: List[JobMatchPreliminary],
        ranking_config: Any,
        match_type: str = "requirements_only",
        final_results_n: Optional[int] = None
    ) -> List[ScoredJobMatch]:
        """
        Score multiple preliminary matches using mode-specific formula.

        Args:
            preliminary_matches: List of preliminary matches from Stage 2
            ranking_config: RankingConfig with mode and mode-specific settings
            match_type: Type of match being performed
            final_results_n: Optional limit on final results (defaults to mode config)

        Returns:
            List of ScoredJobMatch sorted by overall score (highest first),
            limited to final_results_n
        """
        scored_matches = []

        for preliminary in preliminary_matches:
            scored = self.score_preliminary_match_with_mode(
                preliminary, ranking_config, match_type
            )
            scored_matches.append(scored)

        # Sort by overall score descending
        scored_matches.sort(key=lambda x: x.overall_score, reverse=True)

        # Apply final results limit based on mode
        if final_results_n is None:
            if ranking_config.mode == "discovery":
                final_results_n = ranking_config.discovery.final_results_n
            else:
                final_results_n = ranking_config.strict.final_results_n

        limited_matches = scored_matches[:final_results_n]

        logger.info(f"Scored {len(scored_matches)} matches in {ranking_config.mode} mode, "
                   f"returning top {len(limited_matches)}")

        return limited_matches

    def score_preliminary_match_fit_want(
        self,
        preliminary: JobMatchPreliminary,
        user_want_embeddings: List[np.ndarray],
        job_facet_embeddings: Dict[str, np.ndarray],
        match_type: str = "requirements_only"
    ) -> ScoredJobMatch:
        """
        Calculate Fit, Want, and Overall scores for a preliminary match.

        Args:
            preliminary: Preliminary match from MatcherService
            user_want_embeddings: List of embedding vectors for user wants
            job_facet_embeddings: Dict mapping facet_key -> embedding vector
            match_type: Type of match being performed

        Returns:
            ScoredJobMatch with fit_score, want_score, overall_score
        """
        job = preliminary.job

        required_coverage, preferred_coverage = coverage.calculate_coverage(
            preliminary.requirement_matches,
            preliminary.missing_requirements
        )

        fit_penalties, penalty_details = penalty_calculations.calculate_fit_penalties(
            job=job,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            config=self.config,
            resume_fingerprint=preliminary.resume_fingerprint,
            repo=self.repo
        )

        fit_score_value, fit_components = fit_score.calculate_fit_score(
            job_similarity=preliminary.job_similarity,
            required_coverage=required_coverage,
            preferred_coverage=preferred_coverage,
            fit_penalties=fit_penalties,
            config=self.config
        )

        want_score_value, want_components = want_score.calculate_want_score(
            user_want_embeddings=user_want_embeddings,
            job_facet_embeddings=job_facet_embeddings,
            facet_weights=self.config.facet_weights
        )

        overall_score = min(100.0,
            self.config.fit_weight * fit_score_value + self.config.want_weight * want_score_value
        )

        logger.debug(f"Job {job.id}: fit={fit_score_value:.1f}, want={want_score_value:.1f}, overall={overall_score:.1f}")

        return ScoredJobMatch(
            job=job,
            fit_score=fit_score_value,
            want_score=want_score_value,
            overall_score=overall_score,
            fit_components=fit_components,
            want_components=want_components,
            base_score=fit_components.get('blended', 0.0) * 100.0,
            preferences_boost=0.0,
            penalties=fit_penalties,
            required_coverage=required_coverage,
            preferred_coverage=preferred_coverage,
            job_similarity=preliminary.job_similarity,
            preferences_alignment=preliminary.preferences_alignment,
            penalty_details=penalty_details,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            resume_fingerprint=preliminary.resume_fingerprint,
            match_type=match_type
        )

    def score_matches_fit_want(
        self,
        preliminary_matches: List[JobMatchPreliminary],
        user_want_embeddings: List[np.ndarray],
        job_facet_embeddings_map: Dict[str, Dict[str, np.ndarray]],
        match_type: str = "requirements_only"
    ) -> List[ScoredJobMatch]:
        """
        Score multiple preliminary matches with Fit/Want/Overall scores.

        Args:
            preliminary_matches: List of preliminary matches
            user_want_embeddings: User wants embeddings
            job_facet_embeddings_map: Dict mapping job_id -> facet embeddings
            match_type: Type of match being performed

        Returns:
            List of ScoredJobMatch sorted by overall score
        """
        scored_matches = []

        for preliminary in preliminary_matches:
            job_id = str(preliminary.job.id)
            job_facets = job_facet_embeddings_map.get(job_id, {})

            scored = self.score_preliminary_match_fit_want(
                preliminary=preliminary,
                user_want_embeddings=user_want_embeddings,
                job_facet_embeddings=job_facets,
                match_type=match_type
            )
            scored_matches.append(scored)

        scored_matches.sort(key=lambda x: x.overall_score, reverse=True)

        return scored_matches
