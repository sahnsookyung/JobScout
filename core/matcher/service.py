#!/usr/bin/env python3
"""
Matcher Service - Stage 1: Vector Retrieval.

Performs two-level matching:
1. Job-level: Resume vs JobPost.summary_embedding (JD alignment)
2. Requirement-level: Resume Evidence Units (REUs) vs JobRequirementUnit embeddings

Designed to be microservice-ready and can run independently
of the ScoringService.
"""
from typing import List, Dict, Any, Optional
import logging

from database.repository import JobRepository
from database.models import JobPost, generate_resume_fingerprint
from core.config_loader import MatcherConfig
from core.matcher.models import (
    RequirementMatchResult, JobMatchPreliminary
)
from core.matcher.requirement_matcher import RequirementMatcher
from etl.resume import ResumeProfiler, ResumeEvidenceUnit

logger = logging.getLogger(__name__)


class MatcherService:
    """
    Service for Stage 1: Vector Retrieval with Preferences.

    Matches resume to jobs at two levels:
    - Job-level: Overall JD alignment using summary embeddings + preferences
    - Requirement-level: Skills matching using requirement embeddings

    Designed to be independent - can be run as separate microservice.
    """

    def __init__(
        self,
        repo: JobRepository,
        resume_profiler: ResumeProfiler,
        config: MatcherConfig
    ):
        """
        Initialize matcher service with dependencies.

        Args:
            repo: JobRepository for DB operations
            resume_profiler: ResumeProfiler for extraction and embedding
            config: MatcherConfig with matching parameters
        """
        self.repo = repo
        self.resume_profiler = resume_profiler
        self.config = config
        self.requirement_matcher = RequirementMatcher(
            repo=repo,
            similarity_threshold=config.similarity_threshold
        )

    def match_resume_to_job(
        self,
        evidence_units: List['ResumeEvidenceUnit'],
        job: JobPost,
        resume_fingerprint: str,
        preferences: Optional[Dict[str, Any]] = None
    ) -> JobMatchPreliminary:
        """
        Match resume to a single job at both levels.

        Args:
            evidence_units: Extracted resume evidence
            job: Job to match against
            resume_fingerprint: Fingerprint for tracking
            preferences: Optional preferences for enhanced matching

        Returns:
            JobMatchPreliminary with all similarities computed
        """
        matched_requirements, missing_requirements = self.requirement_matcher.match_requirements(
            evidence_units, job.requirements, resume_fingerprint
        )

        return JobMatchPreliminary(
            job=job,
            job_similarity=0.0,
            requirement_matches=matched_requirements,
            missing_requirements=missing_requirements,
            resume_fingerprint=resume_fingerprint
        )

    def match_resume_to_jobs(
        self,
        evidence_units: List['ResumeEvidenceUnit'],
        jobs: List[JobPost],
        resume_data: Dict[str, Any],
        preferences: Optional[Dict[str, Any]] = None
    ) -> List[JobMatchPreliminary]:
        """
        Match resume to multiple jobs.

        Args:
            evidence_units: Extracted resume evidence
            jobs: Jobs to match against
            resume_data: Full resume data
            preferences: Optional preferences for enhanced matching

        Returns:
            List of preliminary matches for all jobs, sorted by combined score
        """
        resume_fingerprint = generate_resume_fingerprint(resume_data)

        results = []
        for job in jobs:
            preliminary = self.match_resume_to_job(
                evidence_units, job, resume_fingerprint, preferences
            )
            results.append(preliminary)

        results.sort(key=lambda p: p.job_similarity, reverse=True)
        return results

    def match_resume_two_stage(
        self,
        resume_data: Dict[str, Any],
        preferences: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[Any] = None,
    ) -> List[JobMatchPreliminary]:
        """
        Two-stage matching pipeline: retrieve candidates -> compute preliminaries.

        Stage 1: Retrieve top-K candidates using vector similarity on summary_embedding
        Stage 2: Compute requirement-level matching only on retrieved candidates

        Args:
            resume_data: Full resume data
            preferences: Optional user preferences for enhanced matching
            tenant_id: Optional tenant filter for Stage 1

        Returns:
            List of preliminary matches for top-K candidates
        """
        profile, evidence_units, _ = self.resume_profiler.profile_resume(resume_data)

        if not evidence_units:
            logger.warning("No evidence units extracted from resume")
            return []

        resume_fingerprint = generate_resume_fingerprint(resume_data)

        # Stage 1: Get top jobs by summary embedding similarity
        resume_embedding = self.repo.get_resume_summary_embedding(resume_fingerprint)
        if resume_embedding:
            candidate_jobs = self.repo.get_top_jobs_by_summary_embedding(
                resume_embedding=resume_embedding,
                limit=self.config.batch_size
            )
            logger.debug(f"Stage 1: Retrieved {len(candidate_jobs)} candidates via vector similarity")
        else:
            # Fallback: Get all embedded jobs if no summary embedding
            candidate_jobs = self.repo.get_jobs_for_matching(limit=self.config.batch_size)
            logger.warning(f"Stage 1: No summary embedding, using fallback ({len(candidate_jobs)} jobs)")

        if not candidate_jobs:
            logger.warning("No matching candidates found in Stage 1")
            return []

        logger.debug(f"Stage 1: Retrieved {len(candidate_jobs)} candidates")

        results = []
        for job in candidate_jobs:
            preliminary = self.match_resume_to_job(
                evidence_units, job, resume_fingerprint, preferences
            )
            results.append(preliminary)

        results.sort(key=lambda p: p.job_similarity, reverse=True)

        return results

    def get_jobs_for_matching(
        self,
        limit: Optional[int] = None
    ) -> List[JobPost]:
        """
        Fetch jobs that are ready for matching.
        
        TODO: This method is deprecated. Use get_top_jobs_by_summary_embedding() instead
        which uses vector similarity for Stage 1 filtering.
        """
        # TODO: Use get_top_jobs_by_summary_embedding() with resume embedding
        return self.repo.get_jobs_for_matching(limit=limit)
