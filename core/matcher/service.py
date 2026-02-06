#!/usr/bin/env python3
"""
Matcher Service - Stage 1: Vector Retrieval.

Performs two-level matching:
1. Job-level: Resume vs JobPost.summary_embedding (JD alignment)
2. Requirement-level: Resume Evidence Units (REUs) vs JobRequirementUnit embeddings

Designed to be microservice-ready and can run independently
of the ScoringService.
"""
from typing import List, Dict, Any, Optional, Tuple
import logging
import warnings

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
        resume_profiler: ResumeProfiler,
        config: MatcherConfig
    ):
        """
        Initialize matcher service with dependencies.

        Args:
            resume_profiler: ResumeProfiler for extraction and embedding
            config: MatcherConfig with matching parameters
        """
        self.resume_profiler = resume_profiler
        self.config = config
        self.requirement_matcher = RequirementMatcher(
            similarity_threshold=config.similarity_threshold
        )

    def match_resume_to_job(
        self,
        repo: JobRepository,
        job: JobPost,
        resume_fingerprint: str,
        job_similarity: float = 0.0,
    ) -> JobMatchPreliminary:
        """
        Match resume to a single job at both levels.

        Args:
            repo: JobRepository for DB operations
            job: Job to match against
            resume_fingerprint: Fingerprint for tracking
            job_similarity: Pre-computed job-level similarity (from summary embedding)

        Returns:
            JobMatchPreliminary with all similarities computed
        """
        matched_requirements, missing_requirements = self.requirement_matcher.match_requirements(
            repo, job.requirements, resume_fingerprint
        )

        return JobMatchPreliminary(
            job=job,
            job_similarity=job_similarity,
            requirement_matches=matched_requirements,
            missing_requirements=missing_requirements,
            resume_fingerprint=resume_fingerprint
        )

    def match_resume_two_stage(
        self,
        repo: JobRepository,
        resume_data: Dict[str, Any],
        tenant_id: Optional[Any] = None,
    ) -> List[JobMatchPreliminary]:
        """
        Two-stage matching pipeline: retrieve candidates -> compute preliminaries.

        Stage 1: Retrieve top-K candidates using vector similarity on summary_embedding
        Stage 2: Compute requirement-level matching only on retrieved candidates

        Args:
            repo: JobRepository for DB operations
            resume_data: Full resume data
            tenant_id: Optional tenant filter for Stage 1

        Returns:
            List of preliminary matches for top-K candidates
        """
        profile, evidence_units, _ = self.resume_profiler.profile_resume(resume_data)

        if not evidence_units:
            logger.warning("No evidence units extracted from resume")
            return []

        resume_fingerprint = generate_resume_fingerprint(resume_data)

        results: List[JobMatchPreliminary] = []

        resume_embedding = repo.get_resume_summary_embedding(resume_fingerprint)
        if resume_embedding:
            job_similarity_pairs: List[Tuple[JobPost, float]] = repo.get_top_jobs_by_summary_embedding(
                resume_embedding=resume_embedding,
                limit=self.config.batch_size,
                tenant_id=tenant_id
            )
            logger.debug(f"Stage 1: Retrieved {len(job_similarity_pairs)} candidates via vector similarity")

            for job, job_similarity in job_similarity_pairs:
                preliminary = self.match_resume_to_job(
                    repo, job, resume_fingerprint, job_similarity
                )
                results.append(preliminary)
        else:
            candidate_jobs: List[JobPost] = repo.get_jobs_for_matching(limit=self.config.batch_size)
            logger.warning(f"Stage 1: No summary embedding, using fallback ({len(candidate_jobs)} jobs)")

            for job in candidate_jobs:
                preliminary = self.match_resume_to_job(
                    repo, job, resume_fingerprint, job_similarity=0.0
                )
                results.append(preliminary)

        if not results:
            logger.warning("No matching candidates found in Stage 1")
            return []

        logger.debug(f"Stage 1: Retrieved {len(results)} candidates")

        results.sort(key=lambda p: p.job_similarity, reverse=True)

        return results

    def get_jobs_for_matching(
        self,
        repo: JobRepository,
        limit: Optional[int] = None
    ) -> List[JobPost]:
        """
        Fetch jobs that are ready for matching.

        Deprecated: Use get_top_jobs_by_summary_embedding() instead
        which uses vector similarity for Stage 1 filtering.

        Args:
            repo: JobRepository for DB operations
            limit: Maximum number of jobs to fetch
        """
        warnings.warn(
            "get_jobs_for_matching() is deprecated. "
            "Use get_top_jobs_by_summary_embedding() instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return repo.get_jobs_for_matching(limit=limit)
