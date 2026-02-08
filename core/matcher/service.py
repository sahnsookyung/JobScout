from typing import Any, Dict, List, Optional, Tuple
import logging
import threading

from database.repository import JobRepository
from database.models import JobPost, generate_resume_fingerprint
from core.config_loader import MatcherConfig
from core.matcher.models import JobMatchPreliminary
from core.matcher.requirement_matcher import RequirementMatcher
from etl.resume import ResumeProfiler

logger = logging.getLogger(__name__)


class MatcherService:
    def __init__(self, resume_profiler: ResumeProfiler, config: MatcherConfig):
        self.resume_profiler = resume_profiler
        self.config = config
        self.requirement_matcher = RequirementMatcher(
            similarity_threshold=config.similarity_threshold
        )

    def match_resume_two_stage(
        self,
        repo: JobRepository,
        resume_data: Dict[str, Any],
        tenant_id: Optional[Any] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> List[JobMatchPreliminary]:
        profile, evidence_units, _ = self.resume_profiler.profile_resume(resume_data, stop_event=stop_event)

        if not evidence_units:
            logger.warning("No evidence units extracted from resume")
            return []

        resume_fingerprint = generate_resume_fingerprint(resume_data)
        resume_embedding = self._get_resume_embedding_or_raise(repo, resume_fingerprint)

        job_similarity_pairs = self._retrieve_candidates(
            repo=repo,
            resume_embedding=resume_embedding,
            tenant_id=tenant_id,
        )

        if not job_similarity_pairs:
            logger.warning("No matching candidates found in Stage 1")
            return []

        preliminaries = []
        for job, sim in job_similarity_pairs:
            if stop_event and stop_event.is_set():
                logger.info("MatcherService stopped by user")
                return []
            preliminaries.append(self._build_preliminary(repo, job, sim, resume_fingerprint))

        preliminaries.sort(key=lambda p: p.job_similarity, reverse=True)
        return preliminaries

    def _get_resume_embedding_or_raise(self, repo: JobRepository, resume_fingerprint: str):
        embedding = repo.get_resume_summary_embedding(resume_fingerprint)
        if embedding is None:
            logger.error("No summary embedding found for resume")
            raise ValueError("No summary embedding found for resume")
        return embedding

    def _retrieve_candidates(
        self,
        repo: JobRepository,
        resume_embedding,
        tenant_id: Optional[Any],
    ) -> List[Tuple[JobPost, float]]:
        pairs = repo.get_top_jobs_by_summary_embedding(
            resume_embedding=resume_embedding,
            limit=self.config.batch_size,
            tenant_id=tenant_id,
        )
        logger.debug(
            "Stage 1: Retrieved %d candidates via vector similarity",
            len(pairs),
        )
        return pairs

    def _build_preliminary(
        self,
        repo: JobRepository,
        job: JobPost,
        job_similarity: float,
        resume_fingerprint: str,
    ) -> JobMatchPreliminary:
        matched, missing = self.requirement_matcher.match_requirements(
            repo, job.requirements, resume_fingerprint
        )
        return JobMatchPreliminary(
            job=job,
            job_similarity=job_similarity,
            requirement_matches=matched,
            missing_requirements=missing,
            resume_fingerprint=resume_fingerprint,
        )
