from dataclasses import dataclass
import logging
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from core.config_loader import MatcherConfig
from core.matcher.evidence_reranker import (
    CrossEncoderLike,
    rerank_requirement_evidence,
)
from core.matcher.models import JobMatchPreliminary
from core.matcher.requirement_matcher import RequirementMatcher
from core.metrics import evidence_rerank_latency_ms
from database.models import JobPost
from database.repository import JobRepository
from etl.resume import ResumeProfiler

logger = logging.getLogger(__name__)

LEXICAL_STOP_TOKENS = {
    "a", "an", "and", "as", "at", "be", "for", "from", "in", "into", "is", "it",
    "of", "on", "or", "that", "the", "to", "with",
}


@dataclass(frozen=True)
class RetrievedCandidate:
    job: JobPost
    job_similarity: float
    retrieval_score: float
    lexical_score: float = 0.0


class MatcherService:
    def __init__(
        self,
        resume_profiler: ResumeProfiler,
        config: MatcherConfig,
        *,
        requirement_recall_top_k: int = 5,
        cross_encoder_provider: Optional[CrossEncoderLike] = None,
    ):
        self.resume_profiler = resume_profiler
        self.config = config
        self.requirement_recall_top_k = max(1, int(requirement_recall_top_k))
        self.requirement_matcher = RequirementMatcher(
            similarity_threshold=config.similarity_threshold,
            default_top_k=self.requirement_recall_top_k,
        )
        self.cross_encoder_provider = cross_encoder_provider

    def match_resume_two_stage(
        self,
        repo: JobRepository,
        resume_data: Dict[str, Any],
        tenant_id: Optional[Any] = None,
        stop_event: Optional[threading.Event] = None,
        pre_extracted_resume: Optional[Any] = None,
        resume_fingerprint: Optional[str] = None,
        owner_id: Optional[Any] = None,
    ) -> List[JobMatchPreliminary]:
        if not resume_fingerprint:
            raise ValueError("resume_fingerprint is required for matching")
        if self._is_cancelled(stop_event):
            logger.info("MatcherService interrupted before resume preparation")
            return []

        if self._can_reuse_ready_resume(repo, resume_fingerprint, pre_extracted_resume):
            logger.info(
                "Reusing persisted resume artifacts for matching (fingerprint: %s)",
                resume_fingerprint,
            )
        else:
            _, evidence_units, _ = self.resume_profiler.profile_resume(
                resume_data,
                stop_event=stop_event,
                pre_extracted_resume=pre_extracted_resume,
                resume_fingerprint=resume_fingerprint,
            )

            if not evidence_units:
                logger.warning("No evidence units extracted from resume")
                return []

        if self._is_cancelled(stop_event):
            logger.info("MatcherService interrupted before candidate retrieval")
            return []

        resume_embedding = self._get_resume_embedding_or_raise(repo, resume_fingerprint)
        candidates = self._retrieve_candidates(
            repo=repo,
            resume_data=resume_data,
            resume_embedding=resume_embedding,
            tenant_id=tenant_id,
        )

        if not candidates:
            logger.warning("No matching candidates found in Stage 1")
            return []

        preliminaries = []
        for candidate in candidates:
            if stop_event and stop_event.is_set():
                logger.info("MatcherService interrupted")
                return []
            preliminaries.append(
                self._build_preliminary(
                    repo,
                    candidate.job,
                    candidate.job_similarity,
                    resume_fingerprint,
                    owner_id=owner_id,
                    retrieval_score=candidate.retrieval_score,
                    lexical_score=candidate.lexical_score,
                )
            )

        preliminaries.sort(key=lambda p: (p.retrieval_score, p.job_similarity), reverse=True)
        return preliminaries

    @staticmethod
    def _can_reuse_ready_resume(
        repo: JobRepository,
        resume_fingerprint: str,
        pre_extracted_resume: Optional[Any],
    ) -> bool:
        return pre_extracted_resume is not None and repo.is_resume_ready(resume_fingerprint)

    @staticmethod
    def _is_cancelled(stop_event: Optional[threading.Event]) -> bool:
        return bool(stop_event and stop_event.is_set())

    def _get_resume_embedding_or_raise(self, repo: JobRepository, resume_fingerprint: str):
        embedding = repo.get_resume_summary_embedding(resume_fingerprint)
        if embedding is None:
            logger.error("No summary embedding found for resume")
            raise ValueError("No summary embedding found for resume")
        return embedding

    def _retrieve_candidates(
        self,
        repo: JobRepository,
        resume_data: Dict[str, Any],
        resume_embedding,
        tenant_id: Optional[Any],
    ) -> List[RetrievedCandidate]:
        dense_pairs = repo.get_top_jobs_by_summary_embedding(
            resume_embedding=resume_embedding,
            limit=self.config.batch_size,
            tenant_id=tenant_id,
        )
        logger.debug("Stage 1: Retrieved %d dense candidates via vector similarity", len(dense_pairs))

        if not getattr(self.config, "hybrid_retrieval_enabled", False):
            return [
                RetrievedCandidate(job=job, job_similarity=similarity, retrieval_score=similarity)
                for job, similarity in dense_pairs
            ]

        lexical_query = self._build_lexical_query_text(resume_data)
        if not lexical_query:
            logger.debug("Hybrid retrieval enabled, but no lexical query tokens were extracted from the resume")
            return [
                RetrievedCandidate(job=job, job_similarity=similarity, retrieval_score=similarity)
                for job, similarity in dense_pairs
            ]

        lexical_pairs = repo.get_top_jobs_by_lexical_query(
            lexical_query,
            resume_embedding=resume_embedding,
            limit=getattr(self.config, "lexical_limit", None) or self.config.batch_size,
            tenant_id=tenant_id,
        )
        logger.debug(
            "Stage 1: Retrieved %d lexical candidates and %d dense candidates for hybrid fusion",
            len(lexical_pairs),
            len(dense_pairs),
        )
        return self._fuse_candidates(dense_pairs, lexical_pairs)

    def _fuse_candidates(
        self,
        dense_pairs: List[Tuple[JobPost, float]],
        lexical_pairs: List[Tuple[JobPost, float, float]],
    ) -> List[RetrievedCandidate]:
        fusion_k = max(1, int(getattr(self.config, "fusion_rank_constant", 60)))
        fused: Dict[str, Dict[str, Any]] = {}

        for rank, (job, similarity) in enumerate(dense_pairs, start=1):
            job_id = str(getattr(job, "id", ""))
            candidate = fused.setdefault(
                job_id,
                {
                    "job": job,
                    "job_similarity": float(similarity or 0.0),
                    "retrieval_score": 0.0,
                    "lexical_score": 0.0,
                },
            )
            candidate["job_similarity"] = max(candidate["job_similarity"], float(similarity or 0.0))
            candidate["retrieval_score"] += 1.0 / (fusion_k + rank)

        for rank, (job, lexical_score, dense_similarity) in enumerate(lexical_pairs, start=1):
            job_id = str(getattr(job, "id", ""))
            candidate = fused.setdefault(
                job_id,
                {
                    "job": job,
                    "job_similarity": float(dense_similarity or 0.0),
                    "retrieval_score": 0.0,
                    "lexical_score": 0.0,
                },
            )
            candidate["job_similarity"] = max(candidate["job_similarity"], float(dense_similarity or 0.0))
            candidate["lexical_score"] = max(candidate["lexical_score"], float(lexical_score or 0.0))
            candidate["retrieval_score"] += 1.0 / (fusion_k + rank)

        candidates = [
            RetrievedCandidate(
                job=values["job"],
                job_similarity=values["job_similarity"],
                retrieval_score=values["retrieval_score"],
                lexical_score=values["lexical_score"],
            )
            for values in fused.values()
        ]
        candidates.sort(
            key=lambda candidate: (
                candidate.retrieval_score,
                candidate.job_similarity,
                candidate.lexical_score,
            ),
            reverse=True,
        )
        limit = self.config.batch_size
        return candidates[:limit] if limit is not None else candidates

    def _build_lexical_query_text(self, resume_data: Dict[str, Any]) -> str:
        token_limit = max(1, int(getattr(self.config, "lexical_query_token_limit", 24)))
        seen: set[str] = set()
        tokens: List[str] = []

        for text in self._iter_resume_strings(resume_data):
            for token in re.findall(r"[a-z0-9]+", text.lower()):
                if len(token) < 2 or token in LEXICAL_STOP_TOKENS or token in seen:
                    continue
                seen.add(token)
                tokens.append(token)
                if len(tokens) >= token_limit:
                    return " | ".join(tokens)

        return " | ".join(tokens)

    def _iter_resume_strings(self, value: Any) -> List[str]:
        if isinstance(value, str):
            stripped = value.strip()
            return [stripped] if stripped else []
        if isinstance(value, dict):
            strings: List[str] = []
            for item in value.values():
                strings.extend(self._iter_resume_strings(item))
            return strings
        if isinstance(value, list):
            strings: List[str] = []
            for item in value:
                strings.extend(self._iter_resume_strings(item))
            return strings
        return []

    def _build_preliminary(
        self,
        repo: JobRepository,
        job: JobPost,
        job_similarity: float,
        resume_fingerprint: str,
        *,
        owner_id: Optional[Any] = None,
        retrieval_score: Optional[float] = None,
        lexical_score: Optional[float] = None,
    ) -> JobMatchPreliminary:
        matched, missing = self.requirement_matcher.match_requirements(
            repo,
            job.requirements,
            resume_fingerprint,
            top_k=self.requirement_recall_top_k,
        )
        if self.cross_encoder_provider is not None:
            rerank_start = time.perf_counter()
            try:
                rerank_requirement_evidence(
                    provider=self.cross_encoder_provider,
                    requirement_matches=list(matched) + list(missing),
                )
            finally:
                # Histogram buckets are in ms; Histogram.time() records seconds,
                # so measure manually and observe the ms value.
                evidence_rerank_latency_ms.observe(
                    (time.perf_counter() - rerank_start) * 1000.0
                )
        return JobMatchPreliminary(
            job=job,
            job_similarity=job_similarity,
            requirement_matches=matched,
            missing_requirements=missing,
            resume_fingerprint=resume_fingerprint,
            owner_id=owner_id,
            retrieval_score=float(retrieval_score if retrieval_score is not None else job_similarity or 0.0),
            lexical_score=lexical_score,
        )
