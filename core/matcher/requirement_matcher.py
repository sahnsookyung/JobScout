from typing import List, Tuple, Optional
import logging

from database.models import JobRequirementUnit
from database.repository import JobRepository
from core.matcher.models import RequirementMatchResult
from etl.resume import ResumeEvidenceUnit

logger = logging.getLogger(__name__)


class RequirementMatcher:
    """Match resume evidence to job requirements using pgvector."""

    def __init__(self, similarity_threshold: float):
        self.similarity_threshold = similarity_threshold

    def match_requirements(
        self,
        repo: JobRepository,
        job_requirements: List[JobRequirementUnit],
        resume_fingerprint: str,
        top_k: int = 1,
    ) -> Tuple[List[RequirementMatchResult], List[RequirementMatchResult]]:
        matched: List[RequirementMatchResult] = []
        missing: List[RequirementMatchResult] = []

        for req in job_requirements:
            req_id = getattr(req, "id", str(req))

            embedding = self._requirement_embedding(req)
            if embedding is None:
                logger.debug("Requirement %s: no embedding, marking as missing", req_id)
                missing.append(self._missing(req))
                continue

            best = self._best_match(repo, embedding, resume_fingerprint, top_k)
            if best is None:
                logger.debug("Requirement %s: no evidence found, marking as missing", req_id)
                missing.append(self._missing(req))
                continue

            best_row, similarity = best
            is_covered = similarity >= self.similarity_threshold

            logger.debug(
                "Requirement %s: similarity=%.3f, threshold=%.3f, covered=%s",
                req_id,
                similarity,
                self.similarity_threshold,
                is_covered,
            )

            result = RequirementMatchResult(
                requirement=req,
                evidence=self._to_evidence(best_row),
                similarity=similarity,
                is_covered=is_covered,
            )
            (matched if is_covered else missing).append(result)

        logger.debug(
            "Matched %d/%d requirements (threshold=%.3f)",
            len(matched),
            len(job_requirements),
            self.similarity_threshold,
        )
        return matched, missing

    def _requirement_embedding(self, req: JobRequirementUnit):
        row = getattr(req, "embedding_row", None)
        if row is None:
            return None
        return getattr(row, "embedding", None)

    def _best_match(self, repo: JobRepository, requirement_embedding, resume_fingerprint: str, top_k: int):
        matches = repo.find_best_evidence_for_requirement(
            requirement_embedding=requirement_embedding,
            resume_fingerprint=resume_fingerprint,
            top_k=top_k,
        )
        return matches[0] if matches else None

    def _missing(self, req: JobRequirementUnit) -> RequirementMatchResult:
        return RequirementMatchResult(
            requirement=req,
            evidence=None,
            similarity=0.0,
            is_covered=False,
        )

    def _to_evidence(self, best_row) -> ResumeEvidenceUnit:
        return ResumeEvidenceUnit(
            id=best_row.evidence_unit_id,
            text=best_row.source_text,
            source_section=best_row.source_section or "",
            tags=best_row.tags or {},
            embedding=list(best_row.embedding) if best_row.embedding is not None else None,
            years_value=best_row.years_value,
            years_context=best_row.years_context,
            is_total_years_claim=bool(best_row.is_total_years_claim),
        )
