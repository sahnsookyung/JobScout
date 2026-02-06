#!/usr/bin/env python3
"""
Requirement Matcher - Match resume evidence to job requirements.

For each requirement, find best matching evidence by pgvector cosine similarity
and determine if it's covered (above threshold).

This is single source of truth for requirement matching logic.
"""
from typing import List, Tuple
import logging

from database.models import JobRequirementUnit
from database.repository import JobRepository
from core.matcher.models import RequirementMatchResult
from etl.resume import ResumeEvidenceUnit

logger = logging.getLogger(__name__)


class RequirementMatcher:
    """Match resume evidence to job requirements using pgvector."""

    def __init__(
        self,
        similarity_threshold: float
    ):
        """
        Initialize requirement matcher.

        Args:
            similarity_threshold: Minimum similarity for a match (from config)
        """
        self.similarity_threshold = similarity_threshold

    def match_requirements(
        self,
        repo: JobRepository,
        job_requirements: List[JobRequirementUnit],
        resume_fingerprint: str,
        top_k: int = 1
    ) -> Tuple[List[RequirementMatchResult], List[RequirementMatchResult]]:
        """
        Match resume evidence units to job requirements using pgvector.

        Args:
            repo: JobRepository for DB operations
            job_requirements: List of job requirements to match against
            resume_fingerprint: Fingerprint for DB lookups
            top_k: Number of top matches to retrieve (default: 1)

        Returns:
            (matched_requirements, missing_requirements)
        """
        matched_requirements = []
        missing_requirements = []

        for req in job_requirements:
            req_id = getattr(req, 'id', str(req))

            if not req.embedding_row or req.embedding_row.embedding is None:
                logger.debug(f"Requirement {req_id}: no embedding, marking as missing")
                missing_requirements.append(RequirementMatchResult(
                    requirement=req,
                    evidence=None,
                    similarity=0.0,
                    is_covered=False
                ))
                continue

            best_matches = repo.find_best_evidence_for_requirement(
                requirement_embedding=req.embedding_row.embedding,
                resume_fingerprint=resume_fingerprint,
                top_k=top_k
            )

            if not best_matches:
                logger.debug(f"Requirement {req_id}: no evidence found, marking as missing")
                missing_requirements.append(RequirementMatchResult(
                    requirement=req,
                    evidence=None,
                    similarity=0.0,
                    is_covered=False
                ))
                continue

            best_row, similarity = best_matches[0]
            is_covered = similarity >= self.similarity_threshold

            logger.debug(
                f"Requirement {req_id}: similarity={similarity:.3f}, "
                f"threshold={self.similarity_threshold:.3f}, covered={is_covered}"
            )

            best_evidence = ResumeEvidenceUnit(
                id=best_row.evidence_unit_id,
                text=best_row.source_text,
                source_section=best_row.source_section or '',
                tags=best_row.tags or {},
                embedding=list(best_row.embedding) if best_row.embedding is not None else None,
                years_value=best_row.years_value,
                years_context=best_row.years_context,
                is_total_years_claim=best_row.is_total_years_claim or False,
            )

            req_match = RequirementMatchResult(
                requirement=req,
                evidence=best_evidence,
                similarity=similarity,
                is_covered=is_covered
            )

            if is_covered:
                matched_requirements.append(req_match)
            else:
                missing_requirements.append(req_match)

        logger.debug(
            f"Matched {len(matched_requirements)}/{len(job_requirements)} requirements "
            f"(threshold={self.similarity_threshold:.3f})"
        )

        return matched_requirements, missing_requirements
