#!/usr/bin/env python3
"""
Requirement Matcher - Match resume evidence to job requirements.

For each requirement, find best matching evidence by pgvector cosine similarity
and determine if it's covered (above threshold).

This is single source of truth for requirement matching logic.
"""
from typing import List, Tuple, Dict, Optional
import logging

from database.models import JobRequirementUnit
from database.repository import JobRepository
from core.matcher.models import RequirementMatchResult
from etl.resume import ResumeEvidenceUnit

logger = logging.getLogger(__name__)


def _cosine_sim_from_distance(distance: float) -> float:
    """pgvector cosine distance -> cosine similarity."""
    return 1.0 - float(distance)


class RequirementMatcher:
    """Match resume evidence to job requirements using pgvector."""

    def __init__(
        self,
        repo: JobRepository,
        similarity_threshold: float
    ):
        """
        Initialize requirement matcher.

        Args:
            repo: JobRepository for pgvector queries
            similarity_threshold: Minimum similarity for a match (from config)
        """
        self.repo = repo
        self.similarity_threshold = similarity_threshold

    def match_requirements(
        self,
        evidence_units: List[ResumeEvidenceUnit],
        job_requirements: List[JobRequirementUnit],
        resume_fingerprint: str
    ) -> Tuple[List[RequirementMatchResult], List[RequirementMatchResult]]:
        """
        Match resume evidence units to job requirements using pgvector.

        Args:
            evidence_units: Resume evidence units
            job_requirements: List of job requirements to match against
            resume_fingerprint: Fingerprint for DB lookups

        Returns:
            (matched_requirements, missing_requirements)
        """
        matched_requirements = []
        missing_requirements = []

        for req in job_requirements:
            if not req.embedding_row or req.embedding_row.embedding is None:
                missing_requirements.append(RequirementMatchResult(
                    requirement=req,
                    evidence=None,
                    similarity=0.0,
                    is_covered=False
                ))
                continue

            best_matches = self.repo.find_best_evidence_for_requirement(
                requirement_embedding=req.embedding_row.embedding,
                resume_fingerprint=resume_fingerprint,
                top_k=1
            )

            if not best_matches:
                missing_requirements.append(RequirementMatchResult(
                    requirement=req,
                    evidence=None,
                    similarity=0.0,
                    is_covered=False
                ))
                continue

            best_row, distance = best_matches[0]
            similarity = _cosine_sim_from_distance(distance)
            is_covered = similarity >= self.similarity_threshold

            best_evidence = None
            if is_covered:
                best_evidence = ResumeEvidenceUnit(
                    id=best_row.evidence_unit_id,
                    text=best_row.source_text,
                    source_section='',
                    tags={}
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

        return matched_requirements, missing_requirements
