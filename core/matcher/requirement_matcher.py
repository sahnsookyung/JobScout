#!/usr/bin/env python3
"""
Requirement Matcher - Match resume evidence to job requirements.
 
For each requirement, find best matching evidence by cosine similarity
and determine if it's covered (above threshold).
 
This is single source of truth for requirement matching logic.
"""
from typing import List, Tuple, Dict, Optional
import logging

from database.models import JobRequirementUnit
from core.matcher.models import RequirementMatchResult, ResumeEvidenceUnit
from core.matcher.similarity import SimilarityCalculator
from core.llm.interfaces import LLMProvider

logger = logging.getLogger(__name__)


def hydrate_embeddings(
    evidence_units: List[ResumeEvidenceUnit],
    job_requirements: List[JobRequirementUnit],
    ai_service: LLMProvider
) -> Tuple[List[ResumeEvidenceUnit], Dict[JobRequirementUnit, Optional[List[float]]]]:
    """
    Hydrate embeddings for evidence units and job requirements.
    
    Ensures embeddings exist before matching, avoiding lazy generation
    in the nested matching loop.
    
    Args:
        evidence_units: Resume evidence units (will hydrate embeddings in-place)
        job_requirements: Job requirements to hydrate
        ai_service: AI service for embedding generation
    
    Returns:
        Tuple of (hydrated_evidence_units, requirement_embeddings_dict)
        where requirement_embeddings_dict maps requirement -> embedding or None
    """
    requirement_embeddings: Dict[JobRequirementUnit, Optional[List[float]]] = {}
    
    # Hydrate requirement embeddings
    for req in job_requirements:
        if req.embedding_row and req.embedding_row.embedding is not None:
            requirement_embeddings[req] = req.embedding_row.embedding
        else:
            try:
                req_embedding = ai_service.generate_embedding(str(req.text))
                requirement_embeddings[req] = req_embedding
            except Exception as e:
                logger.error(f"Failed to generate embedding for requirement: {str(req.text)[:50]}... - {e}")
                requirement_embeddings[req] = None
    
    # Hydrate evidence embeddings (once per evidence unit)
    for evidence in evidence_units:
        if evidence.embedding is None:
            try:
                evidence.embedding = ai_service.generate_embedding(evidence.text)
            except Exception as e:
                logger.error(f"Failed to generate embedding for evidence: {evidence.text[:50]}... - {e}")
                evidence.embedding = None
    
    return evidence_units, requirement_embeddings


class RequirementMatcher:
    """Match resume evidence to job requirements (single source of truth)."""
    
    def __init__(
        self,
        ai_service: LLMProvider,
        similarity_calc: SimilarityCalculator,
        similarity_threshold: float
    ):
        """
        Initialize requirement matcher.
        
        Args:
            ai_service: AI service for embedding generation (lazy-embed requirements/evidence)
            similarity_calc: SimilarityCalculator instance
            similarity_threshold: Minimum similarity for a match (from config)
        """
        self.ai = ai_service
        self.similarity_calc = similarity_calc
        self.similarity_threshold = similarity_threshold
    
    def match_requirements(
        self,
        evidence_units: List[ResumeEvidenceUnit],
        job_requirements: List[JobRequirementUnit]
    ) -> Tuple[List[RequirementMatchResult], List[RequirementMatchResult]]:
        """
        Match resume evidence units to job requirements.
        
        Hydrates embeddings first, then performs pure matching
        without side effects or AI calls.
        
        Args:
            evidence_units: Resume evidence with embeddings (will hydrate if needed)
            job_requirements: List of job requirements to match against
        
        Returns:
            (matched_requirements, missing_requirements)
        """
        # Hydrate embeddings before matching
        evidence_units, requirement_embeddings = hydrate_embeddings(
            evidence_units, job_requirements, self.ai
        )
        
        # Perform pure matching using pre-hydrated embeddings
        return self._match_with_embeddings(evidence_units, job_requirements, requirement_embeddings)
    
    def _match_with_embeddings(
        self,
        evidence_units: List[ResumeEvidenceUnit],
        job_requirements: List[JobRequirementUnit],
        requirement_embeddings: Dict[JobRequirementUnit, Optional[List[float]]]
    ) -> Tuple[List[RequirementMatchResult], List[RequirementMatchResult]]:
        """
        Pure matching function using pre-hydrated embeddings.
        
        Args:
            evidence_units: Resume evidence units with embeddings populated
            job_requirements: Job requirements to match
            requirement_embeddings: Pre-hydrated requirement embeddings
        
        Returns:
            (matched_requirements, missing_requirements)
        """
        matched_requirements = []
        missing_requirements = []
        
        for req in job_requirements:
            req_embedding = requirement_embeddings[req]
            
            # Handle missing requirement embedding
            if req_embedding is None:
                missing_requirements.append(RequirementMatchResult(
                    requirement=req,
                    evidence=None,
                    similarity=0.0,
                    is_covered=False
                ))
                continue
            
            # Find best matching evidence
            best_match = None
            best_similarity = 0.0
            
            for evidence in evidence_units:
                if evidence.embedding is None:
                    continue
                
                similarity = self.similarity_calc.calculate(req_embedding, evidence.embedding)
                
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = evidence
            
            is_covered = best_similarity >= self.similarity_threshold
            
            req_match = RequirementMatchResult(
                requirement=req,
                evidence=best_match if is_covered else None,
                similarity=best_similarity,
                is_covered=is_covered
            )
            
            if is_covered:
                matched_requirements.append(req_match)
            else:
                missing_requirements.append(req_match)
        
        return matched_requirements, missing_requirements
