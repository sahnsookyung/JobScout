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
from core.llm.interfaces import LLMProvider
from core.config_loader import MatcherConfig
from core.matcher.models import (
    ResumeEvidenceUnit, RequirementMatchResult, JobMatchPreliminary,
    PreferencesAlignmentScore
)
from core.matcher.requirement_matcher import RequirementMatcher
from core.matcher.similarity import SimilarityCalculator

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
        ai_service: LLMProvider,
        config: MatcherConfig
    ):
        """
        Initialize matcher service with dependencies.
        
        Args:
            repo: JobRepository for DB operations
            ai_service: LLMProvider for embeddings and extraction
            config: MatcherConfig with matching parameters
        """
        self.repo = repo
        self.ai = ai_service
        self.config = config
        self.similarity_calc = SimilarityCalculator()
        self.requirement_matcher = RequirementMatcher(
            ai_service=ai_service,
            similarity_calc=self.similarity_calc,
            similarity_threshold=config.similarity_threshold
        )
    
    def match_resume_to_job(
        self,
        evidence_units: List[ResumeEvidenceUnit],
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
        # Job-level matching (if enabled and job has summary embedding)
        job_similarity = 0.0
        if self.config.include_job_level_matching and job.summary_embedding is not None:
            resume_text = " ".join([e.text for e in evidence_units[:5]])
            resume_embedding = self.ai.generate_embedding(resume_text)
            job_similarity = self.similarity_calc.calculate(resume_embedding, job.summary_embedding)
        
        # Requirement-level matching using RequirementMatcher
        matched_requirements, missing_requirements = self.requirement_matcher.match_requirements(
            evidence_units, job.requirements
        )
        
        return JobMatchPreliminary(
            job=job,
            job_similarity=job_similarity,
            preferences_alignment=None,
            requirement_matches=matched_requirements,
            missing_requirements=missing_requirements,
            resume_fingerprint=resume_fingerprint
        )
    
    def match_resume_to_jobs(
        self,
        evidence_units: List[ResumeEvidenceUnit],
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
        require_remote: Optional[bool] = None
    ) -> List[JobMatchPreliminary]:
        """
        Two-stage matching pipeline: retrieve candidates -> compute preliminaries.
        
        Stage 1: Retrieve top-K candidates using vector similarity on summary_embedding
        Stage 2: Compute requirement-level matching only on retrieved candidates
        
        Args:
            resume_data: Full resume data
            preferences: Optional user preferences for enhanced matching
            tenant_id: Optional tenant filter for Stage 1
            require_remote: Optional remote-only filter for Stage 1
        
        Returns:
            List of preliminary matches for top-K candidates
        """
        # Extract evidence units from resume
        evidence_units = self._extract_resume_evidence(resume_data)
        
        # Embed evidence units if needed
        self._embed_evidence_units(evidence_units)
        
        # Generate resume embedding for Stage 1 retrieval
        resume_text = " ".join([e.text for e in evidence_units[:5]])
        resume_embedding = self.ai.generate_embedding(resume_text)
        
        # Stage 1: Retrieve top-K candidates using vector similarity
        candidate_pool_size = (
            self.config.ranking.discovery.candidate_pool_size_k 
            if self.config.ranking.mode == "discovery" 
            else self.config.ranking.strict.candidate_pool_size_k
        )
        candidate_jobs = self.repo.get_top_jobs_by_summary_embedding(
            resume_embedding=resume_embedding,
            limit=candidate_pool_size,
            tenant_id=tenant_id,
            require_remote=require_remote
        )
        
        logger.debug(f"Stage 1: Retrieved {len(candidate_jobs)} candidates")
        
        if not candidate_jobs:
            logger.warning("No matching candidates found in Stage 1")
            return []
        
        # Stage 2: Compute requirement-level matching for each candidate
        resume_fingerprint = generate_resume_fingerprint(resume_data)
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
        
        Jobs must have been extracted (have requirements) and embedded.
        
        Args:
            limit: Maximum number of jobs to return
        
        Returns:
            List of JobPost objects ready for matching
        """
        return self.repo.get_jobs_for_matching(limit=limit)
    
    def extract_resume_evidence(self, resume_data: Dict[str, Any]) -> List[ResumeEvidenceUnit]:
        """
        Extract Resume Evidence Units from resume JSON.
        
        Args:
            resume_data: Raw resume data with sections
        
        Returns:
            List of ResumeEvidenceUnit objects
        """
        return self._extract_resume_evidence(resume_data)
    
    def calculate_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """
        Calculate cosine similarity between two vectors.
        
        Args:
            vec1: First vector
            vec2: Second vector
        
        Returns:
            Cosine similarity in range [-1.0, 1.0]
        """
        return self.similarity_calc.calculate(vec1, vec2)
    
    def _extract_resume_evidence(self, resume_data: Dict[str, Any]) -> List[ResumeEvidenceUnit]:
        """
        Extract Resume Evidence Units from resume JSON.
        
        Args:
            resume_data: Raw resume data with sections
        
        Returns:
            List of ResumeEvidenceUnit objects
        """
        evidence_units = []
        unit_id = 0
        
        for section in resume_data.get('sections', []):
            section_title = section.get('title', '')
            
            for item in section.get('items', []):
                if item.get('description'):
                    evidence_units.append(ResumeEvidenceUnit(
                        id=f"reu_{unit_id}",
                        text=item['description'],
                        source_section=section_title,
                        tags={
                            'company': item.get('company', ''),
                            'role': item.get('role', ''),
                            'period': item.get('period', ''),
                            'type': 'description'
                        }
                    ))
                    unit_id += 1
                
                for highlight in item.get('highlights', []):
                    if highlight and not highlight.startswith('<'):
                        evidence_units.append(ResumeEvidenceUnit(
                            id=f"reu_{unit_id}",
                            text=highlight,
                            source_section=section_title,
                            tags={
                                'company': item.get('company', ''),
                                'role': item.get('role', ''),
                                'type': 'highlight'
                            }
                        ))
                        unit_id += 1
        
        logger.info(f"Extracted {len(evidence_units)} evidence units from resume")
        return evidence_units
    
    def _embed_evidence_units(self, evidence_units: List[ResumeEvidenceUnit]) -> None:
        """
        Generate embeddings for evidence units in-place.
        
        Args:
            evidence_units: List of evidence units (modified in-place)
        """
        for unit in evidence_units:
            if unit.embedding is None:
                unit.embedding = self.ai.generate_embedding(unit.text)
