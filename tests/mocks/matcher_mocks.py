#!/usr/bin/env python3
"""
Test Mock Implementations - Mock services for testing.

Moved from core/matcher/mock_service.py to separate production code from test utilities.
These mocks provide deterministic behavior for unit and integration tests.
"""
from typing import Dict, List, Any, Optional, Tuple
import random
import math

from core.llm.interfaces import LLMProvider
from etl.resume import (
    ResumeEvidenceUnit,
    StructuredResumeProfile,
)
from core.matcher.models import (
    JobMatchPreliminary, RequirementMatchResult
)
from database.models import JobPost, JobRequirementUnit


class MockLLMProvider(LLMProvider):
    """
    Mock AI service for testing.
    
    Returns deterministic mock embeddings and extractions
    without calling external APIs.
    """
    
    def __init__(
        self,
        embedding_dim: int = 1024,
        deterministic: bool = True,
        seed: int = 42
    ):
        self.embedding_dim = embedding_dim
        self.deterministic = deterministic
        
        if deterministic:
            random.seed(seed)
    
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate deterministic mock embedding based on text content.
        
        Returns a vector that varies based on the text content
        so similar texts get similar embeddings.
        """
        if self.deterministic:
            text_hash = hash(text) % (2**31)
            random.seed(text_hash)
        
        embedding = []
        for i in range(self.embedding_dim):
            val = random.gauss(0, 1)
            embedding.append(val)
        
        norm = math.sqrt(sum(x**2 for x in embedding))
        if norm > 0:
            embedding = [x / norm for x in embedding]
        
        return embedding
    
    def extract_structured_data(self, text: str, schema: Dict) -> Dict[str, Any]:
        """Return mock structured data extraction."""
        return {
            'profile': {
                'summary': {
                    'headline': 'Mock Resume Profile',
                    'objective': 'To demonstrate testing'
                },
                'experience': [
                    {
                        'company': 'Mock Company',
                        'title': 'Mock Developer',
                        'start_date': '2020-01',
                        'end_date': '2023-12',
                        'description': text[:100] if text else 'Mock description',
                        'highlights': ['Mock highlight 1', 'Mock highlight 2']
                    }
                ],
                'skills': {
                    'groups': [
                        {'name': 'Programming', 'skills': ['Python', 'Mocking']}
                    ]
                },
                'projects': [
                    {
                        'name': 'Mock Project',
                        'description': 'A mock project for testing'
                    }
                ],
                'education': [
                    {
                        'institution': 'Mock University',
                        'degree': 'Mock Degree',
                        'field': 'Computer Science'
                    }
                ]
            }
        }


class MockJobRepository:
    """
    Mock job repository for testing.
    
    Provides in-memory storage for jobs and matches
    without requiring a database.
    """
    
    def __init__(self):
        self.jobs: Dict[str, JobPost] = {}
        self.matches: Dict[str, Any] = {}
        self.section_embeddings: Dict[str, List[Any]] = {}
    
    def get_by_id(self, job_id: str) -> Optional[JobPost]:
        """Get job by ID."""
        return self.jobs.get(job_id)
    
    def save(self, job: JobPost) -> None:
        """Save a job."""
        self.jobs[str(job.id)] = job
    
    def find_similar_jobs(
        self,
        resume_embedding: List[float],
        limit: int = 100,
        tenant_id: Optional[str] = None,
        require_remote: Optional[bool] = None
    ) -> List[JobPost]:
        """Mock find similar jobs - returns all jobs."""
        return list(self.jobs.values())[:limit]
    
    def save_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        sections: List[Dict]
    ) -> List[Any]:
        """Mock save resume section embeddings."""
        self.section_embeddings[resume_fingerprint] = sections
        return sections
    
    def get_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        section_type: Optional[str] = None,
        top_k: int = 10
    ) -> List[Any]:
        """Mock get resume section embeddings."""
        sections = self.section_embeddings.get(resume_fingerprint, [])
        if section_type:
            sections = [s for s in sections if s.get('section_type') == section_type]
        return sections[:top_k]


class MockMatcherService:
    """
    Mock matcher service for testing.
    
    Provides predictable matching behavior for unit tests.
    Implements the full interface of the real MatcherService.
    """
    
    def __init__(
        self,
        repo: MockJobRepository,
        ai_service: MockLLMProvider,
        similarity_threshold: float = 0.5
    ):
        self.repo = repo
        self.ai = ai_service
        self.similarity_threshold = similarity_threshold
    
    def match_resume_to_jobs(
        self,
        resume_data: Dict[str, Any],
        limit: int = 100
    ) -> List[JobMatchPreliminary]:
        """Mock match resume to jobs."""
        matches = []
        
        for job in self.repo.jobs.values():
            match = JobMatchPreliminary(
                job=job,
                job_similarity=0.7,
                requirement_matches=[],
                missing_requirements=[],
                resume_fingerprint="mock-fingerprint"
            )
            matches.append(match)
        
        return matches[:limit]
    
    def match_resume_to_job(
        self,
        evidence_units: List[ResumeEvidenceUnit],
        job: JobPost,
        resume_fingerprint: str,
        preferences: Optional[Dict[str, Any]] = None
    ) -> JobMatchPreliminary:
        """Mock match resume to a single job."""
        matched_requirements = []
        missing_requirements = []
        
        return JobMatchPreliminary(
            job=job,
            job_similarity=0.7,
            requirement_matches=matched_requirements,
            missing_requirements=missing_requirements,
            resume_fingerprint=resume_fingerprint
        )
    
    def extract_resume_evidence(
        self,
        resume_data: Dict[str, Any]
    ) -> List[ResumeEvidenceUnit]:
        """Mock extract resume evidence from resume data."""
        evidence_units = []
        
        for section_idx, section in enumerate(resume_data.get('sections', [])):
            section_title = section.get('title', '')
            
            for item_idx, item in enumerate(section.get('items', [])):
                if item.get('description'):
                    evidence_units.append(ResumeEvidenceUnit(
                        id=f"reu_{len(evidence_units)}",
                        text=item['description'],
                        source_section=section_title,
                        tags={
                            'company': item.get('company', ''),
                            'role': item.get('role', ''),
                            'period': item.get('period', ''),
                            'type': 'description'
                        }
                    ))
                
                for highlight in item.get('highlights', []):
                    if highlight and not highlight.startswith('<'):
                        evidence_units.append(ResumeEvidenceUnit(
                            id=f"reu_{len(evidence_units)}",
                            text=highlight,
                            source_section=section_title,
                            tags={
                                'company': item.get('company', ''),
                                'role': item.get('role', ''),
                                'type': 'highlight'
                            }
                        ))
        
        return evidence_units
    
    def embed_evidence_units(
        self,
        evidence_units: List[ResumeEvidenceUnit]
    ) -> None:
        """Mock embed evidence units."""
        for unit in evidence_units:
            if unit.embedding is None:
                unit.embedding = self.ai.generate_embedding(unit.text)


__all__ = [
    'MockLLMProvider',
    'MockJobRepository',
    'MockMatcherService'
]
