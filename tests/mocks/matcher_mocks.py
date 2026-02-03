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
from core.matcher.models import (
    ResumeEvidenceUnit, StructuredResumeProfile,
    JobMatchPreliminary, PreferencesAlignmentScore, RequirementMatchResult
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
                preferences_alignment=None,
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
        preferences_alignment = None
        if preferences:
            preferences_alignment = self.calculate_preferences_alignment(job, preferences)
        
        matched_requirements = []
        missing_requirements = []
        
        return JobMatchPreliminary(
            job=job,
            job_similarity=0.7,
            preferences_alignment=preferences_alignment,
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
    
    def calculate_location_match(
        self,
        job: JobPost,
        preferences: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """Calculate location match score based on preferences."""
        job_prefs = preferences.get('job_preferences', {}) if preferences else {}
        location_prefs = job_prefs.get('location_preferences', {})
        
        preferred_locations = location_prefs.get('preferred_locations', [])
        avoid_locations = location_prefs.get('avoid_locations', [])
        wants_remote = job_prefs.get('wants_remote', True)
        
        details = {
            'job_location': job.location_text,
            'job_is_remote': job.is_remote,
            'user_wants_remote': wants_remote,
            'preferred_locations': preferred_locations,
            'avoid_locations': avoid_locations
        }
        
        if wants_remote:
            if job.is_remote:
                return 1.0, details
            else:
                job_loc = (job.location_text or '').lower()
                for pref_loc in preferred_locations:
                    if pref_loc.lower() in job_loc:
                        return 0.7, details
                for avoid_loc in avoid_locations:
                    if avoid_loc.lower() in job_loc:
                        return 0.0, details
                return 0.3, details
        else:
            if job.is_remote:
                return 0.8, details
            else:
                job_loc = (job.location_text or '').lower()
                for pref_loc in preferred_locations:
                    if pref_loc.lower() in job_loc:
                        return 1.0, details
                return 0.6, details
    
    def calculate_company_size_match(
        self,
        job: JobPost,
        preferences: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """Calculate company size match score."""
        company_prefs = preferences.get('company_preferences', {}) if preferences else {}
        size_prefs = company_prefs.get('company_size', {})
        
        details = {
            'job_company_size': job.company_num_employees,
            'preferred_size': size_prefs
        }
        
        if not job.company_num_employees:
            return 0.5, details
        
        try:
            emp_count = int(job.company_num_employees)
        except (ValueError, TypeError):
            return 0.5, details
        
        min_employees = size_prefs.get('employee_count', {}).get('minimum', 0)
        max_employees = size_prefs.get('employee_count', {}).get('maximum', float('inf'))
        
        if min_employees <= emp_count <= max_employees:
            return 1.0, details
        elif emp_count < min_employees:
            ratio = emp_count / min_employees if min_employees > 0 else 0
            return max(0.0, ratio * 0.5), details
        else:
            ratio = max_employees / emp_count if emp_count > 0 else 0
            return max(0.0, ratio * 0.5), details
    
    def calculate_industry_match(
        self,
        job: JobPost,
        preferences: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """Calculate industry match score."""
        company_prefs = preferences.get('company_preferences', {}) if preferences else {}
        industry_prefs = company_prefs.get('industry', {})
        
        preferred = industry_prefs.get('preferred', [])
        avoid = industry_prefs.get('avoid', [])
        
        job_industry = getattr(job, 'company_industry', None) or getattr(job, 'industry', None)
        
        details = {
            'job_industry': job_industry,
            'preferred_industries': preferred,
            'avoided_industries': avoid
        }
        
        if not job_industry:
            return 0.5, details
        
        job_ind = job_industry.lower()
        
        for avoid_ind in avoid:
            if avoid_ind.lower() in job_ind:
                return 0.0, details
        
        for pref_ind in preferred:
            if pref_ind.lower() in job_ind:
                return 1.0, details
        
        return 0.5, details
    
    def calculate_role_match(
        self,
        job: JobPost,
        preferences: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """Calculate role match score."""
        career_prefs = preferences.get('career_preferences', {}) if preferences else {}
        
        preferred_roles = career_prefs.get('role_types', [])
        avoid_roles = career_prefs.get('avoid_roles', [])
        target_seniority = career_prefs.get('seniority_level', None)
        
        details = {
            'job_title': job.title,
            'job_level': job.job_level,
            'preferred_roles': preferred_roles,
            'avoided_roles': avoid_roles,
            'target_seniority': target_seniority
        }
        
        if not job.title:
            return 0.5, details
        
        job_title_lower = job.title.lower()
        score = 0.5
        
        for avoid_role in avoid_roles:
            if avoid_role.lower() in job_title_lower:
                return 0.0, details
        
        role_matched = False
        for pref_role in preferred_roles:
            if pref_role.lower() in job_title_lower:
                score = 1.0
                role_matched = True
                break
        
        if target_seniority and job.job_level:
            job_level_lower = job.job_level.lower()
            if target_seniority.lower() in job_level_lower:
                if role_matched:
                    score = 1.0
                else:
                    score = 0.8
            elif role_matched:
                score = 0.7
        
        return score, details
    
    def calculate_preferences_alignment(
        self,
        job: JobPost,
        preferences: Optional[Dict[str, Any]]
    ) -> Optional[PreferencesAlignmentScore]:
        """Calculate overall preferences alignment."""
        if not preferences:
            return None
        
        location_score, location_details = self.calculate_location_match(job, preferences)
        company_size_score, company_size_details = self.calculate_company_size_match(job, preferences)
        industry_score, industry_details = self.calculate_industry_match(job, preferences)
        role_score, role_details = self.calculate_role_match(job, preferences)
        
        overall_score = (location_score + company_size_score + industry_score + role_score) / 4.0
        
        details = {
            "location": location_details,
            "company_size": company_size_details,
            "industry": industry_details,
            "role": role_details
        }
        
        return PreferencesAlignmentScore(
            overall_score=round(overall_score, 2),
            location_match=location_score,
            company_size_match=company_size_score,
            industry_match=industry_score,
            role_match=role_score,
            details=details
        )


__all__ = [
    'MockLLMProvider',
    'MockJobRepository',
    'MockMatcherService'
]
