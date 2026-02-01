#!/usr/bin/env python3
"""
Matcher Service - Stage 1: Vector Retrieval with Preferences Support

Performs two-level matching:
1. Job-level: Resume vs JobPost.summary_embedding (JD alignment) + Preferences alignment
2. Requirement-level: Resume Evidence Units (REUs) vs JobRequirementUnit embeddings

Supports preferences-based matching for:
- Location preferences
- Company size preferences
- Industry preferences
- Role type preferences

Designed to be microservice-ready and can run independently
of the ScoringService.
"""

from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
import math
import logging
from database.repository import JobRepository
from database.models import (
    JobPost, JobRequirementUnit, JobRequirementUnitEmbedding,
    JobMatch, JobMatchRequirement, generate_resume_fingerprint
)
from core.ai_service import OpenAIService
from core.interfaces import LLMProvider
from core.config_loader import MatcherConfig

logger = logging.getLogger(__name__)


@dataclass
class ResumeEvidenceUnit:
    """Resume Evidence Unit - atomic claim from resume."""
    id: str
    text: str
    source_section: str
    tags: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[List[float]] = None


@dataclass
class RequirementMatchResult:
    """Result of matching a single requirement."""
    requirement: JobRequirementUnit
    evidence: Optional[ResumeEvidenceUnit]
    similarity: float
    is_covered: bool


@dataclass
class PreferencesAlignmentScore:
    """Score indicating how well a job aligns with user preferences."""
    overall_score: float  # 0.0 to 1.0
    location_match: float
    company_size_match: float
    industry_match: float
    role_match: float
    details: Dict[str, Any]


@dataclass
class JobMatchPreliminary:
    """Preliminary match before scoring (output of MatcherService)."""
    job: JobPost
    job_similarity: float  # Job-level similarity from embeddings
    preferences_alignment: Optional[PreferencesAlignmentScore]  # Preferences match
    requirement_matches: List[RequirementMatchResult]
    missing_requirements: List[RequirementMatchResult]
    resume_fingerprint: str


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
        self.repo = repo
        self.ai = ai_service
        self.config = config
        
    def extract_resume_evidence(self, resume_data: Dict[str, Any]) -> List[ResumeEvidenceUnit]:
        """
        Extract Resume Evidence Units from resume JSON.
        """
        evidence_units = []
        unit_id = 0
        
        for section in resume_data.get('sections', []):
            section_title = section.get('title', '')
            
            for item in section.get('items', []):
                # Extract from description
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
                
                # Extract from highlights
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
    
    def embed_evidence_units(self, evidence_units: List[ResumeEvidenceUnit]) -> None:
        """Generate embeddings for evidence units."""
        for unit in evidence_units:
            if unit.embedding is None:
                unit.embedding = self.ai.generate_embedding(unit.text)
    
    def calculate_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        norm1 = math.sqrt(sum(a * a for a in vec1))
        norm2 = math.sqrt(sum(b * b for b in vec2))
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return float(dot_product / (norm1 * norm2))
    
    def calculate_location_match(
        self,
        job: JobPost,
        preferences: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate location match score based on preferences.
        
        Returns: (score, details)
        """
        job_prefs = preferences.get('job_preferences', {})
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
        
        # Remote matching
        if wants_remote:
            if job.is_remote:
                # Perfect match - user wants remote and job is remote
                return 1.0, details
            else:
                # Check if job location is in preferred locations
                job_loc = (job.location_text or '').lower()
                for pref_loc in preferred_locations:
                    if pref_loc.lower() in job_loc:
                        return 0.7, details  # Good match - preferred location
                
                # Check if job location is in avoid locations
                for avoid_loc in avoid_locations:
                    if avoid_loc.lower() in job_loc:
                        return 0.0, details  # Bad match - avoided location
                
                return 0.3, details  # Okay match - not remote but acceptable location
        else:
            # User doesn't care about remote
            if job.is_remote:
                return 0.8, details  # Good - flexible
            else:
                # Check preferred locations
                job_loc = (job.location_text or '').lower()
                for pref_loc in preferred_locations:
                    if pref_loc.lower() in job_loc:
                        return 1.0, details  # Perfect - preferred location
                return 0.6, details  # Okay - not preferred but acceptable
    
    def calculate_company_size_match(
        self,
        job: JobPost,
        preferences: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate company size match score.
        
        Returns: (score, details)
        """
        company_prefs = preferences.get('company_preferences', {})
        size_prefs = company_prefs.get('company_size', {})
        
        details = {
            'job_company_size': job.company_num_employees,
            'preferred_size': size_prefs
        }
        
        if not job.company_num_employees:
            return 0.5, details  # Unknown size - neutral
        
        # Parse employee count
        try:
            emp_count = int(job.company_num_employees)
        except (ValueError, TypeError):
            return 0.5, details  # Can't parse - neutral
        
        employee_range = size_prefs.get('employee_count', {})
        min_size = employee_range.get('minimum', 0)
        max_size = employee_range.get('maximum', float('inf'))
        
        if min_size <= emp_count <= max_size:
            return 1.0, details  # Perfect match
        elif emp_count < min_size:
            # Too small - calculate partial score
            ratio = emp_count / min_size if min_size > 0 else 0
            return max(0.0, ratio * 0.5), details
        else:
            # Too large - calculate partial score
            ratio = max_size / emp_count if emp_count > 0 else 0
            return max(0.0, ratio * 0.5), details
    
    def calculate_industry_match(
        self,
        job: JobPost,
        preferences: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate industry match score.
        
        Returns: (score, details)
        """
        company_prefs = preferences.get('company_preferences', {})
        industry_prefs = company_prefs.get('industry', {})
        
        preferred_industries = industry_prefs.get('preferred', [])
        avoid_industries = industry_prefs.get('avoid', [])
        
        job_industry = (job.company_industry or '').lower()
        
        details = {
            'job_industry': job.company_industry,
            'preferred_industries': preferred_industries,
            'avoid_industries': avoid_industries
        }
        
        # Check avoid list first
        for avoid in avoid_industries:
            if avoid.lower() in job_industry:
                return 0.0, details  # Bad match - avoided industry
        
        # Check preferred list
        for preferred in preferred_industries:
            if preferred.lower() in job_industry:
                return 1.0, details  # Perfect match
        
        # Neutral if no match
        return 0.5, details
    
    def calculate_role_match(
        self,
        job: JobPost,
        preferences: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate role/title match score.
        
        Returns: (score, details)
        """
        career_prefs = preferences.get('career_preferences', {})
        
        preferred_roles = career_prefs.get('role_types', [])
        avoid_roles = career_prefs.get('avoid_roles', [])
        
        job_title = (job.title or '').lower()
        
        details = {
            'job_title': job.title,
            'preferred_roles': preferred_roles,
            'avoid_roles': avoid_roles
        }
        
        # Check avoid list first
        for avoid in avoid_roles:
            if avoid.lower() in job_title:
                return 0.0, details  # Bad match - avoided role
        
        # Check preferred list
        for preferred in preferred_roles:
            if preferred.lower() in job_title:
                return 1.0, details  # Perfect match
        
        # Check seniority level
        target_seniority = career_prefs.get('seniority_level', '')
        job_level = (job.job_level or '').lower()
        
        if target_seniority and job_level:
            if target_seniority.lower() in job_level:
                return 0.8, details  # Good match - seniority matches
        
        # Neutral if no match
        return 0.5, details
    
    def calculate_preferences_alignment(
        self,
        job: JobPost,
        preferences: Optional[Dict[str, Any]]
    ) -> Optional[PreferencesAlignmentScore]:
        """
        Calculate overall preferences alignment score.
        
        Returns None if no preferences provided.
        """
        if not preferences:
            return None
        
        # Calculate individual scores
        location_score, location_details = self.calculate_location_match(job, preferences)
        company_size_score, size_details = self.calculate_company_size_match(job, preferences)
        industry_score, industry_details = self.calculate_industry_match(job, preferences)
        role_score, role_details = self.calculate_role_match(job, preferences)
        
        # Weighted average
        weights = {
            'location': 0.35,
            'company_size': 0.15,
            'industry': 0.25,
            'role': 0.25
        }
        
        overall_score = (
            location_score * weights['location'] +
            company_size_score * weights['company_size'] +
            industry_score * weights['industry'] +
            role_score * weights['role']
        )
        
        return PreferencesAlignmentScore(
            overall_score=overall_score,
            location_match=location_score,
            company_size_match=company_size_score,
            industry_match=industry_score,
            role_match=role_score,
            details={
                'location': location_details,
                'company_size': size_details,
                'industry': industry_details,
                'role': role_details,
                'weights': weights
            }
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
        
        Returns preliminary match with all similarities computed.
        """
        # Job-level matching (if enabled and job has summary embedding)
        job_similarity = 0.0
        if self.config.include_job_level_matching and job.summary_embedding is not None:
            # Create a composite resume text for job-level matching
            resume_text = " ".join([e.text for e in evidence_units[:5]])  # Top 5 evidence units
            resume_embedding = self.ai.generate_embedding(resume_text)
            job_similarity = self.calculate_similarity(resume_embedding, job.summary_embedding)
        
        # Preferences alignment (if preferences provided)
        preferences_alignment = None
        if preferences:
            preferences_alignment = self.calculate_preferences_alignment(job, preferences)
        
        # Requirement-level matching
        matched_requirements = []
        missing_requirements = []
        
        for req in job.requirements:
            best_match = None
            best_similarity = 0.0
            
            # Get requirement embedding
            if req.embedding_row and req.embedding_row.embedding is not None:
                req_embedding = req.embedding_row.embedding
            else:
                req_embedding = self.ai.generate_embedding(req.text)
            
            # Find best matching evidence
            for evidence in evidence_units:
                if evidence.embedding is None:
                    evidence.embedding = self.ai.generate_embedding(evidence.text)
                
                similarity = self.calculate_similarity(req_embedding, evidence.embedding)
                
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = evidence
            
            is_covered = best_similarity >= self.config.similarity_threshold
            
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
        
        return JobMatchPreliminary(
            job=job,
            job_similarity=job_similarity,
            preferences_alignment=preferences_alignment,
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
        
        Returns preliminary matches for all jobs.
        """
        resume_fingerprint = generate_resume_fingerprint(resume_data)
        
        results = []
        for job in jobs:
            preliminary = self.match_resume_to_job(
                evidence_units, job, resume_fingerprint, preferences
            )
            results.append(preliminary)
        
        # Sort by combined score (job_similarity + preferences_alignment)
        def combined_score(p: JobMatchPreliminary) -> float:
            base = p.job_similarity
            if p.preferences_alignment:
                base += p.preferences_alignment.overall_score
            return base
        
        results.sort(key=combined_score, reverse=True)
        
        return results
    
    def get_jobs_for_matching(self, limit: int = None) -> List[JobPost]:
        """
        Fetch jobs that are ready for matching.
        
        Jobs must have:
        - Been extracted (have requirements)
        - Been embedded (have requirement embeddings or summary embedding)
        """
        limit = limit or self.config.batch_size
        
        # Get jobs with embeddings
        from sqlalchemy import select
        from database.models import JobPost
        
        stmt = select(JobPost).where(
            JobPost.is_embedded == True
        ).limit(limit)
        
        return self.repo.db.execute(stmt).scalars().all()


class MockMatcherService(MatcherService):
    """Matcher service with mock embeddings for testing."""
    
    KEYWORD_GROUPS = {
        'java': ['java', 'backend', 'spring', 'jvm', 'enterprise'],
        'python': ['python', 'django', 'flask', 'pandas', 'data'],
        'aws': ['aws', 'amazon', 'cloud', 'ec2', 'eks', 'lambda', 's3', 'infrastructure'],
        'kubernetes': ['kubernetes', 'k8s', 'docker', 'container', 'orchestration'],
        'database': ['database', 'sql', 'postgresql', 'mysql', 'postgres', 'storage'],
        'messaging': ['kafka', 'sqs', 'sns', 'messaging', 'event', 'queue', 'stream'],
        'frontend': ['react', 'frontend', 'javascript', 'typescript', 'web', 'ui', 'css'],
        'observability': ['monitoring', 'logging', 'elasticsearch', 'kibana', 'prometheus'],
        'ai': ['ai', 'llm', 'machine learning', 'openai', 'gemini', 'model', 'inference'],
    }
    
    def embed_evidence_units(self, evidence_units: List[ResumeEvidenceUnit]) -> None:
        """Generate mock embeddings based on keywords."""
        import hashlib
        
        for unit in evidence_units:
            if unit.embedding is None:
                text_lower = unit.text.lower()
                
                # Determine active groups
                active_groups = set()
                for group_name, keywords in self.KEYWORD_GROUPS.items():
                    if any(kw in text_lower for kw in keywords):
                        active_groups.add(group_name)
                
                # Generate deterministic vector
                hash_val = int(hashlib.md5(unit.text.encode()).hexdigest(), 16)
                vector = []
                group_names = list(self.KEYWORD_GROUPS.keys())
                
                for i in range(1024):
                    val = ((hash_val >> (i % 32)) & 0xFF) / 255.0
                    
                    if i < len(group_names):
                        if group_names[i] in active_groups:
                            val = 0.8 + (val * 0.2)
                        else:
                            val = val * 0.1
                    
                    vector.append((val * 2) - 1)
                
                unit.embedding = vector
