"""
Matcher Models - Data structures for matching.
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from database.models import JobRequirementUnit
from etl.resume.models import ResumeEvidenceUnit


@dataclass
class RequirementMatchResult:
    """Result of matching a single requirement."""
    requirement: JobRequirementUnit
    evidence: Optional[ResumeEvidenceUnit]
    similarity: float
    is_covered: bool


@dataclass
class JobMatchPreliminary:
    """Preliminary match before scoring (output of MatcherService)."""
    job: 'database.models.JobPost'
    job_similarity: float
    requirement_matches: List[RequirementMatchResult]
    missing_requirements: List[RequirementMatchResult]
    resume_fingerprint: str
