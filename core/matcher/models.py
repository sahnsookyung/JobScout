"""
Matcher Models - Data structures for matching.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

from database.models import JobRequirementUnit
from etl.resume.models import ResumeEvidenceUnit


@dataclass
class RequirementEvidenceCandidate:
    """Candidate resume evidence recalled for a requirement."""
    evidence: ResumeEvidenceUnit
    similarity: float
    rank: int


@dataclass
class RequirementMatchResult:
    """Result of matching a single requirement."""
    requirement: JobRequirementUnit
    evidence: Optional[ResumeEvidenceUnit]
    similarity: float
    is_covered: bool
    evidence_candidates: List[RequirementEvidenceCandidate] = field(default_factory=list)


@dataclass
class JobMatchPreliminary:
    """Preliminary match before scoring (output of MatcherService)."""
    job: 'database.models.JobPost'
    job_similarity: float
    requirement_matches: List[RequirementMatchResult]
    missing_requirements: List[RequirementMatchResult]
    resume_fingerprint: str
    owner_id: Optional[Any] = None
    retrieval_score: float = 0.0
    lexical_score: Optional[float] = None
