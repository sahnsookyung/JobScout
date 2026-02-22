"""Data Transfer Objects for matcher service.

DTOs are used to transfer data outside of the Unit of Work context,
allowing ORM objects to be converted to plain Python objects that
can be safely used after the database session is closed.
"""

from dataclasses import dataclass, field
from typing import List, Any, Optional


@dataclass
class JobEvidenceDTO:
    """DTO for evidence associated with a requirement match."""
    text: str
    source_section: Optional[str] = None
    tags: dict = field(default_factory=dict)


@dataclass
class JobRequirementDTO:
    """DTO for requirement matched or missing from a job."""
    id: str
    req_type: str


@dataclass
class RequirementMatchDTO:
    """DTO for a matched or missing requirement."""
    requirement: JobRequirementDTO
    evidence: Optional[JobEvidenceDTO] = None
    similarity: float = 0.0
    is_covered: bool = False


@dataclass
class JobMatchDTO:
    """Data transfer object for job match data outside UoW context.

    This DTO holds essential job information needed for saving matches
    and sending notifications, extracted from ORM objects while the
    database session is still active.
    """
    id: str
    title: str
    company: str
    location_text: str
    is_remote: bool
    content_hash: str = ""


@dataclass
class MatchResultDTO:
    """Data transfer object for scored match results.

    Contains all data needed to save a match to the database and
    send notifications, extracted from ScoredJobMatch ORM objects
    while the database session is still active.
    """
    job: JobMatchDTO
    overall_score: float
    fit_score: float
    want_score: float
    job_similarity: float
    jd_required_coverage: float
    jd_preferences_coverage: float
    requirement_matches: List[RequirementMatchDTO]
    missing_requirements: List[RequirementMatchDTO]
    resume_fingerprint: str
    fit_components: dict = field(default_factory=dict)
    want_components: dict = field(default_factory=dict)
    base_score: float = 0.0
    penalties: float = 0.0
    penalty_details: dict = field(default_factory=dict)
    fit_weight: float = 0.7
    want_weight: float = 0.3
    match_type: str = "requirements_only"


def penalty_details_from_orm(orm_penalty_details, total_penalties: float = 0.0, **kwargs) -> dict:
    """Convert ORM penalty_details (List) to dict format for database.
    
    Converts numpy types in list items to native Python types.
    
    Args:
        orm_penalty_details: List of penalty detail dicts from ORM
        total_penalties: Total penalty amount (from ORM penalties field)
    
    Returns:
        Dict with 'details' and 'total'
    """
    from core.scorer.persistence import _to_native_types
    
    if isinstance(orm_penalty_details, dict):
        return orm_penalty_details
    
    # Convert list items to native Python types
    converted_details = _to_native_types(orm_penalty_details)
    
    return {
        'details': converted_details,
        'total': total_penalties,
    }
