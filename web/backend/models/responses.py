#!/usr/bin/env python3
"""
Response models for API endpoints.
"""

from pydantic import BaseModel, ConfigDict, Field
from typing import List, Optional, Dict, Any


class MatchSummary(BaseModel):
    """Summary of a job match."""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "match_id": "550e8400-e29b-41d4-a716-446655440000",
                "job_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                "title": "Senior Python Developer",
                "company": "TechCorp",
                "location": "Remote",
                "is_remote": True,
                "fit_score": 82.5,
                "want_score": 78.0,
                "overall_score": 81.0,
                "base_score": 95.0,
                "penalties": 9.5,
                "required_coverage": 0.9,
                "preferred_coverage": 0.8,
                "match_type": "with_preferences",
                "created_at": "2026-02-01T12:00:00",
                "calculated_at": "2026-02-01T12:00:00"
            }
        }
    )

    match_id: str
    job_id: Optional[str]
    title: str
    company: str
    location: Optional[str]
    is_remote: Optional[bool]

    # Explicit Fit/Want/Overall scores
    fit_score: Optional[float] = Field(None, ge=0, le=100)
    want_score: Optional[float] = Field(None, ge=0, le=100)
    overall_score: float = Field(ge=0, le=100)

    # Legacy fields for backward compatibility
    base_score: float = Field(ge=0, le=100)
    penalties: float = Field(ge=0)
    required_coverage: float = Field(ge=0, le=1)
    preferred_coverage: float = Field(ge=0, le=1)
    match_type: str
    is_hidden: bool = False
    created_at: Optional[str]
    calculated_at: Optional[str]


class RequirementDetail(BaseModel):
    """Details of a requirement match."""
    requirement_id: str
    requirement_text: Optional[str]
    evidence_text: Optional[str]
    evidence_section: Optional[str]
    similarity_score: float = Field(ge=0, le=1)
    is_covered: bool
    req_type: str


class JobDetails(BaseModel):
    """Details of a job posting."""
    job_id: Optional[str]
    title: Optional[str]
    company: Optional[str]
    location: Optional[str]
    is_remote: Optional[bool]
    description: Optional[str]
    salary_min: Optional[float]
    salary_max: Optional[float]
    currency: Optional[str]
    min_years_experience: Optional[int]
    requires_degree: Optional[bool]
    security_clearance: Optional[bool]
    job_level: Optional[str]


class MatchDetail(BaseModel):
    """Detailed match information."""
    match_id: str
    resume_fingerprint: str

    # Explicit Fit/Want/Overall scores
    fit_score: Optional[float] = None
    want_score: Optional[float] = None
    overall_score: float

    # Score breakdowns
    fit_components: Optional[Dict[str, Any]] = None
    want_components: Optional[Dict[str, Any]] = None
    fit_weight: Optional[float] = None
    want_weight: Optional[float] = None

    # Legacy fields
    base_score: float
    penalties: float
    required_coverage: float
    preferred_coverage: float
    total_requirements: int
    matched_requirements_count: int
    match_type: str
    status: str
    created_at: Optional[str]
    calculated_at: Optional[str]
    penalty_details: Dict[str, Any]


class MatchDetailResponse(BaseModel):
    """Response containing full match details."""
    success: bool
    match: MatchDetail
    job: JobDetails
    requirements: List[RequirementDetail]


class MatchesResponse(BaseModel):
    """Response containing list of matches."""
    success: bool
    count: int
    matches: List[MatchSummary]


class ScoreDistribution(BaseModel):
    """Distribution of match scores."""
    excellent: int = Field(ge=0, description="Matches with score >= 80")
    good: int = Field(ge=0, description="Matches with score 60-79")
    average: int = Field(ge=0, description="Matches with score 40-59")
    poor: int = Field(ge=0, description="Matches with score < 40")


class StatsResponse(BaseModel):
    """Response containing overall statistics."""
    success: bool
    stats: Dict[str, Any]


class ScoringWeightsResponse(BaseModel):
    """Response containing scoring weights configuration."""
    fit_weight: float
    want_weight: float
    facet_weights: Dict[str, float]


class PolicyResponse(BaseModel):
    """Response containing result policy configuration."""
    min_fit: float
    top_k: int
    min_jd_required_coverage: Optional[float] = None


class PipelineTaskResponse(BaseModel):
    """Response after starting a pipeline task."""
    success: bool
    task_id: str
    message: str


class PipelineStatusResponse(BaseModel):
    """Response containing pipeline task status."""
    task_id: str
    status: str  # "pending", "running", "completed", "failed"
    matches_count: Optional[int] = None
    saved_count: Optional[int] = None
    notified_count: Optional[int] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None
    step: Optional[str] = None


class NotificationResponse(BaseModel):
    """Response after sending notification."""
    success: bool
    notification_id: str
    message: str


class QueueStatusResponse(BaseModel):
    """Response with queue status."""
    success: bool
    status: str
    queue_length: int
    redis_connected: bool


class HideMatchResponse(BaseModel):
    """Response after toggling match hidden status."""
    success: bool
    match_id: str
    is_hidden: bool


class MatchExplanationResponse(BaseModel):
    """Response containing match explanation."""
    success: bool
    match_id: str
    explanation: Optional[Dict[str, Any]]
    message: Optional[str] = None
