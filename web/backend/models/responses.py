#!/usr/bin/env python3
"""
Response models for API endpoints.
"""

from pydantic import BaseModel, ConfigDict, Field
from typing import List, Optional, Dict, Any


class ApiFieldError(BaseModel):
    """Structured validation metadata for a single request field/path."""

    path: List[str]
    code: str
    message: str


class ApiError(BaseModel):
    """Canonical error body for migrated API endpoints."""

    code: str
    message: str
    detail: Optional[str] = None
    fields: Optional[List[ApiFieldError]] = None


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
                "preference_score": 0.74,
                "penalties": 9.5,
                "required_coverage": 0.9,
                "preferred_coverage": 0.8,
                "match_type": "requirements_only",
                "ranking_mode_used": "balanced",
                "dominant_reason_code": "balanced_blend",
                "explanation_label": "Balanced blend of preference and fit",
                "balanced_primary_score": 0.775,
                "missing_scores": [],
                "created_at": "2026-02-01T12:00:00",
                "calculated_at": "2026-02-01T12:00:00"
            }
        }
    )

    match_id: str
    job_id: Optional[str] = None
    title: str
    company: str
    location: Optional[str] = None
    is_remote: Optional[bool] = None

    fit_score: Optional[float] = Field(default=None, ge=0, le=100)
    preference_score: Optional[float] = Field(default=None, ge=0, le=1)

    penalties: float = Field(ge=0)
    required_coverage: float = Field(ge=0, le=1)
    preferred_coverage: float = Field(ge=0, le=1)
    match_type: str
    is_hidden: bool = False
    created_at: Optional[str] = None
    calculated_at: Optional[str] = None

    # Ranking explanation fields
    ranking_mode_used: Optional[str] = None
    dominant_reason_code: Optional[str] = None
    explanation_label: Optional[str] = None
    balanced_primary_score: Optional[float] = None
    missing_scores: List[str] = Field(default_factory=list)


class RequirementDetail(BaseModel):
    """Details of a requirement match."""
    requirement_id: str
    requirement_text: Optional[str] = None
    evidence_text: Optional[str] = None
    evidence_section: Optional[str] = None
    similarity_score: float = Field(ge=0, le=1)
    is_covered: bool
    req_type: str


class JobDetails(BaseModel):
    """Details of a job posting."""
    job_id: Optional[str] = None
    title: Optional[str] = None
    company: Optional[str] = None
    location: Optional[str] = None
    is_remote: Optional[bool] = None
    description: Optional[str] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    currency: Optional[str] = None
    min_years_experience: Optional[int] = None
    requires_degree: Optional[bool] = None
    security_clearance: Optional[bool] = None
    job_level: Optional[str] = None


class MatchDetail(BaseModel):
    """Detailed match information."""
    match_id: str
    resume_fingerprint: str

    fit_score: Optional[float] = None
    preference_score: Optional[float] = None

    # Score breakdowns
    fit_components: Optional[Dict[str, Any]] = None
    fit_confidence: Optional[float] = Field(default=None, ge=0, le=1)
    fit_explanation: Optional[Dict[str, Any]] = None
    fit_scorer: Optional[Dict[str, Any]] = None

    # Legacy fields
    base_score: float
    penalties: float
    required_coverage: float
    preferred_coverage: float
    total_requirements: int
    matched_requirements_count: int
    match_type: str
    status: str
    created_at: Optional[str] = None
    calculated_at: Optional[str] = None
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
    """Response containing the active final-score source."""

    fit_score_source: str


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
    status: str  # "pending", "running", "completed", "failed", "cancelled"
    upload_id: Optional[str] = None
    resume_fingerprint: Optional[str] = None
    matches_count: Optional[int] = None
    saved_count: Optional[int] = None
    notified_count: Optional[int] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None
    step: Optional[str] = None
    stale_due_to_newer_upload: bool = False
    latest_upload_id: Optional[str] = None
    latest_resume_fingerprint: Optional[str] = None
    stale_message: Optional[str] = None


class NotificationResponse(BaseModel):
    """Response after sending notification."""
    success: bool
    notification_id: str
    message: str


class NotificationChannelSettingsResponse(BaseModel):
    """Per-channel notification settings response."""

    enabled: bool
    configured: bool
    available: bool
    availability_reason: Optional[str] = None
    masked_recipient: Optional[str] = None
    last_test_status: Optional[str] = None
    last_tested_at: Optional[str] = None
    last_test_error: Optional[str] = None


class NotificationSettingsResponse(BaseModel):
    """Per-user notification settings response."""

    notifications_enabled: bool
    min_score_threshold: int
    notify_on_new_match: bool
    notify_on_batch_complete: bool
    revision: int
    channels: Dict[str, NotificationChannelSettingsResponse]


class NotificationSettingsTestResponse(BaseModel):
    """Response after queueing a test notification."""

    success: bool
    notification_id: Optional[str] = None
    message: str


class CandidatePreferencesResponse(BaseModel):
    """Per-user candidate preferences response."""

    remote_mode: str
    target_locations: List[str]
    visa_sponsorship_required: bool
    salary_min: Optional[int] = None
    employment_types: List[str]
    soft_preferences: str
    soft_preference_summary: Optional[str] = None
    preference_mode: str
    allowed_preference_modes: List[str] = Field(default_factory=list)
    effective_preference_mode: str
    revision: int


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
    explanation: Optional[Dict[str, Any]] = None
    message: Optional[str] = None


class ResumeHashCheckResponse(BaseModel):
    """Response for checking if a resume hash exists."""
    exists: bool
    resume_hash: str


class ResumeUploadResponse(BaseModel):
    """Response after uploading a resume."""
    success: bool
    resume_hash: str
    message: str
    upload_id: Optional[str] = None
    task_id: Optional[str] = None
    status: Optional[str] = None


class ResumeStatusResponse(BaseModel):
    """Response for querying background resume processing status."""
    task_id: str
    status: str  # processing | completed | failed
    step: Optional[str] = None  # extracting | embedding
    message: Optional[str] = None
    error: Optional[str] = None


class ResumeEligibilityResponse(BaseModel):
    """Authoritative matching eligibility for the latest uploaded resume."""
    can_run: bool
    status: str
    message: str
    retryable: bool
    upload_id: Optional[str] = None
    resume_hash: Optional[str] = None
    task_id: Optional[str] = None


class ResumePreflightResponse(BaseModel):
    """Read-only preflight result for a locally computed resume hash."""
    status: str
    message: str
    retryable: bool
    can_skip_upload: bool
    resume_hash: str
    upload_id: Optional[str] = None
    task_id: Optional[str] = None

class ScrapeJobsResponse(BaseModel):
    """Response after triggering job scraping."""
    success: bool
    message: str
    jobs_gathered: int = 0
    extraction_triggered: bool = False
    embeddings_triggered: bool = False
