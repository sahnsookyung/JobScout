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


class MatchLlmEvaluationSummary(BaseModel):
    """Safe public summary of a cached match-level LLM evaluation."""

    id: str
    match_id: Optional[str] = None
    job_id: str
    status: str
    llm_score: Optional[float] = Field(default=None, ge=0, le=100)
    confidence: Optional[float] = Field(default=None, ge=0, le=1)
    verdict: Optional[str] = None
    summary: Optional[str] = None
    reason_codes: List[str] = Field(default_factory=list)
    requirement_verdicts: List[Dict[str, Any]] = Field(default_factory=list)
    provider: str
    model: str
    prompt_version: str
    schema_version: str
    error_code: Optional[str] = None
    retryable: bool = False
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


class MatchLlmEvaluationListResponse(BaseModel):
    """Response containing active LLM evaluations for a match."""

    success: bool
    count: int
    evaluations: List[MatchLlmEvaluationSummary] = Field(default_factory=list)


class MatchLlmEvaluationMutationResponse(BaseModel):
    """Response after generating, regenerating, or deleting an LLM evaluation."""

    success: bool
    evaluation: Optional[MatchLlmEvaluationSummary] = None
    reused: bool = False
    message: str


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
                "preferred_requirement_coverage": 0.8,
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
    preferred_requirement_coverage: float = Field(ge=0, le=1)
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

    scoring_degraded_reason: Optional[str] = None
    selection_tier: str = "primary"
    excluded_reason: Optional[str] = None
    llm_evaluation_status: Optional[str] = None
    llm_evaluation_id: Optional[str] = None
    llm_score: Optional[float] = Field(default=None, ge=0, le=100)
    llm_confidence: Optional[float] = Field(default=None, ge=0, le=1)
    llm_judged_at: Optional[str] = None


class RequirementDetail(BaseModel):
    """Details of a requirement match."""
    requirement_id: str
    requirement_text: Optional[str] = None
    evidence_text: Optional[str] = None
    evidence_section: Optional[str] = None
    similarity_score: float = Field(ge=0, le=1)
    evidence_score: Optional[float] = Field(default=None, ge=0, le=1)
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
    preference_components: Optional[Dict[str, Any]] = None
    fit_confidence: Optional[float] = Field(default=None, ge=0, le=1)
    fit_explanation: Optional[Dict[str, Any]] = None
    fit_scorer: Optional[Dict[str, Any]] = None
    scoring_degraded_reason: Optional[str] = None
    preference_status: Optional[Dict[str, Any]] = None
    llm_evaluation_status: Optional[str] = None
    llm_evaluation_id: Optional[str] = None
    llm_score: Optional[float] = Field(default=None, ge=0, le=100)
    llm_confidence: Optional[float] = Field(default=None, ge=0, le=1)
    llm_judged_at: Optional[str] = None

    # Legacy fields
    base_score: float
    penalties: float
    required_coverage: float
    preferred_requirement_coverage: float
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
    llm_judge_enabled: bool = False
    llm_judge_top_n: int = 5
    llm_judge_top_n_max: int = 10
    llm_judge_available: bool = False
    llm_judge_revision: int = 0


class ProcessingProgress(BaseModel):
    """User-safe task progress metadata."""

    current_step: int = Field(ge=0)
    total_steps: int = Field(ge=1)
    percent: int = Field(ge=0, le=100)
    started_at: Optional[str] = None
    updated_at: Optional[str] = None


class ProcessingWarning(BaseModel):
    """Stable user-facing warning from a background pipeline."""

    code: str
    message: str


class ProcessingFailure(BaseModel):
    """Stable user-facing failure from a background pipeline."""

    code: str
    user_message: str
    retryable: bool = False
    next_action: Optional[str] = None


class PipelineTaskResponse(BaseModel):
    """Response after starting a pipeline task."""
    success: bool
    task_id: str
    message: str


class PipelineStatusResponse(BaseModel):
    """Response containing pipeline task status."""
    task_id: str
    status: str  # "pending", "running", "completed", "failed", "cancelled"
    phase: Optional[str] = None
    progress: Optional[ProcessingProgress] = None
    stats: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[ProcessingWarning] = Field(default_factory=list)
    failure: Optional[ProcessingFailure] = None
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


class FetchSourceHealthResponse(BaseModel):
    """Reachability status for the API backing a fetch source."""

    available: bool
    status: str
    endpoint: Optional[str] = None
    status_code: Optional[int] = None
    response_time_ms: Optional[int] = None
    error: Optional[str] = None


class FetchSourceExternalStatusResponse(BaseModel):
    """Non-secret status for the external seed fetcher backing a source."""

    enabled: bool = False
    configured: bool = False
    status: str = "unconfigured"
    provider: Optional[str] = None
    last_attempt_at: Optional[str] = None
    last_success_at: Optional[str] = None
    next_eligible_at: Optional[str] = None
    failure_class: Optional[str] = None
    budget_remaining: Optional[int] = None


class FetchSourceResponse(BaseModel):
    """Configured job source exposed to the dashboard."""

    site_type: str
    display_name: str
    seed_url: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    search_keywords: List[str] = Field(default_factory=list)
    fetch_mode: str = "jobspy_api"
    provider_name: Optional[str] = None
    search_term: Optional[str] = None
    location: Optional[str] = None
    country: Optional[str] = None
    results_wanted: int = 0
    hours_old: Optional[int] = None
    options: Dict[str, Any] = Field(default_factory=dict)
    api_health: Optional[FetchSourceHealthResponse] = None
    external_fetch_status: Optional[FetchSourceExternalStatusResponse] = None


class FetchSourcesResponse(BaseModel):
    """Response containing configured seed websites and API fetch metadata."""

    success: bool
    jobspy_url: Optional[str] = None
    api_based_fetching: bool
    search_query: Optional[str] = None
    total_count: int = 0
    filtered_count: int = 0
    seed_websites: List[str] = Field(default_factory=list)
    sources: List[FetchSourceResponse] = Field(default_factory=list)


class NotificationResponse(BaseModel):
    """Response after sending notification."""
    success: bool
    notification_id: Optional[str] = None
    message: str

class SourceFetchResponse(BaseModel):
    """Response after a hosted seed website fetch attempt."""

    success: bool
    source: str
    status: str
    fetched_count: int = 0
    imported_count: int = 0
    skipped_count: int = 0
    warnings: List[str] = Field(default_factory=list)
    next_eligible_at: Optional[str] = None
    failure_class: Optional[str] = None
    budget_remaining: Optional[int] = None

class NotificationDeliveryResponse(BaseModel):
    """Sanitized notification delivery history row."""

    id: str
    job_match_id: Optional[str] = None
    channel_type: str
    event_type: str
    recipient_masked: str
    subject: Optional[str] = None
    sent_successfully: bool
    failure_class: Optional[str] = None
    error_message: Optional[str] = None
    first_sent_at: Optional[str] = None
    last_sent_at: Optional[str] = None
    send_count: int = 0
    metadata_summary: Dict[str, Any] = Field(default_factory=dict)


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
    effective_recipient: Optional[str] = None
    override_address: Optional[str] = None
    override_status: Optional[str] = None
    override_verified_at: Optional[str] = None


class NotificationSettingsResponse(BaseModel):
    """Per-user notification settings response."""

    notifications_enabled: bool
    min_fit_for_alerts: int
    notify_on_new_match: bool
    notify_on_batch_complete: bool
    revision: int
    channels: Dict[str, NotificationChannelSettingsResponse]


class NotificationSettingsTestResponse(BaseModel):
    """Response after queueing a test notification."""

    success: bool
    notification_id: Optional[str] = None
    message: str


class NotificationEmailOverrideResponse(BaseModel):
    """Response for email override operations."""

    success: bool
    message: str
    channel: Optional[NotificationChannelSettingsResponse] = None


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
    failed_job_count: int = 0
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
    matching_task_id: Optional[str] = None
    status: Optional[str] = None
    phase: Optional[str] = None
    progress: Optional[ProcessingProgress] = None
    stats: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[ProcessingWarning] = Field(default_factory=list)
    failure: Optional[ProcessingFailure] = None


class ResumeStatusResponse(BaseModel):
    """Response for querying background resume processing status."""
    task_id: str
    status: str  # processing | completed | failed
    step: Optional[str] = None  # extracting | embedding
    matching_task_id: Optional[str] = None
    phase: Optional[str] = None
    progress: Optional[ProcessingProgress] = None
    stats: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[ProcessingWarning] = Field(default_factory=list)
    failure: Optional[ProcessingFailure] = None
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
