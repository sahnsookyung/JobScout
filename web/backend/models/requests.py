#!/usr/bin/env python3
"""Request models for API endpoints."""

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class PolicyUpdate(BaseModel):
    """Request to update result policy."""
    min_fit: Optional[float] = Field(None, ge=0, le=100, description="Minimum fit score (0-100)")
    top_k: Optional[int] = Field(None, ge=1, le=500, description="Maximum results to return (1-500)")
    min_jd_required_coverage: Optional[float] = Field(
        None,
        ge=0,
        le=1,
        description="Minimum job description coverage (0-1), or null to disable"
    )
    llm_judge_enabled: Optional[bool] = Field(
        None,
        description="Whether to run optional LLM judging for top selected matches",
    )
    llm_judge_auto_enqueue_enabled: Optional[bool] = Field(
        None,
        description="Whether matching and policy updates should automatically queue top-N LLM judging",
    )
    llm_judge_top_n: Optional[int] = Field(
        None,
        ge=1,
        le=100,
        description="Requested top-N matches to LLM judge, capped by the server",
    )
    active_default_mode: Optional[
        Literal["preference_first", "fit_first", "balanced"]
    ] = Field(None, description="Default ordering applied to new match lists")
    balanced_w_pref: Optional[float] = Field(
        None,
        ge=0,
        le=1,
        description="Preference share of balanced ranking (0-1)",
    )
    balanced_w_fit: Optional[float] = Field(
        None,
        ge=0,
        le=1,
        description="Fit share of balanced ranking (0-1)",
    )


class MatchLlmEvaluationRequest(BaseModel):
    """Request to generate or regenerate an LLM match evaluation."""

    force: bool = Field(
        default=False,
        description="When true, tombstone the active evaluation and create a fresh one",
    )


class LlmEvaluationQueuePauseRequest(BaseModel):
    """Request to pause application-side LLM queue execution."""

    reason: Optional[str] = Field(default="manual", max_length=200)
    ttl_seconds: Optional[int] = Field(
        default=None,
        ge=30,
        le=86400,
        description="Optional automatic resume window in seconds.",
    )


class LlmProviderCircuitResetRequest(BaseModel):
    """Request to reset one provider/model circuit."""

    provider: str = Field(..., min_length=1, max_length=80)
    model: str = Field(..., min_length=1, max_length=160)


class NotificationRequest(BaseModel):
    """Request to send a notification."""
    type: str = Field(
        ...,
        description="Notification type: email, discord, telegram",
    )
    recipient: str = Field(
        ...,
        description="Recipient (email address, Discord webhook URL, or Telegram chat ID)",
    )
    subject: str = Field(..., description="Notification subject")
    body: str = Field(..., description="Notification body")
    priority: str = Field(default="normal", description="Priority: low, normal, high, urgent")
    idempotency_key: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=128,
        description="Optional client idempotency key for manual sends",
    )


class NotificationChannelSettingsUpdate(BaseModel):
    """Per-channel notification settings update."""

    enabled: bool = Field(default=False, description="Whether the channel is enabled")
    secret_value: Optional[str] = Field(
        default=None,
        description="Optional secret value. Omit to keep existing, null to clear.",
    )


class NotificationSettingsUpdateRequest(BaseModel):
    """Per-user notification settings update request."""

    notifications_enabled: bool = Field(default=True)
    min_fit_for_alerts: int = Field(default=70, ge=0, le=100)
    notify_on_new_match: bool = Field(default=True)
    notify_on_batch_complete: bool = Field(default=True)
    channels: Dict[str, NotificationChannelSettingsUpdate] = Field(default_factory=dict)


class NotificationSettingsTestRequest(BaseModel):
    """Request to send a saved-config test notification."""

    channel_type: str = Field(
        ...,
        description="Channel to test: email, discord, telegram",
    )


class NotificationEmailOverrideRequest(BaseModel):
    """Request to start email override verification."""

    address: str = Field(..., min_length=3, max_length=320)

class NotificationEmailVerificationRequest(BaseModel):
    """Request to verify a pending email override token."""

    token: str = Field(..., min_length=10)


class CandidatePreferencesUpdateRequest(BaseModel):
    """Per-user candidate preference update request."""

    remote_mode: Literal["any", "remote", "hybrid", "onsite"] = Field(default="any")
    target_locations: List[str] = Field(default_factory=list)
    visa_sponsorship_required: bool = Field(default=False)
    salary_min: Optional[int] = Field(default=None, ge=0)
    employment_types: List[str] = Field(default_factory=list)
    soft_preferences: str = Field(default="")
    preference_mode: Literal["semantic_rerank", "llm_judge"] = Field(default="semantic_rerank")
    preference_rerank_top_n: Optional[int] = Field(default=None, ge=1)


class ResumeHashCheckRequest(BaseModel):
    """Request to check if a resume hash already exists in the database."""
    resume_hash: str = Field(..., description="Client-computed resume hash")


class ResumePreflightRequest(BaseModel):
    """Read-only preflight check for a locally computed resume hash."""
    resume_hash: str = Field(..., description="Client-computed resume hash")


class ResumeSelectRequest(BaseModel):
    """Select a previously processed resume as the latest upload intent."""
    resume_hash: str = Field(..., description="Client-computed resume hash")
    original_filename: Optional[str] = Field(None, description="Original filename for display")


class ResumeRetryRequest(BaseModel):
    """Retry a failed upload attempt."""
    upload_id: str = Field(..., description="Upload attempt identifier to retry")

class SourceFetchRequest(BaseModel):
    """Request to fetch a configured seed website source."""

    source: Literal["tokyodev", "japandev"] = Field(
        ...,
        description="Configured seed website source to fetch",
    )
    limit: Optional[int] = Field(
        default=None,
        ge=1,
        le=250,
        description="Optional source-specific max jobs to fetch, capped at 250",
    )
