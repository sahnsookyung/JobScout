#!/usr/bin/env python3
"""
Request models for API endpoints.
"""

from pydantic import BaseModel, Field
from typing import Optional


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


class NotificationRequest(BaseModel):
    """Request to send a notification."""
    type: str = Field(..., description="Notification type: email, slack, webhook, push")
    recipient: str = Field(..., description="Recipient (email, user ID, webhook URL)")
    subject: str = Field(..., description="Notification subject")
    body: str = Field(..., description="Notification body")
    priority: str = Field(default="normal", description="Priority: low, normal, high, urgent")


class ResumeHashCheckRequest(BaseModel):
    """Request to check if a resume hash already exists in the database."""
    resume_hash: str = Field(..., description="SHA-256 hash of the resume file (first 32 chars)")


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
