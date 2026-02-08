#!/usr/bin/env python3
"""
Request models for API endpoints.
"""

from pydantic import BaseModel, Field
from typing import Optional


class PolicyUpdate(BaseModel):
    """Request to update result policy."""
    min_fit: float = Field(ge=0, le=100, description="Minimum fit score (0-100)")
    top_k: int = Field(ge=1, le=500, description="Maximum results to return (1-500)")
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
