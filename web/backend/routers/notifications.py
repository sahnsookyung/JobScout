#!/usr/bin/env python3
"""
Notification endpoints - send and manage notifications.
"""

from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi import HTTPException
from sqlalchemy.orm import Session

from ..dependencies import get_current_user, get_db
from ..services.notification_service import NotificationServiceWrapper
from ..models.requests import (
    NotificationRequest,
    NotificationEmailOverrideRequest,
    NotificationEmailVerificationRequest,
    NotificationSettingsTestRequest,
    NotificationSettingsUpdateRequest,
)
from ..models.responses import (
    NotificationResponse,
    NotificationEmailOverrideResponse,
    NotificationSettingsResponse,
    NotificationSettingsTestResponse,
    QueueStatusResponse,
)
from notification import NotificationPriority
from notification.exceptions import NotificationConfigurationError
from ..services.notification_service import NotificationRateLimitError

router = APIRouter(tags=["notifications"])


def get_notification_service(db: Annotated[Session, Depends(get_db)]) -> NotificationServiceWrapper:
    """Dependency to get notification service."""
    return NotificationServiceWrapper(db)


@router.post(
    "/api/notifications/send",
    response_model=NotificationResponse,
    responses={400: {"description": "Invalid notification priority"}},
)
def send_notification(
    request: NotificationRequest,
    notification_service: Annotated[NotificationServiceWrapper, Depends(get_notification_service)],
    user: Annotated[object, Depends(get_current_user)],
):
    """
    Send a notification via the message queue.
    
    Supports: email, discord, telegram, webhook, in_app
    """
    try:
        priority = NotificationPriority(request.priority)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid priority '{request.priority}'. "
                   f"Valid options: low, normal, high, urgent"
        )
    
    try:
        notification_id = notification_service.send_notification(
            channel_type=request.type,
            recipient=request.recipient,
            subject=request.subject,
            body=request.body,
            user_id=str(user.id),
            priority=priority,
        )
    except NotificationConfigurationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    
    return NotificationResponse(
        success=True,
        notification_id=notification_id,
        message=f"Notification queued successfully ({request.type})"
    )


@router.get("/api/v1/notification-settings", response_model=NotificationSettingsResponse)
def get_notification_settings(
    notification_service: Annotated[NotificationServiceWrapper, Depends(get_notification_service)],
    user: Annotated[object, Depends(get_current_user)],
):
    """Fetch the authenticated user's effective notification settings."""
    return NotificationSettingsResponse(**notification_service.get_settings(user))


@router.put(
    "/api/v1/notification-settings",
    response_model=NotificationSettingsResponse,
    responses={400: {"description": "Invalid notification settings"}},
)
def update_notification_settings(
    request: NotificationSettingsUpdateRequest,
    notification_service: Annotated[NotificationServiceWrapper, Depends(get_notification_service)],
    user: Annotated[object, Depends(get_current_user)],
):
    """Persist per-user notification settings."""
    payload = {
        "notifications_enabled": request.notifications_enabled,
        "min_fit_for_alerts": request.min_fit_for_alerts,
        "notify_on_new_match": request.notify_on_new_match,
        "notify_on_batch_complete": request.notify_on_batch_complete,
        "channels": {
            name: {
                "enabled": channel.enabled,
                **(
                    {"secret_value": channel.secret_value}
                    if "secret_value" in channel.model_fields_set
                    else {}
                ),
            }
            for name, channel in request.channels.items()
        },
    }
    try:
        return NotificationSettingsResponse(**notification_service.update_settings(user, payload))
    except NotificationConfigurationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/api/v1/notification-settings/test",
    response_model=NotificationSettingsTestResponse,
    responses={400: {"description": "Invalid notification test request"}},
)
def send_notification_settings_test(
    request: NotificationSettingsTestRequest,
    notification_service: Annotated[NotificationServiceWrapper, Depends(get_notification_service)],
    user: Annotated[object, Depends(get_current_user)],
):
    """Queue a test notification using the saved settings for a channel."""
    try:
        notification_id = notification_service.send_test_notification(
            user,
            request.channel_type,
        )
    except NotificationConfigurationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return NotificationSettingsTestResponse(
        success=True,
        notification_id=notification_id,
        message=f"Queued test notification for {request.channel_type}",
    )

@router.post(
    "/api/v1/notification-settings/email/override",
    response_model=NotificationEmailOverrideResponse,
    responses={
        400: {"description": "Invalid override email address"},
        429: {"description": "Verification send rate-limited"},
    },
)
def create_email_override(
    request: NotificationEmailOverrideRequest,
    notification_service: Annotated[NotificationServiceWrapper, Depends(get_notification_service)],
    user: Annotated[object, Depends(get_current_user)],
):
    """Send an email verification link for a custom notification address."""
    try:
        channel = notification_service.send_email_override_verification(user, request.address)
    except NotificationRateLimitError as exc:
        headers = {"Retry-After": str(exc.retry_after)} if exc.retry_after is not None else None
        raise HTTPException(status_code=429, detail=str(exc), headers=headers) from exc
    except NotificationConfigurationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return NotificationEmailOverrideResponse(
        success=True,
        message="Verification email sent",
        channel=channel,
    )

@router.post(
    "/api/v1/notification-settings/email/verify",
    response_model=NotificationEmailOverrideResponse,
    responses={
        400: {"description": "Invalid or expired verification token"},
    },
)
def verify_email_override(
    request: NotificationEmailVerificationRequest,
    notification_service: Annotated[NotificationServiceWrapper, Depends(get_notification_service)],
):
    """Verify a pending notification email override token."""
    try:
        channel = notification_service.verify_email_override(request.token)
    except NotificationConfigurationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return NotificationEmailOverrideResponse(
        success=True,
        message="Email override verified",
        channel=channel,
    )

@router.delete(
    "/api/v1/notification-settings/email/override",
    response_model=NotificationEmailOverrideResponse,
)
def delete_email_override(
    notification_service: Annotated[NotificationServiceWrapper, Depends(get_notification_service)],
    user: Annotated[object, Depends(get_current_user)],
):
    """Clear any configured notification email override."""
    channel = notification_service.clear_email_override(user)
    return NotificationEmailOverrideResponse(
        success=True,
        message="Email override cleared",
        channel=channel,
    )


@router.get("/api/notifications/queue-status", response_model=QueueStatusResponse)
def get_queue_status(
    notification_service: Annotated[NotificationServiceWrapper, Depends(get_notification_service)],
    _user: Annotated[object, Depends(get_current_user)],
):
    """
    Get the status of the notification queue.
    
    Shows queue length and Redis connection status.
    """
    status = notification_service.get_queue_status()
    
    return QueueStatusResponse(
        success=True,
        status=status.get('status', 'unknown'),
        queue_length=status.get('queue_length', 0),
        failed_job_count=status.get('failed_job_count', 0),
        redis_connected=status.get('redis_connected', False)
    )
