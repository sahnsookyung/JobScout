#!/usr/bin/env python3
"""
Notification endpoints - send and manage notifications.
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..dependencies import get_db
from ..services.notification_service import NotificationServiceWrapper
from ..models.requests import NotificationRequest
from ..models.responses import (
    NotificationResponse,
    QueueStatusResponse
)
from notification import NotificationPriority

router = APIRouter(prefix="/api/notifications", tags=["notifications"])


def get_notification_service(db: Session = Depends(get_db)) -> NotificationServiceWrapper:
    """Dependency to get notification service."""
    return NotificationServiceWrapper(db)


@router.post("/send", response_model=NotificationResponse)
def send_notification(
    request: NotificationRequest,
    notification_service: NotificationServiceWrapper = Depends(get_notification_service)
):
    """
    Send a notification via the message queue.
    
    Supports: email, slack, webhook, push
    """
    try:
        priority = NotificationPriority(request.priority)
    except ValueError:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=400,
            detail=f"Invalid priority '{request.priority}'. "
                   f"Valid options: low, normal, high, urgent"
        )
    
    notification_id = notification_service.queue_notification(
        type=request.type,
        recipient=request.recipient,
        subject=request.subject,
        body=request.body,
        priority=priority
    )
    
    return NotificationResponse(
        success=True,
        notification_id=notification_id,
        message=f"Notification queued successfully ({request.type})"
    )


@router.get("/queue-status", response_model=QueueStatusResponse)
def get_queue_status(
    notification_service: NotificationServiceWrapper = Depends(get_notification_service)
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
        redis_connected=status.get('redis_connected', False)
    )
