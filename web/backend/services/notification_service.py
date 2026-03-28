#!/usr/bin/env python3
"""
Notification service wrapper for the web application.
"""

import logging
from typing import Any, Dict
from sqlalchemy.orm import Session

from notification import NotificationService, NotificationPriority
from database.repository import JobRepository
from web.backend.config import get_config

logger = logging.getLogger(__name__)


class NotificationServiceWrapper:
    """Wrapper for NotificationService with database session."""
    
    def __init__(self, db: Session):
        self.db = db
        self.repo = JobRepository(db)
        config = get_config()
        notification_config = config.notifications
        self.notification_service = NotificationService(
            self.repo,
            redis_url=notification_config.redis_url,
            base_url=notification_config.base_url,
            use_async_queue=notification_config.use_async_queue,
            channel_configs=notification_config.channels,
        )
    
    def send_notification(
        self,
        channel_type: str,
        recipient: str,
        subject: str,
        body: str,
        user_id: str,
        priority: NotificationPriority = NotificationPriority.NORMAL,
    ) -> str:
        """
        Queue a notification for sending.
        
        Args:
            channel_type: Notification type (email, discord, telegram, webhook, in_app).
            recipient: Recipient address/ID.
            subject: Notification subject.
            body: Notification body.
            user_id: Authenticated user identity for delivery tracking.
            priority: Notification priority.
        
        Returns:
            Notification ID.
        """
        return self.notification_service.send_notification(
            channel_type=channel_type,
            recipient=recipient,
            subject=subject,
            body=body,
            user_id=user_id,
            priority=priority,
            event_type="manual_send",
            allow_resend=True,
            skip_dedup=True,
        )
    
    def get_queue_status(self) -> Dict[str, Any]:
        """
        Get notification queue status.
        
        Returns:
            Queue status information.
        """
        return self.notification_service.get_queue_status()
    
