#!/usr/bin/env python3
"""
Notification service wrapper for the web application.
"""

import logging
from typing import Dict, Any
from sqlalchemy.orm import Session

from notification import NotificationService, NotificationPriority
from database.repository import JobRepository

logger = logging.getLogger(__name__)


class NotificationServiceWrapper:
    """Wrapper for NotificationService with database session."""
    
    def __init__(self, db: Session):
        self.db = db
        self.repo = JobRepository(db)
        self.notification_service = NotificationService(self.repo)
    
    def queue_notification(
        self,
        type: str,
        recipient: str,
        subject: str,
        body: str,
        priority: NotificationPriority = NotificationPriority.NORMAL
    ) -> str:
        """
        Queue a notification for sending.
        
        Args:
            type: Notification type (email, slack, webhook, push).
            recipient: Recipient address/ID.
            subject: Notification subject.
            body: Notification body.
            priority: Notification priority.
        
        Returns:
            Notification ID.
        """
        return self.notification_service.queue_notification(
            type=type,
            recipient=recipient,
            subject=subject,
            body=body,
            priority=priority
        )
    
    def get_queue_status(self) -> Dict[str, Any]:
        """
        Get notification queue status.
        
        Returns:
            Queue status information.
        """
        return self.notification_service.get_queue_status()
    