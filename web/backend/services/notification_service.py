#!/usr/bin/env python3
"""
Notification service wrapper for the web application.
"""

import logging
from typing import Any, Dict
from sqlalchemy.orm import Session

from notification import NotificationService, NotificationPriority
from notification.exceptions import NotificationConfigurationError
from notification.user_settings import (
    USER_FACING_CHANNELS,
    UserNotificationSettingsService,
)
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
        self.settings_service = UserNotificationSettingsService(db)
    
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
            channel_type: Notification type (email, discord, telegram).
            recipient: Recipient address/ID.
            subject: Notification subject.
            body: Notification body.
            user_id: Authenticated user identity for delivery tracking.
            priority: Notification priority.
        
        Returns:
            Notification ID.
        """
        channel_type = channel_type.lower()
        if channel_type not in USER_FACING_CHANNELS:
            raise NotificationConfigurationError(
                f"Unsupported notification channel '{channel_type}'",
                failure_class="channel_unsupported",
            )
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

    def get_settings(self, user) -> Dict[str, Any]:
        """Return effective per-user notification settings."""
        snapshot = self.settings_service.get_settings_snapshot(user)
        return self._snapshot_to_response(snapshot)

    def update_settings(self, user, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Persist per-user notification settings and return the effective state."""
        requested_channels = {
            channel_type.lower(): channel_payload
            for channel_type, channel_payload in payload.get("channels", {}).items()
        }
        unsupported_channels = [
            channel_type for channel_type in requested_channels if channel_type not in USER_FACING_CHANNELS
        ]
        if unsupported_channels:
            raise NotificationConfigurationError(
                f"Unsupported notification channel '{unsupported_channels[0]}'",
                failure_class="channel_unsupported",
            )

        payload = {
            **payload,
            "channels": requested_channels,
        }
        snapshot = self.settings_service.update_settings(user, payload)
        return self._snapshot_to_response(snapshot)

    def send_test_notification(self, user, channel_type: str) -> str:
        """Queue a test notification using the saved per-user channel configuration."""
        channel_type = channel_type.lower()
        if channel_type not in USER_FACING_CHANNELS:
            raise NotificationConfigurationError(
                f"Unsupported notification channel '{channel_type}'",
                failure_class="channel_unsupported",
            )
        target = self.settings_service.resolve_delivery_target(
            owner_id=user.id,
            channel_type=channel_type,
            require_enabled=False,
        )
        notification_id = self.notification_service.send_notification(
            channel_type=channel_type,
            recipient=None,
            subject=f"JobScout test notification via {channel_type}",
            body="This is a saved-configuration test notification from JobScout.",
            user_id=str(user.id),
            event_type="settings_test",
            priority=NotificationPriority.NORMAL,
            metadata={
                "test_notification": True,
                "channel_type": channel_type,
                "settings_revision": target.settings_revision,
            },
            allow_resend=True,
            skip_dedup=True,
            resolve_user_settings=True,
            require_enabled_delivery=False,
        )
        self.settings_service.mark_test_result(
            owner_id=user.id,
            channel_type=channel_type,
            status="queued",
        )
        return notification_id
    
    def get_queue_status(self) -> Dict[str, Any]:
        """
        Get notification queue status.
        
        Returns:
            Queue status information.
        """
        return self.notification_service.get_queue_status()

    @staticmethod
    def _snapshot_to_response(snapshot) -> Dict[str, Any]:
        channels = {}
        for name in USER_FACING_CHANNELS:
            channel = snapshot.channels.get(name)
            if channel is None:
                continue
            channels[name] = {
                "enabled": channel.enabled,
                "configured": channel.configured,
                "available": channel.available,
                "availability_reason": channel.availability_reason,
                "masked_recipient": channel.masked_recipient,
                "last_test_status": channel.last_test_status,
                "last_tested_at": channel.last_tested_at.isoformat() if channel.last_tested_at else None,
                "last_test_error": channel.last_test_error,
            }
        return {
            "notifications_enabled": snapshot.notifications_enabled,
            "min_fit_for_alerts": snapshot.min_fit_for_alerts,
            "notify_on_new_match": snapshot.notify_on_new_match,
            "notify_on_batch_complete": snapshot.notify_on_batch_complete,
            "revision": snapshot.revision,
            "channels": channels,
        }
    
