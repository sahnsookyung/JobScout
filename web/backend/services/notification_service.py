#!/usr/bin/env python3
"""Notification service wrapper for the web application."""

import hashlib
import logging
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from sqlalchemy.orm import Session

from database.models import User, UserNotificationChannel
from notification import NotificationService, NotificationPriority
from notification.exceptions import NotificationConfigurationError
from notification.user_settings import (
    USER_FACING_CHANNELS,
    UserNotificationSettingsService,
)
from database.repository import JobRepository
from web.backend.config import get_config

logger = logging.getLogger(__name__)
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class NotificationRateLimitError(Exception):
    """Raised when email override verification is rate-limited."""

    def __init__(self, message: str, *, retry_after: int | None = None):
        super().__init__(message)
        self.retry_after = retry_after


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

    @staticmethod
    def _hash_token(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    @staticmethod
    def _email_channel(channel_payload: Dict[str, Any]) -> Dict[str, Any] | None:
        channels = channel_payload.get("channels", {})
        email_channel = channels.get("email")
        return email_channel if isinstance(email_channel, dict) else None

    @staticmethod
    def _verification_window(config_json: Dict[str, Any], now: datetime) -> tuple[datetime, int]:
        started_at_raw = config_json.get("verification_window_started_at")
        count = int(config_json.get("verification_send_count", 0) or 0)
        if isinstance(started_at_raw, str):
            try:
                started_at = datetime.fromisoformat(started_at_raw)
            except ValueError:
                started_at = now
        else:
            started_at = now
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        if now - started_at >= timedelta(hours=24):
            return now, 0
        return started_at, count
    
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

    def send_email_override_verification(self, user, address: str) -> Dict[str, Any]:
        normalized = address.strip().lower()
        if not EMAIL_RE.match(normalized):
            raise NotificationConfigurationError(
                "Enter a valid email address",
                failure_class="email_invalid",
            )
        if normalized == (user.email or "").strip().lower():
            raise NotificationConfigurationError(
                "Override email must be different from the account email",
                failure_class="email_override_same_as_account",
            )

        channel = self.repo.notification_settings.get_or_create_channel(user.id, "email")
        now = datetime.now(timezone.utc)
        if channel.verification_sent_at is not None:
            elapsed = (now - channel.verification_sent_at).total_seconds()
            if elapsed < 60:
                raise NotificationRateLimitError(
                    "Verification email was sent recently. Please wait before retrying.",
                    retry_after=max(1, int(60 - elapsed)),
                )

        config_json = dict(channel.config_json or {})
        window_started_at, send_count = self._verification_window(config_json, now)
        if send_count >= 5:
            retry_after = int((window_started_at + timedelta(hours=24) - now).total_seconds())
            raise NotificationRateLimitError(
                "Verification email rate limit reached. Try again later.",
                retry_after=max(1, retry_after),
            )

        token = secrets.token_urlsafe(32)
        verify_link = f"{self.notification_service.base_url.rstrip('/')}/verify-email#token={token}"

        channel.override_address = normalized
        channel.override_verified_at = None
        channel.verification_token_hash = self._hash_token(token)
        channel.verification_token_expires_at = now + timedelta(hours=24)
        channel.verification_sent_at = now
        channel.config_json = {
            **config_json,
            "verification_window_started_at": window_started_at.isoformat(),
            "verification_send_count": send_count + 1,
        }

        try:
            self.notification_service.send_notification(
                channel_type="email",
                recipient=normalized,
                subject="Verify your JobScout notification email",
                body=(
                    "Confirm this email address for JobScout notifications.\n\n"
                    f"Verify: {verify_link}\n\n"
                    "This link expires in 24 hours."
                ),
                user_id=str(user.id),
                priority=NotificationPriority.NORMAL,
                event_type="email_override_verification",
                allow_resend=True,
                skip_dedup=True,
            )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise
        return self._snapshot_to_response(
            self.settings_service.get_settings_snapshot(user)
        )["channels"]["email"]

    def verify_email_override(self, token: str) -> Dict[str, Any]:
        token_hash = self._hash_token(token.strip())
        channel = (
            self.db.query(UserNotificationChannel)
            .filter(UserNotificationChannel.verification_token_hash == token_hash)
            .one_or_none()
        )
        if channel is None or not channel.override_address:
            raise NotificationConfigurationError(
                "Verification link is invalid",
                failure_class="verification_invalid",
            )
        now = datetime.now(timezone.utc)
        if channel.verification_token_expires_at is None or channel.verification_token_expires_at < now:
            raise NotificationConfigurationError(
                "Verification link has expired",
                failure_class="verification_expired",
            )

        channel.override_verified_at = now
        channel.verification_token_hash = None
        channel.verification_token_expires_at = None
        channel.masked_recipient = None
        self.db.commit()
        user = self.db.get(User, channel.owner_id)
        if user is None:
            raise NotificationConfigurationError(
                "Notification user does not exist",
                failure_class="user_missing",
            )
        return self._snapshot_to_response(
            self.settings_service.get_settings_snapshot(user)
        )["channels"]["email"]

    def clear_email_override(self, user) -> Dict[str, Any]:
        channel = self.repo.notification_settings.get_or_create_channel(user.id, "email")
        channel.override_address = None
        channel.override_verified_at = None
        channel.verification_token_hash = None
        channel.verification_token_expires_at = None
        channel.verification_sent_at = None
        channel.masked_recipient = None
        self.db.commit()
        return self._snapshot_to_response(
            self.settings_service.get_settings_snapshot(user)
        )["channels"]["email"]
    
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
                "effective_recipient": getattr(channel, "effective_recipient", None),
                "override_address": getattr(channel, "override_address", None),
                "override_status": getattr(channel, "override_status", None),
                "override_verified_at": (
                    getattr(channel, "override_verified_at").isoformat()
                    if getattr(channel, "override_verified_at", None)
                    else None
                ),
            }
        return {
            "notifications_enabled": snapshot.notifications_enabled,
            "min_fit_for_alerts": snapshot.min_fit_for_alerts,
            "notify_on_new_match": snapshot.notify_on_new_match,
            "notify_on_batch_complete": snapshot.notify_on_batch_complete,
            "revision": snapshot.revision,
            "channels": channels,
        }
    
