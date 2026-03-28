"""Per-user notification settings resolution and validation."""

from __future__ import annotations

import os
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

from sqlalchemy.orm import Session

from core.auth import _auth_mode, DEV_BYPASS_AUTH_MODE
from database.models import User, UserNotificationChannel, UserNotificationSettings
from database.repository import JobRepository
from notification.channels import (
    EmailChannel,
    NotificationChannelFactory,
    TelegramChannel,
    _validate_webhook_url,
)
from notification.exceptions import NotificationConfigurationError
from notification.secrets import FernetSecretEncryptionProvider, SecretEncryptionProvider

SUPPORTED_CHANNELS = ("email", "discord", "telegram", "webhook", "in_app")
SECRET_CHANNELS = {"discord", "telegram", "webhook"}


def _mask_email(email: str | None) -> str | None:
    if not email or "@" not in email:
        return None
    _, domain = email.rsplit("@", 1)
    return f"***@{domain}"


def _mask_webhook(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urllib.parse.urlparse(url)
    if not parsed.scheme or not parsed.hostname:
        return None
    return f"{parsed.scheme}://{parsed.hostname}{parsed.path}"


def _mask_telegram(chat_id: str | None) -> str | None:
    if not chat_id:
        return None
    suffix = chat_id[-4:] if len(chat_id) > 4 else chat_id
    return f"chat-***{suffix}"


def _mask_channel_recipient(channel_type: str, value: str | None, user: User | None = None) -> str | None:
    if channel_type == "email":
        return _mask_email(value or getattr(user, "email", None))
    if channel_type in {"discord", "webhook"}:
        return _mask_webhook(value)
    if channel_type == "telegram":
        return _mask_telegram(value)
    if channel_type == "in_app":
        return "In-app inbox"
    return None


def _channel_available(channel_type: str, user: User) -> tuple[bool, Optional[str]]:
    if channel_type == "email":
        if not user.email:
            return False, "A user email is required for email notifications"
        if _auth_mode() != DEV_BYPASS_AUTH_MODE and user.email_verified_at is None:
            return False, "A verified account email is required for email notifications"
        if not EmailChannel().validate_config():
            return False, "SMTP is not configured in this runtime"
        return True, None

    if channel_type == "telegram":
        if not TelegramChannel().validate_config():
            return False, "Telegram bot credentials are not configured"
        return True, None

    if channel_type in {"discord", "webhook", "in_app"}:
        NotificationChannelFactory.get_channel(channel_type)
        return True, None

    return False, f"Unsupported notification channel '{channel_type}'"


def _validate_secret_value(channel_type: str, secret_value: str) -> None:
    if channel_type == "telegram":
        normalized = secret_value.strip()
        if not normalized or any(ch.isspace() for ch in normalized):
            raise NotificationConfigurationError(
                "Telegram chat ID must be a non-empty single token",
                failure_class="telegram_chat_invalid",
            )
        return

    parsed = urllib.parse.urlparse(secret_value)
    if parsed.scheme != "https":
        raise NotificationConfigurationError(
            f"{channel_type.title()} endpoints must use HTTPS",
            failure_class="webhook_scheme_invalid",
        )

    if not _validate_webhook_url(secret_value):
        raise NotificationConfigurationError(
            f"{channel_type.title()} endpoint is invalid or not allowed in this runtime",
            failure_class="webhook_url_invalid",
        )


@dataclass(frozen=True)
class ChannelSnapshot:
    channel_type: str
    enabled: bool
    configured: bool
    available: bool
    availability_reason: Optional[str]
    masked_recipient: Optional[str]
    last_test_status: Optional[str]
    last_tested_at: Optional[datetime]
    last_test_error: Optional[str]
    config_json: dict[str, Any]


@dataclass(frozen=True)
class NotificationSettingsSnapshot:
    owner_id: UUID
    notifications_enabled: bool
    min_score_threshold: int
    notify_on_new_match: bool
    notify_on_batch_complete: bool
    revision: int
    channels: dict[str, ChannelSnapshot]


@dataclass(frozen=True)
class NotificationDeliveryTarget:
    recipient: str
    masked_recipient: Optional[str]
    settings_revision: int
    channel_type: str


class UserNotificationSettingsService:
    """Resolve, validate, and persist per-user notification settings."""

    def __init__(
        self,
        db: Session,
        *,
        encryption_provider: SecretEncryptionProvider | None = None,
    ):
        self.db = db
        self.repo = JobRepository(db)
        self.encryption_provider = encryption_provider or FernetSecretEncryptionProvider()

    def get_settings_snapshot(self, user: User) -> NotificationSettingsSnapshot:
        settings = self.repo.notification_settings.get_settings(user.id)
        channels_by_type = {
            channel.channel_type: channel
            for channel in (settings.channels if settings is not None else [])
        }

        snapshot_channels = {
            channel_type: self._channel_snapshot(
                user=user,
                settings_channel=channels_by_type.get(channel_type),
                channel_type=channel_type,
            )
            for channel_type in SUPPORTED_CHANNELS
        }

        return NotificationSettingsSnapshot(
            owner_id=user.id,
            notifications_enabled=self._root_value(settings, "notifications_enabled", True),
            min_score_threshold=self._root_value(settings, "min_score_threshold", 70),
            notify_on_new_match=self._root_value(settings, "notify_on_new_match", True),
            notify_on_batch_complete=self._root_value(settings, "notify_on_batch_complete", True),
            revision=self._root_value(settings, "revision", 0),
            channels=snapshot_channels,
        )

    def update_settings(self, user: User, payload: dict[str, Any]) -> NotificationSettingsSnapshot:
        settings = self.repo.notification_settings.get_or_create_settings(user.id)
        settings.notifications_enabled = bool(payload["notifications_enabled"])
        settings.min_score_threshold = int(payload["min_score_threshold"])
        settings.notify_on_new_match = bool(payload["notify_on_new_match"])
        settings.notify_on_batch_complete = bool(payload["notify_on_batch_complete"])

        for channel_type, channel_payload in payload["channels"].items():
            normalized = channel_type.lower()
            if normalized not in SUPPORTED_CHANNELS:
                raise NotificationConfigurationError(
                    f"Unsupported notification channel '{channel_type}'",
                    failure_class="channel_unsupported",
                )

            channel = self.repo.notification_settings.get_or_create_channel(user.id, normalized)
            available, reason = _channel_available(normalized, user)
            requested_enabled = bool(channel_payload["enabled"])
            if requested_enabled and not available:
                raise NotificationConfigurationError(
                    reason or f"{normalized} is not available",
                    failure_class="channel_unavailable",
                )

            self._apply_channel_update(user, channel, channel_payload)

            if requested_enabled and not channel.configured:
                raise NotificationConfigurationError(
                    f"{normalized.title()} must be configured before it can be enabled",
                    failure_class="channel_not_configured",
                )

            channel.enabled = requested_enabled

        settings.revision = int(settings.revision or 0) + 1
        self.db.commit()
        self.db.refresh(settings)
        return self.get_settings_snapshot(user)

    def mark_test_result(
        self,
        *,
        owner_id: UUID,
        channel_type: str,
        status: str,
        error_message: str | None = None,
    ) -> None:
        channel = self.repo.notification_settings.get_or_create_channel(owner_id, channel_type)
        channel.last_test_status = status
        channel.last_test_error = error_message
        channel.last_tested_at = datetime.now(timezone.utc)
        self.db.commit()

    def resolve_delivery_target(
        self,
        *,
        owner_id: UUID,
        channel_type: str,
        require_enabled: bool = True,
    ) -> NotificationDeliveryTarget:
        user = self.db.get(User, owner_id)
        if user is None or not user.is_active:
            raise NotificationConfigurationError(
                "Notification user does not exist or is inactive",
                failure_class="user_missing",
            )

        snapshot = self.get_settings_snapshot(user)
        if require_enabled and not snapshot.notifications_enabled:
            raise NotificationConfigurationError(
                "Notifications are disabled for this user",
                failure_class="notifications_disabled",
            )

        channel = snapshot.channels[channel_type]
        if require_enabled and not channel.enabled:
            raise NotificationConfigurationError(
                f"{channel_type.title()} notifications are disabled for this user",
                failure_class="channel_disabled",
            )
        if not channel.available:
            raise NotificationConfigurationError(
                channel.availability_reason or f"{channel_type.title()} is not available",
                failure_class="channel_unavailable",
            )
        if not channel.configured:
            raise NotificationConfigurationError(
                f"{channel_type.title()} is not configured for this user",
                failure_class="channel_not_configured",
            )

        recipient = self._resolved_recipient(user, channel_type)
        return NotificationDeliveryTarget(
            recipient=recipient,
            masked_recipient=channel.masked_recipient,
            settings_revision=snapshot.revision,
            channel_type=channel_type,
        )

    def _resolved_recipient(self, user: User, channel_type: str) -> str:
        if channel_type == "email":
            return user.email
        if channel_type == "in_app":
            return str(user.id)

        channel = self.repo.notification_settings.get_channel(user.id, channel_type)
        if channel is None or not channel.secret_ciphertext:
            raise NotificationConfigurationError(
                f"{channel_type.title()} secret is missing",
                failure_class="secret_missing",
            )
        return self.encryption_provider.decrypt(
            channel.secret_ciphertext,
            channel.secret_key_version,
        )

    def _apply_channel_update(
        self,
        user: User,
        channel: UserNotificationChannel,
        channel_payload: dict[str, Any],
    ) -> None:
        channel_type = channel.channel_type
        secret_in_payload = "secret_value" in channel_payload
        secret_value = channel_payload.get("secret_value")

        if channel_type == "email":
            channel.configured = bool(user.email)
            channel.masked_recipient = _mask_email(user.email)
            channel.secret_ciphertext = None
            channel.secret_key_version = None
            return

        if channel_type == "in_app":
            channel.configured = True
            channel.masked_recipient = "In-app inbox"
            channel.secret_ciphertext = None
            channel.secret_key_version = None
            return

        if secret_in_payload and secret_value is None:
            channel.configured = False
            channel.masked_recipient = None
            channel.secret_ciphertext = None
            channel.secret_key_version = None
            return

        if secret_in_payload and isinstance(secret_value, str):
            normalized_secret = secret_value.strip()
            _validate_secret_value(channel_type, normalized_secret)
            encrypted = self.encryption_provider.encrypt(normalized_secret)
            channel.secret_ciphertext = encrypted.ciphertext
            channel.secret_key_version = encrypted.key_version
            channel.masked_recipient = _mask_channel_recipient(channel_type, normalized_secret, user)
            channel.configured = True

        if channel.secret_ciphertext:
            channel.configured = True
            if not channel.masked_recipient:
                decrypted = self.encryption_provider.decrypt(
                    channel.secret_ciphertext,
                    channel.secret_key_version,
                )
                channel.masked_recipient = _mask_channel_recipient(channel_type, decrypted, user)
        else:
            channel.configured = False
            channel.masked_recipient = None

    @staticmethod
    def _root_value(settings: UserNotificationSettings | None, key: str, default: Any) -> Any:
        if settings is None:
            return default
        return getattr(settings, key, default)

    def _channel_snapshot(
        self,
        *,
        user: User,
        settings_channel: UserNotificationChannel | None,
        channel_type: str,
    ) -> ChannelSnapshot:
        available, reason = _channel_available(channel_type, user)
        configured = False
        masked_recipient = None
        config_json: dict[str, Any] = {}
        last_test_status = None
        last_tested_at = None
        last_test_error = None
        enabled = False

        if settings_channel is not None:
            configured = bool(settings_channel.configured)
            masked_recipient = settings_channel.masked_recipient
            config_json = dict(settings_channel.config_json or {})
            last_test_status = settings_channel.last_test_status
            last_tested_at = settings_channel.last_tested_at
            last_test_error = settings_channel.last_test_error
            enabled = bool(settings_channel.enabled)

        if channel_type == "email":
            configured = bool(user.email)
            masked_recipient = _mask_email(user.email)
        elif channel_type == "in_app":
            configured = True
            masked_recipient = "In-app inbox"

        return ChannelSnapshot(
            channel_type=channel_type,
            enabled=enabled,
            configured=configured,
            available=available,
            availability_reason=reason,
            masked_recipient=masked_recipient,
            last_test_status=last_test_status,
            last_tested_at=last_tested_at,
            last_test_error=last_test_error,
            config_json=config_json,
        )
