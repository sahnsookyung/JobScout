from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4
from unittest.mock import Mock, patch

import pytest

from notification.exceptions import NotificationConfigurationError
from notification.secrets import EncryptedSecret
from notification.user_settings import (
    UserNotificationSettingsService,
    _channel_available,
    _mask_channel_recipient,
    _mask_email,
    _mask_telegram,
    _mask_webhook,
    _validate_secret_value,
)


class StubEncryptionProvider:
    def encrypt(self, plaintext: str) -> EncryptedSecret:
        return EncryptedSecret(ciphertext=f"enc:{plaintext}", key_version="v1")

    def decrypt(self, ciphertext: str, key_version: str | None) -> str:
        del key_version
        return ciphertext.replace("enc:", "", 1)


def _make_user(**overrides):
    defaults = dict(
        id=uuid4(),
        email="user@example.com",
        email_verified_at=datetime.now(timezone.utc),
        is_active=True,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_channel(channel_type: str, **overrides):
    defaults = dict(
        channel_type=channel_type,
        enabled=False,
        configured=False,
        masked_recipient=None,
        secret_ciphertext=None,
        secret_key_version=None,
        config_json={},
        last_test_status=None,
        last_tested_at=None,
        last_test_error=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_settings(owner_id, **overrides):
    defaults = dict(
        owner_id=owner_id,
        notifications_enabled=True,
        min_score_threshold=70,
        notify_on_new_match=True,
        notify_on_batch_complete=True,
        revision=0,
        channels=[],
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_service(*, user=None, settings=None, channels=None):
    db = Mock()
    db.get.return_value = user
    repo = Mock()
    repo.notification_settings.get_settings.return_value = settings
    repo.notification_settings.get_or_create_settings.return_value = settings
    repo.notification_settings.get_channel.side_effect = (
        lambda owner_id, channel_type: (channels or {}).get(channel_type)
    )
    repo.notification_settings.get_or_create_channel.side_effect = (
        lambda owner_id, channel_type: (channels or {}).setdefault(channel_type, _make_channel(channel_type))
    )
    with patch("notification.user_settings.JobRepository", return_value=repo):
        service = UserNotificationSettingsService(
            db,
            encryption_provider=StubEncryptionProvider(),
        )
    return service, db, repo


class TestNotificationUserSettingsHelpers:
    def test_mask_helpers(self):
        assert _mask_email("user@example.com") == "***@example.com"
        assert _mask_webhook("https://example.com/path") == "https://example.com/path"
        assert _mask_telegram("12345678") == "chat-***5678"
        assert _mask_channel_recipient("in_app", None) == "In-app inbox"

    @patch("notification.user_settings.EmailChannel.validate_config", return_value=True)
    @patch("notification.user_settings._auth_mode", return_value="dev-bypass")
    def test_channel_available_email_in_dev_bypass(self, mock_auth_mode, mock_validate):
        available, reason = _channel_available("email", _make_user(email_verified_at=None))

        assert available is True
        assert reason is None

    @patch("notification.user_settings.EmailChannel.validate_config", return_value=False)
    @patch("notification.user_settings._auth_mode", return_value="password")
    def test_channel_available_email_requires_verified_runtime_and_smtp(self, mock_auth_mode, mock_validate):
        available, reason = _channel_available("email", _make_user(email_verified_at=None))

        assert available is False
        assert "verified account email" in reason

        available, reason = _channel_available(
            "email",
            _make_user(email_verified_at=datetime.now(timezone.utc)),
        )
        assert available is False
        assert "SMTP" in reason

    @patch("notification.user_settings.TelegramChannel.validate_config", return_value=True)
    def test_channel_available_telegram(self, mock_validate):
        available, reason = _channel_available("telegram", _make_user())

        assert available is True
        assert reason is None

    @patch("notification.user_settings.NotificationChannelFactory.get_channel")
    def test_channel_available_factory_backed_channels(self, mock_get_channel):
        available, reason = _channel_available("discord", _make_user())

        assert available is True
        assert reason is None
        mock_get_channel.assert_called_once_with("discord")

    @patch("notification.user_settings._validate_webhook_url", return_value=True)
    def test_validate_secret_value_accepts_valid_inputs(self, mock_validate_webhook):
        _validate_secret_value("telegram", "123456")
        _validate_secret_value("webhook", "https://example.com/hook")

    @patch("notification.user_settings._validate_webhook_url", return_value=False)
    def test_validate_secret_value_rejects_invalid_inputs(self, mock_validate_webhook):
        with pytest.raises(NotificationConfigurationError, match="non-empty single token"):
            _validate_secret_value("telegram", "bad value")

        with pytest.raises(NotificationConfigurationError, match="must use HTTPS"):
            _validate_secret_value("webhook", "http://example.com/hook")

        with pytest.raises(NotificationConfigurationError, match="invalid or not allowed"):
            _validate_secret_value("webhook", "https://example.com/hook")


class TestUserNotificationSettingsService:
    @patch("notification.user_settings.EmailChannel.validate_config", return_value=True)
    @patch("notification.user_settings._auth_mode", return_value="dev-bypass")
    def test_get_settings_snapshot_returns_defaults(self, mock_auth_mode, mock_validate_email):
        user = _make_user()
        service, _, _ = _make_service(user=user, settings=None, channels={})

        snapshot = service.get_settings_snapshot(user)

        assert snapshot.notifications_enabled is True
        assert snapshot.min_score_threshold == 70
        assert snapshot.channels["email"].configured is True
        assert snapshot.channels["in_app"].masked_recipient == "In-app inbox"

    @patch("notification.user_settings.NotificationChannelFactory.get_channel")
    @patch("notification.user_settings.EmailChannel.validate_config", return_value=True)
    @patch("notification.user_settings._auth_mode", return_value="dev-bypass")
    def test_get_settings_snapshot_includes_saved_channel_state(
        self,
        mock_auth_mode,
        mock_validate_email,
        mock_get_channel,
    ):
        user = _make_user()
        discord_channel = _make_channel(
            "discord",
            enabled=True,
            configured=True,
            masked_recipient="https://discord.com/api/webhooks/example",
            last_test_status="queued",
            config_json={"x": 1},
        )
        settings = _make_settings(user.id, revision=3, channels=[discord_channel])
        service, _, _ = _make_service(
            user=user,
            settings=settings,
            channels={"discord": discord_channel},
        )

        snapshot = service.get_settings_snapshot(user)

        assert snapshot.revision == 3
        assert snapshot.channels["discord"].enabled is True
        assert snapshot.channels["discord"].masked_recipient.endswith("/example")
        assert snapshot.channels["discord"].config_json == {"x": 1}

    @patch("notification.user_settings._channel_available", return_value=(True, None))
    def test_update_settings_persists_changes_and_encrypts_secret(self, mock_available):
        user = _make_user()
        discord_channel = _make_channel("discord")
        settings = _make_settings(user.id)
        channels = {"discord": discord_channel}
        service, db, _ = _make_service(user=user, settings=settings, channels=channels)
        service.get_settings_snapshot = Mock(return_value="snapshot")

        snapshot = service.update_settings(
            user,
            {
                "notifications_enabled": False,
                "min_score_threshold": 85,
                "notify_on_new_match": False,
                "notify_on_batch_complete": True,
                "channels": {
                    "discord": {
                        "enabled": True,
                        "secret_value": "https://discord.com/api/webhooks/test",
                    }
                },
            },
        )

        assert snapshot == "snapshot"
        assert settings.notifications_enabled is False
        assert settings.min_score_threshold == 85
        assert discord_channel.enabled is True
        assert discord_channel.secret_ciphertext == "enc:https://discord.com/api/webhooks/test"
        assert discord_channel.masked_recipient.endswith("/test")
        db.commit.assert_called_once()
        db.refresh.assert_called_once_with(settings)

    @patch("notification.user_settings._channel_available", return_value=(False, "Channel unavailable"))
    def test_update_settings_rejects_unavailable_channel(self, mock_available):
        user = _make_user()
        settings = _make_settings(user.id)
        service, _, _ = _make_service(user=user, settings=settings, channels={})

        with pytest.raises(NotificationConfigurationError, match="Channel unavailable"):
            service.update_settings(
                user,
                {
                    "notifications_enabled": True,
                    "min_score_threshold": 70,
                    "notify_on_new_match": True,
                    "notify_on_batch_complete": True,
                    "channels": {"discord": {"enabled": True}},
                },
            )

    def test_update_settings_rejects_unsupported_channel(self):
        user = _make_user()
        settings = _make_settings(user.id)
        service, _, _ = _make_service(user=user, settings=settings, channels={})

        with pytest.raises(NotificationConfigurationError, match="Unsupported notification channel"):
            service.update_settings(
                user,
                {
                    "notifications_enabled": True,
                    "min_score_threshold": 70,
                    "notify_on_new_match": True,
                    "notify_on_batch_complete": True,
                    "channels": {"sms": {"enabled": False}},
                },
            )

    @patch("notification.user_settings._channel_available", return_value=(True, None))
    def test_update_settings_rejects_enabling_unconfigured_channel(self, mock_available):
        user = _make_user()
        discord_channel = _make_channel("discord")
        settings = _make_settings(user.id)
        service, _, _ = _make_service(
            user=user,
            settings=settings,
            channels={"discord": discord_channel},
        )

        with pytest.raises(NotificationConfigurationError, match="must be configured before it can be enabled"):
            service.update_settings(
                user,
                {
                    "notifications_enabled": True,
                    "min_score_threshold": 70,
                    "notify_on_new_match": True,
                    "notify_on_batch_complete": True,
                    "channels": {"discord": {"enabled": True}},
                },
            )

    def test_mark_test_result_updates_channel_and_commits(self):
        owner_id = uuid4()
        webhook_channel = _make_channel("webhook")
        service, db, _ = _make_service(user=_make_user(id=owner_id), settings=None, channels={"webhook": webhook_channel})

        service.mark_test_result(
            owner_id=owner_id,
            channel_type="webhook",
            status="failed",
            error_message="boom",
        )

        assert webhook_channel.last_test_status == "failed"
        assert webhook_channel.last_test_error == "boom"
        assert webhook_channel.last_tested_at is not None
        db.commit.assert_called_once()

    def test_resolve_delivery_target_requires_active_user(self):
        owner_id = uuid4()
        service, _, _ = _make_service(user=None, settings=None, channels={})

        with pytest.raises(NotificationConfigurationError, match="does not exist or is inactive"):
            service.resolve_delivery_target(owner_id=owner_id, channel_type="email")

    def test_resolve_delivery_target_enforces_notifications_enabled(self):
        user = _make_user()
        service, _, _ = _make_service(user=user, settings=None, channels={})
        service.get_settings_snapshot = Mock(
            return_value=SimpleNamespace(
                notifications_enabled=False,
                revision=2,
                channels={"email": SimpleNamespace(enabled=True, available=True, configured=True, masked_recipient="***@example.com")},
            )
        )

        with pytest.raises(NotificationConfigurationError, match="Notifications are disabled"):
            service.resolve_delivery_target(owner_id=user.id, channel_type="email")

    def test_resolve_delivery_target_enforces_channel_state(self):
        user = _make_user()
        channel = SimpleNamespace(enabled=False, available=True, configured=True, masked_recipient="***@example.com")
        service, _, _ = _make_service(user=user, settings=None, channels={})
        service.get_settings_snapshot = Mock(
            return_value=SimpleNamespace(
                notifications_enabled=True,
                revision=2,
                channels={"email": channel},
            )
        )

        with pytest.raises(NotificationConfigurationError, match="disabled"):
            service.resolve_delivery_target(owner_id=user.id, channel_type="email")

        channel.enabled = True
        channel.available = False
        channel.availability_reason = "Not available"
        with pytest.raises(NotificationConfigurationError, match="Not available"):
            service.resolve_delivery_target(owner_id=user.id, channel_type="email")

        channel.available = True
        channel.configured = False
        with pytest.raises(NotificationConfigurationError, match="not configured"):
            service.resolve_delivery_target(owner_id=user.id, channel_type="email")

    def test_resolve_delivery_target_returns_email_and_in_app_recipients(self):
        user = _make_user()
        service, _, _ = _make_service(user=user, settings=None, channels={})
        service.get_settings_snapshot = Mock(
            return_value=SimpleNamespace(
                notifications_enabled=True,
                revision=4,
                channels={
                    "email": SimpleNamespace(enabled=True, available=True, configured=True, masked_recipient="***@example.com"),
                    "in_app": SimpleNamespace(enabled=True, available=True, configured=True, masked_recipient="In-app inbox"),
                },
            )
        )

        email_target = service.resolve_delivery_target(owner_id=user.id, channel_type="email")
        in_app_target = service.resolve_delivery_target(owner_id=user.id, channel_type="in_app")

        assert email_target.recipient == user.email
        assert in_app_target.recipient == str(user.id)

    def test_resolve_delivery_target_decrypts_secret_channels(self):
        user = _make_user()
        discord_channel = _make_channel(
            "discord",
            enabled=True,
            configured=True,
            masked_recipient="https://discord.com/api/webhooks/test",
            secret_ciphertext="enc:https://discord.com/api/webhooks/test",
            secret_key_version="v1",
        )
        service, _, _ = _make_service(
            user=user,
            settings=None,
            channels={"discord": discord_channel},
        )
        service.get_settings_snapshot = Mock(
            return_value=SimpleNamespace(
                notifications_enabled=True,
                revision=5,
                channels={"discord": SimpleNamespace(enabled=True, available=True, configured=True, masked_recipient=discord_channel.masked_recipient)},
            )
        )

        target = service.resolve_delivery_target(owner_id=user.id, channel_type="discord")

        assert target.recipient == "https://discord.com/api/webhooks/test"
        assert target.settings_revision == 5

    def test_apply_channel_update_handles_clear_and_cached_mask(self):
        user = _make_user()
        service, _, _ = _make_service(user=user, settings=None, channels={})
        channel = _make_channel(
            "webhook",
            secret_ciphertext="enc:https://hooks.example.com/h",
            secret_key_version="v1",
        )

        service._apply_channel_update(user, channel, {"secret_value": None})
        assert channel.configured is False
        assert channel.secret_ciphertext is None

        channel.secret_ciphertext = "enc:https://hooks.example.com/h"
        channel.secret_key_version = "v1"
        channel.masked_recipient = None
        service._apply_channel_update(user, channel, {})
        assert channel.configured is True
        assert channel.masked_recipient == "https://hooks.example.com/h"

    @patch("notification.user_settings._channel_available", return_value=(True, None))
    def test_channel_snapshot_uses_channel_state(self, mock_available):
        user = _make_user()
        service, _, _ = _make_service(user=user, settings=None, channels={})
        settings_channel = _make_channel(
            "discord",
            enabled=True,
            configured=True,
            masked_recipient="https://discord.com/api/webhooks/test",
            config_json={"x": 1},
            last_test_status="sent",
        )

        snapshot = service._channel_snapshot(
            user=user,
            settings_channel=settings_channel,
            channel_type="discord",
        )

        assert snapshot.enabled is True
        assert snapshot.config_json == {"x": 1}
        assert snapshot.last_test_status == "sent"
