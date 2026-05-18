"""
Tests for the web notification service wrapper.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from notification import NotificationPriority
from notification.exceptions import NotificationConfigurationError
from web.backend.services.notification_service import (
    EMAIL_RE,
    NotificationRateLimitError,
    NotificationServiceWrapper,
)


class TestNotificationServiceWrapper:
    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_init_passes_config_to_notification_service(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
    ):
        db = Mock()
        repo = Mock()
        mock_repo_class.return_value = repo
        notification_settings = SimpleNamespace(
            redis_url="redis://example/0",
            base_url="https://jobscout.app",
            use_async_queue=False,
            channels={"email": {"recipient": "user@example.com"}},
        )
        mock_get_config.return_value = SimpleNamespace(notifications=notification_settings)

        wrapper = NotificationServiceWrapper(db)

        mock_repo_class.assert_called_once_with(db)
        mock_notification_service_class.assert_called_once_with(
            repo,
            redis_url="redis://example/0",
            base_url="https://jobscout.app",
            use_async_queue=False,
            channel_configs={"email": {"recipient": "user@example.com"}},
        )
        assert wrapper.db is db

    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_send_notification_uses_manual_send_defaults(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
    ):
        db = Mock()
        service = Mock()
        service.send_notification.return_value = "notif-123"
        mock_notification_service_class.return_value = service
        mock_repo_class.return_value = Mock()
        mock_get_config.return_value = SimpleNamespace(
            notifications=SimpleNamespace(
                redis_url="redis://example/0",
                base_url="https://jobscout.app",
                use_async_queue=True,
                channels={},
            )
        )

        wrapper = NotificationServiceWrapper(db)
        result = wrapper.send_notification(
            channel_type="email",
            recipient="user@example.com",
            subject="Test",
            body="Body",
            user_id="user-123",
            priority=NotificationPriority.HIGH,
        )

        assert result == "notif-123"
        service.send_notification.assert_called_once_with(
            channel_type="email",
            recipient="user@example.com",
            subject="Test",
            body="Body",
            user_id="user-123",
            priority=NotificationPriority.HIGH,
            event_type="manual_send",
            metadata=None,
            allow_resend=True,
            skip_dedup=True,
        )

    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_send_notification_uses_idempotency_key_for_dedupe(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
    ):
        db = Mock()
        service = Mock()
        service.send_notification.return_value = None
        mock_notification_service_class.return_value = service
        mock_repo_class.return_value = Mock()
        mock_get_config.return_value = SimpleNamespace(
            notifications=SimpleNamespace(
                redis_url="redis://example/0",
                base_url="https://jobscout.app",
                use_async_queue=True,
                channels={},
            )
        )

        wrapper = NotificationServiceWrapper(db)
        result = wrapper.send_notification(
            channel_type="email",
            recipient="user@example.com",
            subject="Test",
            body="Body",
            user_id="user-123",
            priority=NotificationPriority.HIGH,
            idempotency_key="client-key",
        )

        assert result is None
        call = service.send_notification.call_args.kwargs
        assert call["event_type"].startswith("manual_send:")
        assert call["metadata"]["idempotency_key_digest"] in call["event_type"]
        assert call["allow_resend"] is False
        assert call["skip_dedup"] is False

    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_get_queue_status_delegates(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
    ):
        service = Mock()
        service.get_queue_status.return_value = {"status": "healthy", "queue_length": 2}
        mock_notification_service_class.return_value = service
        mock_repo_class.return_value = Mock()
        mock_get_config.return_value = SimpleNamespace(
            notifications=SimpleNamespace(
                redis_url="redis://example/0",
                base_url="https://jobscout.app",
                use_async_queue=True,
                channels={},
            )
        )

        wrapper = NotificationServiceWrapper(Mock())

        assert wrapper.get_queue_status() == {"status": "healthy", "queue_length": 2}
        service.get_queue_status.assert_called_once_with()

    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_list_deliveries_filters_and_sanitizes_rows(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
    ):
        row = SimpleNamespace(
            id="delivery-1",
            job_match_id=None,
            channel_type="email",
            event_type="manual_send:abcdef",
            recipient="***@example.com",
            subject="Subject",
            sent_successfully=True,
            failure_class=None,
            error_message=None,
            first_sent_at=datetime(2026, 5, 18, 0, 0, tzinfo=timezone.utc),
            last_sent_at=datetime(2026, 5, 18, 0, 1, tzinfo=timezone.utc),
            send_count=2,
            event_data={
                "idempotency_key_digest": "abcdef",
                "resolved_recipient_masked": "***@example.com",
                "secret": "must-not-leak",
            },
        )
        scalars = Mock()
        scalars.all.return_value = [row]
        db = Mock()
        db.execute.return_value.scalars.return_value = scalars
        mock_notification_service_class.return_value = Mock()
        mock_repo_class.return_value = Mock()
        mock_get_config.return_value = SimpleNamespace(
            notifications=SimpleNamespace(
                redis_url="redis://example/0",
                base_url="https://jobscout.app",
                use_async_queue=True,
                channels={},
            )
        )

        wrapper = NotificationServiceWrapper(db)
        result = wrapper.list_deliveries(
            SimpleNamespace(id="user-123"),
            channel_type="email",
            event_type="manual_send",
            status="sent",
        )

        assert result[0]["event_type"] == "manual_send"
        assert result[0]["recipient_masked"] == "***@example.com"
        assert result[0]["metadata_summary"] == {
            "idempotency_key_digest": "abcdef",
            "resolved_recipient_masked": "***@example.com",
        }

    @patch("web.backend.services.notification_service.UserNotificationSettingsService")
    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_get_settings_serializes_snapshot(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
        mock_settings_service_class,
    ):
        snapshot = SimpleNamespace(
            notifications_enabled=True,
            min_fit_for_alerts=77,
            notify_on_new_match=True,
            notify_on_batch_complete=False,
            revision=6,
            channels={
                "email": SimpleNamespace(
                    enabled=True,
                    configured=True,
                    available=True,
                    availability_reason=None,
                    masked_recipient="***@example.com",
                    last_test_status="queued",
                    last_tested_at=None,
                    last_test_error=None,
                )
            },
        )
        mock_settings_service = Mock()
        mock_settings_service.get_settings_snapshot.return_value = snapshot
        mock_settings_service_class.return_value = mock_settings_service
        mock_notification_service_class.return_value = Mock()
        mock_repo_class.return_value = Mock()
        mock_get_config.return_value = SimpleNamespace(
            notifications=SimpleNamespace(
                redis_url="redis://example/0",
                base_url="https://jobscout.app",
                use_async_queue=True,
                channels={},
            )
        )

        wrapper = NotificationServiceWrapper(Mock())
        result = wrapper.get_settings(SimpleNamespace(id="user-123"))

        assert result["revision"] == 6
        assert result["channels"]["email"]["masked_recipient"] == "***@example.com"

    @patch("web.backend.services.notification_service.UserNotificationSettingsService")
    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_update_settings_serializes_updated_snapshot(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
        mock_settings_service_class,
    ):
        snapshot = SimpleNamespace(
            notifications_enabled=False,
            min_fit_for_alerts=88,
            notify_on_new_match=False,
            notify_on_batch_complete=True,
            revision=7,
            channels={
                "email": SimpleNamespace(
                    enabled=True,
                    configured=True,
                    available=True,
                    availability_reason=None,
                    masked_recipient="***@example.com",
                    last_test_status="sent",
                    last_tested_at=None,
                    last_test_error=None,
                )
            },
        )
        mock_settings_service = Mock()
        mock_settings_service.update_settings.return_value = snapshot
        mock_settings_service_class.return_value = mock_settings_service
        mock_notification_service_class.return_value = Mock()
        mock_repo_class.return_value = Mock()
        mock_get_config.return_value = SimpleNamespace(
            notifications=SimpleNamespace(
                redis_url="redis://example/0",
                base_url="https://jobscout.app",
                use_async_queue=True,
                channels={},
            )
        )

        wrapper = NotificationServiceWrapper(Mock())
        user = SimpleNamespace(id="user-123")
        payload = {"notifications_enabled": False}

        result = wrapper.update_settings(user, payload)

        mock_settings_service.update_settings.assert_called_once_with(
            user,
            {"notifications_enabled": False, "channels": {}},
        )
        assert result["revision"] == 7
        assert result["channels"]["email"]["enabled"] is True

    @patch("web.backend.services.notification_service.UserNotificationSettingsService")
    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_get_settings_filters_non_user_facing_channels(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
        mock_settings_service_class,
    ):
        snapshot = SimpleNamespace(
            notifications_enabled=True,
            min_fit_for_alerts=70,
            notify_on_new_match=True,
            notify_on_batch_complete=True,
            revision=3,
            channels={
                "email": SimpleNamespace(
                    enabled=True,
                    configured=True,
                    available=True,
                    availability_reason=None,
                    masked_recipient="***@example.com",
                    last_test_status=None,
                    last_tested_at=None,
                    last_test_error=None,
                ),
                "webhook": SimpleNamespace(
                    enabled=True,
                    configured=True,
                    available=True,
                    availability_reason=None,
                    masked_recipient="https://hooks.example.com/notify",
                    last_test_status=None,
                    last_tested_at=None,
                    last_test_error=None,
                ),
                "in_app": SimpleNamespace(
                    enabled=True,
                    configured=True,
                    available=True,
                    availability_reason=None,
                    masked_recipient="In-app inbox",
                    last_test_status=None,
                    last_tested_at=None,
                    last_test_error=None,
                ),
            },
        )
        mock_settings_service = Mock()
        mock_settings_service.get_settings_snapshot.return_value = snapshot
        mock_settings_service_class.return_value = mock_settings_service
        mock_notification_service_class.return_value = Mock()
        mock_repo_class.return_value = Mock()
        mock_get_config.return_value = SimpleNamespace(
            notifications=SimpleNamespace(
                redis_url="redis://example/0",
                base_url="https://jobscout.app",
                use_async_queue=True,
                channels={},
            )
        )

        wrapper = NotificationServiceWrapper(Mock())

        result = wrapper.get_settings(SimpleNamespace(id="user-123"))

        assert set(result["channels"]) == {"email"}

    @patch("web.backend.services.notification_service.UserNotificationSettingsService")
    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_send_notification_rejects_non_user_facing_channel(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
        mock_settings_service_class,
    ):
        from notification.exceptions import NotificationConfigurationError

        mock_settings_service_class.return_value = Mock()
        mock_notification_service_class.return_value = Mock()
        mock_repo_class.return_value = Mock()
        mock_get_config.return_value = SimpleNamespace(
            notifications=SimpleNamespace(
                redis_url="redis://example/0",
                base_url="https://jobscout.app",
                use_async_queue=True,
                channels={},
            )
        )

        wrapper = NotificationServiceWrapper(Mock())

        with pytest.raises(NotificationConfigurationError, match="Unsupported notification channel"):
            wrapper.send_notification(
                channel_type="webhook",
                recipient="https://hooks.example.com/notify",
                subject="Alert",
                body="Body",
                user_id="user-123",
            )

    @patch("web.backend.services.notification_service.UserNotificationSettingsService")
    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_update_settings_rejects_non_user_facing_channel(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
        mock_settings_service_class,
    ):
        from notification.exceptions import NotificationConfigurationError

        mock_settings_service_class.return_value = Mock()
        mock_notification_service_class.return_value = Mock()
        mock_repo_class.return_value = Mock()
        mock_get_config.return_value = SimpleNamespace(
            notifications=SimpleNamespace(
                redis_url="redis://example/0",
                base_url="https://jobscout.app",
                use_async_queue=True,
                channels={},
            )
        )

        wrapper = NotificationServiceWrapper(Mock())

        with pytest.raises(NotificationConfigurationError, match="Unsupported notification channel"):
            wrapper.update_settings(
                SimpleNamespace(id="user-123"),
                {
                    "notifications_enabled": True,
                    "min_fit_for_alerts": 70,
                    "notify_on_new_match": True,
                    "notify_on_batch_complete": True,
                    "channels": {
                        "webhook": {
                            "enabled": False,
                        }
                    },
                },
            )

    @patch("web.backend.services.notification_service.UserNotificationSettingsService")
    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_send_test_notification_queues_saved_config_delivery(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
        mock_settings_service_class,
    ):
        mock_settings_service = Mock()
        mock_settings_service.resolve_delivery_target.return_value = SimpleNamespace(
            settings_revision=8,
        )
        mock_settings_service_class.return_value = mock_settings_service
        service = Mock()
        service.send_notification.return_value = "notif-test"
        mock_notification_service_class.return_value = service
        mock_repo_class.return_value = Mock()
        mock_get_config.return_value = SimpleNamespace(
            notifications=SimpleNamespace(
                redis_url="redis://example/0",
                base_url="https://jobscout.app",
                use_async_queue=True,
                channels={},
            )
        )

        wrapper = NotificationServiceWrapper(Mock())
        result = wrapper.send_test_notification(SimpleNamespace(id="user-123"), "discord")

        assert result == "notif-test"
        service.send_notification.assert_called_once_with(
            channel_type="discord",
            recipient=None,
            subject="JobScout test notification via discord",
            body="This is a saved-configuration test notification from JobScout.",
            user_id="user-123",
            event_type="settings_test",
            priority=NotificationPriority.NORMAL,
            metadata={
                "test_notification": True,
                "channel_type": "discord",
                "settings_revision": 8,
            },
            allow_resend=True,
            skip_dedup=True,
            resolve_user_settings=True,
            require_enabled_delivery=False,
        )
        mock_settings_service.mark_test_result.assert_called_once_with(
            owner_id="user-123",
            channel_type="discord",
            status="queued",
        )

    @patch("web.backend.services.notification_service.UserNotificationSettingsService")
    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_send_test_notification_rejects_unsupported_channel(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
        mock_settings_service_class,
    ):
        from notification.exceptions import NotificationConfigurationError

        mock_settings_service_class.return_value = Mock()
        mock_notification_service_class.return_value = Mock()
        mock_repo_class.return_value = Mock()
        mock_get_config.return_value = SimpleNamespace(
            notifications=SimpleNamespace(
                redis_url="redis://example/0",
                base_url="https://jobscout.app",
                use_async_queue=True,
                channels={},
            )
        )

        wrapper = NotificationServiceWrapper(Mock())

        with pytest.raises(NotificationConfigurationError, match="Unsupported notification channel"):
            wrapper.send_test_notification(SimpleNamespace(id="user-123"), "webhook")

    @patch("web.backend.services.notification_service.UserNotificationSettingsService")
    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_send_email_override_verification_commits_after_send(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
        mock_settings_service_class,
    ):
        db = Mock()
        channel = SimpleNamespace(
            verification_sent_at=None,
            config_json={},
            override_address=None,
            override_verified_at=None,
            verification_token_hash=None,
            verification_token_expires_at=None,
        )
        repo = Mock()
        repo.notification_settings.get_or_create_channel.return_value = channel
        mock_repo_class.return_value = repo
        service = Mock()
        mock_notification_service_class.return_value = service
        snapshot = SimpleNamespace(
            notifications_enabled=True,
            min_fit_for_alerts=70,
            notify_on_new_match=True,
            notify_on_batch_complete=True,
            revision=1,
            channels={"email": SimpleNamespace(
                enabled=True,
                configured=True,
                available=True,
                availability_reason=None,
                masked_recipient="***@example.com",
                last_test_status=None,
                last_tested_at=None,
                last_test_error=None,
                effective_recipient="alerts@example.com",
                override_address="alerts@example.com",
                override_status="pending",
                override_verified_at=None,
            )}
        )
        mock_settings_service = Mock()
        mock_settings_service.get_settings_snapshot.return_value = snapshot
        mock_settings_service_class.return_value = mock_settings_service
        mock_get_config.return_value = SimpleNamespace(
            notifications=SimpleNamespace(
                redis_url="redis://example/0",
                base_url="https://jobscout.app",
                use_async_queue=True,
                channels={},
            )
        )

        wrapper = NotificationServiceWrapper(db)
        user = SimpleNamespace(id="user-123", email="account@example.com")

        result = wrapper.send_email_override_verification(user, "alerts@example.com")

        service.send_notification.assert_called_once()
        db.rollback.assert_not_called()
        db.commit.assert_called_once()
        sent_body = service.send_notification.call_args.kwargs["body"]
        assert "/verify-email#token=" in sent_body
        assert result["override_status"] == "pending"

    @patch("web.backend.services.notification_service.UserNotificationSettingsService")
    @patch("web.backend.services.notification_service.NotificationService")
    @patch("web.backend.services.notification_service.JobRepository")
    @patch("web.backend.services.notification_service.get_config")
    def test_send_email_override_verification_rolls_back_on_send_error(
        self,
        mock_get_config,
        mock_repo_class,
        mock_notification_service_class,
        mock_settings_service_class,
    ):
        db = Mock()
        channel = SimpleNamespace(
            verification_sent_at=None,
            config_json={},
            override_address=None,
            override_verified_at=None,
            verification_token_hash=None,
            verification_token_expires_at=None,
        )
        repo = Mock()
        repo.notification_settings.get_or_create_channel.return_value = channel
        mock_repo_class.return_value = repo
        service = Mock()
        service.send_notification.side_effect = RuntimeError("queue unavailable")
        mock_notification_service_class.return_value = service
        mock_settings_service_class.return_value = Mock()
        mock_get_config.return_value = SimpleNamespace(
            notifications=SimpleNamespace(
                redis_url="redis://example/0",
                base_url="https://jobscout.app",
                use_async_queue=True,
                channels={},
            )
        )

        wrapper = NotificationServiceWrapper(db)
        user = SimpleNamespace(id="user-123", email="account@example.com")

        with pytest.raises(RuntimeError, match="queue unavailable"):
            wrapper.send_email_override_verification(user, "alerts@example.com")

        db.rollback.assert_called_once()
        db.commit.assert_not_called()


def _baseline_config():
    return SimpleNamespace(
        notifications=SimpleNamespace(
            redis_url="redis://example/0",
            base_url="https://jobscout.app",
            use_async_queue=True,
            channels={},
        )
    )


def _channel_stub():
    return SimpleNamespace(
        verification_sent_at=None,
        config_json={},
        override_address=None,
        override_verified_at=None,
        verification_token_hash=None,
        verification_token_expires_at=None,
        masked_recipient=None,
        owner_id="user-123",
    )


def _pending_snapshot():
    return SimpleNamespace(
        notifications_enabled=True,
        min_fit_for_alerts=70,
        notify_on_new_match=True,
        notify_on_batch_complete=True,
        revision=1,
        channels={"email": SimpleNamespace(
            enabled=True,
            configured=True,
            available=True,
            availability_reason=None,
            masked_recipient="***@example.com",
            last_test_status=None,
            last_tested_at=None,
            last_test_error=None,
            effective_recipient="alerts@example.com",
            override_address="alerts@example.com",
            override_status="pending",
            override_verified_at=None,
        )},
    )


class TestEmailRegexAndHelpers:
    def test_email_regex_matches_standard_addresses(self):
        assert EMAIL_RE.match("alerts@example.com")
        assert EMAIL_RE.match("first.last+tag@sub.example.co.uk")

    def test_email_regex_rejects_missing_at(self):
        assert EMAIL_RE.match("no-at-sign") is None

    def test_email_regex_rejects_no_tld(self):
        assert EMAIL_RE.match("local@nodotdomain") is None

    def test_verification_window_resets_after_24h(self):
        now = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)
        old = (now - timedelta(hours=25)).isoformat()
        started, count = NotificationServiceWrapper._verification_window(
            {"verification_window_started_at": old, "verification_send_count": 4},
            now,
        )
        assert started == now
        assert count == 0

    def test_verification_window_preserves_active_window(self):
        now = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)
        recent = (now - timedelta(hours=2)).isoformat()
        started, count = NotificationServiceWrapper._verification_window(
            {"verification_window_started_at": recent, "verification_send_count": 3},
            now,
        )
        assert started.replace(tzinfo=timezone.utc).isoformat() == recent
        assert count == 3

    def test_verification_window_handles_unparseable_timestamp(self):
        now = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)
        started, count = NotificationServiceWrapper._verification_window(
            {"verification_window_started_at": "not-iso", "verification_send_count": 2},
            now,
        )
        assert started == now
        # Active window with unparseable start → treated as 'now started' with existing count preserved.
        assert count == 2

    def test_verification_window_first_send_defaults_to_zero(self):
        now = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)
        started, count = NotificationServiceWrapper._verification_window({}, now)
        assert started == now
        assert count == 0

    def test_email_channel_returns_none_for_missing_or_invalid_payload(self):
        assert NotificationServiceWrapper._email_channel({}) is None
        assert NotificationServiceWrapper._email_channel({"channels": {"email": "not-a-dict"}}) is None

    def test_verification_window_normalizes_naive_timestamp_to_utc(self):
        now = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)
        started, count = NotificationServiceWrapper._verification_window(
            {
                "verification_window_started_at": "2026-04-18T10:00:00",
                "verification_send_count": 2,
            },
            now,
        )
        assert started == datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc)
        assert count == 2

    def test_hash_token_produces_stable_hex(self):
        h1 = NotificationServiceWrapper._hash_token("abc")
        h2 = NotificationServiceWrapper._hash_token("abc")
        assert h1 == h2
        assert len(h1) == 64


@patch("web.backend.services.notification_service.UserNotificationSettingsService")
@patch("web.backend.services.notification_service.NotificationService")
@patch("web.backend.services.notification_service.JobRepository")
@patch("web.backend.services.notification_service.get_config", side_effect=_baseline_config)
def _build_wrapper(
    mock_get_config,
    mock_repo_class,
    mock_notification_service_class,
    mock_settings_service_class,
    *,
    db,
    channel,
    send_notification=None,
    snapshot=None,
    channel_for_token=None,
    user=None,
):
    repo = Mock()
    if channel is not None:
        repo.notification_settings.get_or_create_channel.return_value = channel
    mock_repo_class.return_value = repo
    service = Mock()
    if send_notification is not None:
        service.send_notification.side_effect = send_notification
    mock_notification_service_class.return_value = service
    settings_service = Mock()
    settings_service.get_settings_snapshot.return_value = snapshot or _pending_snapshot()
    mock_settings_service_class.return_value = settings_service
    wrapper = NotificationServiceWrapper(db)
    return wrapper, service, settings_service


class TestSendEmailOverrideErrorPaths:
    def test_address_failing_format_is_rejected(self):
        db = Mock()
        wrapper, *_ = _build_wrapper(db=db, channel=_channel_stub())
        user = SimpleNamespace(id="user-123", email="account@example.com")
        with pytest.raises(NotificationConfigurationError, match="valid email"):
            wrapper.send_email_override_verification(user, "no-at-sign")
        db.commit.assert_not_called()

    def test_overly_long_address_is_rejected(self):
        db = Mock()
        wrapper, *_ = _build_wrapper(db=db, channel=_channel_stub())
        user = SimpleNamespace(id="user-123", email="account@example.com")
        long_local = "a" * 400
        with pytest.raises(NotificationConfigurationError, match="valid email"):
            wrapper.send_email_override_verification(user, f"{long_local}@example.com")

    def test_same_as_account_email_is_rejected(self):
        db = Mock()
        wrapper, *_ = _build_wrapper(db=db, channel=_channel_stub())
        user = SimpleNamespace(id="user-123", email="account@example.com")
        with pytest.raises(NotificationConfigurationError, match="different from the account"):
            wrapper.send_email_override_verification(user, "Account@Example.com")

    def test_rate_limit_within_60s_window(self):
        db = Mock()
        channel = _channel_stub()
        channel.verification_sent_at = datetime.now(timezone.utc) - timedelta(seconds=10)
        wrapper, *_ = _build_wrapper(db=db, channel=channel)
        user = SimpleNamespace(id="user-123", email="account@example.com")
        with pytest.raises(NotificationRateLimitError) as exc_info:
            wrapper.send_email_override_verification(user, "alerts@example.com")
        assert exc_info.value.retry_after is not None and exc_info.value.retry_after > 0

    def test_rate_limit_after_five_sends_in_24h(self):
        db = Mock()
        channel = _channel_stub()
        now = datetime.now(timezone.utc)
        channel.verification_sent_at = now - timedelta(minutes=5)
        channel.config_json = {
            "verification_window_started_at": (now - timedelta(hours=3)).isoformat(),
            "verification_send_count": 5,
        }
        wrapper, *_ = _build_wrapper(db=db, channel=channel)
        user = SimpleNamespace(id="user-123", email="account@example.com")
        with pytest.raises(NotificationRateLimitError):
            wrapper.send_email_override_verification(user, "alerts@example.com")


class TestVerifyAndClearEmailOverride:
    def _query_returns(self, db, channel):
        query = Mock()
        query.filter.return_value.one_or_none.return_value = channel
        db.query.return_value = query
        return query

    def test_verify_accepts_valid_token_and_marks_verified(self):
        db = Mock()
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        channel = SimpleNamespace(
            override_address="alerts@example.com",
            override_verified_at=None,
            verification_token_hash=NotificationServiceWrapper._hash_token("raw-token"),
            verification_token_expires_at=future,
            masked_recipient="***@example.com",
            owner_id="user-123",
        )
        self._query_returns(db, channel)
        db.get.return_value = SimpleNamespace(id="user-123", email="account@example.com")
        wrapper, *_ = _build_wrapper(db=db, channel=channel)
        result = wrapper.verify_email_override("raw-token")
        assert channel.override_verified_at is not None
        assert channel.verification_token_hash is None
        assert channel.verification_token_expires_at is None
        db.commit.assert_called_once()
        assert result["override_status"] == "pending"  # snapshot-driven

    def test_verify_rejects_unknown_token(self):
        db = Mock()
        self._query_returns(db, None)
        wrapper, *_ = _build_wrapper(db=db, channel=None)
        with pytest.raises(NotificationConfigurationError, match="invalid"):
            wrapper.verify_email_override("bad")

    def test_verify_rejects_channel_without_override_address(self):
        db = Mock()
        channel = SimpleNamespace(
            override_address=None,
            override_verified_at=None,
            verification_token_hash="whatever",
            verification_token_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            masked_recipient=None,
            owner_id="user-123",
        )
        self._query_returns(db, channel)
        wrapper, *_ = _build_wrapper(db=db, channel=None)
        with pytest.raises(NotificationConfigurationError, match="invalid"):
            wrapper.verify_email_override("raw")

    def test_verify_rejects_expired_token(self):
        db = Mock()
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        channel = SimpleNamespace(
            override_address="alerts@example.com",
            override_verified_at=None,
            verification_token_hash=NotificationServiceWrapper._hash_token("raw"),
            verification_token_expires_at=past,
            masked_recipient=None,
            owner_id="user-123",
        )
        self._query_returns(db, channel)
        wrapper, *_ = _build_wrapper(db=db, channel=None)
        with pytest.raises(NotificationConfigurationError, match="expired"):
            wrapper.verify_email_override("raw")

    def test_verify_raises_when_user_missing(self):
        db = Mock()
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        channel = SimpleNamespace(
            override_address="alerts@example.com",
            override_verified_at=None,
            verification_token_hash=NotificationServiceWrapper._hash_token("raw"),
            verification_token_expires_at=future,
            masked_recipient=None,
            owner_id="user-123",
        )
        self._query_returns(db, channel)
        db.get.return_value = None
        wrapper, *_ = _build_wrapper(db=db, channel=None)
        with pytest.raises(NotificationConfigurationError, match="user does not exist"):
            wrapper.verify_email_override("raw")

    def test_clear_resets_override_and_returns_snapshot(self):
        db = Mock()
        channel = _channel_stub()
        channel.override_address = "alerts@example.com"
        channel.override_verified_at = datetime.now(timezone.utc)
        channel.verification_token_hash = "abc"
        channel.verification_token_expires_at = datetime.now(timezone.utc)
        channel.verification_sent_at = datetime.now(timezone.utc)
        channel.masked_recipient = "***@example.com"
        wrapper, *_ = _build_wrapper(db=db, channel=channel)
        wrapper.clear_email_override(SimpleNamespace(id="user-123"))
        assert channel.override_address is None
        assert channel.override_verified_at is None
        assert channel.verification_token_hash is None
        assert channel.verification_token_expires_at is None
        assert channel.verification_sent_at is None
        assert channel.masked_recipient is None
        db.commit.assert_called_once()
