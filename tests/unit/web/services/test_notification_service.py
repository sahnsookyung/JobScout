"""
Tests for the web notification service wrapper.
"""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from notification import NotificationPriority
from notification.exceptions import NotificationConfigurationError
from web.backend.services.notification_service import NotificationServiceWrapper


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
            allow_resend=True,
            skip_dedup=True,
        )

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
