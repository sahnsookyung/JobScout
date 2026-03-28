"""
Tests for the web notification service wrapper.
"""

from types import SimpleNamespace
from unittest.mock import Mock, patch

from notification import NotificationPriority
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
