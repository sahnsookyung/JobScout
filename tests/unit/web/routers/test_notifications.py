"""
Tests for notification router endpoints.
"""

from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.testclient import TestClient

from notification import NotificationPriority
from web.backend.dependencies import get_current_user
from web.backend.routers.notifications import (
    get_notification_service,
    router,
)


class _User:
    def __init__(self, user_id: str):
        self.id = user_id


@pytest.fixture
def mock_notification_service():
    return Mock()


@pytest.fixture
def app(mock_notification_service):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_notification_service] = lambda: mock_notification_service
    app.dependency_overrides[get_current_user] = lambda: _User("user-123")
    yield app
    app.dependency_overrides.clear()


@pytest.fixture
def client(app):
    return TestClient(app, raise_server_exceptions=False)


class TestNotificationsRouter:
    def test_send_notification_success_email(self, client, mock_notification_service):
        mock_notification_service.send_notification.return_value = "notif-123"

        response = client.post(
            "/api/notifications/send",
            json={
                "type": "email",
                "recipient": "user@example.com",
                "subject": "New Job Match",
                "body": "You have a new job match!",
                "priority": "normal",
            },
        )

        assert response.status_code == 200
        assert response.json()["notification_id"] == "notif-123"
        mock_notification_service.send_notification.assert_called_once_with(
            channel_type="email",
            recipient="user@example.com",
            subject="New Job Match",
            body="You have a new job match!",
            user_id="user-123",
            priority=NotificationPriority.NORMAL,
        )

    def test_send_notification_success_discord(self, client, mock_notification_service):
        mock_notification_service.send_notification.return_value = "notif-789"

        response = client.post(
            "/api/notifications/send",
            json={
                "type": "discord",
                "recipient": "https://discord.com/api/webhooks/notify",
                "subject": "Alert",
                "body": "{\"event\": \"match_complete\"}",
                "priority": "urgent",
            },
        )

        assert response.status_code == 200
        mock_notification_service.send_notification.assert_called_once_with(
            channel_type="discord",
            recipient="https://discord.com/api/webhooks/notify",
            subject="Alert",
            body="{\"event\": \"match_complete\"}",
            user_id="user-123",
            priority=NotificationPriority.URGENT,
        )

    def test_send_notification_invalid_channel_returns_400(self, client, mock_notification_service):
        from notification.exceptions import NotificationConfigurationError

        mock_notification_service.send_notification.side_effect = NotificationConfigurationError(
            "Unsupported notification channel 'webhook'",
            failure_class="channel_unsupported",
        )

        response = client.post(
            "/api/notifications/send",
            json={
                "type": "webhook",
                "recipient": "https://hooks.example.com/notify",
                "subject": "Alert",
                "body": "{\"event\": \"match_complete\"}",
                "priority": "urgent",
            },
        )

        assert response.status_code == 400
        assert "Unsupported notification channel" in response.json()["detail"]

    def test_send_notification_all_priority_levels(self, client, mock_notification_service):
        mock_notification_service.send_notification.return_value = "notif-123"

        for priority in ["low", "normal", "high", "urgent"]:
            response = client.post(
                "/api/notifications/send",
                json={
                    "type": "email",
                    "recipient": "user@example.com",
                    "subject": "Test",
                    "body": "Test body",
                    "priority": priority,
                },
            )

            assert response.status_code == 200
            expected_priority = getattr(NotificationPriority, priority.upper())
            assert (
                mock_notification_service.send_notification.call_args[1]["priority"]
                == expected_priority
            )
            mock_notification_service.reset_mock()

    def test_send_notification_invalid_priority(self, client):
        response = client.post(
            "/api/notifications/send",
            json={
                "type": "email",
                "recipient": "user@example.com",
                "subject": "Test",
                "body": "Test body",
                "priority": "invalid_priority",
            },
        )

        assert response.status_code == 400
        assert "Invalid priority" in response.json()["detail"]

    def test_send_notification_missing_fields(self, client):
        response = client.post(
            "/api/notifications/send",
            json={
                "type": "email",
                "subject": "Test",
                "body": "Test body",
                "priority": "normal",
            },
        )

        assert response.status_code == 422

    def test_send_notification_service_error(self, client, mock_notification_service):
        mock_notification_service.send_notification.side_effect = Exception("Redis down")

        response = client.post(
            "/api/notifications/send",
            json={
                "type": "email",
                "recipient": "user@example.com",
                "subject": "Test",
                "body": "Test body",
                "priority": "normal",
            },
        )

        assert response.status_code == 500

    def test_send_notification_requires_authentication(
        self,
        mock_notification_service,
    ):
        app = FastAPI()
        app.include_router(router)
        app.dependency_overrides[get_notification_service] = lambda: mock_notification_service
        app.dependency_overrides[get_current_user] = (
            lambda: (_ for _ in ()).throw(
                HTTPException(status_code=401, detail="Authentication required")
            )
        )
        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/api/notifications/send",
            json={
                "type": "email",
                "recipient": "user@example.com",
                "subject": "Test",
                "body": "Test body",
                "priority": "normal",
            },
        )

        assert response.status_code == 401

    def test_get_queue_status_success(self, client, mock_notification_service):
        mock_notification_service.get_queue_status.return_value = {
            "status": "healthy",
            "queue_length": 5,
            "redis_connected": True,
        }

        response = client.get("/api/notifications/queue-status")

        assert response.status_code == 200
        assert response.json()["queue_length"] == 5
        mock_notification_service.get_queue_status.assert_called_once()

    def test_send_notification_documents_invalid_priority_response(self, app):
        schema = app.openapi()
        responses = schema["paths"]["/api/notifications/send"]["post"]["responses"]

        assert responses["400"]["description"] == "Invalid notification priority"

    def test_get_notification_settings_success(self, client, mock_notification_service):
        mock_notification_service.get_settings.return_value = {
            "notifications_enabled": True,
            "min_score_threshold": 80,
            "notify_on_new_match": True,
            "notify_on_batch_complete": False,
            "revision": 3,
            "channels": {
                "email": {
                    "enabled": True,
                    "configured": True,
                    "available": True,
                    "availability_reason": None,
                    "masked_recipient": "***@example.com",
                    "last_test_status": "queued",
                    "last_tested_at": None,
                    "last_test_error": None,
                }
            },
        }

        response = client.get("/api/v1/notification-settings")

        assert response.status_code == 200
        assert response.json()["revision"] == 3
        mock_notification_service.get_settings.assert_called_once()

    def test_update_notification_settings_success(self, client, mock_notification_service):
        mock_notification_service.update_settings.return_value = {
            "notifications_enabled": True,
            "min_score_threshold": 75,
            "notify_on_new_match": True,
            "notify_on_batch_complete": True,
            "revision": 4,
            "channels": {
                "discord": {
                    "enabled": True,
                    "configured": True,
                    "available": True,
                    "availability_reason": None,
                    "masked_recipient": "https://discord.com/api/webhooks/...",
                    "last_test_status": None,
                    "last_tested_at": None,
                    "last_test_error": None,
                }
            },
        }

        response = client.put(
            "/api/v1/notification-settings",
            json={
                "notifications_enabled": True,
                "min_score_threshold": 75,
                "notify_on_new_match": True,
                "notify_on_batch_complete": True,
                "channels": {
                    "discord": {
                        "enabled": True,
                        "secret_value": "https://discord.com/api/webhooks/test",
                    }
                },
            },
        )

        assert response.status_code == 200
        assert response.json()["channels"]["discord"]["enabled"] is True
        payload = mock_notification_service.update_settings.call_args[0][1]
        assert payload["channels"]["discord"]["secret_value"] == "https://discord.com/api/webhooks/test"

    def test_update_notification_settings_omits_secret_when_not_provided(self, client, mock_notification_service):
        mock_notification_service.update_settings.return_value = {
            "notifications_enabled": True,
            "min_score_threshold": 70,
            "notify_on_new_match": True,
            "notify_on_batch_complete": True,
            "revision": 5,
            "channels": {},
        }

        response = client.put(
            "/api/v1/notification-settings",
            json={
                "notifications_enabled": True,
                "min_score_threshold": 70,
                "notify_on_new_match": True,
                "notify_on_batch_complete": True,
                "channels": {
                    "telegram": {
                        "enabled": False,
                    }
                },
            },
        )

        assert response.status_code == 200
        payload = mock_notification_service.update_settings.call_args[0][1]
        assert "secret_value" not in payload["channels"]["telegram"]

    def test_update_notification_settings_rejects_hidden_channel(self, client, mock_notification_service):
        from notification.exceptions import NotificationConfigurationError

        mock_notification_service.update_settings.side_effect = NotificationConfigurationError(
            "Unsupported notification channel 'webhook'",
            failure_class="channel_unsupported",
        )

        response = client.put(
            "/api/v1/notification-settings",
            json={
                "notifications_enabled": True,
                "min_score_threshold": 70,
                "notify_on_new_match": True,
                "notify_on_batch_complete": True,
                "channels": {
                    "webhook": {
                        "enabled": False,
                    }
                },
            },
        )

        assert response.status_code == 400
        assert "Unsupported notification channel" in response.json()["detail"]

    def test_send_notification_settings_test_success(self, client, mock_notification_service):
        mock_notification_service.send_test_notification.return_value = "notif-test-123"

        response = client.post(
            "/api/v1/notification-settings/test",
            json={"channel_type": "email"},
        )

        assert response.status_code == 200
        assert response.json()["notification_id"] == "notif-test-123"
        mock_notification_service.send_test_notification.assert_called_once()

    def test_send_notification_settings_test_invalid_channel_returns_400(self, client, mock_notification_service):
        from notification.exceptions import NotificationConfigurationError

        mock_notification_service.send_test_notification.side_effect = NotificationConfigurationError(
            "Unsupported notification channel 'sms'",
            failure_class="channel_unsupported",
        )

        response = client.post(
            "/api/v1/notification-settings/test",
            json={"channel_type": "sms"},
        )

        assert response.status_code == 400
        assert "Unsupported notification channel" in response.json()["detail"]
