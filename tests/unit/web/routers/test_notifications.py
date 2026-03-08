#!/usr/bin/env python3
"""
Tests for Notifications Router
Covers: web/backend/routers/notifications.py
"""

import pytest
from unittest.mock import Mock, patch
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from web.backend.routers.notifications import router, get_notification_service
from notification import NotificationPriority


class TestNotificationsRouter:
    """Test notifications router endpoints."""

    @pytest.fixture
    def app(self):
        """Create test FastAPI app with notifications router."""
        app = FastAPI()
        app.include_router(router)
        return app

    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app, raise_server_exceptions=False)

    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return Mock(spec=Session)

    @pytest.fixture
    def mock_notification_service(self):
        """Create mock notification service."""
        with patch('web.backend.routers.notifications.NotificationServiceWrapper') as mock:
            yield mock

    def test_send_notification_success_email(self, client, mock_notification_service):
        """Test successful send notification - email."""
        mock_service_instance = Mock()
        mock_service_instance.queue_notification.return_value = "notif-123"
        mock_notification_service.return_value = mock_service_instance

        response = client.post(
            '/api/notifications/send',
            json={
                'type': 'email',
                'recipient': 'user@example.com',
                'subject': 'New Job Match',
                'body': 'You have a new job match!',
                'priority': 'normal'
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['notification_id'] == 'notif-123'
        assert 'queued successfully' in data['message']

        mock_service_instance.queue_notification.assert_called_once_with(
            type='email',
            recipient='user@example.com',
            subject='New Job Match',
            body='You have a new job match!',
            priority=NotificationPriority.NORMAL
        )

    def test_send_notification_success_slack(self, client, mock_notification_service):
        """Test successful send notification - slack."""
        mock_service_instance = Mock()
        mock_service_instance.queue_notification.return_value = "notif-456"
        mock_notification_service.return_value = mock_service_instance

        response = client.post(
            '/api/notifications/send',
            json={
                'type': 'slack',
                'recipient': '#jobs-channel',
                'subject': 'Pipeline Complete',
                'body': 'Matching pipeline completed successfully',
                'priority': 'high'
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['notification_id'] == 'notif-456'

        mock_service_instance.queue_notification.assert_called_once_with(
            type='slack',
            recipient='#jobs-channel',
            subject='Pipeline Complete',
            body='Matching pipeline completed successfully',
            priority=NotificationPriority.HIGH
        )

    def test_send_notification_success_webhook(self, client, mock_notification_service):
        """Test successful send notification - webhook."""
        mock_service_instance = Mock()
        mock_service_instance.queue_notification.return_value = "notif-789"
        mock_notification_service.return_value = mock_service_instance

        response = client.post(
            '/api/notifications/send',
            json={
                'type': 'webhook',
                'recipient': 'https://hooks.example.com/notify',
                'subject': 'Alert',
                'body': '{"event": "match_complete"}',
                'priority': 'urgent'
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True

        mock_service_instance.queue_notification.assert_called_once_with(
            type='webhook',
            recipient='https://hooks.example.com/notify',
            subject='Alert',
            body='{"event": "match_complete"}',
            priority=NotificationPriority.URGENT
        )

    def test_send_notification_success_push(self, client, mock_notification_service):
        """Test successful send notification - push."""
        mock_service_instance = Mock()
        mock_service_instance.queue_notification.return_value = "notif-push"
        mock_notification_service.return_value = mock_service_instance

        response = client.post(
            '/api/notifications/send',
            json={
                'type': 'push',
                'recipient': 'device-token-123',
                'subject': 'New Match',
                'body': 'You have a 90% match!',
                'priority': 'low'
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True

        mock_service_instance.queue_notification.assert_called_once_with(
            type='push',
            recipient='device-token-123',
            subject='New Match',
            body='You have a 90% match!',
            priority=NotificationPriority.LOW
        )

    def test_send_notification_all_priority_levels(self, client, mock_notification_service):
        """Test send notification with all priority levels."""
        mock_service_instance = Mock()
        mock_service_instance.queue_notification.return_value = "notif-123"
        mock_notification_service.return_value = mock_service_instance

        for priority in ['low', 'normal', 'high', 'urgent']:
            response = client.post(
                '/api/notifications/send',
                json={
                    'type': 'email',
                    'recipient': 'user@example.com',
                    'subject': 'Test',
                    'body': 'Test body',
                    'priority': priority
                }
            )

            assert response.status_code == 200

            expected_priority = getattr(NotificationPriority, priority.upper())
            call_kwargs = mock_service_instance.queue_notification.call_args[1]
            assert call_kwargs['priority'] == expected_priority

            mock_service_instance.reset_mock()

    def test_send_notification_invalid_priority(self, client):
        """Test send notification with invalid priority."""
        response = client.post(
            '/api/notifications/send',
            json={
                'type': 'email',
                'recipient': 'user@example.com',
                'subject': 'Test',
                'body': 'Test body',
                'priority': 'invalid_priority'
            }
        )

        assert response.status_code == 400
        data = response.json()
        assert 'Invalid priority' in data['detail']
        assert 'low' in data['detail']
        assert 'normal' in data['detail']
        assert 'high' in data['detail']
        assert 'urgent' in data['detail']

    def test_send_notification_missing_fields(self, client):
        """Test send notification with missing required fields."""
        # Missing recipient
        response = client.post(
            '/api/notifications/send',
            json={
                'type': 'email',
                'subject': 'Test',
                'body': 'Test body',
                'priority': 'normal'
            }
        )

        assert response.status_code == 422

    def test_send_notification_service_error(self, client, mock_notification_service):
        """Test send notification when service raises error."""
        mock_service_instance = Mock()
        mock_service_instance.queue_notification.side_effect = Exception("Redis connection failed")
        mock_notification_service.return_value = mock_service_instance

        response = client.post(
            '/api/notifications/send',
            json={
                'type': 'email',
                'recipient': 'user@example.com',
                'subject': 'Test',
                'body': 'Test body',
                'priority': 'normal'
            }
        )

        # Should propagate the exception as 500
        assert response.status_code == 500

    def test_get_queue_status_success(self, client, mock_notification_service):
        """Test successful get queue status."""
        mock_service_instance = Mock()
        mock_service_instance.get_queue_status.return_value = {
            'status': 'healthy',
            'queue_length': 5,
            'redis_connected': True
        }
        mock_notification_service.return_value = mock_service_instance

        response = client.get('/api/notifications/queue-status')

        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert data['status'] == 'healthy'
        assert data['queue_length'] == 5
        assert data['redis_connected'] is True

        mock_service_instance.get_queue_status.assert_called_once()

    def test_get_queue_status_empty(self, client, mock_notification_service):
        """Test get queue status with empty queue."""
        mock_service_instance = Mock()
        mock_service_instance.get_queue_status.return_value = {
            'status': 'healthy',
            'queue_length': 0,
            'redis_connected': True
        }
        mock_notification_service.return_value = mock_service_instance

        response = client.get('/api/notifications/queue-status')

        assert response.status_code == 200
        data = response.json()
        assert data['queue_length'] == 0

    def test_get_queue_status_redis_disconnected(self, client, mock_notification_service):
        """Test get queue status when Redis is disconnected."""
        mock_service_instance = Mock()
        mock_service_instance.get_queue_status.return_value = {
            'status': 'degraded',
            'queue_length': 0,
            'redis_connected': False
        }
        mock_notification_service.return_value = mock_service_instance

        response = client.get('/api/notifications/queue-status')

        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'degraded'
        assert data['redis_connected'] is False

    def test_get_queue_status_error(self, client, mock_notification_service):
        """Test get queue status when service raises error."""
        mock_service_instance = Mock()
        mock_service_instance.get_queue_status.side_effect = Exception("Redis connection failed")
        mock_notification_service.return_value = mock_service_instance

        with pytest.raises(Exception):
            client.get('/api/notifications/queue-status')


class TestNotificationsRouterIntegration:
    """Integration tests for notifications router."""

    @pytest.fixture
    def app(self):
        """Create test FastAPI app with notifications router."""
        app = FastAPI()
        app.include_router(router)
        return app

    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app, raise_server_exceptions=False)

    def test_full_notification_workflow(self, client):
        """Test complete notification workflow: send and check status."""
        with patch('web.backend.routers.notifications.NotificationServiceWrapper') as MockService:
            mock_service_instance = Mock()
            mock_service_instance.queue_notification.return_value = "notif-workflow-123"
            mock_service_instance.get_queue_status.return_value = {
                'status': 'healthy',
                'queue_length': 1,
                'redis_connected': True
            }
            MockService.return_value = mock_service_instance

            # Send notification
            send_response = client.post(
                '/api/notifications/send',
                json={
                    'type': 'email',
                    'recipient': 'user@example.com',
                    'subject': 'Workflow Test',
                    'body': 'Testing notification workflow',
                    'priority': 'normal'
                }
            )

            assert send_response.status_code == 200
            assert send_response.json()['notification_id'] == 'notif-workflow-123'

            # Check queue status
            status_response = client.get('/api/notifications/queue-status')

            assert status_response.status_code == 200
            assert status_response.json()['queue_length'] == 1
            assert status_response.json()['redis_connected'] is True

    def test_multiple_notification_types(self, client):
        """Test sending multiple notification types."""
        with patch('web.backend.routers.notifications.NotificationServiceWrapper') as MockService:
            mock_service_instance = Mock()
            mock_service_instance.queue_notification.side_effect = [
                "notif-email-1",
                "notif-slack-1",
                "notif-webhook-1",
                "notif-push-1"
            ]
            MockService.return_value = mock_service_instance

            notifications = [
                {'type': 'email', 'recipient': 'user@example.com'},
                {'type': 'slack', 'recipient': '#channel'},
                {'type': 'webhook', 'recipient': 'https://hook.url'},
                {'type': 'push', 'recipient': 'device-token'}
            ]

            for notif in notifications:
                response = client.post(
                    '/api/notifications/send',
                    json={
                        'type': notif['type'],
                        'recipient': notif['recipient'],
                        'subject': 'Test',
                        'body': 'Test body',
                        'priority': 'normal'
                    }
                )
                assert response.status_code == 200

            assert mock_service_instance.queue_notification.call_count == 4

    def test_notification_priority_impact(self, client):
        """Test that different priorities are correctly passed to service."""
        with patch('web.backend.routers.notifications.NotificationServiceWrapper') as MockService:
            mock_service_instance = Mock()
            mock_service_instance.queue_notification.return_value = "notif-priority"
            MockService.return_value = mock_service_instance

            priorities = [
                ('low', NotificationPriority.LOW),
                ('normal', NotificationPriority.NORMAL),
                ('high', NotificationPriority.HIGH),
                ('urgent', NotificationPriority.URGENT)
            ]

            for priority_str, priority_enum in priorities:
                response = client.post(
                    '/api/notifications/send',
                    json={
                        'type': 'email',
                        'recipient': 'user@example.com',
                        'subject': 'Priority Test',
                        'body': 'Testing priority',
                        'priority': priority_str
                    }
                )

                assert response.status_code == 200

                call_kwargs = mock_service_instance.queue_notification.call_args[1]
                assert call_kwargs['priority'] == priority_enum

                mock_service_instance.reset_mock()
