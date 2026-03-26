#!/usr/bin/env python3
"""
Tests for notification worker module.

Tests cover:
1. Worker startup and configuration
2. Queue subscription
3. Argument parsing
4. Redis connection handling
5. Burst mode operation
"""

import os
import sys
import pytest
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

# Import worker module components
from notification.worker import start_worker, main


class TestWorkerStartup:
    """Test worker startup functionality."""

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_start_worker_basic(self, mock_worker_class, mock_redis_class):
        """Test basic worker startup."""
        # Setup mocks
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis

        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        # Call start_worker
        start_worker(burst=False, queues=['notifications'])

        # Verify Redis connection
        mock_redis_class.from_url.assert_called_once()
        mock_redis.ping.assert_called_once()

        # Verify worker creation with queue names (not Queue objects)
        mock_worker_class.assert_called_once()
        call_args = mock_worker_class.call_args[0]
        assert call_args[0] == ['notifications']  # Queue names passed as list

        # Verify worker start
        mock_worker.work.assert_called_once_with()

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_start_worker_burst_mode(self, mock_worker_class, mock_redis_class):
        """Test worker startup in burst mode."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis

        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        start_worker(burst=True, queues=['notifications'])

        mock_worker.work.assert_called_once_with(burst=True)

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_start_worker_multiple_queues(self, mock_worker_class, mock_redis_class):
        """Test worker startup with multiple queues."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis

        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        start_worker(burst=False, queues=['notifications', 'emails', 'alerts'])

        # Worker should be created with multiple queue names
        call_args = mock_worker_class.call_args[0]
        assert call_args[0] == ['notifications', 'emails', 'alerts']

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_start_worker_custom_redis_url(self, mock_worker_class, mock_redis_class, monkeypatch):
        """Test worker startup with custom Redis URL."""
        monkeypatch.setenv('REDIS_URL', 'redis://custom-host:6379/1')

        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis

        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        start_worker(burst=False, queues=['notifications'])

        mock_redis_class.from_url.assert_called_once_with('redis://custom-host:6379/1')

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_start_worker_default_redis_url(self, mock_worker_class, mock_redis_class, monkeypatch):
        """Test worker startup with default Redis URL."""
        # Ensure REDIS_URL is not set
        monkeypatch.delenv('REDIS_URL', raising=False)

        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis

        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        start_worker(burst=False, queues=['notifications'])

        mock_redis_class.from_url.assert_called_once_with('redis://localhost:6379/0')

    @patch('notification.worker.Redis', autospec=True)
    def test_start_worker_redis_connection_failure(self, mock_redis_class, caplog):
        """Test worker handles Redis connection failure."""
        mock_redis_class.from_url.side_effect = Exception("Connection refused")

        # Should raise SystemExit(1) on Redis connection failure
        with pytest.raises(SystemExit) as exc_info:
            start_worker(burst=False, queues=['notifications'])

        assert exc_info.value.code == 1
        assert "Connection refused" in caplog.text


class TestWorkerMain:
    """Test main function and argument parsing."""

    @patch('notification.worker.start_worker')
    def test_main_default_args(self, mock_start_worker):
        """Test main with default arguments."""
        with patch.object(sys, 'argv', ['worker.py']):
            main()

        mock_start_worker.assert_called_once_with(
            burst=False,
            queues=['notifications']
        )

    @patch('notification.worker.start_worker')
    def test_main_burst_flag(self, mock_start_worker):
        """Test main with --burst flag."""
        with patch.object(sys, 'argv', ['worker.py', '--burst']):
            main()

        mock_start_worker.assert_called_once_with(
            burst=True,
            queues=['notifications']
        )

    @patch('notification.worker.start_worker')
    def test_main_custom_queues(self, mock_start_worker):
        """Test main with custom queues."""
        with patch.object(sys, 'argv', ['worker.py', '--queues', 'queue1', 'queue2']):
            main()

        mock_start_worker.assert_called_once_with(
            burst=False,
            queues=['queue1', 'queue2']
        )

    @patch('notification.worker.start_worker')
    def test_main_verbose_flag(self, mock_start_worker, caplog):
        """Test main with --verbose flag."""
        import logging
        logging.getLogger().setLevel(logging.INFO)

        with patch.object(sys, 'argv', ['worker.py', '--verbose']):
            main()

        # Log level should be set to DEBUG
        assert logging.getLogger().level == logging.DEBUG

    @patch('notification.worker.start_worker')
    def test_main_combined_args(self, mock_start_worker):
        """Test main with combined arguments."""
        with patch.object(sys, 'argv', [
            'worker.py',
            '--burst',
            '--queues', 'notifications', 'emails',
            '--verbose'
        ]):
            main()

        mock_start_worker.assert_called_once_with(
            burst=True,
            queues=['notifications', 'emails']
        )


class TestWorkerLogging:
    """Test worker logging configuration."""

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_logs_startup_info(self, mock_worker_class, mock_redis_class, caplog):
        """Test worker logs startup information."""
        import logging
        logging.getLogger().setLevel(logging.INFO)

        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis


        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        start_worker(burst=True, queues=['notifications'])

        assert "Starting RQ Worker" in caplog.text
        assert "Queues:" in caplog.text
        assert "Burst mode: True" in caplog.text

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_logs_redis_connection(self, mock_worker_class, mock_redis_class, caplog):
        """Test worker logs Redis connection status."""
        import logging
        logging.getLogger().setLevel(logging.INFO)

        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis


        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        start_worker(burst=False, queues=['notifications'])

        assert "Connected to Redis" in caplog.text


class TestWorkerKeyboardInterrupt:
    """Test worker handles keyboard interrupt."""

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_handles_keyboard_interrupt(self, mock_worker_class, mock_redis_class, caplog):
        """Test worker gracefully handles Ctrl+C."""
        import logging
        logging.getLogger().setLevel(logging.INFO)

        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis


        mock_worker = Mock()
        mock_worker.work.side_effect = KeyboardInterrupt()
        mock_worker_class.return_value = mock_worker

        # Should not raise
        start_worker(burst=False, queues=['notifications'])

        assert "Worker stopped" in caplog.text


class TestWorkerEdgeCases:
    """Test worker edge cases."""

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_empty_queue_list(self, mock_worker_class, mock_redis_class):
        """Test worker with empty queue list."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis


        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        start_worker(burst=False, queues=[])

        # Worker should still be created (RQ handles empty queues)
        mock_worker_class.assert_called_once()

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_redis_ping_failure(self, mock_worker_class, mock_redis_class, caplog):
        """Test worker handles Redis ping failure."""
        mock_redis = Mock()
        mock_redis.ping.side_effect = Exception("Ping failed")
        mock_redis_class.from_url.return_value = mock_redis

        # Should raise SystemExit(1) on Redis ping failure
        with pytest.raises(SystemExit) as exc_info:
            start_worker(burst=False, queues=['notifications'])

        assert exc_info.value.code == 1
        assert "Ping failed" in caplog.text

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_with_special_queue_names(self, mock_worker_class, mock_redis_class):
        """Test worker with special queue names."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis


        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        special_queues = [
            'notifications-high-priority',
            'notifications_low_priority',
            'notifications.email',
            'notifications:discord'
        ]

        start_worker(burst=False, queues=special_queues)

        # Should handle special characters in queue names
        # Worker should be created with special queue names
        call_args = mock_worker_class.call_args[0]
        assert call_args[0] == special_queues


class TestWorkerImport:
    """Test worker module imports."""

    def test_worker_imports_process_notification_task(self):
        """Test worker imports task function correctly."""
        from notification import process_notification_task

        # Should be importable
        assert process_notification_task is not None
        assert callable(process_notification_task)

    def test_worker_has_required_dependencies(self):
        """Test worker has required dependencies available."""
        # These should be importable
        from redis import Redis
        from rq import Worker, Queue

        assert Redis is not None
        assert Worker is not None
        assert Queue is not None
