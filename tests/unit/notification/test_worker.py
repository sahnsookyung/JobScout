"""
Tests for notification worker module.
"""

import logging
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from notification.worker import main, start_worker


def _runtime_config(redis_url: str = "redis://runtime:6379/0") -> SimpleNamespace:
    return SimpleNamespace(redis_url=redis_url)


class TestWorkerStartup:
    """Test worker startup functionality."""

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_start_worker_basic(self, mock_worker_class, mock_redis_class):
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis
        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        with patch('notification.worker.get_notification_runtime_config', return_value=_runtime_config()):
            start_worker(burst=False, queues=['notifications'])

        mock_redis_class.from_url.assert_called_once_with('redis://runtime:6379/0')
        mock_redis.ping.assert_called_once()
        mock_worker_class.assert_called_once()
        assert mock_worker_class.call_args[0][0] == ['notifications']
        mock_worker.work.assert_called_once_with()

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_start_worker_burst_mode(self, mock_worker_class, mock_redis_class):
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis
        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        with patch('notification.worker.get_notification_runtime_config', return_value=_runtime_config()):
            start_worker(burst=True, queues=['notifications'])

        mock_worker.work.assert_called_once_with(burst=True)

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_start_worker_multiple_queues(self, mock_worker_class, mock_redis_class):
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis
        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        with patch('notification.worker.get_notification_runtime_config', return_value=_runtime_config()):
            start_worker(burst=False, queues=['notifications', 'emails', 'alerts'])

        assert mock_worker_class.call_args[0][0] == ['notifications', 'emails', 'alerts']

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_start_worker_uses_runtime_config_redis_url(self, mock_worker_class, mock_redis_class):
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis
        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        with patch(
            'notification.worker.get_notification_runtime_config',
            return_value=_runtime_config('redis://custom-host:6379/1'),
        ):
            start_worker(burst=False, queues=['notifications'])

        mock_redis_class.from_url.assert_called_once_with('redis://custom-host:6379/1')

    @patch('notification.worker.Redis', autospec=True)
    def test_start_worker_redis_connection_failure(self, mock_redis_class, caplog):
        mock_redis_class.from_url.side_effect = Exception("Connection refused")

        with patch('notification.worker.get_notification_runtime_config', return_value=_runtime_config()), \
             pytest.raises(SystemExit) as exc_info:
            start_worker(burst=False, queues=['notifications'])

        assert exc_info.value.code == 1
        assert "Connection refused" in caplog.text


class TestWorkerMain:
    """Test main function and argument parsing."""

    @patch('notification.worker.start_worker')
    def test_main_default_args(self, mock_start_worker):
        with patch.object(sys, 'argv', ['worker.py']):
            main()

        mock_start_worker.assert_called_once_with(
            burst=False,
            queues=['notifications']
        )

    @patch('notification.worker.start_worker')
    def test_main_burst_flag(self, mock_start_worker):
        with patch.object(sys, 'argv', ['worker.py', '--burst']):
            main()

        mock_start_worker.assert_called_once_with(
            burst=True,
            queues=['notifications']
        )

    @patch('notification.worker.start_worker')
    def test_main_custom_queues(self, mock_start_worker):
        with patch.object(sys, 'argv', ['worker.py', '--queues', 'queue1', 'queue2']):
            main()

        mock_start_worker.assert_called_once_with(
            burst=False,
            queues=['queue1', 'queue2']
        )

    @patch('notification.worker.start_worker')
    def test_main_verbose_flag(self, mock_start_worker):
        logging.getLogger().setLevel(logging.INFO)

        with patch.object(sys, 'argv', ['worker.py', '--verbose']):
            main()

        assert logging.getLogger().level == logging.DEBUG

    @patch('notification.worker.start_worker')
    def test_main_combined_args(self, mock_start_worker):
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
        logging.getLogger().setLevel(logging.INFO)

        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis
        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        with patch('notification.worker.get_notification_runtime_config', return_value=_runtime_config()):
            start_worker(burst=True, queues=['notifications'])

        assert "Starting RQ Worker" in caplog.text
        assert "Queues:" in caplog.text
        assert "Burst mode: True" in caplog.text

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_logs_redis_connection(self, mock_worker_class, mock_redis_class, caplog):
        logging.getLogger().setLevel(logging.INFO)

        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis
        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        with patch('notification.worker.get_notification_runtime_config', return_value=_runtime_config()):
            start_worker(burst=False, queues=['notifications'])

        assert "Connected to Redis" in caplog.text


class TestWorkerKeyboardInterrupt:
    """Test worker handles keyboard interrupt."""

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_handles_keyboard_interrupt(self, mock_worker_class, mock_redis_class, caplog):
        logging.getLogger().setLevel(logging.INFO)

        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis
        mock_worker = Mock()
        mock_worker.work.side_effect = KeyboardInterrupt()
        mock_worker_class.return_value = mock_worker

        with patch('notification.worker.get_notification_runtime_config', return_value=_runtime_config()):
            start_worker(burst=False, queues=['notifications'])

        assert "Worker stopped" in caplog.text


class TestWorkerEdgeCases:
    """Test worker edge cases."""

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_empty_queue_list(self, mock_worker_class, mock_redis_class):
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis
        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        with patch('notification.worker.get_notification_runtime_config', return_value=_runtime_config()):
            start_worker(burst=False, queues=[])

        mock_worker_class.assert_called_once()

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_redis_ping_failure(self, mock_worker_class, mock_redis_class, caplog):
        mock_redis = Mock()
        mock_redis.ping.side_effect = Exception("Ping failed")
        mock_redis_class.from_url.return_value = mock_redis

        with patch('notification.worker.get_notification_runtime_config', return_value=_runtime_config()), \
             pytest.raises(SystemExit) as exc_info:
            start_worker(burst=False, queues=['notifications'])

        assert exc_info.value.code == 1
        assert "Ping failed" in caplog.text

    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_with_special_queue_names(self, mock_worker_class, mock_redis_class):
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

        with patch('notification.worker.get_notification_runtime_config', return_value=_runtime_config()):
            start_worker(burst=False, queues=special_queues)

        assert mock_worker_class.call_args[0][0] == special_queues


class TestWorkerImport:
    """Test worker module imports."""

    def test_worker_imports_process_notification_task(self):
        from notification import process_notification_task

        assert process_notification_task is not None
        assert callable(process_notification_task)

    def test_worker_has_required_dependencies(self):
        from redis import Redis
        from rq import Worker, Queue

        assert Redis is not None
        assert Worker is not None
        assert Queue is not None
