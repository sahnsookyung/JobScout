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
import threading
import pytest
from unittest.mock import Mock, patch, MagicMock

from notification.worker import start_worker, main, _monitor_dlq


class TestWorkerStartup:
    """Test worker startup functionality."""

    @patch('notification.worker.FailedJobRegistry', autospec=True)
    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_start_worker_basic(self, mock_worker_class, mock_redis_class, mock_registry_class):
        """Test basic worker startup."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis

        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker
        mock_registry_class.return_value.__len__ = Mock(return_value=0)

        start_worker(burst=False, queues=['notifications'])

        mock_redis_class.from_url.assert_called_once()
        mock_redis.ping.assert_called_once()

        mock_worker_class.assert_called_once()
        call_args = mock_worker_class.call_args[0]
        assert call_args[0] == ['notifications']

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

    @patch('notification.worker.FailedJobRegistry', autospec=True)
    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_start_worker_multiple_queues(self, mock_worker_class, mock_redis_class, mock_registry_class):
        """Test worker startup with multiple queues."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis

        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        start_worker(burst=False, queues=['notifications', 'emails', 'alerts'])

        call_args = mock_worker_class.call_args[0]
        assert call_args[0] == ['notifications', 'emails', 'alerts']

    @patch('notification.worker.FailedJobRegistry', autospec=True)
    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_start_worker_custom_redis_url(self, mock_worker_class, mock_redis_class, mock_registry_class, monkeypatch):
        """Test worker startup with custom Redis URL."""
        monkeypatch.setenv('REDIS_URL', 'redis://custom-host:6379/1')

        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis

        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        start_worker(burst=False, queues=['notifications'])

        mock_redis_class.from_url.assert_called_once_with('redis://custom-host:6379/1')

    @patch('notification.worker.FailedJobRegistry', autospec=True)
    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_start_worker_default_redis_url(self, mock_worker_class, mock_redis_class, mock_registry_class, monkeypatch):
        """Test worker startup with default Redis URL."""
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

    @patch('notification.worker.FailedJobRegistry', autospec=True)
    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_logs_redis_connection(self, mock_worker_class, mock_redis_class, mock_registry_class, caplog):
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

    @patch('notification.worker.FailedJobRegistry', autospec=True)
    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_handles_keyboard_interrupt(self, mock_worker_class, mock_redis_class, mock_registry_class, caplog):
        """Test worker gracefully handles Ctrl+C."""
        import logging
        logging.getLogger().setLevel(logging.INFO)

        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis

        mock_worker = Mock()
        mock_worker.work.side_effect = KeyboardInterrupt()
        mock_worker_class.return_value = mock_worker

        start_worker(burst=False, queues=['notifications'])

        assert "Worker stopped" in caplog.text


class TestWorkerEdgeCases:
    """Test worker edge cases."""

    @patch('notification.worker.FailedJobRegistry', autospec=True)
    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_empty_queue_list(self, mock_worker_class, mock_redis_class, mock_registry_class):
        """Test worker with empty queue list."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        mock_redis_class.from_url.return_value = mock_redis

        mock_worker = Mock()
        mock_worker_class.return_value = mock_worker

        start_worker(burst=False, queues=[])

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

    @patch('notification.worker.FailedJobRegistry', autospec=True)
    @patch('notification.worker.Redis', autospec=True)
    @patch('notification.worker.Worker', autospec=True)
    def test_worker_with_special_queue_names(self, mock_worker_class, mock_redis_class, mock_registry_class):
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

        call_args = mock_worker_class.call_args[0]
        assert call_args[0] == special_queues


class TestWorkerImport:
    """Test worker module imports."""

    def test_worker_imports_process_notification_task(self):
        """Test worker imports task function correctly."""
        from notification import process_notification_task

        assert process_notification_task is not None
        assert callable(process_notification_task)

    def test_worker_has_required_dependencies(self):
        """Test worker has required dependencies available."""
        from redis import Redis
        from rq import Worker, Queue

        assert Redis is not None
        assert Worker is not None
        assert Queue is not None


class TestDLQMonitor:
    """Tests for the background DLQ monitor thread."""

    def _run_one_cycle(self, registry_len: int, last_count: int = 0):
        """Run _monitor_dlq for one poll cycle then stop it."""
        stop = threading.Event()
        mock_redis = Mock()

        mock_registry = Mock()
        mock_registry.__len__ = Mock(return_value=registry_len)

        with patch('notification.worker.Queue') as mock_queue_class, \
             patch('notification.worker.FailedJobRegistry', return_value=mock_registry):
            # Stop after first wait so only one cycle runs.
            real_wait = stop.wait
            call_count = 0

            def stop_after_first(timeout=None):
                nonlocal call_count
                call_count += 1
                if call_count >= 1:
                    stop.set()
                return real_wait(timeout=0)

            stop.wait = stop_after_first
            _monitor_dlq(mock_redis, 'notifications', stop)

        return mock_registry

    def test_zero_failed_jobs_logs_nothing_initially(self, caplog):
        """No log output when DLQ is empty and was previously empty."""
        import logging
        with caplog.at_level(logging.DEBUG, logger='notification.worker'):
            self._run_one_cycle(registry_len=0, last_count=0)
        assert "DLQ" not in caplog.text

    def test_new_failures_log_error(self, caplog):
        """ERROR logged when failed count increases."""
        import logging
        with caplog.at_level(logging.ERROR, logger='notification.worker'):
            self._run_one_cycle(registry_len=3)
        assert "DLQ growing" in caplog.text
        assert "3" in caplog.text

    def test_stable_failures_log_warning(self, caplog):
        """WARNING logged each poll when DLQ is non-zero but not growing."""
        import logging
        stop = threading.Event()
        mock_redis = Mock()
        mock_registry = Mock()
        mock_registry.__len__ = Mock(return_value=2)

        cycle = 0

        def stop_after_second(timeout=None):
            nonlocal cycle
            cycle += 1
            if cycle >= 2:
                stop.set()
            return stop.is_set()

        stop.wait = stop_after_second

        with patch('notification.worker.Queue'), \
             patch('notification.worker.FailedJobRegistry', return_value=mock_registry), \
             caplog.at_level(logging.WARNING, logger='notification.worker'):
            _monitor_dlq(mock_redis, 'notifications', stop)

        warnings = [r for r in caplog.records if r.levelname == 'WARNING' and 'DLQ non-empty' in r.message]
        assert len(warnings) >= 1

    def test_dlq_cleared_logs_info(self, caplog):
        """INFO logged when DLQ drops from non-zero to zero."""
        import logging
        stop = threading.Event()
        mock_redis = Mock()
        counts = [2, 0]
        call_idx = 0

        def varying_len(_self=None):
            nonlocal call_idx
            val = counts[min(call_idx, len(counts) - 1)]
            call_idx += 1
            return val

        mock_registry = Mock()
        mock_registry.__len__ = varying_len

        cycle = 0

        def stop_after_second(timeout=None):
            nonlocal cycle
            cycle += 1
            if cycle >= 2:
                stop.set()
            return stop.is_set()

        stop.wait = stop_after_second

        with patch('notification.worker.Queue'), \
             patch('notification.worker.FailedJobRegistry', return_value=mock_registry), \
             caplog.at_level(logging.INFO, logger='notification.worker'):
            _monitor_dlq(mock_redis, 'notifications', stop)

        assert "DLQ cleared" in caplog.text

    def test_registry_exception_logs_warning_and_continues(self, caplog):
        """A Redis error during polling logs a warning but doesn't crash the thread."""
        import logging
        stop = threading.Event()
        mock_redis = Mock()
        mock_registry = Mock()
        mock_registry.__len__ = Mock(side_effect=Exception("Redis timeout"))

        call_count = 0

        def stop_after_first(timeout=None):
            nonlocal call_count
            call_count += 1
            stop.set()
            return True

        stop.wait = stop_after_first

        with patch('notification.worker.Queue'), \
             patch('notification.worker.FailedJobRegistry', return_value=mock_registry), \
             caplog.at_level(logging.WARNING, logger='notification.worker'):
            _monitor_dlq(mock_redis, 'notifications', stop)

        assert "DLQ monitor check failed" in caplog.text

    def test_burst_mode_does_not_start_monitor(self):
        """Monitor thread is not started in burst mode."""
        with patch('notification.worker.Redis') as mock_redis_class, \
             patch('notification.worker.Worker') as mock_worker_class, \
             patch('notification.worker.threading') as mock_threading:
            mock_redis_class.from_url.return_value.ping.return_value = True
            mock_worker_class.return_value.work.return_value = None

            start_worker(burst=True, queues=['notifications'])

            mock_threading.Thread.assert_not_called()
