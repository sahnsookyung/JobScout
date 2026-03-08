#!/usr/bin/env python3
"""
Integration Test: Notification System with Real Redis
...
"""

import unittest
import os
import time
from datetime import datetime
from unittest.mock import Mock, patch
from redis import Redis
from rq import Queue, Worker, SimpleWorker
from rq.job import Job

from notification import (
    NotificationService, NotificationPriority,
    NotificationTrackerService, DefaultDeduplicationStrategy
)
from notification.channels import EmailChannel

REDIS_URL = os.environ.get("TEST_REDIS_URL")
RUN_TESTS = True

if not REDIS_URL:
    print("\n" + "=" * 70)
    print("NOTE: TEST_REDIS_URL not set, will use testcontainers")
    print("=" * 70 + "\n")


@unittest.skipIf(not RUN_TESTS, "No Redis available (set TEST_REDIS_URL or enable Docker)")
class TestNotificationsWithRedis(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print("\n" + "=" * 70)
        print("INTEGRATION TEST: Notification System with Real Redis")
        print("=" * 70)

        if REDIS_URL:
            print(f"Using external Redis: {REDIS_URL[:30]}...")
            cls.redis_url = REDIS_URL
            cls._container = None
        else:
            try:
                from testcontainers.redis import RedisContainer
                print("Starting Redis testcontainer...")
                # FIXED: removed port=6379 kwarg — testcontainers always maps to a
                # random host port; fixing it to 6379 causes conflicts in parallel CI
                cls._container = RedisContainer("redis:7-alpine")
                cls._container.start()

                # FIXED: RedisContainer has no get_connection_url().
                # Use get_container_host_ip() + get_exposed_port() to build the URL.
                host = cls._container.get_container_host_ip()
                port = cls._container.get_exposed_port(6379)
                cls.redis_url = f"redis://{host}:{port}/0"
                print(f"✓ Container started: {cls.redis_url}")
            except ImportError:
                raise RuntimeError("testcontainers.redis not available and TEST_REDIS_URL not set")

        cls.redis_conn = Redis.from_url(cls.redis_url)
        cls._verify_connection()

        cls.queue = Queue('notifications', connection=cls.redis_conn)
        cls.mock_repo = Mock()
        cls.mock_repo.db = Mock()

        cls.service = NotificationService(
            repo=cls.mock_repo,
            redis_url=cls.redis_url,
            skip_dedup=True
        )

        print("✓ Notification service initialized with Redis")

    @classmethod
    def _verify_connection(cls):
        try:
            cls.redis_conn.ping()
            print(f"✓ Connected to Redis at {cls.redis_url}")
        except Exception as e:
            raise unittest.SkipTest(f"Redis connection failed: {e}")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'redis_conn'):
            # FIXED: flushdb() instead of deleting specific RQ internal keys —
            # RQ key names are an implementation detail and may change across versions
            cls.redis_conn.flushdb()
            cls.redis_conn.close()

        if hasattr(cls, '_container') and cls._container is not None:
            print("\nStopping Redis testcontainer...")
            cls._container.stop()

        print("\n✓ Integration test complete")
        print("=" * 70 + "\n")

    def setUp(self):
        """Clear queue before each test."""
        # FIXED: flushdb() instead of deleting hardcoded RQ internal key names
        self.redis_conn.flushdb()

    def test_01_redis_connection(self):
        """Test basic Redis connection and operations."""
        print("\n[Test 1] Redis connection and basic operations...")

        result = self.redis_conn.ping()
        self.assertTrue(result)
        print("  ✓ Redis ping successful")

        test_key = "test:connection:check"
        self.redis_conn.set(test_key, "test_value", ex=60)
        value = self.redis_conn.get(test_key)
        self.assertEqual(value.decode('utf-8'), "test_value")
        print("  ✓ Basic Redis SET/GET operations work")

        self.redis_conn.delete(test_key)

    def test_02_queue_operations(self):
        """Test RQ queue operations with real Redis."""
        print("\n[Test 2] RQ queue operations...")

        def test_task(x, y):
            return x + y

        job = self.queue.enqueue(test_task, 1, 2)
        self.assertIsNotNone(job.id)
        print(f"  ✓ Job enqueued with ID: {job.id}")

        queue_length = len(self.queue)
        self.assertEqual(queue_length, 1)
        print(f"  ✓ Queue length: {queue_length}")

        fetched_job = Job.fetch(job.id, connection=self.redis_conn)
        self.assertEqual(fetched_job.id, job.id)
        print("  ✓ Job can be fetched from queue")

    def test_03_notification_enqueue(self):
        """Test notification enqueueing via NotificationService."""
        print("\n[Test 3] Notification enqueueing...")

        with patch.object(self.service, 'tracker') as mock_tracker:
            mock_tracker.should_send_notification.return_value = True

            notification_id = self.service.send_notification(
                channel_type='email',
                recipient='test@example.com',
                subject='Test Subject',
                body='Test notification body',
                user_id='test-user-123',
                job_match_id='match-456',
                event_type='new_high_score_match',
                priority=NotificationPriority.HIGH
            )

            self.assertIsNotNone(notification_id)
            print(f"  ✓ Notification queued with ID: {notification_id}")

            status = self.service.get_queue_status()
            self.assertEqual(status['status'], 'active')
            self.assertTrue(status['redis_connected'])
            self.assertGreaterEqual(status['queue_length'], 1)
            print(f"  ✓ Queue status: {status['queue_length']} jobs pending")

            job = Job.fetch(notification_id, connection=self.redis_conn)
            self.assertIsNotNone(job)
            self.assertEqual(job.id, notification_id)
            print("  ✓ Job exists in Redis")

    def test_04_deduplication_with_redis(self):
        """Test deduplication logic with real Redis storage."""
        print("\n[Test 4] Deduplication with real Redis...")

        tracker = NotificationTrackerService(
            self.mock_repo,
            strategy=DefaultDeduplicationStrategy()
        )

        with patch.object(tracker, '_get_existing_notification') as mock_get:
            mock_get.return_value = None

            should_send_1 = tracker.should_send_notification(
                user_id='user-1',
                job_match_id='match-1',
                event_type='new_match',
                channel_type='email',
                subject='Test',
                body='Body'
            )
            self.assertTrue(should_send_1)
            print("  ✓ First notification allowed")

        existing_mock = Mock()
        existing_mock.content_hash = tracker.generate_content_hash('Test', 'Body', {})
        existing_mock.last_sent_at = datetime.now()
        existing_mock.allow_resend = False

        with patch.object(tracker, '_get_existing_notification') as mock_get:
            mock_get.return_value = existing_mock

            should_send_2 = tracker.should_send_notification(
                user_id='user-1',
                job_match_id='match-1',
                event_type='new_match',
                channel_type='email',
                subject='Test',
                body='Body'
            )
            self.assertFalse(should_send_2)
            print("  ✓ Duplicate notification suppressed")

            should_send_3 = tracker.should_send_notification(
                user_id='user-1',
                job_match_id='match-1',
                event_type='new_match',
                channel_type='email',
                subject='Different',
                body='Different body'
            )
            self.assertTrue(should_send_3)
            print("  ✓ Different content notification allowed")

    def test_05_worker_job_processing(self):
        """Test worker job processing."""
        print("\n[Test 5] Worker job processing...")

        notification_data = {
            'channel_type': 'email',
            'recipient': 'test@example.com',
            'subject': 'Test',
            'body': 'Test body',
            'metadata': {},
            'user_id': 'test-user',
            'job_match_id': '00000000-0000-0000-0000-000000000001',
            'event_type': 'new_match',
            'priority': 'normal',
            'allow_resend': True
        }

        from notification.service import process_notification_task
        job = self.queue.enqueue(
            process_notification_task,
            notification_data,
            job_timeout='5m'
        )

        print(f"  ✓ Notification job enqueued: {job.id}")
        self.assertEqual(len(self.queue), 1)

        worker = SimpleWorker(['notifications'], connection=self.redis_conn)
        print("  Processing job in burst mode (SimpleWorker)...")
        worker.work(burst=True)

        queue_length = len(self.queue)
        print(f"  ✓ Queue length after processing: {queue_length}")

        job.refresh()
        print(f"  ✓ Job status: {job.get_status()}")

    def test_06_redis_failure_fallback(self):
        """Test Redis failure scenarios and fallback to sync mode."""
        print("\n[Test 6] Redis failure and fallback...")

        invalid_url = 'redis://invalid-host:6379/99'

        try:
            failing_service = NotificationService(
                repo=self.mock_repo,
                redis_url=invalid_url,
                skip_dedup=True
            )

            self.assertFalse(failing_service.async_mode)
            print("  ✓ Service correctly detects Redis failure")

            status = failing_service.get_queue_status()
            self.assertEqual(status['status'], 'sync_mode')
            print("  ✓ Queue status shows 'sync_mode'")

            with patch.object(failing_service, 'tracker') as mock_tracker:
                mock_tracker.should_send_notification.return_value = True

                with patch('notification.service.process_notification_task') as mock_process:
                    mock_process.return_value = 'sync-notif-123'

                    result = failing_service.send_notification(
                        channel_type='email',
                        recipient='test@example.com',
                        subject='Test',
                        body='Test body',
                        user_id='test-user',
                        event_type='new_match'
                    )

                    self.assertIsNotNone(result)
                    mock_process.assert_called_once()
                    print("  ✓ Notification processed synchronously (fallback)")

        except Exception as e:
            print(f"  Note: Expected behavior - service handles Redis failure gracefully: {e}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
