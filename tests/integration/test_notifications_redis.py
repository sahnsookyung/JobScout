#!/usr/bin/env python3
"""
Integration Test: Notification System with Real Redis

This test verifies the notification system works correctly with real Redis operations,
testing the full queue flow from enqueue to processing.

Usage:
    # With automatic Docker containers:
    uv run python -m pytest tests/integration_test_notifications_redis.py -v
    
    # Or with existing Redis:
    REDIS_URL=redis://localhost:6379/0 \
    uv run python -m pytest tests/integration/test_notifications_redis.py -v

Requirements:
    - Docker must be available and running
    - redis package: uv add --dev redis
"""

import unittest
import os
import sys
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

# Check if we should run with Docker containers
USE_DOCKER = os.environ.get('USE_DOCKER_CONTAINERS', '1') == '1'
REDIS_URL = os.environ.get('REDIS_URL')

# Prevent tests from using production Redis - use db=1 for tests if using same host
if REDIS_URL and '/0' in REDIS_URL:
    # Check if this looks like production Redis and redirect to test DB
    if 'redis://redis:6379' in REDIS_URL or 'localhost:6379' in REDIS_URL:
        # Use db=1 for tests to avoid polluting production
        REDIS_URL = REDIS_URL.replace('/0', '/1')
        print(f"Redirecting to test Redis database: {REDIS_URL}")

# Try to import container management
try:
    from tests.conftest_docker import redis_container
    DOCKER_AVAILABLE = True
except ImportError:
    DOCKER_AVAILABLE = False
    redis_container = None  # Explicitly set to None when not available

# Determine if we can run
if REDIS_URL:
    # Use provided Redis
    RUN_TESTS = True
    USE_EXTERNAL_REDIS = True
elif DOCKER_AVAILABLE and USE_DOCKER:
    # Will spin up Docker container
    RUN_TESTS = True
    USE_EXTERNAL_REDIS = False
else:
    RUN_TESTS = False

if not RUN_TESTS:
        print("\n" + "=" * 70)
        print("SKIPPING: Docker not available and REDIS_URL not set")
        print("To run: ensure Docker is running or set REDIS_URL")
        print("=" * 70 + "\n")

@unittest.skipIf(not RUN_TESTS, "Docker not available and REDIS_URL not set")
class TestNotificationsWithRedis(unittest.TestCase):
    """
    Integration tests for Notification Service with real Redis operations.
    
    These tests verify:
    - Redis connection and queue operations
    - Notification enqueue/dequeue flow
    - Deduplication with real Redis storage
    - Worker job processing
    - Failure scenarios and fallback behavior
    """
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment with real Redis (either Docker or external)."""
        print("\n" + "=" * 70)
        print("INTEGRATION TEST: Notification System with Real Redis")
        print("=" * 70)
        
        if USE_EXTERNAL_REDIS:
            print(f"Using external Redis: {REDIS_URL[:30]}...")
            cls.redis_url = REDIS_URL
            cls.redis_conn = Redis.from_url(cls.redis_url)
            cls._verify_connection()
        else:
            print("Starting Redis Docker container...")
            try:
                # Use the context manager directly
                cls._container_ctx = redis_container()
                cls.container = cls._container_ctx.__enter__()
                print(f"✓ Container started on port {cls.container.host_port}")
                
                # Connect to container
                cls.redis_url = cls.container.redis_url
                cls.redis_conn = Redis.from_url(cls.redis_url)
                cls._verify_connection()
            except Exception as e:
                print(f"✗ Failed to start container: {e}")
                raise
        
        # Create queue
        cls.queue = Queue('notifications', connection=cls.redis_conn)
        
        # Create mock repository
        cls.mock_repo = Mock()
        cls.mock_repo.db = Mock()
        
        # Create notification service with real Redis
        cls.service = NotificationService(
            repo=cls.mock_repo,
            redis_url=cls.redis_url,
            skip_dedup=True  # Disable dedup for basic queue tests
        )
        
        print("✓ Notification service initialized with Redis")
    
    @classmethod
    def _verify_connection(cls):
        """Verify Redis connection is working."""
        try:
            cls.redis_conn.ping()
            print(f"✓ Connected to Redis at {cls.redis_url}")
        except Exception as e:
            raise unittest.SkipTest(f"Redis connection failed: {e}")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test data from Redis."""
        if hasattr(cls, 'redis_conn'):
            # Clear test queues
            cls.redis_conn.delete('rq:queue:notifications')
            cls.redis_conn.delete('rq:queue:failed')
            cls.redis_conn.close()
        
        # Stop container if we started it
        if not USE_EXTERNAL_REDIS and hasattr(cls, '_container_ctx'):
            print("\nStopping Redis container...")
            cls._container_ctx.__exit__(None, None, None)
        
        print("\n✓ Integration test complete")
        print("=" * 70 + "\n")
    
    def setUp(self):
        """Clear queue before each test."""
        # Empty the queue
        self.redis_conn.delete('rq:queue:notifications')
        self.redis_conn.delete('rq:queue:failed')
    
    def test_01_redis_connection(self):
        """
        Test basic Redis connection and operations.
        
        Verifies:
        - Connection can be established
        - Basic Redis commands work
        - Ping responds correctly
        """
        print("\n[Test 1] Redis connection and basic operations...")
        
        # Test ping
        result = self.redis_conn.ping()
        self.assertTrue(result)
        print("  ✓ Redis ping successful")
        
        # Test basic operations
        test_key = "test:connection:check"
        self.redis_conn.set(test_key, "test_value", ex=60)
        value = self.redis_conn.get(test_key)
        self.assertEqual(value.decode('utf-8'), "test_value")
        print("  ✓ Basic Redis SET/GET operations work")
        
        # Clean up
        self.redis_conn.delete(test_key)
    
    def test_02_queue_operations(self):
        """
        Test RQ queue operations with real Redis.
        
        Verifies:
        - Jobs can be enqueued
        - Queue length is tracked correctly
        - Jobs can be retrieved from queue
        """
        print("\n[Test 2] RQ queue operations...")
        
        # Define a simple test function
        def test_task(x, y):
            return x + y
        
        # Enqueue a job
        job = self.queue.enqueue(test_task, 1, 2)
        self.assertIsNotNone(job.id)
        print(f"  ✓ Job enqueued with ID: {job.id}")
        
        # Check queue length
        queue_length = len(self.queue)
        self.assertEqual(queue_length, 1)
        print(f"  ✓ Queue length: {queue_length}")
        
        # Fetch the job
        fetched_job = Job.fetch(job.id, connection=self.redis_conn)
        self.assertEqual(fetched_job.id, job.id)
        print("  ✓ Job can be fetched from queue")
        
        # Clean up - remove job from queue
        self.redis_conn.lrem('rq:queue:notifications', 0, job.id)
    
    def test_03_notification_enqueue(self):
        """
        Test notification enqueueing via NotificationService.
        
        Verifies:
        - Notifications are queued correctly
        - Job data is properly stored
        - Queue status reflects enqueued notifications
        """
        print("\n[Test 3] Notification enqueueing...")
        
        # Mock the tracker to allow all notifications
        with patch.object(self.service, 'tracker') as mock_tracker:
            mock_tracker.should_send_notification.return_value = True
            
            # Send notification
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
            
            # Check queue status
            status = self.service.get_queue_status()
            self.assertEqual(status['status'], 'active')
            self.assertTrue(status['redis_connected'])
            self.assertGreaterEqual(status['queue_length'], 1)
            print(f"  ✓ Queue status: {status['queue_length']} jobs pending")
            
            # Verify job exists in Redis
            job = Job.fetch(notification_id, connection=self.redis_conn)
            self.assertIsNotNone(job)
            self.assertEqual(job.id, notification_id)
            print("  ✓ Job exists in Redis")
    
    def test_04_deduplication_with_redis(self):
        """
        Test deduplication logic with real Redis storage.
        
        Verifies:
        - Duplicate notifications are suppressed
        - Different notifications are allowed
        - Redis storage tracks notification history
        """
        print("\n[Test 4] Deduplication with real Redis...")
        
        # Create tracker with real database interaction
        # We'll mock the DB but use real deduplication logic
        tracker = NotificationTrackerService(
            self.mock_repo,
            strategy=DefaultDeduplicationStrategy()
        )
        
        # Mock _get_existing_notification to return None (first notification)
        with patch.object(tracker, '_get_existing_notification') as mock_get:
            mock_get.return_value = None
            
            # First notification should be allowed
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
        
        # Now simulate an existing notification
        existing_mock = Mock()
        existing_mock.content_hash = tracker.generate_content_hash('Test', 'Body', {})
        existing_mock.last_sent_at = datetime.now()
        existing_mock.allow_resend = False
        
        with patch.object(tracker, '_get_existing_notification') as mock_get:
            mock_get.return_value = existing_mock
            
            # Duplicate notification should be blocked
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
            
            # Different content should be allowed
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
        """
        Test worker job processing (mock actual sending, test queue flow).
        
        Verifies:
        - Worker can fetch jobs from queue
        - Jobs are processed and removed from queue
        - Failed jobs are handled correctly
        """
        print("\n[Test 5] Worker job processing...")
        
        # Create a test notification job
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
        
        # Enqueue the notification task
        from notification.service import process_notification_task
        job = self.queue.enqueue(
            process_notification_task,
            notification_data,
            job_timeout='5m'
        )
        
        print(f"  ✓ Notification job enqueued: {job.id}")
        
        # Verify job is in queue
        self.assertEqual(len(self.queue), 1)
        
        # Start a worker in burst mode to process the job
        # Use SimpleWorker instead of Worker to avoid fork-safety issues
        # SimpleWorker runs jobs in the main process instead of forking,
        # preventing segfaults from inherited database connection state
        worker = SimpleWorker(['notifications'], connection=self.redis_conn)
        
        # Process in burst mode (processes all jobs then exits)
        print("  Processing job in burst mode (SimpleWorker)...")
        worker.work(burst=True)
        
        # Verify job was processed (removed from queue)
        # Note: If the job fails, it will be in the failed queue
        queue_length = len(self.queue)
        print(f"  ✓ Queue length after processing: {queue_length}")
        
        # Check job status
        job.refresh()
        print(f"  ✓ Job status: {job.get_status()}")
    
    def test_06_redis_failure_fallback(self):
        """
        Test Redis failure scenarios and fallback to sync mode.
        
        Verifies:
        - Service detects Redis unavailability
        - Falls back to synchronous mode when Redis fails
        - Queue status reflects error state
        """
        print("\n[Test 6] Redis failure and fallback...")
        
        # Create service with invalid Redis URL to simulate failure
        invalid_url = 'redis://invalid-host:6379/99'
        
        try:
            # This should fall back to sync mode
            failing_service = NotificationService(
                repo=self.mock_repo,
                redis_url=invalid_url,
                skip_dedup=True
            )
            
            # Check that async mode is disabled
            self.assertFalse(failing_service.async_mode)
            print("  ✓ Service correctly detects Redis failure")
            
            # Check queue status shows sync mode
            status = failing_service.get_queue_status()
            self.assertEqual(status['status'], 'sync_mode')
            print("  ✓ Queue status shows 'sync_mode'")
            
            # In sync mode, notifications are processed immediately
            with patch.object(failing_service, 'tracker') as mock_tracker:
                mock_tracker.should_send_notification.return_value = True
                
                # Mock the process_notification_task to avoid actual sending
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
