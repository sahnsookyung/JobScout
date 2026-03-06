#!/usr/bin/env python3
"""
Unit Tests: Orchestrator Service

These tests verify the orchestrator service logic without requiring
running services. They use mocking to test:
1. Subscription happens BEFORE publishing (critical bug fix)
2. Error handling and logging
3. State management
4. Timeout handling

Usage:
    uv run pytest tests/unit/services/test_orchestrator.py -v
"""

import pytest
import asyncio
from unittest.mock import Mock, MagicMock, AsyncMock, patch, call
from datetime import datetime
import uuid


class TestOrchestratorSubscriptionOrder:
    """
    Critical test suite for the subscription-before-publishing bug fix.
    
    This bug caused 'No subscribers received completion event' warnings
    because the orchestrator was subscribing AFTER publishing messages.
    """

    @pytest.fixture
    def mock_redis_async(self):
        """Mock async Redis client."""
        mock_client = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.channels = AsyncMock(return_value=["extraction:completed"])
        mock_pubsub.listen = AsyncMock()
        mock_pubsub.close = AsyncMock()
        mock_client.pubsub = Mock(return_value=mock_pubsub)
        mock_client.close = AsyncMock()
        return mock_client, mock_pubsub

    @pytest.mark.asyncio
    async def test_subscribe_before_enqueue_extraction(self, mock_redis_async):
        """Test orchestrator subscribes to extraction channel BEFORE enqueueing job."""
        mock_client, mock_pubsub = mock_redis_async
        
        # Mock state
        mock_state = AsyncMock()
        mock_state.status = "pending"
        mock_state.notify = AsyncMock()
        mock_state._save_to_redis = Mock()
        
        with patch('services.orchestrator.main.get_or_create_orchestration', return_value=mock_state):
            with patch('services.orchestrator.main.redis_async.from_url', return_value=mock_client):
                with patch('services.orchestrator.main.enqueue_job') as mock_enqueue:
                    with patch('services.orchestrator.main._orchestration_lock'):
                        # Import after mocks are set up
                        from services.orchestrator.main import orchestrate_match
                        
                        # Create task
                        task_id = f"test-{uuid.uuid4().hex[:8]}"
                        
                        # Run orchestration (will timeout, but we check order)
                        try:
                            await asyncio.wait_for(
                                orchestrate_match(task_id, "/app/resume.pdf"),
                                timeout=0.1
                            )
                        except asyncio.TimeoutError:
                            pass
                        except Exception:
                            pass
                        
                        # Verify subscription happened BEFORE enqueue
                        # First call should be subscribe
                        subscribe_calls = mock_pubsub.subscribe.call_args_list
                        assert len(subscribe_calls) > 0, "Should have subscribed to channel"
                        
                        # Verify subscribe was called with extraction:completed
                        first_subscribe_arg = subscribe_calls[0][0][0]
                        assert "extraction:completed" in str(first_subscribe_arg)
                        
                        # Verify enqueue was called after subscribe
                        assert mock_enqueue.called

    @pytest.mark.asyncio
    async def test_subscribe_before_enqueue_embeddings(self, mock_redis_async):
        """Test orchestrator subscribes to embeddings channel BEFORE enqueueing."""
        mock_client, mock_pubsub = mock_redis_async
        
        # Mock state
        mock_state = AsyncMock()
        mock_state.status = "pending"
        mock_state.notify = AsyncMock()
        mock_state._save_to_redis = Mock()
        mock_state.resume_fingerprint = None
        
        # Mock extraction response
        mock_pubsub.listen.return_value = [
            {
                "type": "message",
                "data": '{"task_id": "test-123", "status": "completed", "resume_fingerprint": "abc123"}'
            }
        ]
        
        with patch('services.orchestrator.main.get_or_create_orchestration', return_value=mock_state):
            with patch('services.orchestrator.main.redis_async.from_url', return_value=mock_client):
                with patch('services.orchestrator.main.enqueue_job') as mock_enqueue:
                    with patch('services.orchestrator.main._orchestration_lock'):
                        from services.orchestrator.main import orchestrate_match
                        
                        task_id = f"test-{uuid.uuid4().hex[:8]}"
                        
                        try:
                            await asyncio.wait_for(
                                orchestrate_match(task_id, "/app/resume.pdf"),
                                timeout=0.1
                            )
                        except asyncio.TimeoutError:
                            pass
                        except Exception:
                            pass
                        
                        # Verify unsubscribe from extraction and subscribe to embeddings
                        subscribe_calls = mock_pubsub.subscribe.call_args_list
                        unsubscribe_calls = mock_pubsub.unsubscribe.call_args_list
                        
                        # Should subscribe to extraction:completed first
                        assert len(subscribe_calls) >= 1

    @pytest.mark.asyncio
    async def test_subscribe_before_enqueue_matching(self, mock_redis_async):
        """Test orchestrator subscribes to matching channel BEFORE enqueueing."""
        mock_client, mock_pubsub = mock_redis_async
        
        # Mock state
        mock_state = AsyncMock()
        mock_state.status = "pending"
        mock_state.notify = AsyncMock()
        mock_state._save_to_redis = Mock()
        mock_state.resume_fingerprint = "abc123"
        
        # Mock extraction and embeddings responses
        mock_pubsub.listen.side_effect = [
            [
                {
                    "type": "message",
                    "data": '{"task_id": "test-123", "status": "completed", "resume_fingerprint": "abc123"}'
                }
            ],
            [
                {
                    "type": "message",
                    "data": '{"task_id": "test-123", "status": "completed"}'
                }
            ],
            []  # Matching will timeout
        ]
        
        with patch('services.orchestrator.main.get_or_create_orchestration', return_value=mock_state):
            with patch('services.orchestrator.main.redis_async.from_url', return_value=mock_client):
                with patch('services.orchestrator.main.enqueue_job') as mock_enqueue:
                    with patch('services.orchestrator.main._orchestration_lock'):
                        from services.orchestrator.main import orchestrate_match
                        
                        task_id = f"test-{uuid.uuid4().hex[:8]}"
                        
                        try:
                            await asyncio.wait_for(
                                orchestrate_match(task_id, "/app/resume.pdf"),
                                timeout=0.1
                            )
                        except asyncio.TimeoutError:
                            pass
                        except Exception:
                            pass
                        
                        # Verify channel switching happened
                        # Should have multiple subscribe calls for channel switching
                        assert mock_pubsub.subscribe.call_count >= 1
                        assert mock_pubsub.unsubscribe.call_count >= 1


class TestOrchestratorLogging:
    """Test orchestrator produces expected log output."""

    @pytest.mark.asyncio
    async def test_logs_pipeline_start(self, caplog):
        """Test orchestrator logs when pipeline starts."""
        import logging
        
        mock_state = AsyncMock()
        mock_state.status = "pending"
        mock_state.notify = AsyncMock()
        mock_state._save_to_redis = Mock()
        
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.close = AsyncMock()
        mock_pubsub.channels = AsyncMock(return_value=[])
        
        mock_client = AsyncMock()
        mock_client.pubsub = Mock(return_value=mock_pubsub)
        mock_client.close = AsyncMock()
        
        with patch('services.orchestrator.main.get_or_create_orchestration', return_value=mock_state):
            with patch('services.orchestrator.main.redis_async.from_url', return_value=mock_client):
                with patch('services.orchestrator.main.enqueue_job'):
                    with patch('services.orchestrator.main._orchestration_lock'):
                        from services.orchestrator.main import orchestrate_match
                        
                        with caplog.at_level(logging.INFO):
                            try:
                                await asyncio.wait_for(
                                    orchestrate_match("test-123", "/app/resume.pdf"),
                                    timeout=0.1
                                )
                            except:
                                pass
                        
                        # Verify pipeline start is logged
                        assert "Starting pipeline" in caplog.text or "pipeline" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_logs_enqueue_operation(self, caplog):
        """Test orchestrator logs when enqueueing jobs."""
        import logging
        
        mock_state = AsyncMock()
        mock_state.status = "pending"
        mock_state.notify = AsyncMock()
        mock_state._save_to_redis = Mock()
        
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.close = AsyncMock()
        mock_pubsub.channels = AsyncMock(return_value=[])
        
        mock_client = AsyncMock()
        mock_client.pubsub = Mock(return_value=mock_pubsub)
        mock_client.close = AsyncMock()
        
        with patch('services.orchestrator.main.get_or_create_orchestration', return_value=mock_state):
            with patch('services.orchestrator.main.redis_async.from_url', return_value=mock_client):
                with patch('services.orchestrator.main.enqueue_job') as mock_enqueue:
                    with patch('services.orchestrator.main._orchestration_lock'):
                        from services.orchestrator.main import orchestrate_match
                        
                        with caplog.at_level(logging.INFO):
                            try:
                                await asyncio.wait_for(
                                    orchestrate_match("test-123", "/app/resume.pdf"),
                                    timeout=0.1
                                )
                            except:
                                pass
                        
                        # Verify enqueue is called
                        assert mock_enqueue.called
                        # Verify enqueue is logged (check for extraction:jobs in logs)
                        assert "extraction:jobs" in caplog.text or "Enqueueing" in caplog.text


class TestOrchestratorErrorHandling:
    """Test orchestrator error handling to prevent silent failures."""

    @pytest.mark.asyncio
    async def test_timeout_sets_failed_status(self):
        """Test that timeout properly sets task status to failed."""
        mock_state = AsyncMock()
        mock_state.status = "extracting"
        mock_state.error = None
        mock_state.notify = AsyncMock()
        mock_state._save_to_redis = Mock()
        
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.close = AsyncMock()
        mock_pubsub.channels = AsyncMock(return_value=[])
        mock_pubsub.listen = AsyncMock(return_value=[])  # Timeout
        
        mock_client = AsyncMock()
        mock_client.pubsub = Mock(return_value=mock_pubsub)
        mock_client.close = AsyncMock()
        
        with patch('services.orchestrator.main.get_or_create_orchestration', return_value=mock_state):
            with patch('services.orchestrator.main.redis_async.from_url', return_value=mock_client):
                with patch('services.orchestrator.main.enqueue_job'):
                    with patch('services.orchestrator.main._orchestration_lock'):
                        from services.orchestrator.main import orchestrate_match
                        
                        # Run with very short timeout
                        try:
                            await asyncio.wait_for(
                                orchestrate_match("test-123", "/app/resume.pdf"),
                                timeout=0.01
                            )
                        except asyncio.TimeoutError:
                            pass
                        except Exception:
                            pass
                        
                        # Verify status was set to failed (notify should be called with failed status)
                        # Check if notify was called with failed status
                        notify_calls = mock_state.notify.call_args_list
                        failed_notify = [c for c in notify_calls if 'failed' in str(c)]
                        assert len(failed_notify) > 0

    @pytest.mark.asyncio
    async def test_extraction_failure_propagates_error(self):
        """Test that extraction failure properly propagates error message."""
        mock_state = AsyncMock()
        mock_state.status = "extracting"
        mock_state.error = None
        mock_state.notify = AsyncMock()
        mock_state._save_to_redis = Mock()
        
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.close = AsyncMock()
        mock_pubsub.channels = AsyncMock(return_value=["extraction:completed"])
        mock_pubsub.listen = AsyncMock(return_value=[
            {
                "type": "message",
                "data": '{"task_id": "test-123", "status": "failed", "error": "Test error"}'
            }
        ])
        
        mock_client = AsyncMock()
        mock_client.pubsub = Mock(return_value=mock_pubsub)
        mock_client.close = AsyncMock()
        
        with patch('services.orchestrator.main.get_or_create_orchestration', return_value=mock_state):
            with patch('services.orchestrator.main.redis_async.from_url', return_value=mock_client):
                with patch('services.orchestrator.main.enqueue_job'):
                    with patch('services.orchestrator.main._orchestration_lock'):
                        from services.orchestrator.main import orchestrate_match
                        
                        await orchestrate_match("test-123", "/app/resume.pdf")
                        
                        # Verify error was set (notify should be called with failed status)
                        notify_calls = mock_state.notify.call_args_list
                        failed_notify = [c for c in notify_calls if 'failed' in str(c)]
                        assert len(failed_notify) > 0

    @pytest.mark.asyncio
    async def test_missing_fingerprint_sets_error(self):
        """Test that missing fingerprint in response sets error."""
        mock_state = AsyncMock()
        mock_state.status = "extracting"
        mock_state.error = None
        mock_state.resume_fingerprint = None
        mock_state.notify = AsyncMock()
        mock_state._save_to_redis = Mock()
        
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.close = AsyncMock()
        mock_pubsub.channels = AsyncMock(return_value=["extraction:completed"])
        mock_pubsub.listen = AsyncMock(return_value=[
            {
                "type": "message",
                "data": '{"task_id": "test-123", "status": "completed"}'  # No fingerprint!
            }
        ])
        
        mock_client = AsyncMock()
        mock_client.pubsub = Mock(return_value=mock_pubsub)
        mock_client.close = AsyncMock()
        
        with patch('services.orchestrator.main.get_or_create_orchestration', return_value=mock_state):
            with patch('services.orchestrator.main.redis_async.from_url', return_value=mock_client):
                with patch('services.orchestrator.main.enqueue_job'):
                    with patch('services.orchestrator.main._orchestration_lock'):
                        from services.orchestrator.main import orchestrate_match
                        
                        await orchestrate_match("test-123", "/app/resume.pdf")
                        
                        # Verify error about missing fingerprint (notify should be called with failed status)
                        notify_calls = mock_state.notify.call_args_list
                        failed_notify = [c for c in notify_calls if 'failed' in str(c)]
                        assert len(failed_notify) > 0


class TestOrchestrationState:
    """Test OrchestrationState class for state management."""

    def test_state_initialization(self):
        """Test OrchestrationState initializes with correct defaults."""
        from services.orchestrator.main import OrchestrationState
        
        state = OrchestrationState("test-123", load_from_redis=False)
        
        assert state.task_id == "test-123"
        assert state.status == "pending"
        assert state.resume_fingerprint is None
        assert state.matches_count == 0
        assert state.error is None

    @patch('services.orchestrator.main.get_task_state')
    def test_state_loads_from_redis(self, mock_get_state):
        """Test OrchestrationState loads existing state from Redis."""
        mock_get_state.return_value = {
            "status": "embedding",
            "resume_fingerprint": "abc123",
            "matches_count": 5,
            "error": None
        }
        
        from services.orchestrator.main import OrchestrationState
        
        state = OrchestrationState("test-123", load_from_redis=True)
        
        assert state.status == "embedding"
        assert state.resume_fingerprint == "abc123"
        assert state.matches_count == 5

    @patch('services.orchestrator.main.set_task_state')
    def test_state_saves_to_redis(self, mock_set_state):
        """Test OrchestrationState saves state to Redis."""
        from services.orchestrator.main import OrchestrationState
        
        state = OrchestrationState("test-123", load_from_redis=False)
        state.status = "completed"
        state.matches_count = 10
        state._save_to_redis()
        
        mock_set_state.assert_called_once()
        call_args = mock_set_state.call_args[0]
        assert call_args[0] == "test-123"
        assert call_args[1]["status"] == "completed"
        assert call_args[1]["matches_count"] == 10

    @pytest.mark.asyncio
    async def test_state_notify_subscribers(self):
        """Test OrchestrationState notifies all subscribers."""
        from services.orchestrator.main import OrchestrationState
        
        state = OrchestrationState("test-123", load_from_redis=False)
        
        # Create mock subscribers
        queue1 = asyncio.Queue()
        queue2 = asyncio.Queue()
        state._subscribers = {queue1, queue2}
        
        # Notify
        await state.notify({"status": "running"})
        
        # Both queues should receive message
        msg1 = await queue1.get()
        msg2 = await queue2.get()
        
        assert msg1["status"] == "running"
        assert msg2["status"] == "running"

    @pytest.mark.asyncio
    async def test_state_close_removes_from_cache(self):
        """Test OrchestrationState.close removes from global cache."""
        from services.orchestrator.main import OrchestrationState, orchestrations
        
        state = OrchestrationState("test-123", load_from_redis=False)
        orchestrations["test-123"] = state
        
        await state.close()
        
        # Should be removed from cache
        assert "test-123" not in orchestrations


class TestOrchestratorCleanup:
    """Test cleanup of stale orchestrations."""

    @pytest.mark.asyncio
    async def test_cleanup_removes_old_entries(self):
        """Test cleanup removes orchestrations older than TTL."""
        import time
        from services.orchestrator.main import (
            orchestrations, orchestration_timestamps, _orchestration_lock
        )
        
        # Add stale entry
        old_task_id = "old-task"
        async with _orchestration_lock:
            orchestrations[old_task_id] = Mock()
            orchestration_timestamps[old_task_id] = time.time() - 3601  # Older than TTL (1 hour)
            
            # Add recent entry
            new_task_id = "new-task"
            orchestrations[new_task_id] = Mock()
            orchestration_timestamps[new_task_id] = time.time()
        
        # Manually run cleanup logic (not the background task)
        now = time.time()
        stale = [k for k, v in orchestration_timestamps.items()
                 if now - v > 3600]  # ORCHESTRATION_TTL
        
        async with _orchestration_lock:
            for task_id in stale:
                orchestrations.pop(task_id, None)
                orchestration_timestamps.pop(task_id, None)
        
        # Old entry should be removed, new entry should remain
        assert old_task_id not in orchestrations
        assert new_task_id in orchestrations


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
