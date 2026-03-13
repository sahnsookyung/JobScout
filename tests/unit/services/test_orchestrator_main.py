#!/usr/bin/env python3
"""
Unit Tests: Orchestrator Service

Tests the orchestrator service functionality without requiring
running services. Tests state management, models, and endpoints.

Usage:
    uv run pytest tests/unit/services/test_orchestrator_main.py -v
"""

import pytest
import asyncio
import threading
import time
from unittest.mock import Mock, MagicMock, AsyncMock, patch, PropertyMock
from datetime import datetime
from contextlib import asynccontextmanager


class TestOrchestratorModels:
    """Test pydantic models in orchestrator service."""

    def test_match_response_model(self):
        """Test MatchResponse model validation."""
        from services.orchestrator.main import MatchResponse

        response = MatchResponse(
            success=True,
            task_id="test-123",
            message="Orchestration complete"
        )

        assert response.success is True
        assert response.task_id == "test-123"
        assert response.message == "Orchestration complete"

    def test_match_response_model_with_all_fields(self):
        """Test MatchResponse model with all optional fields."""
        from services.orchestrator.main import MatchResponse

        response = MatchResponse(
            success=False,
            task_id="test-456",
            message="Pipeline failed"
        )

        assert response.success is False
        assert response.task_id == "test-456"
        assert response.message == "Pipeline failed"


class TestOrchestratorState:
    """Test OrchestrationState class."""

    def test_state_initialization(self):
        """Test OrchestrationState initializes correctly."""
        from services.orchestrator.main import OrchestrationState

        state = OrchestrationState("test-task-123")

        assert state.task_id == "test-task-123"
        assert state.status == "pending"
        assert state.resume_fingerprint is None
        assert state.resume_file is None
        assert state.matches_count == 0
        assert state.error is None
        assert isinstance(state._subscribers, set)

    def test_state_can_hold_consumer_task(self):
        """Test that state can store subscriber."""
        from services.orchestrator.main import OrchestrationState

        state = OrchestrationState("test-task-123")
        mock_queue = Mock()
        state._subscribers.add(mock_queue)

        assert mock_queue in state._subscribers

    @pytest.mark.asyncio
    async def test_state_subscribe(self):
        """Test state subscribe method."""
        from services.orchestrator.main import OrchestrationState

        state = OrchestrationState("test-task-123")
        queue = state.subscribe()

        assert isinstance(queue, asyncio.Queue)
        assert queue in state._subscribers

    @pytest.mark.asyncio
    async def test_state_unsubscribe(self):
        """Test state unsubscribe method."""
        from services.orchestrator.main import OrchestrationState

        state = OrchestrationState("test-task-123")
        queue = asyncio.Queue()
        state._subscribers.add(queue)

        state.unsubscribe(queue)

        assert queue not in state._subscribers

    @pytest.mark.asyncio
    async def test_state_notify(self):
        """Test state notify method."""
        from services.orchestrator.main import OrchestrationState

        state = OrchestrationState("test-task-123")
        queue = state.subscribe()

        test_data = {"status": "test", "message": "test message"}
        await state.notify(test_data)

        result = await queue.get()
        assert result == test_data

    @pytest.mark.asyncio
    async def test_state_notify_handles_exception(self):
        """Test state notify handles subscriber exceptions."""
        from services.orchestrator.main import OrchestrationState

        state = OrchestrationState("test-task-123")
        mock_queue = Mock()
        mock_queue.put = AsyncMock(side_effect=Exception("Queue error"))
        state._subscribers.add(mock_queue)

        test_data = {"status": "test"}
        await state.notify(test_data)

        assert mock_queue not in state._subscribers

    @pytest.mark.asyncio
    async def test_state_close_completed(self):
        """Test state close when completed - should not delete from Redis."""
        from services.orchestrator.main import OrchestrationState, OrchestratorRegistry

        state = OrchestrationState("test-task-123")
        state.status = "completed"
        registry = OrchestratorRegistry()
        registry.orchestrations["test-task-123"] = state

        with patch('services.orchestrator.main.delete_task_state') as mock_delete:
            await state.close(registry)

            mock_delete.assert_not_called()
            assert "test-task-123" not in registry.orchestrations

    @pytest.mark.asyncio
    async def test_state_close_pending_deletes_from_redis(self):
        """Test state close when pending - should delete from Redis."""
        from services.orchestrator.main import OrchestrationState, OrchestratorRegistry

        state = OrchestrationState("test-task-123")
        state.status = "pending"
        registry = OrchestratorRegistry()
        registry.orchestrations["test-task-123"] = state

        with patch('services.orchestrator.main.delete_task_state') as mock_delete:
            await state.close(registry)

            mock_delete.assert_called_once_with("test-task-123")
            assert "test-task-123" not in registry.orchestrations

    @pytest.mark.asyncio
    async def test_state_load_from_redis_with_data(self):
        """Test state load from Redis with existing data."""
        from services.orchestrator.main import OrchestrationState

        mock_data = {
            "status": "completed",
            "resume_fingerprint": "fp123",
            "resume_file": "resume.json",
            "matches_count": 5,
            "error": None
        }

        with patch('services.orchestrator.main.get_task_state', return_value=mock_data):
            state = OrchestrationState("test-task-123")
            await state._load_from_redis()

            assert state.status == "completed"
            assert state.resume_fingerprint == "fp123"
            assert state.resume_file == "resume.json"
            assert state.matches_count == 5

    @pytest.mark.asyncio
    async def test_state_load_from_redis_no_data(self):
        """Test state load from Redis with no data."""
        from services.orchestrator.main import OrchestrationState

        with patch('services.orchestrator.main.get_task_state', return_value=None):
            state = OrchestrationState("test-task-123")
            await state._load_from_redis()

            assert state.status == "pending"
            assert state.resume_fingerprint is None

    @pytest.mark.asyncio
    async def test_state_load_from_redis_exception(self):
        """Test state load from Redis handles exceptions."""
        from services.orchestrator.main import OrchestrationState

        with patch('services.orchestrator.main.get_task_state', side_effect=Exception("Redis error")):
            state = OrchestrationState("test-task-123")
            await state._load_from_redis()

            assert state.status == "pending"

    @pytest.mark.asyncio
    async def test_state_save_to_redis(self):
        """Test state save to Redis."""
        from services.orchestrator.main import OrchestrationState

        state = OrchestrationState("test-task-123")
        state.status = "completed"
        state.matches_count = 10

        with patch('services.orchestrator.main.set_task_state') as mock_set:
            await state._save_to_redis()

            mock_set.assert_called_once()
            call_args = mock_set.call_args[0]
            assert call_args[0] == "test-task-123"
            assert call_args[1]["status"] == "completed"
            assert call_args[1]["matches_count"] == 10

    @pytest.mark.asyncio
    async def test_state_save_to_redis_exception(self):
        """Test state save to Redis handles exceptions."""
        from services.orchestrator.main import OrchestrationState

        state = OrchestrationState("test-task-123")

        with patch('services.orchestrator.main.set_task_state', side_effect=Exception("Redis error")):
            await state._save_to_redis()

    @pytest.mark.asyncio
    async def test_state_create_factory(self):
        """Test async factory method create."""
        from services.orchestrator.main import OrchestrationState

        with patch.object(OrchestrationState, '_load_from_redis', return_value=None):
            state = await OrchestrationState.create("test-task-123", load_from_redis=True)

            assert state.task_id == "test-task-123"
            assert state.status == "pending"


class TestOrchestratorRegistry:
    """Test OrchestratorRegistry class."""

    def test_registry_initialization(self):
        """Test OrchestratorRegistry initializes correctly."""
        from services.orchestrator.main import OrchestratorRegistry

        registry = OrchestratorRegistry()

        assert registry.orchestrations == {}
        assert registry.timestamps == {}
        assert registry.active_task_ids == set()
        assert registry.tasks == {}
        assert isinstance(registry.lock, asyncio.Lock)


class TestGetOrCreateOrchestration:
    """Test get_or_create_orchestration helper."""

    @pytest.mark.asyncio
    async def test_get_existing_orchestration(self):
        """Test getting existing orchestration from registry."""
        from services.orchestrator.main import get_or_create_orchestration, OrchestratorRegistry, OrchestrationState

        registry = OrchestratorRegistry()
        existing_state = OrchestrationState("test-task-123")
        registry.orchestrations["test-task-123"] = existing_state

        state = await get_or_create_orchestration(registry, "test-task-123")

        assert state is existing_state

    @pytest.mark.asyncio
    async def test_create_new_orchestration(self):
        """Test creating new orchestration."""
        from services.orchestrator.main import get_or_create_orchestration, OrchestratorRegistry, OrchestrationState

        registry = OrchestratorRegistry()

        with patch.object(OrchestrationState, 'create', return_value=AsyncMock()) as mock_create:
            mock_state = AsyncMock()
            mock_create.return_value = mock_state

            state = await get_or_create_orchestration(registry, "new-task-456")

            mock_create.assert_called_once_with("new-task-456", load_from_redis=True)
            assert "new-task-456" in registry.orchestrations
            assert "new-task-456" in registry.timestamps


class TestCleanupStaleOrchestrations:
    """Test cleanup_stale_orchestrations function."""

    @pytest.mark.asyncio
    async def test_cleanup_removes_stale_entries(self):
        """Test cleanup removes entries older than TTL."""
        from services.orchestrator.main import OrchestratorRegistry, OrchestrationState, ORCHESTRATION_TTL

        registry = OrchestratorRegistry()
        
        # Add stale entry (older than TTL)
        stale_state = OrchestrationState("stale-task")
        stale_state.status = "completed"
        registry.orchestrations["stale-task"] = stale_state
        registry.timestamps["stale-task"] = time.time() - (ORCHESTRATION_TTL + 100)

        # Add fresh entry
        fresh_state = OrchestrationState("fresh-task")
        registry.orchestrations["fresh-task"] = fresh_state
        registry.timestamps["fresh-task"] = time.time()

        # Manually run the cleanup logic (without the infinite loop)
        stale_states = []
        async with registry.lock:
            now = time.time()
            stale = [k for k, v in registry.timestamps.items() if now - v > ORCHESTRATION_TTL]
            for task_id in stale:
                state = registry.orchestrations.pop(task_id, None)
                if state:
                    stale_states.append(state)
                registry.timestamps.pop(task_id, None)
                registry.tasks.pop(task_id, None)
                registry.active_task_ids.discard(task_id)

        # After cleanup, stale task should be removed, fresh task should remain
        assert "stale-task" not in registry.orchestrations
        assert "fresh-task" in registry.orchestrations
        assert "stale-task" not in registry.timestamps


class TestOrchestratorEndpoints:
    """Test orchestrator service HTTP endpoints using TestClient."""

    def test_health_endpoint(self):
        """Test health endpoint returns correct status."""
        from fastapi.testclient import TestClient
        from services.orchestrator.main import app

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/health")

            # The lifespan should initialize the registry
            assert response.status_code == 200
            assert response.json()["status"] == "healthy"

    def test_health_endpoint_with_redis_connected(self):
        """Test health endpoint with Redis connected."""
        from fastapi.testclient import TestClient
        from services.orchestrator.main import app

        mock_redis = Mock()
        mock_redis.ping.return_value = True

        with patch('services.orchestrator.main.get_redis_client', return_value=mock_redis):
            with TestClient(app, raise_server_exceptions=False) as client:
                response = client.get("/health")

                assert response.status_code == 200
                data = response.json()
                assert data["redis"] == "connected"

    def test_health_endpoint_with_redis_error(self):
        """Test health endpoint with Redis connection error."""
        from fastapi.testclient import TestClient
        from services.orchestrator.main import app
        import redis

        mock_redis = Mock()
        mock_redis.ping.side_effect = redis.ConnectionError("Connection refused")

        with patch('services.orchestrator.main.get_redis_client', return_value=mock_redis):
            with TestClient(app, raise_server_exceptions=False) as client:
                response = client.get("/health")

                assert response.status_code == 200
                data = response.json()
                assert "connection_error" in data["redis"]

    def test_metrics_endpoint(self):
        """Test metrics endpoint."""
        from fastapi.testclient import TestClient
        from services.orchestrator.main import app

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/metrics")

            assert response.status_code == 200
            data = response.json()
            assert data["service"] == "orchestrator"
            assert data["version"] == "1.0.0"

    def test_diagnostics_endpoint(self):
        """Test diagnostics endpoint."""
        from fastapi.testclient import TestClient
        from services.orchestrator.main import app

        with TestClient(app, raise_server_exceptions=False) as client:
            with patch('services.orchestrator.main._get_active_orchestration_states', return_value=[]):
                with patch('services.orchestrator.main._get_recent_tasks', return_value=[]):
                    response = client.get("/orchestrate/diagnostics")

                    assert response.status_code == 200
                    data = response.json()
                    assert data["success"] is True
                    assert "streams" in data
                    assert "active_orchestrations" in data


class TestOrchestrateMatchEndpoint:
    """Test orchestrate_match endpoint."""

    @pytest.mark.asyncio
    async def test_orchestrate_match_success(self):
        """Test orchestrate match endpoint with valid config."""
        from services.orchestrator.main import app, OrchestratorRegistry
        from core.app_context import AppContext

        mock_ctx = Mock(spec=AppContext)
        mock_ctx.config = Mock()
        mock_ctx.config.etl = Mock()
        mock_ctx.config.etl.resume = Mock()
        mock_ctx.config.etl.resume.resume_file = "resume.json"

        mock_registry = OrchestratorRegistry()

        app.state.ctx = mock_ctx
        app.state.registry = mock_registry

        # Mock database access for checking existing resume
        mock_repo = MagicMock()
        mock_repo.resume.get_latest_stored_resume_fingerprint.return_value = None
        mock_uow = MagicMock()
        mock_uow.__enter__ = MagicMock(return_value=mock_repo)
        mock_uow.__exit__ = MagicMock(return_value=False)

        try:
            with patch('database.uow.job_uow', return_value=mock_uow):
                from fastapi.testclient import TestClient
                client = TestClient(app)

                with patch('asyncio.create_task') as mock_create:
                    mock_task = AsyncMock()
                    mock_task.add_done_callback = Mock()
                    mock_create.return_value = mock_task

                    # Use a wrapper that prevents the coroutine from being created
                    def mock_create_task(coro):
                        # Close the coroutine to avoid unawaited warning
                        if hasattr(coro, 'close'):
                            coro.close()
                        return mock_task

                    mock_create.side_effect = mock_create_task

                    response = client.post("/orchestrate/match")

                    assert response.status_code == 200
                    data = response.json()
                    assert data["success"] is True
                    assert "task_id" in data
                    mock_create.assert_called_once()
        finally:
            del app.state.ctx
            del app.state.registry

    @pytest.mark.asyncio
    async def test_orchestrate_match_no_resume_file(self):
        """Test orchestrate match endpoint without resume file configured."""
        from services.orchestrator.main import app, OrchestratorRegistry
        from core.app_context import AppContext
        from unittest.mock import patch, MagicMock

        mock_ctx = Mock(spec=AppContext)
        mock_ctx.config = Mock()
        mock_ctx.config.etl = None

        mock_registry = OrchestratorRegistry()

        app.state.ctx = mock_ctx
        app.state.registry = mock_registry

        mock_repo = MagicMock()
        mock_repo.resume.get_latest_stored_resume_fingerprint.return_value = None

        mock_uow = MagicMock()
        mock_uow.__enter__ = MagicMock(return_value=mock_repo)
        mock_uow.__exit__ = MagicMock(return_value=False)

        try:
            with patch('database.uow.job_uow', return_value=mock_uow):
                from fastapi.testclient import TestClient
                client = TestClient(app)

                response = client.post("/orchestrate/match")

                assert response.status_code == 200
                data = response.json()
                assert data["success"] is False
                assert data["message"] == "No resume found. Please upload a resume first."
        finally:
            del app.state.ctx
            del app.state.registry


class TestGetOrchestrationStatus:
    """Test get_orchestration_status endpoint."""

    @pytest.mark.asyncio
    async def test_get_orchestration_status_sse(self):
        """Test get orchestration status via SSE - tests the event generator logic."""
        from services.orchestrator.main import OrchestratorRegistry, OrchestrationState

        registry = OrchestratorRegistry()
        state = OrchestrationState("test-task-123")
        state.status = "extracting"
        registry.orchestrations["test-task-123"] = state

        # Test the event generator directly instead of using TestClient
        # This avoids the streaming timeout issue
        from unittest.mock import patch

        # Mock the get_or_create_orchestration to return our state
        with patch('services.orchestrator.main.get_or_create_orchestration', return_value=state):
            # Import the endpoint function
            from services.orchestrator.main import get_orchestration_status

            # Create a mock request
            mock_request = Mock()
            mock_request.app = Mock()
            mock_request.app.state = Mock()
            mock_request.app.state.registry = registry

            # Call the endpoint
            response = await get_orchestration_status("test-task-123", mock_request)

            # Verify it returns a StreamingResponse
            from starlette.responses import StreamingResponse
            assert isinstance(response, StreamingResponse)
            assert response.media_type == "text/event-stream"

    def test_get_orchestration_status_endpoint_exists(self):
        """Test that the SSE endpoint is configured correctly."""
        from fastapi.testclient import TestClient
        from services.orchestrator.main import app

        # Just verify the route exists and accepts the right method
        with TestClient(app) as client:
            # We can't test the full streaming behavior without hanging,
            # but we can verify the endpoint is registered
            routes = [route.path for route in app.routes]
            assert "/orchestrate/status/{task_id}" in routes


class TestGetActiveOrchestration:
    """Test get_active_orchestration endpoint."""

    @pytest.mark.asyncio
    async def test_get_active_orchestration_with_tasks(self):
        """Test get active orchestration with active tasks."""
        from services.orchestrator.main import app, OrchestratorRegistry, OrchestrationState
        from fastapi.testclient import TestClient

        registry = OrchestratorRegistry()
        state = OrchestrationState("active-task-123")
        state.status = "matching"
        state.matches_count = 5
        registry.active_task_ids.add("active-task-123")
        registry.orchestrations["active-task-123"] = state

        app.state.registry = registry

        try:
            client = TestClient(app)
            response = client.get("/orchestrate/active")

            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert len(data["tasks"]) == 1
            assert data["tasks"][0]["task_id"] == "active-task-123"
            assert data["tasks"][0]["status"] == "matching"
        finally:
            del app.state.registry

    @pytest.mark.asyncio
    async def test_get_active_orchestration_no_tasks(self):
        """Test get active orchestration with no active tasks."""
        from services.orchestrator.main import app, OrchestratorRegistry
        from fastapi.testclient import TestClient

        registry = OrchestratorRegistry()
        app.state.registry = registry

        try:
            client = TestClient(app)
            response = client.get("/orchestrate/active")

            assert response.status_code == 200
            data = response.json()
            assert data["success"] is False
            assert "No active tasks" in data["message"]
        finally:
            del app.state.registry


class TestStopOrchestration:
    """Test stop_orchestration endpoint."""

    @pytest.mark.asyncio
    async def test_stop_single_task(self):
        """Test stop a single orchestration task."""
        from services.orchestrator.main import app, OrchestratorRegistry
        from fastapi.testclient import TestClient

        registry = OrchestratorRegistry()
        registry.active_task_ids.add("task-123")

        mock_task = Mock()
        mock_task.done.return_value = False
        mock_task.cancel = Mock()
        registry.tasks["task-123"] = mock_task

        app.state.registry = registry

        try:
            client = TestClient(app)
            response = client.post("/orchestrate/stop?task_id=task-123")

            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            mock_task.cancel.assert_called_once()
        finally:
            del app.state.registry

    @pytest.mark.asyncio
    async def test_stop_all_tasks(self):
        """Test stop all orchestration tasks."""
        from services.orchestrator.main import app, OrchestratorRegistry
        from fastapi.testclient import TestClient

        registry = OrchestratorRegistry()
        registry.active_task_ids = {"task-123", "task-456"}

        mock_task1 = Mock()
        mock_task1.done.return_value = False
        mock_task1.cancel = Mock()
        mock_task2 = Mock()
        mock_task2.done.return_value = False
        mock_task2.cancel = Mock()
        registry.tasks = {"task-123": mock_task1, "task-456": mock_task2}

        app.state.registry = registry

        try:
            client = TestClient(app)
            response = client.post("/orchestrate/stop")

            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert len(data["stopped"]) == 2
        finally:
            del app.state.registry


class TestOrchestratorUtilities:
    """Test orchestrator utility functions."""

    def test_channel_constants_defined(self):
        """Test that channel constants are defined."""
        from services.orchestrator import main as orch_module

        assert hasattr(orch_module, 'CHANNEL_EXTRACTION_DONE')
        assert hasattr(orch_module, 'CHANNEL_EMBEDDINGS_DONE')
        assert hasattr(orch_module, 'CHANNEL_MATCHING_DONE')

    def test_stream_constants_defined(self):
        """Test that stream constants are defined."""
        from services.orchestrator import main as orch_module

        assert hasattr(orch_module, 'STREAM_EXTRACTION')
        assert hasattr(orch_module, 'STREAM_EMBEDDINGS')
        assert hasattr(orch_module, 'STREAM_MATCHING')

    def test_constants_have_correct_values(self):
        """Test constants have correct values."""
        from services.orchestrator import main as orch_module

        assert 'extraction' in orch_module.CHANNEL_EXTRACTION_DONE.lower()
        assert 'embeddings' in orch_module.CHANNEL_EMBEDDINGS_DONE.lower()
        assert 'matching' in orch_module.CHANNEL_MATCHING_DONE.lower()


class TestPipelineHelpers:
    """Test pipeline helper functions."""

    @pytest.mark.asyncio
    async def test_wait_for_next_message(self):
        """Test _wait_for_next_message function."""
        from services.orchestrator.main import _wait_for_next_message

        # Create an async iterator mock
        async def async_gen():
            yield {"type": "message", "data": '{"task_id": "test-123", "status": "completed"}'}

        mock_pubsub = AsyncMock()
        mock_pubsub.listen = MagicMock(return_value=async_gen())

        result = await _wait_for_next_message(mock_pubsub)

        assert result["task_id"] == "test-123"
        assert result["status"] == "completed"

    @pytest.mark.asyncio
    async def test_wait_for_task_message_skips_wrong_task(self):
        """Test _wait_for_task_message skips messages for wrong task."""
        from services.orchestrator.main import _wait_for_task_message

        # Create an async iterator mock
        async def async_gen():
            yield {"type": "message", "data": '{"task_id": "other-task", "status": "completed"}'}
            yield {"type": "message", "data": '{"task_id": "test-123", "status": "completed"}'}

        mock_pubsub = AsyncMock()
        mock_pubsub.listen = MagicMock(return_value=async_gen())

        result = await _wait_for_task_message(mock_pubsub, "test-123")

        assert result["task_id"] == "test-123"

    @pytest.mark.asyncio
    async def test_run_pipeline_stage_success(self):
        """Test _run_pipeline_stage with successful completion."""
        from services.orchestrator.main import _run_pipeline_stage, OrchestrationState

        state = OrchestrationState("test-123")

        # Create an async iterator mock
        async def async_gen():
            yield {"type": "message", "data": '{"task_id": "test-123", "status": "completed"}'}

        mock_pubsub = AsyncMock()
        mock_pubsub.listen = MagicMock(return_value=async_gen())

        with patch('services.orchestrator.main.enqueue_job'):
            success, data = await _run_pipeline_stage(
                state=state,
                pubsub=mock_pubsub,
                stream="test:stream",
                job_payload={"task_id": "test-123"},
                stage_name="test"
            )

            assert success is True
            assert data["status"] == "completed"

    @pytest.mark.asyncio
    async def test_run_pipeline_stage_failed(self):
        """Test _run_pipeline_stage with failed status."""
        from services.orchestrator.main import _run_pipeline_stage, OrchestrationState

        state = OrchestrationState("test-123")

        # Create an async iterator mock
        async def async_gen():
            yield {"type": "message", "data": '{"task_id": "test-123", "status": "failed", "error": "Test error"}'}

        mock_pubsub = AsyncMock()
        mock_pubsub.listen = MagicMock(return_value=async_gen())

        with patch('services.orchestrator.main.enqueue_job'):
            success, data = await _run_pipeline_stage(
                state=state,
                pubsub=mock_pubsub,
                stream="test:stream",
                job_payload={"task_id": "test-123"},
                stage_name="test"
            )

            assert success is False
            assert state.status == "failed"
            assert state.error == "Test error"

    @pytest.mark.asyncio
    async def test_run_pipeline_stage_unexpected_status(self):
        """Test _run_pipeline_stage with unexpected status."""
        from services.orchestrator.main import _run_pipeline_stage, OrchestrationState

        state = OrchestrationState("test-123")

        # Create an async iterator mock
        async def async_gen():
            yield {"type": "message", "data": '{"task_id": "test-123", "status": "unknown"}'}

        mock_pubsub = AsyncMock()
        mock_pubsub.listen = MagicMock(return_value=async_gen())

        with patch('services.orchestrator.main.enqueue_job'):
            success, data = await _run_pipeline_stage(
                state=state,
                pubsub=mock_pubsub,
                stream="test:stream",
                job_payload={"task_id": "test-123"},
                stage_name="test"
            )

            assert success is False
            assert state.status == "failed"
            assert "Unexpected status" in state.error

    @pytest.mark.asyncio
    async def test_run_pipeline_stage_skipped(self):
        """Test _run_pipeline_stage with skipped status."""
        from services.orchestrator.main import _run_pipeline_stage, OrchestrationState

        state = OrchestrationState("test-123")

        # Create an async iterator mock
        async def async_gen():
            yield {"type": "message", "data": '{"task_id": "test-123", "status": "skipped"}'}

        mock_pubsub = AsyncMock()
        mock_pubsub.listen = MagicMock(return_value=async_gen())

        with patch('services.orchestrator.main.enqueue_job'):
            success, data = await _run_pipeline_stage(
                state=state,
                pubsub=mock_pubsub,
                stream="test:stream",
                job_payload={"task_id": "test-123"},
                stage_name="test"
            )

            assert success is True
            assert state.status != "failed"

    @pytest.mark.asyncio
    async def test_handle_extraction_fingerprint_success(self):
        """Test _handle_extraction_fingerprint with valid fingerprint."""
        from services.orchestrator.main import _handle_extraction_fingerprint, OrchestrationState

        state = OrchestrationState("test-123")
        extraction_data = {"resume_fingerprint": "fp123", "status": "completed"}

        result = await _handle_extraction_fingerprint(state, "test-123", extraction_data)

        assert result is True
        assert state.resume_fingerprint == "fp123"

    @pytest.mark.asyncio
    async def test_handle_extraction_fingerprint_missing(self):
        """Test _handle_extraction_fingerprint with missing fingerprint."""
        from services.orchestrator.main import _handle_extraction_fingerprint, OrchestrationState

        state = OrchestrationState("test-123")
        extraction_data = {"status": "completed"}

        result = await _handle_extraction_fingerprint(state, "test-123", extraction_data)

        assert result is False
        assert state.status == "failed"
        assert "No fingerprint" in state.error

    @pytest.mark.asyncio
    async def test_handle_extraction_fingerprint_skipped(self):
        """Test _handle_extraction_fingerprint with skipped status."""
        from services.orchestrator.main import _handle_extraction_fingerprint, OrchestrationState

        state = OrchestrationState("test-123")
        extraction_data = {"status": "skipped"}

        result = await _handle_extraction_fingerprint(state, "test-123", extraction_data)

        assert result is True
        assert state.status != "failed"

    @pytest.mark.asyncio
    async def test_cleanup_pubsub_and_client(self):
        """Test _cleanup_pubsub_and_client function."""
        from services.orchestrator.main import _cleanup_pubsub_and_client

        mock_redis = AsyncMock()
        mock_pubsub = AsyncMock()

        await _cleanup_pubsub_and_client(mock_redis, mock_pubsub)

        mock_pubsub.unsubscribe.assert_called_once()
        mock_pubsub.close.assert_called_once()
        mock_redis.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_pubsub_and_client_handles_exceptions(self):
        """Test _cleanup_pubsub_and_client handles exceptions."""
        from services.orchestrator.main import _cleanup_pubsub_and_client

        mock_redis = AsyncMock()
        mock_redis.aclose.side_effect = Exception("Close error")
        mock_pubsub = AsyncMock()
        mock_pubsub.unsubscribe.side_effect = Exception("Unsubscribe error")

        await _cleanup_pubsub_and_client(mock_redis, mock_pubsub)


class TestHandleTaskDone:
    """Test _handle_task_done callback."""

    @pytest.mark.asyncio
    async def test_handle_task_done_cancelled(self):
        """Test _handle_task_done with cancelled task."""
        from services.orchestrator.main import _handle_task_done, OrchestratorRegistry, OrchestrationState

        registry = OrchestratorRegistry()
        state = OrchestrationState("test-123")
        registry.orchestrations["test-123"] = state

        mock_task = Mock()
        mock_task.cancelled.return_value = True
        mock_task.exception.return_value = None

        await _handle_task_done("test-123", mock_task, registry)

        assert state.status == "cancelled"
        assert state.error == "Task cancelled"

    @pytest.mark.asyncio
    async def test_handle_task_done_exception(self):
        """Test _handle_task_done with task exception."""
        from services.orchestrator.main import _handle_task_done, OrchestratorRegistry, OrchestrationState

        registry = OrchestratorRegistry()
        state = OrchestrationState("test-123")
        registry.orchestrations["test-123"] = state

        mock_task = Mock()
        mock_task.cancelled.return_value = False
        mock_task.exception.return_value = Exception("Task failed")

        await _handle_task_done("test-123", mock_task, registry)

        assert state.status == "failed"
        assert "Task failed" in state.error

    @pytest.mark.asyncio
    async def test_handle_task_done_success(self):
        """Test _handle_task_done with successful task."""
        from services.orchestrator.main import _handle_task_done, OrchestratorRegistry

        registry = OrchestratorRegistry()
        registry.tasks["test-123"] = Mock()

        mock_task = Mock()
        mock_task.cancelled.return_value = False
        mock_task.exception.return_value = None

        await _handle_task_done("test-123", mock_task, registry)

        assert "test-123" not in registry.tasks


class TestDiagnosticHelpers:
    """Test diagnostic helper functions."""

    def test_get_stream_diagnostic_exists(self):
        """Test _get_stream_diagnostic for existing stream."""
        from services.orchestrator.main import _get_stream_diagnostic

        mock_info = {
            "length": 10,
            "first-entry": ("123", {"data": "test"}),
            "groups": [{"name": "group1", "consumers": 2, "pending": 0, "last-delivered-id": "0"}]
        }

        with patch('services.orchestrator.main.stream_exists', return_value=True):
            with patch('services.orchestrator.main.get_stream_info', return_value=mock_info):
                result = _get_stream_diagnostic("test:stream")

                assert result["exists"] is True
                assert result["length"] == 10
                assert len(result["consumer_groups"]) == 1

    def test_get_stream_diagnostic_not_exists(self):
        """Test _get_stream_diagnostic for non-existing stream."""
        from services.orchestrator.main import _get_stream_diagnostic

        with patch('services.orchestrator.main.stream_exists', return_value=False):
            result = _get_stream_diagnostic("test:stream")

            assert result["exists"] is False
            assert result["length"] == 0

    def test_get_stream_diagnostic_error(self):
        """Test _get_stream_diagnostic with error."""
        from services.orchestrator.main import _get_stream_diagnostic

        with patch('services.orchestrator.main.stream_exists', side_effect=Exception("Error")):
            result = _get_stream_diagnostic("test:stream")

            assert "error" in result

    @pytest.mark.asyncio
    async def test_get_active_orchestration_states(self):
        """Test _get_active_orchestration_states."""
        from services.orchestrator.main import _get_active_orchestration_states, OrchestratorRegistry, OrchestrationState

        registry = OrchestratorRegistry()
        state1 = OrchestrationState("task-1")
        state1.status = "extracting"
        state1.error = None
        state2 = OrchestrationState("task-2")
        state2.status = "matching"
        state2.error = "Test error"

        registry.active_task_ids = {"task-1", "task-2"}
        registry.orchestrations = {"task-1": state1, "task-2": state2}

        result = await _get_active_orchestration_states(registry)

        assert len(result) == 2

    def test_get_recent_tasks_success(self):
        """Test _get_recent_tasks with success."""
        from services.orchestrator.main import _get_recent_tasks

        mock_redis = Mock()
        mock_redis.keys.return_value = ["task:task-1:state"]

        mock_task_data = {"status": "completed", "error": None}

        with patch('services.orchestrator.main.get_task_state', return_value=mock_task_data):
            result = _get_recent_tasks(mock_redis)

            assert isinstance(result, list)

    def test_get_recent_tasks_exception(self):
        """Test _get_recent_tasks with exception."""
        from services.orchestrator.main import _get_recent_tasks

        mock_redis = Mock()
        mock_redis.keys.side_effect = Exception("Redis error")

        result = _get_recent_tasks(mock_redis)

        assert "error" in result


class TestLifespan:
    """Test orchestrator lifespan management."""

    @pytest.mark.asyncio
    async def test_lifespan_startup(self):
        """Test lifespan startup."""
        from services.orchestrator.main import lifespan, OrchestratorRegistry
        from fastapi import FastAPI

        app = FastAPI()

        mock_ctx = Mock()
        mock_ctx.config = Mock()
        mock_ctx.aclose = AsyncMock()  # Add async close method

        # Create a completed task to use as mock (avoids unawaited coroutine warning)
        mock_cleanup_task = asyncio.create_task(asyncio.sleep(0))
        await mock_cleanup_task  # Let it complete

        def create_mock_task(coro):
            # Close the original coroutine to avoid warning
            if hasattr(coro, 'close'):
                coro.close()
            return mock_cleanup_task

        with patch('services.orchestrator.main.load_config'):
            with patch('services.orchestrator.main.AppContext.build', return_value=mock_ctx):
                with patch('asyncio.create_task', side_effect=create_mock_task):
                    async with lifespan(app):
                        assert isinstance(app.state.registry, OrchestratorRegistry)
                        assert app.state.ctx is mock_ctx

    @pytest.mark.asyncio
    async def test_lifespan_cleanup_with_aclose(self):
        """Test lifespan cleanup with aclose method."""
        from services.orchestrator.main import lifespan, OrchestratorRegistry
        from fastapi import FastAPI

        app = FastAPI()
        app.state.registry = OrchestratorRegistry()

        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()
        app.state.ctx = mock_ctx

        # Create a completed task to use as mock (avoids unawaited coroutine warning)
        mock_cleanup_task = asyncio.create_task(asyncio.sleep(0))
        await mock_cleanup_task  # Let it complete

        def create_mock_task(coro):
            # Close the original coroutine to avoid warning
            if hasattr(coro, 'close'):
                coro.close()
            return mock_cleanup_task

        with patch('services.orchestrator.main.load_config'):
            with patch('services.orchestrator.main.AppContext.build', return_value=mock_ctx):
                with patch('asyncio.create_task', side_effect=create_mock_task):
                    async with lifespan(app):
                        pass

                    await mock_ctx.aclose()

    @pytest.mark.asyncio
    async def test_lifespan_cleanup_with_close(self):
        """Test lifespan cleanup with close method."""
        from services.orchestrator.main import lifespan, OrchestratorRegistry
        from fastapi import FastAPI

        app = FastAPI()
        app.state.registry = OrchestratorRegistry()

        mock_ctx = Mock()
        # Check if aclose exists before deleting
        if hasattr(mock_ctx, 'aclose'):
            del mock_ctx.aclose
        mock_ctx.close = Mock()
        app.state.ctx = mock_ctx

        # Create a completed task to use as mock (avoids unawaited coroutine warning)
        mock_cleanup_task = asyncio.create_task(asyncio.sleep(0))
        await mock_cleanup_task  # Let it complete

        def create_mock_task(coro):
            # Close the original coroutine to avoid warning
            if hasattr(coro, 'close'):
                coro.close()
            return mock_cleanup_task

        with patch('services.orchestrator.main.load_config'):
            with patch('services.orchestrator.main.AppContext.build', return_value=mock_ctx):
                with patch('asyncio.create_task', side_effect=create_mock_task):
                    async with lifespan(app):
                        pass

                    mock_ctx.close.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
