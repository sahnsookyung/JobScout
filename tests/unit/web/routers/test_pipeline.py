#!/usr/bin/env python3
"""
Tests for Pipeline Router
Covers: web/backend/routers/pipeline.py
"""

import pytest
import io
import json
from unittest.mock import Mock, MagicMock, AsyncMock, patch
from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.testclient import TestClient


class TestPipelineRouter:
    """Test pipeline router endpoints."""

    @pytest.fixture
    def app(self):
        """Create test FastAPI app with pipeline router."""
        from web.backend.routers.pipeline import router
        from fastapi import FastAPI

        app = FastAPI()
        app.include_router(router)
        return app

    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app, raise_server_exceptions=False)

    def test_run_matching_pipeline_success(self, client):
        """Test successful pipeline start via Redis stream."""
        mock_r = Mock()
        mock_r.get.return_value = None  # no active task, no resume upload in progress
        with patch('web.backend.routers.pipeline.get_redis_client', return_value=mock_r), \
             patch('web.backend.routers.pipeline.set_task_state') as mock_set_task_state, \
             patch('web.backend.routers.pipeline.get_task_state', return_value=None), \
             patch('web.backend.routers.pipeline.enqueue_job'), \
             patch('web.backend.routers.pipeline.job_uow') as mock_uow:
            mock_repo = Mock()
            mock_repo.resume.get_latest_stored_resume_fingerprint.return_value = 'abc123'
            mock_uow.return_value.__enter__ = Mock(return_value=mock_repo)
            mock_uow.return_value.__exit__ = Mock(return_value=False)

            response = client.post('/api/pipeline/run-matching')

            assert response.status_code == 200
            data = response.json()
            assert data['success'] is True
            assert data['task_id']
            task_id = data['task_id']
            mock_set_task_state.assert_called_with(
                task_id,
                {"status": "pending", "step": "initializing"},
                ttl=3600,
            )

    def test_run_matching_pipeline_locked(self, client):
        """Test pipeline start returns 409 when a task is already running in Redis."""
        mock_r = Mock()
        mock_r.get.return_value = b'existing-task-id'
        with patch('web.backend.routers.pipeline.get_redis_client', return_value=mock_r), \
             patch('web.backend.routers.pipeline.get_task_state', return_value={'status': 'running'}):

            response = client.post('/api/pipeline/run-matching')

            assert response.status_code == 409
            assert 'already running' in response.json()['detail'].lower()

    def test_run_matching_pipeline_already_running(self, client):
        """Test pipeline start returns 409 when a task is pending in Redis."""
        mock_r = Mock()
        mock_r.get.return_value = b'existing-task-id'
        with patch('web.backend.routers.pipeline.get_redis_client', return_value=mock_r), \
             patch('web.backend.routers.pipeline.get_task_state', return_value={'status': 'pending'}):

            response = client.post('/api/pipeline/run-matching')

            assert response.status_code == 409
            assert 'already running' in response.json()['detail'].lower()

    def test_run_matching_pipeline_internal_error(self, client):
        """Test pipeline start returns 500 when enqueue fails."""
        mock_r = Mock()
        mock_r.get.return_value = None
        with patch('web.backend.routers.pipeline.get_redis_client', return_value=mock_r), \
             patch('web.backend.routers.pipeline.set_task_state'), \
             patch('web.backend.routers.pipeline.get_task_state', return_value=None), \
             patch('web.backend.routers.pipeline.enqueue_job', side_effect=Exception("Stream error")), \
             patch('web.backend.routers.pipeline.job_uow') as mock_uow:
            mock_repo = Mock()
            mock_repo.resume.get_latest_stored_resume_fingerprint.return_value = 'abc123'
            mock_uow.return_value.__enter__ = Mock(return_value=mock_repo)
            mock_uow.return_value.__exit__ = Mock(return_value=False)

            response = client.post('/api/pipeline/run-matching')

            assert response.status_code == 500
            assert 'Failed to start' in response.json()['detail']

    def test_stop_pipeline_success(self, client):
        """Cancels running pipeline by marking it cancelled in Redis."""
        with patch('web.backend.routers.pipeline.get_redis_client') as mock_redis_fn:
            with patch('web.backend.routers.pipeline.get_task_state') as mock_state:
                with patch('web.backend.routers.pipeline.set_task_state') as mock_set_task_state:
                    mock_r = Mock()
                    mock_r.get.return_value = b'task-xyz'
                    mock_redis_fn.return_value = mock_r
                    mock_state.return_value = {'status': 'running', 'step': 'vector_matching'}

                    response = client.post('/api/pipeline/stop')

                    assert response.status_code == 200
                    data = response.json()
                    assert data['success'] is True
                    assert data['task_id'] == 'task-xyz'
                    mock_set_task_state.assert_called_once_with(
                        'task-xyz',
                        {'status': 'cancelled', 'step': 'vector_matching'},
                        ttl=3600,
                    )

    def test_stop_pipeline_not_found(self, client):
        """Returns 404 when no active pipeline task is in Redis."""
        with patch('web.backend.routers.pipeline.get_redis_client') as mock_redis_fn:
            mock_r = Mock()
            mock_r.get.return_value = None
            mock_redis_fn.return_value = mock_r

            response = client.post('/api/pipeline/stop')

            assert response.status_code == 404
            assert 'No active pipeline' in response.json()['detail']

    def test_check_resume_hash_exists(self, client):
        """Test resume hash check when exists."""
        with patch('database.uow.job_uow') as mock_uow:
            mock_repo = Mock()
            mock_repo.resume.resume_hash_exists.return_value = True
            mock_uow.return_value.__enter__.return_value = mock_repo

            response = client.post(
                '/api/pipeline/check-resume-hash',
                json={'resume_hash': 'abc123'}
            )

            assert response.status_code == 200
            data = response.json()
            assert data['exists'] is True
            assert data['resume_hash'] == 'abc123'

    def test_check_resume_hash_not_exists(self, client):
        """Test resume hash check when not exists."""
        with patch('database.uow.job_uow') as mock_uow:
            mock_repo = Mock()
            mock_repo.resume.resume_hash_exists.return_value = False
            mock_uow.return_value.__enter__.return_value = mock_repo

            response = client.post(
                '/api/pipeline/check-resume-hash',
                json={'resume_hash': 'xyz789'}
            )

            assert response.status_code == 200
            data = response.json()
            assert data['exists'] is False

    def test_upload_resume_json_success(self, client):
        """Test successful JSON resume upload."""
        file_content = b'{"name": "John Doe", "skills": ["Python"]}'

        with patch('web.backend.routers.pipeline._validate_resume_file', return_value=file_content):
            with patch('web.backend.routers.pipeline._compute_and_verify_hash', return_value='hash123'):
                with patch('web.backend.routers.pipeline.get_pipeline_manager') as mock_manager:
                    with patch('database.uow.job_uow') as mock_job_uow:
                        mock_mgr_instance = Mock()
                        mock_mgr_instance.create_task.return_value = 'task-456'
                        mock_manager.return_value = mock_mgr_instance
                        
                        # Mock DB check - resume doesn't exist yet
                        mock_repo = Mock()
                        mock_repo.resume.resume_hash_exists.return_value = False
                        mock_context = MagicMock()
                        mock_context.__enter__ = Mock(return_value=mock_repo)
                        mock_context.__exit__ = Mock(return_value=False)
                        mock_job_uow.return_value = mock_context

                        response = client.post(
                            '/api/pipeline/upload-resume',
                            files={'file': ('resume.json', file_content, 'application/json')},
                            data={'resume_hash': 'hash123'}
                        )

                        assert response.status_code == 200
                        data = response.json()
                        assert data['success'] is True
                        assert data['resume_hash'] == 'hash123'
                        assert 'task_id' in data

    def test_upload_resume_no_file(self, client):
        """Test upload with no file."""
        response = client.post(
            '/api/pipeline/upload-resume',
            files={}
        )

        # FastAPI returns 422 for missing required file field
        assert response.status_code == 422

    def test_upload_resume_unsupported_format(self, client):
        """Test upload with unsupported file format."""
        file_content = b'fake content'

        with patch('web.backend.routers.pipeline._validate_resume_file') as mock_validate:
            mock_validate.side_effect = HTTPException(
                status_code=415,
                detail="Unsupported file format"
            )

            response = client.post(
                '/api/pipeline/upload-resume',
                files={'file': ('resume.xyz', file_content, 'application/octet-stream')}
            )

            assert response.status_code == 415

    def test_upload_resume_empty_file(self, client):
        """Test upload with empty file."""
        with patch('web.backend.routers.pipeline._validate_resume_file') as mock_validate:
            mock_validate.side_effect = HTTPException(
                status_code=400,
                detail="Empty file"
            )

            response = client.post(
                '/api/pipeline/upload-resume',
                files={'file': ('resume.json', b'', 'application/json')}
            )

            assert response.status_code == 400

    def test_upload_resume_too_large(self, client):
        """Test upload with file exceeding size limit."""
        from web.shared.constants import RESUME_MAX_SIZE

        large_content = b'x' * (RESUME_MAX_SIZE + 1)

        with patch('web.backend.routers.pipeline._validate_resume_file') as mock_validate:
            mock_validate.side_effect = HTTPException(
                status_code=413,
                detail=f"File size exceeds {RESUME_MAX_SIZE / (1024*1024):.1f}MB limit"
            )

            response = client.post(
                '/api/pipeline/upload-resume',
                files={'file': ('resume.json', large_content, 'application/json')}
            )

            assert response.status_code == 413

    def test_upload_resume_hash_mismatch(self, client):
        """Test upload with hash mismatch."""
        file_content = b'{"name": "test"}'

        with patch('web.backend.routers.pipeline._validate_resume_file', return_value=file_content):
            with patch('web.backend.routers.pipeline._compute_and_verify_hash') as mock_hash:
                mock_hash.return_value = 'computed-hash'
                mock_hash.side_effect = HTTPException(
                    status_code=400,
                    detail="File hash mismatch"
                )

                response = client.post(
                    '/api/pipeline/upload-resume',
                    files={'file': ('resume.json', file_content, 'application/json')},
                    data={'resume_hash': 'different-hash'}
                )

                assert response.status_code == 400


class TestGetActivePipelineTask:
    """Test GET /api/pipeline/active."""

    @pytest.fixture
    def app(self):
        from web.backend.routers.pipeline import router
        app = FastAPI()
        app.include_router(router)
        return app

    @pytest.fixture
    def client(self, app):
        return TestClient(app, raise_server_exceptions=False)

    def test_no_active_task(self, client):
        """Returns null when no active_task_id in Redis."""
        with patch('web.backend.routers.pipeline.get_redis_client') as mock_redis_fn:
            mock_r = Mock()
            mock_r.get.return_value = None
            mock_redis_fn.return_value = mock_r

            response = client.get('/api/pipeline/active')

            assert response.status_code == 200
            assert response.json() is None

    def test_has_running_task(self, client):
        """Returns running task found in Redis."""
        with patch('web.backend.routers.pipeline.get_redis_client') as mock_redis_fn:
            with patch('web.backend.routers.pipeline.get_task_state') as mock_state:
                mock_r = Mock()
                mock_r.get.return_value = b'task-abc'
                mock_redis_fn.return_value = mock_r
                mock_state.return_value = {'status': 'running', 'step': 'vector_matching'}

                response = client.get('/api/pipeline/active')

                assert response.status_code == 200
                data = response.json()
                assert data['task_id'] == 'task-abc'
                assert data['status'] == 'running'
                assert data['step'] == 'vector_matching'

    def test_completed_task_returns_none(self, client):
        """Completed task is not considered active."""
        with patch('web.backend.routers.pipeline.get_redis_client') as mock_redis_fn:
            with patch('web.backend.routers.pipeline.get_task_state') as mock_state:
                mock_r = Mock()
                mock_r.get.return_value = b'task-abc'
                mock_redis_fn.return_value = mock_r
                mock_state.return_value = {'status': 'completed'}

                response = client.get('/api/pipeline/active')

                assert response.status_code == 200
                assert response.json() is None

    def test_redis_error_returns_none(self, client):
        """Redis failure is swallowed — returns null, no 500."""
        with patch('web.backend.routers.pipeline.get_redis_client') as mock_redis_fn:
            mock_redis_fn.side_effect = Exception("Redis unavailable")

            response = client.get('/api/pipeline/active')

            assert response.status_code == 200
            assert response.json() is None



class TestValidateResumeFile:
    """Test _validate_resume_file function."""

    @pytest.mark.asyncio
    async def test_valid_pdf_file(self):
        """Test validation of valid PDF file."""
        from web.backend.routers.pipeline import _validate_resume_file
        from fastapi import UploadFile
        import io

        file = UploadFile(
            filename="resume.pdf",
            file=io.BytesIO(b"%PDF-1.4 fake pdf content")
        )

        content = await _validate_resume_file(file)
        assert len(content) > 0

    @pytest.mark.asyncio
    async def test_valid_json_file(self):
        """Test validation of valid JSON file."""
        from web.backend.routers.pipeline import _validate_resume_file
        from fastapi import UploadFile
        import io

        file = UploadFile(
            filename="resume.json",
            file=io.BytesIO(b'{"name": "test"}')
        )

        content = await _validate_resume_file(file)
        assert content == b'{"name": "test"}'

    @pytest.mark.asyncio
    async def test_no_filename(self):
        """Test validation with no filename."""
        from web.backend.routers.pipeline import _validate_resume_file
        from fastapi import UploadFile, HTTPException
        import io

        file = UploadFile(
            filename=None,
            file=io.BytesIO(b"content")
        )

        with pytest.raises(HTTPException) as exc_info:
            await _validate_resume_file(file)

        assert exc_info.value.status_code == 400
        assert 'No file provided' in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_empty_file(self):
        """Test validation of empty file."""
        from web.backend.routers.pipeline import _validate_resume_file
        from fastapi import UploadFile, HTTPException
        import io

        file = UploadFile(
            filename="resume.json",
            file=io.BytesIO(b"")
        )

        with pytest.raises(HTTPException) as exc_info:
            await _validate_resume_file(file)

        assert exc_info.value.status_code == 400
        assert 'Empty file' in str(exc_info.value.detail)


class TestComputeAndVerifyHash:
    """Test _compute_and_verify_hash function."""

    def test_hash_computation_without_provided_hash(self):
        """Test hash computation when no hash provided."""
        from web.backend.routers.pipeline import _compute_and_verify_hash

        content = b"test file content"
        result = _compute_and_verify_hash(content, None)

        assert isinstance(result, str)
        assert len(result) > 0

    def test_hash_verification_success(self):
        """Test hash verification when hashes match."""
        from web.backend.routers.pipeline import _compute_and_verify_hash
        from database.models.resume import generate_file_fingerprint

        content = b"test file content"
        computed_hash = generate_file_fingerprint(content)
        result = _compute_and_verify_hash(content, computed_hash)

        assert result == computed_hash

    def test_hash_verification_failure(self):
        """Test hash verification when hashes don't match."""
        from web.backend.routers.pipeline import _compute_and_verify_hash
        from fastapi import HTTPException

        content = b"test file content"

        with pytest.raises(HTTPException) as exc_info:
            _compute_and_verify_hash(content, "wrong-hash")

        assert exc_info.value.status_code == 400
        assert 'hash mismatch' in str(exc_info.value.detail).lower()


class TestStreamOrchestratorSSE:
    """Test _stream_orchestrator_sse function."""

    @pytest.mark.asyncio
    async def test_successful_stream(self):
        """Test successful SSE stream from orchestrator."""
        import httpx
        from web.backend.routers.pipeline import _stream_orchestrator_sse

        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.is_error = False
        mock_response.aiter_raw = AsyncMock(return_value=iter([b"data: test\n\n"]))

        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.stream = MagicMock(return_value=mock_response)

        with patch.object(httpx, 'AsyncClient', return_value=mock_client):
            chunks = []
            async for chunk in _stream_orchestrator_sse("http://test:8080", "task-123"):
                chunks.append(chunk)

            assert len(chunks) > 0

    @pytest.mark.asyncio
    async def test_task_not_found(self):
        """Test SSE stream when task not found."""
        import httpx
        from web.backend.routers.pipeline import _stream_orchestrator_sse

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock()
        mock_client.stream = MagicMock(return_value=mock_response)

        with patch.object(httpx, 'AsyncClient', return_value=mock_client):
            chunks = []
            async for chunk in _stream_orchestrator_sse("http://test:8080", "nonexistent"):
                chunks.append(chunk)

            assert any('not found' in chunk for chunk in chunks)

    @pytest.mark.asyncio
    async def test_orchestrator_error(self):
        """Test SSE stream when orchestrator returns error."""
        import httpx
        from web.backend.routers.pipeline import _stream_orchestrator_sse

        mock_response = AsyncMock()
        mock_response.status_code = 500
        mock_response.is_error = True

        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.stream = MagicMock(return_value=mock_response)

        with patch.object(httpx, 'AsyncClient', return_value=mock_client):
            chunks = []
            async for chunk in _stream_orchestrator_sse("http://test:8080", "task-123"):
                chunks.append(chunk)

            assert any('Failed to get' in chunk for chunk in chunks)

    @pytest.mark.asyncio
    async def test_connection_failure(self):
        """Test SSE stream when connection fails."""
        import httpx
        from web.backend.routers.pipeline import _stream_orchestrator_sse

        mock_client = AsyncMock()
        mock_client.__aenter__.side_effect = Exception("Connection refused")

        with patch.object(httpx, 'AsyncClient', return_value=mock_client):
            chunks = []
            async for chunk in _stream_orchestrator_sse("http://test:8080", "task-123"):
                chunks.append(chunk)

            assert any('Failed to connect' in chunk for chunk in chunks)


class TestPreflightTaskCheck:
    """Test _preflight_task_check function."""

    @pytest.mark.asyncio
    async def test_task_exists(self):
        """Test preflight check when task exists."""
        import httpx
        from web.backend.routers.pipeline import _preflight_task_check

        mock_response = AsyncMock()
        mock_response.status_code = 200

        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch.object(httpx, 'AsyncClient', return_value=mock_client):
            # Should not raise
            await _preflight_task_check("http://test:8080", "task-123")

    @pytest.mark.asyncio
    async def test_task_not_found_raises(self):
        """Test preflight check when task not found."""
        import httpx
        from web.backend.routers.pipeline import _preflight_task_check
        from fastapi import HTTPException

        mock_response = AsyncMock()
        mock_response.status_code = 404

        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch.object(httpx, 'AsyncClient', return_value=mock_client):
            with pytest.raises(HTTPException) as exc_info:
                await _preflight_task_check("http://test:8080", "nonexistent")

            assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_probe_failure_logged(self):
        """Test preflight check logs probe failure but doesn't raise."""
        import httpx
        from web.backend.routers.pipeline import _preflight_task_check

        mock_client = AsyncMock()
        mock_client.__aenter__.side_effect = Exception("Connection refused")

        with patch.object(httpx, 'AsyncClient', return_value=mock_client):
            # Should not raise, just log warning
            await _preflight_task_check("http://test:8080", "task-123")


class TestPipelineEventsEndpoint:
    """Test pipeline_events endpoint."""

    @pytest.mark.asyncio
    async def test_events_success(self):
        """Test SSE events endpoint proxies to orchestrator when no Redis state."""
        from web.backend.routers.pipeline import pipeline_events

        with patch.dict('os.environ', {'ORCHESTRATOR_URL': 'http://localhost:8084'}):
            with patch('web.backend.routers.pipeline.get_task_state', return_value=None):
                with patch('web.backend.routers.pipeline._preflight_task_check', new_callable=AsyncMock):
                    with patch('web.backend.routers.pipeline._stream_orchestrator_sse') as mock_stream:
                        mock_stream.return_value = iter([b"data: test\n\n"])

                        response = await pipeline_events("task-123")

                        assert response is not None
                        assert response.media_type == "text/event-stream"

    @pytest.mark.asyncio
    async def test_events_task_not_found(self):
        """Test SSE events endpoint when task not found (split mode via orchestrator)."""
        import os
        from web.backend.routers.pipeline import pipeline_events
        from fastapi import HTTPException

        with patch.dict('os.environ', {'ORCHESTRATOR_URL': 'http://localhost:8084'}):
            with patch('web.backend.routers.pipeline._preflight_task_check', new_callable=AsyncMock) as mock_check:
                mock_check.side_effect = HTTPException(status_code=404, detail="Task not found")

                with pytest.raises(HTTPException) as exc_info:
                    await pipeline_events("nonexistent")

                assert exc_info.value.status_code == 404


class TestProcessResumeBackground:
    """Test _process_resume_background function."""

    def test_successful_processing(self):
        """Test successful resume processing - verifies function exists and accepts correct params."""
        from web.backend.routers.pipeline import _process_resume_background

        mock_manager = Mock()
        mock_task = Mock()
        mock_task.status = 'completed'
        mock_manager.get_task.return_value = mock_task

        # Function should not raise with valid params
        with patch('pathlib.Path.mkdir'), \
             patch('pathlib.Path.write_bytes'), \
             patch('web.backend.routers.pipeline.set_task_state'), \
             patch('web.backend.routers.pipeline.orchestrator_client', create=True):
            _process_resume_background(
                file_content=b'{"name": "test"}',
                filename='resume.json',
                task_id='task-123',
                manager=mock_manager,
                known_fingerprint='test-fp-123'
            )

        assert True

    def test_processing_error(self):
        """Test resume processing with error - verifies error handling."""
        from web.backend.routers.pipeline import _process_resume_background

        mock_manager = Mock()
        mock_manager.process_resume.side_effect = Exception("Processing failed")

        # Should not raise, just log error
        with patch('pathlib.Path.mkdir'), \
             patch('pathlib.Path.write_bytes'), \
             patch('web.backend.routers.pipeline.set_task_state'), \
             patch('web.backend.routers.pipeline.orchestrator_client', create=True):
            _process_resume_background(
                file_content=b'invalid json',
                filename='resume.json',
                task_id='task-123',
                manager=mock_manager,
                known_fingerprint='test-fp-123'
            )

        assert True
