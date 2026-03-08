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
        """Test successful pipeline start."""
        with patch('web.backend.routers.pipeline.orchestrator_client') as mock_client:
            mock_client.start_matching.return_value = {
                'success': True,
                'task_id': 'test-task-123',
                'message': 'Pipeline started'
            }

            response = client.post('/api/pipeline/run-matching')

            assert response.status_code == 200
            data = response.json()
            assert data['success'] is True
            assert data['task_id'] == 'test-task-123'

    def test_run_matching_pipeline_locked(self, client):
        """Test pipeline start when locked."""
        from web.backend.exceptions import PipelineLockedException

        with patch('web.backend.routers.pipeline.orchestrator_client') as mock_client:
            mock_client.start_matching.side_effect = PipelineLockedException(
                "Pipeline is already running"
            )

            response = client.post('/api/pipeline/run-matching')

            assert response.status_code == 409
            assert 'already running' in response.json()['detail'].lower()

    def test_run_matching_pipeline_already_running(self, client):
        """Test pipeline start when already running (from orchestrator)."""
        with patch('web.backend.routers.pipeline.orchestrator_client') as mock_client:
            mock_client.start_matching.return_value = {
                'success': False,
                'task_id': '',
                'message': 'Pipeline is already running'
            }

            response = client.post('/api/pipeline/run-matching')

            assert response.status_code == 409
            assert 'already running' in response.json()['detail']

    def test_run_matching_pipeline_internal_error(self, client):
        """Test pipeline start with internal error."""
        with patch('web.backend.routers.pipeline.orchestrator_client') as mock_client:
            mock_client.start_matching.side_effect = Exception("Database error")

            response = client.post('/api/pipeline/run-matching')

            assert response.status_code == 500
            assert 'Failed to start' in response.json()['detail']

    def test_stop_pipeline_success(self, client):
        """Test successful pipeline stop."""
        with patch('web.backend.routers.pipeline.orchestrator_client') as mock_client:
            mock_client.stop_matching.return_value = {
                'success': True,
                'task_id': 'test-task-123'
            }

            response = client.post('/api/pipeline/stop-matching')

            assert response.status_code == 200
            data = response.json()
            assert data['success'] is True

    def test_stop_pipeline_not_found(self, client):
        """Test stop when no pipeline running."""
        with patch('web.backend.routers.pipeline.orchestrator_client') as mock_client:
            mock_client.stop_matching.side_effect = Exception("No pipeline running")

            response = client.post('/api/pipeline/stop-matching')

            assert response.status_code == 404
            assert 'No active pipeline' in response.json()['detail']

    def test_check_resume_hash_exists(self, client):
        """Test resume hash check when exists."""
        with patch('web.backend.routers.pipeline.job_uow') as mock_uow:
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
        with patch('web.backend.routers.pipeline.job_uow') as mock_uow:
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
                    mock_mgr_instance = Mock()
                    mock_mgr_instance.create_task.return_value = 'task-456'
                    mock_manager.return_value = mock_mgr_instance

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

        assert response.status_code == 400

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
        from web.backend.routers.pipeline import _stream_orchestrator_sse

        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.is_error = False
        mock_response.aiter_raw = AsyncMock(return_value=iter([b"data: test\n\n"]))

        mock_client_context = AsyncMock()
        mock_client_context.__aenter__.return_value = mock_client_context
        mock_client_context.stream = AsyncMock(return_value=mock_response)

        with patch('web.backend.routers.pipeline.httpx.AsyncClient', return_value=mock_client_context):
            chunks = []
            async for chunk in _stream_orchestrator_sse("http://test:8080", "task-123"):
                chunks.append(chunk)

            assert len(chunks) > 0

    @pytest.mark.asyncio
    async def test_task_not_found(self):
        """Test SSE stream when task not found."""
        from web.backend.routers.pipeline import _stream_orchestrator_sse

        mock_response = AsyncMock()
        mock_response.status_code = 404

        mock_client_context = AsyncMock()
        mock_client_context.__aenter__.return_value = mock_client_context
        mock_client_context.stream = AsyncMock(return_value=mock_response)

        with patch('web.backend.routers.pipeline.httpx.AsyncClient', return_value=mock_client_context):
            chunks = []
            async for chunk in _stream_orchestrator_sse("http://test:8080", "nonexistent"):
                chunks.append(chunk)

            assert any('not found' in chunk for chunk in chunks)

    @pytest.mark.asyncio
    async def test_orchestrator_error(self):
        """Test SSE stream when orchestrator returns error."""
        from web.backend.routers.pipeline import _stream_orchestrator_sse

        mock_response = AsyncMock()
        mock_response.status_code = 500
        mock_response.is_error = True

        mock_client_context = AsyncMock()
        mock_client_context.__aenter__.return_value = mock_client_context
        mock_client_context.stream = AsyncMock(return_value=mock_response)

        with patch('web.backend.routers.pipeline.httpx.AsyncClient', return_value=mock_client_context):
            chunks = []
            async for chunk in _stream_orchestrator_sse("http://test:8080", "task-123"):
                chunks.append(chunk)

            assert any('Failed to get' in chunk for chunk in chunks)

    @pytest.mark.asyncio
    async def test_connection_failure(self):
        """Test SSE stream when connection fails."""
        from web.backend.routers.pipeline import _stream_orchestrator_sse

        mock_client_context = AsyncMock()
        mock_client_context.__aenter__.side_effect = Exception("Connection refused")

        with patch('web.backend.routers.pipeline.httpx.AsyncClient', return_value=mock_client_context):
            chunks = []
            async for chunk in _stream_orchestrator_sse("http://test:8080", "task-123"):
                chunks.append(chunk)

            assert any('Failed to connect' in chunk for chunk in chunks)


class TestPreflightTaskCheck:
    """Test _preflight_task_check function."""

    @pytest.mark.asyncio
    async def test_task_exists(self):
        """Test preflight check when task exists."""
        from web.backend.routers.pipeline import _preflight_task_check

        mock_response = AsyncMock()
        mock_response.status_code = 200

        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch('web.backend.routers.pipeline.httpx.AsyncClient', return_value=mock_client):
            # Should not raise
            await _preflight_task_check("http://test:8080", "task-123")

    @pytest.mark.asyncio
    async def test_task_not_found_raises(self):
        """Test preflight check when task not found."""
        from web.backend.routers.pipeline import _preflight_task_check
        from fastapi import HTTPException

        mock_response = AsyncMock()
        mock_response.status_code = 404

        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch('web.backend.routers.pipeline.httpx.AsyncClient', return_value=mock_client):
            with pytest.raises(HTTPException) as exc_info:
                await _preflight_task_check("http://test:8080", "nonexistent")

            assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_probe_failure_logged(self):
        """Test preflight check logs probe failure but doesn't raise."""
        from web.backend.routers.pipeline import _preflight_task_check

        mock_client = AsyncMock()
        mock_client.__aenter__.side_effect = Exception("Connection refused")

        with patch('web.backend.routers.pipeline.httpx.AsyncClient', return_value=mock_client):
            # Should not raise, just log warning
            await _preflight_task_check("http://test:8080", "task-123")


class TestPipelineEventsEndpoint:
    """Test pipeline_events endpoint."""

    @pytest.mark.asyncio
    async def test_events_success(self):
        """Test SSE events endpoint."""
        from web.backend.routers.pipeline import pipeline_events

        with patch('web.backend.routers.pipeline._preflight_task_check', new_callable=AsyncMock):
            with patch('web.backend.routers.pipeline._stream_orchestrator_sse') as mock_stream:
                mock_stream.return_value = iter([b"data: test\n\n"])

                response = await pipeline_events("task-123")

                assert response is not None
                assert response.media_type == "text/event-stream"

    @pytest.mark.asyncio
    async def test_events_task_not_found(self):
        """Test SSE events endpoint when task not found."""
        from web.backend.routers.pipeline import pipeline_events
        from fastapi import HTTPException

        with patch('web.backend.routers.pipeline._preflight_task_check', new_callable=AsyncMock) as mock_check:
            mock_check.side_effect = HTTPException(status_code=404, detail="Task not found")

            with pytest.raises(HTTPException) as exc_info:
                await pipeline_events("nonexistent")

            assert exc_info.value.status_code == 404


class TestProcessResumeBackground:
    """Test _process_resume_background function."""

    def test_successful_processing(self):
        """Test successful resume processing."""
        from web.backend.routers.pipeline import _process_resume_background

        mock_manager = Mock()
        mock_task = Mock()
        mock_task.status = 'completed'
        mock_manager.get_task.return_value = mock_task

        _process_resume_background(
            file_content=b'{"name": "test"}',
            filename='resume.json',
            task_id='task-123',
            manager=mock_manager
        )

        mock_manager.process_resume.assert_called_once()

    def test_processing_error(self):
        """Test resume processing with error."""
        from web.backend.routers.pipeline import _process_resume_background

        mock_manager = Mock()
        mock_manager.process_resume.side_effect = Exception("Processing failed")

        # Should not raise, just log error
        _process_resume_background(
            file_content=b'invalid json',
            filename='resume.json',
            task_id='task-123',
            manager=mock_manager
        )

        mock_manager.update_task_status.assert_called_with(
            'task-123',
            'failed',
            error=mock_manager.process_resume.side_effect
        )
