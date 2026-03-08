#!/usr/bin/env python3
"""
Unit Tests: Extraction Service

Tests the extraction service functionality without requiring
running services. Tests path validation and state management.

Usage:
    uv run pytest tests/unit/services/test_extraction.py -v
"""

import asyncio
import os
import pytest
import threading
from unittest.mock import Mock, patch, MagicMock
import sys


class TestValidateResumePath:
    """Test path validation logic in extraction service."""

    def test_valid_path_in_app_directory(self):
        """Test that paths within /app are accepted."""
        with patch('os.path.realpath') as mock_realpath:
            mock_realpath.side_effect = lambda p: p
            
            # Need to reimport to get the patched version
            import importlib
            import services.extraction.main as extraction_module
            importlib.reload(extraction_module)
            
            is_valid, path = extraction_module._validate_resume_path("/app/resume.pdf")
            
            assert is_valid is True
            assert path == "/app/resume.pdf"

    def test_valid_path_in_data_directory(self):
        """Test that paths within /data are accepted."""
        with patch('os.path.realpath') as mock_realpath:
            mock_realpath.side_effect = lambda p: p
            
            import importlib
            import services.extraction.main as extraction_module
            importlib.reload(extraction_module)
            
            is_valid, path = extraction_module._validate_resume_path("/data/resumes/test.pdf")
            
            assert is_valid is True
            assert path == "/data/resumes/test.pdf"

    def test_valid_path_in_current_working_directory(self):
        """Test that paths within current working directory are accepted."""
        with patch('os.path.realpath') as mock_realpath:
            def realpath_side_effect(path):
                if path.startswith("/"):
                    return path
                return os.path.realpath(path)
            
            mock_realpath.side_effect = realpath_side_effect
            with patch('os.getcwd', return_value="/workspace"):
                
                import importlib
                import services.extraction.main as extraction_module
                importlib.reload(extraction_module)
                
                is_valid, path = extraction_module._validate_resume_path("/workspace/resume.pdf")
                
                assert is_valid is True

    def test_invalid_path_outside_allowed_directories(self):
        """Test that paths outside allowed directories are rejected."""
        with patch('os.path.realpath') as mock_realpath:
            mock_realpath.side_effect = lambda p: p
            
            import importlib
            import services.extraction.main as extraction_module
            importlib.reload(extraction_module)
            
            is_valid, error_msg = extraction_module._validate_resume_path("/etc/passwd")
            
            assert is_valid is False
            assert "Invalid" in error_msg

    def test_path_traversal_attempt_rejected(self):
        """Test that path traversal attempts are rejected."""
        with patch('os.path.realpath') as mock_realpath:
            # Simulate path traversal resolving to /etc
            mock_realpath.side_effect = lambda p: "/etc/passwd" if ".." in p else p
            
            import importlib
            import services.extraction.main as extraction_module
            importlib.reload(extraction_module)
            
            is_valid, error_msg = extraction_module._validate_resume_path("/app/../../../etc/passwd")
            
            assert is_valid is False


class TestExtractionState:
    """Test ExtractionState class."""

    def test_state_initialization(self):
        """Test ExtractionState initializes correctly."""
        from services.extraction.main import ExtractionState
        
        mock_ctx = Mock()
        state = ExtractionState(mock_ctx)
        
        assert state.ctx is mock_ctx
        assert isinstance(state.stop_event, type(threading.Event()))
        assert state.consumer_task is None

    def test_state_can_hold_consumer_task(self):
        """Test that state can store a consumer task."""
        import asyncio
        from services.extraction.main import ExtractionState
        
        mock_ctx = Mock()
        state = ExtractionState(mock_ctx)
        
        # Just verify the attribute can be set (don't create actual tasks)
        state.consumer_task = "dummy_task_reference"
        
        assert state.consumer_task == "dummy_task_reference"


class TestExtractionLifespan:
    """Test extraction service lifespan management."""

    def test_setup_logging_configures_logger(self):
        """Test that _setup_logging configures logging correctly."""
        import logging
        
        with patch('logging.basicConfig') as mock_basic_config:
            import services.extraction.main as extraction_module
            extraction_module._setup_logging()
            
            mock_basic_config.assert_called_once()
            call_kwargs = mock_basic_config.call_args[1]
            assert call_kwargs['level'] == logging.INFO
            assert 'format' in call_kwargs


class TestExtractionEndpoints:
    """Test extraction service HTTP endpoints using TestClient."""

    def test_health_endpoint(self):
        """Test health endpoint returns correct status."""
        from fastapi.testclient import TestClient
        from services.extraction.main import app
        
        client = TestClient(app)
        response = client.get("/health")
        
        assert response.status_code == 200
        assert response.json() == {"status": "healthy", "service": "extraction"}

    def test_metrics_endpoint(self):
        """Test metrics endpoint returns correct info."""
        from fastapi.testclient import TestClient
        from services.extraction.main import app
        
        mock_task = Mock()
        mock_task.done.return_value = False
        
        mock_state = Mock()
        mock_state.consumer_task = mock_task
        
        app.state.extraction = mock_state
        try:
            client = TestClient(app)
            response = client.get("/metrics")
            
            assert response.status_code == 200
            data = response.json()
            assert data["service"] == "extraction"
            assert data["consumer_running"] is True
        finally:
            del app.state.extraction

    def test_metrics_endpoint_no_consumer(self):
        """Test metrics endpoint when no consumer is running."""
        from fastapi.testclient import TestClient
        from services.extraction.main import app
        
        mock_state = Mock()
        mock_state.consumer_task = None
        
        app.state.extraction = mock_state
        try:
            client = TestClient(app)
            response = client.get("/metrics")
            
            assert response.status_code == 200
            data = response.json()
            assert data["consumer_running"] is False
        finally:
            del app.state.extraction

    def test_extract_jobs_endpoint(self):
        """Test extract/jobs endpoint."""
        from fastapi.testclient import TestClient
        from services.extraction.main import app
        
        mock_state = Mock()
        mock_state.ctx = Mock()
        mock_state.stop_event = Mock()
        
        with patch('services.extraction.main.run_job_extraction', return_value=5):
            app.state.extraction = mock_state
            try:
                client = TestClient(app)
                response = client.post("/extract/jobs?limit=10")
                
                assert response.status_code == 200
                data = response.json()
                assert data["success"] is True
                assert data["processed"] == 5
            finally:
                del app.state.extraction

    def test_extract_resume_endpoint_valid_path(self):
        """Test extract/resume endpoint with valid path."""
        from fastapi.testclient import TestClient
        from services.extraction.main import app
        
        mock_state = Mock()
        mock_state.ctx = Mock()
        
        with patch('services.extraction.main._validate_resume_path', return_value=(True, "/app/resume.pdf")):
            with patch('services.extraction.main.process_resume', return_value=(True, "abc123")):
                app.state.extraction = mock_state
                try:
                    client = TestClient(app)
                    response = client.post("/extract/resume", json={"resume_file": "/app/resume.pdf"})
                    
                    assert response.status_code == 200
                    data = response.json()
                    assert data["success"] is True
                    assert data["fingerprint"] == "abc123"
                finally:
                    del app.state.extraction

    def test_extract_resume_endpoint_invalid_path(self):
        """Test extract/resume endpoint with invalid path."""
        from fastapi.testclient import TestClient
        from services.extraction.main import app
        
        mock_state = Mock()
        
        with patch('services.extraction.main._validate_resume_path', return_value=(False, "Invalid path")):
            app.state.extraction = mock_state
            try:
                client = TestClient(app)
                response = client.post("/extract/resume", json={"resume_file": "/etc/passwd"})
                
                assert response.status_code == 200
                data = response.json()
                assert data["success"] is False
                assert "Invalid" in data["message"]
            finally:
                del app.state.extraction

    def test_stop_endpoint(self):
        """Test stop endpoint."""
        from fastapi.testclient import TestClient
        from services.extraction.main import app
        
        mock_state = Mock()
        mock_state.consumer_task = Mock()
        
        app.state.extraction = mock_state
        try:
            client = TestClient(app)
            response = client.post("/extract/stop")
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            mock_state.stop_event.set.assert_called_once()
        finally:
            del app.state.extraction


class TestProcessExtractionMessage:
    """Test _process_extraction_message function."""

    @pytest.mark.asyncio
    async def test_process_extraction_message_success(self):
        """Test successful extraction processing."""
        from services.extraction.main import _process_extraction_message, ExtractionState
        
        mock_ctx = Mock()
        state = ExtractionState(ctx=mock_ctx)
        msg = {"task_id": "task-123", "resume_file": "/app/resume.pdf"}

        with patch('services.extraction.main._validate_resume_path', return_value=(True, "/app/resume.pdf")), \
             patch('services.extraction.main.process_resume', return_value=(True, "fp-abc123")), \
             patch('services.extraction.main.publish_completion') as mock_publish, \
             patch('services.extraction.main.ack_message') as mock_ack:

            result = await _process_extraction_message(state, "msg-1", msg)

        assert result is True
        mock_publish.assert_called_once()
        mock_ack.assert_called_once()
        # Check completed status was published
        published_payload = mock_publish.call_args[0][1]
        assert published_payload["status"] == "completed"

    @pytest.mark.asyncio
    async def test_process_extraction_message_skipped(self):
        """Test extraction skipped when resume unchanged."""
        from services.extraction.main import _process_extraction_message, ExtractionState
        
        mock_ctx = Mock()
        state = ExtractionState(ctx=mock_ctx)
        msg = {"task_id": "task-123", "resume_file": "/app/resume.pdf"}

        with patch('services.extraction.main._validate_resume_path', return_value=(True, "/app/resume.pdf")), \
             patch('services.extraction.main.process_resume', return_value=(False, None)), \
             patch('services.extraction.main.publish_completion') as mock_publish, \
             patch('services.extraction.main.ack_message') as mock_ack:

            result = await _process_extraction_message(state, "msg-1", msg)

        assert result is True
        mock_publish.assert_called_once()
        # Check skipped status was published
        published_payload = mock_publish.call_args[0][1]
        assert published_payload["status"] == "skipped"

    @pytest.mark.asyncio
    async def test_process_extraction_message_invalid_path(self):
        """Test extraction fails with invalid path."""
        from services.extraction.main import _process_extraction_message, ExtractionState
        
        mock_ctx = Mock()
        state = ExtractionState(ctx=mock_ctx)
        msg = {"task_id": "task-456", "resume_file": "/etc/passwd"}

        with patch('services.extraction.main._validate_resume_path', return_value=(False, "Invalid path")), \
             patch('services.extraction.main.publish_completion') as mock_publish, \
             patch('services.extraction.main.ack_message') as mock_ack:

            result = await _process_extraction_message(state, "msg-2", msg)

        assert result is False
        mock_ack.assert_called_once()
        published_payload = mock_publish.call_args[0][1]
        assert published_payload["status"] == "failed"
        assert "Invalid" in published_payload["error"]

    @pytest.mark.asyncio
    async def test_process_extraction_message_failure(self):
        """Test extraction processing failure."""
        from services.extraction.main import _process_extraction_message, ExtractionState
        
        mock_ctx = Mock()
        state = ExtractionState(ctx=mock_ctx)
        msg = {"task_id": "task-789", "resume_file": "/app/resume.pdf"}

        with patch('services.extraction.main._validate_resume_path', return_value=(True, "/app/resume.pdf")), \
             patch('services.extraction.main.process_resume', side_effect=RuntimeError("parse failed")), \
             patch('services.extraction.main.publish_completion') as mock_publish, \
             patch('services.extraction.main.ack_message') as mock_ack:

            result = await _process_extraction_message(state, "msg-3", msg)

        assert result is False
        mock_ack.assert_called_once()
        published_payload = mock_publish.call_args[0][1]
        assert published_payload["status"] == "failed"
        assert "parse failed" in published_payload["error"]


class TestConsumeExtractionJobs:
    """Test consume_extraction_jobs consumer loop."""

    @pytest.mark.asyncio
    async def test_consumer_handles_empty_stream(self):
        """Test consumer handles empty stream (no messages)."""
        from services.extraction.main import consume_extraction_jobs, ExtractionState
        
        mock_ctx = Mock()
        state = ExtractionState(ctx=mock_ctx)
        
        with patch('services.extraction.main.read_stream', return_value=[]):
            task = asyncio.create_task(consume_extraction_jobs(state))
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    import threading
    pytest.main([__file__, "-v"])
