#!/usr/bin/env python3
"""
Unit Tests: Extraction Service

Tests the extraction service functionality without requiring
running services. Tests path validation and state management.

Usage:
    uv run pytest tests/unit/services/test_extraction.py -v
"""

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


if __name__ == "__main__":
    import threading
    pytest.main([__file__, "-v"])
