#!/usr/bin/env python3
"""
Unit Tests: Base Extraction Module

Tests the services/base/extraction.py module functions.

Usage:
    uv run pytest tests/unit/services/test_base_extraction.py -v
"""

import threading
from unittest.mock import Mock, patch, MagicMock


class TestFormatHttpError:
    """Test _format_http_error function."""

    def test_format_http_error_with_response(self):
        """Test formatting HTTP error with response."""
        from services.base.extraction import _format_http_error

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_error = MagicMock()
        mock_error.response = mock_response

        result = _format_http_error(mock_error)

        assert "HTTP 500" in result
        assert "Internal Server Error" in result

    def test_format_http_error_without_response(self):
        """Test formatting error without response."""
        from services.base.extraction import _format_http_error

        mock_error = MagicMock()
        mock_error.response = None

        result = _format_http_error(mock_error)

        assert result == "N/A"


class TestMarkJobFailed:
    """Test _mark_job_failed function."""

    def test_mark_job_failed_success(self):
        """Test marking job as failed."""
        from services.base.extraction import _mark_job_failed
        with patch('services.base.extraction.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            _mark_job_failed(123, "ValueError", "Test error")

            mock_repo.mark_extraction_retryable_failed.assert_called_once_with(
                123, "ValueError: Test error"
            )

    def test_mark_job_failed_handles_exception(self):
        """Test marking job as failed handles exception."""
        from services.base.extraction import _mark_job_failed
        with patch('services.base.extraction.job_uow') as mock_job_uow:
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(side_effect=Exception("DB error"))
            mock_job_uow.return_value = mock_context

            _mark_job_failed(123, "ValueError", "Test error")


class TestOnExtractionError:
    """Test _on_extraction_error function."""

    def test_on_extraction_error_last_attempt(self):
        """Test on extraction error on last attempt."""
        from services.base.extraction import _on_extraction_error

        mock_error = MagicMock()
        mock_error.response = None
        mock_error.__class__.__name__ = "ValueError"
        mock_error.__str__ = Mock(return_value="Test error")
        stop_event = threading.Event()

        result = _on_extraction_error(
            mock_error,
            job_id=123,
            job_title="Test Job",
            attempt=2,
            retry_intervals=[30, 60, 120],
            wait_time=120,
            stop_event=stop_event
        )

        assert result is True

    def test_on_extraction_error_retry(self):
        """Test on extraction error during retry."""
        from services.base.extraction import _on_extraction_error

        mock_error = MagicMock()
        mock_error.response = None
        mock_error.__class__.__name__ = "ValueError"
        mock_error.__str__ = Mock(return_value="Test error")
        stop_event = threading.Event()

        # Mock wait() to return immediately without actually waiting 30 seconds
        with patch.object(stop_event, 'wait', return_value=False):
            result = _on_extraction_error(
                mock_error,
                job_id=123,
                job_title="Test Job",
                attempt=0,
                retry_intervals=[30, 60, 120],
                wait_time=30,
                stop_event=stop_event
            )

        assert result is False


class TestExtractSingleJob:
    """Test _extract_single_job function."""

    def test_extract_single_job_success(self):
        """Test extracting single job successfully."""
        from services.base.extraction import _extract_single_job
        with patch('services.base.extraction.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_job = MagicMock()
            mock_job.id = 123
            mock_job.title = "Test Job"
            mock_repo.get_by_id.return_value = mock_job
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = MagicMock()
            mock_ctx.job_etl_service.extract_one = Mock()
            stop_event = threading.Event()

            result = _extract_single_job(mock_ctx, 123, [30, 60], stop_event)

            assert result is True

    def test_extract_single_job_not_found(self):
        """Test extracting single job that doesn't exist."""
        from services.base.extraction import _extract_single_job
        with patch('services.base.extraction.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_repo.get_by_id.return_value = None
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = MagicMock()
            stop_event = threading.Event()

            result = _extract_single_job(mock_ctx, 123, [30, 60], stop_event)

            assert result is False

    def test_extract_single_job_stops_on_event(self):
        """Test extracting single job stops on event."""
        from services.base.extraction import _extract_single_job
        with patch('services.base.extraction.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_job = MagicMock()
            mock_job.id = 123
            mock_job.title = "Test Job"
            mock_repo.get_by_id.return_value = mock_job
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = MagicMock()
            stop_event = threading.Event()
            stop_event.set()

            result = _extract_single_job(mock_ctx, 123, [30, 60], stop_event)

            assert result is False


class TestRunExtractionBatch:
    """Test _run_extraction_batch function."""

    def test_run_extraction_batch_returns_count(self):
        """Test extraction batch returns count."""
        from services.base.extraction import _run_extraction_batch
        with patch('services.base.extraction.job_uow') as mock_job_uow:
            with patch('services.base.extraction._extract_single_job') as mock_extract:
                mock_repo = MagicMock()
                mock_job = MagicMock()
                mock_job.id = 123
                mock_repo.get_unextracted_jobs.return_value = [mock_job]
                
                mock_context = MagicMock()
                mock_context.__enter__ = Mock(return_value=mock_repo)
                mock_context.__exit__ = Mock(return_value=False)
                mock_job_uow.return_value = mock_context

                mock_extract.return_value = True
                mock_ctx = MagicMock()
                stop_event = threading.Event()

                result = _run_extraction_batch(mock_ctx, stop_event, limit=10)

                assert result == 1


class TestRunJobExtraction:
    """Test run_job_extraction function."""

    def test_run_job_extraction(self):
        """Test job extraction returns extraction batch count."""
        from services.base.extraction import run_job_extraction
        with patch('services.base.extraction._run_extraction_batch') as mock_extraction:
            mock_extraction.return_value = 10

            mock_ctx = MagicMock()
            stop_event = threading.Event()

            result = run_job_extraction(mock_ctx, stop_event, limit=200)

            assert result == 10


class TestRunResumeExtraction:
    """Test run_resume_extraction function."""

    def test_run_resume_extraction_success(self):
        """Test resume extraction success."""
        from services.base.extraction import run_resume_extraction
        from unittest.mock import mock_open
        with patch('services.base.extraction.generate_file_fingerprint') as mock_fingerprint:
            with patch('builtins.open', mock_open(read_data=b'test content')):
                with patch('services.base.extraction.load_resume_with_parser') as mock_load:
                    mock_load.return_value = {"name": "Test"}
                    mock_fingerprint.return_value = "abc123"

                    result = run_resume_extraction(Mock(), "/path/to/resume.json")

                    assert result[0] == {"name": "Test"}
                    assert result[1] == "abc123"

    def test_run_resume_extraction_with_known_fingerprint(self):
        """Test resume extraction with pre-computed fingerprint (skips file read)."""
        from services.base.extraction import run_resume_extraction
        with patch('services.base.extraction.load_resume_with_parser') as mock_load:
            mock_load.return_value = {"name": "Test"}

            result = run_resume_extraction(Mock(), "/path/to/resume.json", known_fingerprint="precomputed-fp")

            assert result[0] == {"name": "Test"}
            assert result[1] == "precomputed-fp"

    def test_run_resume_extraction_file_not_found(self):
        """Test resume extraction file not found."""
        from services.base.extraction import run_resume_extraction
        with patch('services.base.extraction.load_resume_with_parser') as mock_load:
            mock_load.side_effect = FileNotFoundError("File not found")

            result = run_resume_extraction(Mock(), "/nonexistent/resume.json")

            assert result == (None, "")

    def test_run_resume_extraction_parsing_error(self):
        """Test resume extraction parsing error."""
        from services.base.extraction import run_resume_extraction
        from unittest.mock import mock_open
        with patch('services.base.extraction.generate_file_fingerprint') as mock_fingerprint:
            with patch('builtins.open', mock_open(read_data=b'test content')):
                with patch('services.base.extraction.load_resume_with_parser') as mock_load:
                    mock_fingerprint.return_value = "abc123"
                    mock_load.side_effect = ValueError("Parse error")

                    result = run_resume_extraction(Mock(), "/path/to/resume.json")

                    assert result == (None, "abc123")


class TestExtractResume:
    """Test extract_resume function."""

    def test_extract_resume_success(self):
        """Test extract resume returns True."""
        from services.base.extraction import extract_resume
        with patch('services.base.extraction.job_uow') as mock_job_uow:
            mock_ctx = MagicMock()
            mock_ctx.job_etl_service.extract_resume_stage = Mock(return_value=(True, "fingerprint123", {}))
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=MagicMock())
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            result = extract_resume(mock_ctx, "/path/to/resume.json")

            assert result == (True, "fingerprint123")
            mock_ctx.job_etl_service.extract_resume_stage.assert_called_once()

    def test_extract_resume_failure(self):
        """Test extract resume returns False."""
        from services.base.extraction import extract_resume
        with patch('services.base.extraction.job_uow') as mock_job_uow:
            mock_ctx = MagicMock()
            mock_ctx.job_etl_service.extract_resume_stage = Mock(return_value=(False, "", None))
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=MagicMock())
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            result = extract_resume(mock_ctx, "/path/to/resume.json")

            assert result == (False, "")
            mock_ctx.job_etl_service.extract_resume_stage.assert_called_once()


class TestRunExtractionBatchAdditional:
    """Additional branch coverage for _run_extraction_batch."""

    def test_no_jobs_returns_zero(self):
        """When no unextracted jobs exist, returns 0."""
        from services.base.extraction import _run_extraction_batch
        with patch('services.base.extraction.job_uow') as mock_uow:
            mock_repo = MagicMock()
            mock_repo.get_unextracted_jobs.return_value = []
            mock_uow.return_value.__enter__ = Mock(return_value=mock_repo)
            mock_uow.return_value.__exit__ = Mock(return_value=False)

            result = _run_extraction_batch(Mock(), threading.Event(), limit=10)

            assert result == 0

    def test_stop_event_breaks_mid_loop(self):
        """When stop_event is set before processing, the loop exits early."""
        from services.base.extraction import _run_extraction_batch
        with patch('services.base.extraction.job_uow') as mock_uow:
            with patch('services.base.extraction._extract_single_job', return_value=True) as mock_extract:
                mock_repo = MagicMock()
                job1, job2 = MagicMock(id=1), MagicMock(id=2)
                mock_repo.get_unextracted_jobs.return_value = [job1, job2]
                mock_uow.return_value.__enter__ = Mock(return_value=mock_repo)
                mock_uow.return_value.__exit__ = Mock(return_value=False)

                stop_event = threading.Event()
                stop_event.set()  # set before starting

                result = _run_extraction_batch(Mock(), stop_event, limit=10)

                assert result == 0
                mock_extract.assert_not_called()


class TestExtractSingleJobAdditional:
    """Additional branch coverage for _extract_single_job."""

    def test_stop_event_set_before_attempt(self):
        """When stop_event is set, the retry loop never runs."""
        from services.base.extraction import _extract_single_job
        stop = threading.Event()
        stop.set()

        result = _extract_single_job(Mock(), 42, [30, 60], stop)

        assert result is False


class TestRunResumeExtractionAdditional:
    """Additional branch coverage for run_resume_extraction."""

    def test_parser_returns_none_returns_none_with_fp(self):
        """When load_resume_with_parser returns None, returns (None, fingerprint)."""
        from services.base.extraction import run_resume_extraction
        from unittest.mock import mock_open
        with patch('services.base.extraction.generate_file_fingerprint', return_value="fp-123"):
            with patch('builtins.open', mock_open(read_data=b'bytes')):
                with patch('services.base.extraction.load_resume_with_parser', return_value=None):
                    result = run_resume_extraction(Mock(), "/resume.pdf")

        assert result == (None, "fp-123")

    def test_ioerror_reading_file_returns_empty_fp(self):
        """IOError when opening the file returns (None, '')."""
        from services.base.extraction import run_resume_extraction
        with patch('builtins.open', side_effect=IOError("Permission denied")):
            result = run_resume_extraction(Mock(), "/resume.pdf")

        assert result == (None, "")
