#!/usr/bin/env python3
"""
Additional tests for Pipeline Router - helper functions.
Covers: web/backend/routers/pipeline.py (helper functions)
"""

import pytest
from fastapi import HTTPException
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from web.backend.routers.pipeline import _validate_task_id, _sanitize_for_logging


class TestValidateTaskId:
    """Test _validate_task_id function for input validation."""

    def test_valid_alphanumeric_task_id(self):
        """Test valid alphanumeric task ID."""
        assert _validate_task_id("match-a1b2c3d4") is True
        assert _validate_task_id("task123") is True
        assert _validate_task_id("abc123xyz") is True

    def test_valid_task_id_with_hyphens(self):
        """Test valid task ID with hyphens."""
        assert _validate_task_id("match-task-123") is True
        assert _validate_task_id("test-id-456") is True
        assert _validate_task_id("a-b-c") is True

    def test_valid_max_length_task_id(self):
        """Test valid task ID at max length (50 chars)."""
        max_length_id = "a" * 50
        assert _validate_task_id(max_length_id) is True

    def test_valid_short_task_id(self):
        """Test valid short task ID."""
        assert _validate_task_id("a") is True
        assert _validate_task_id("ab") is True

    def test_invalid_empty_task_id(self):
        """Test empty task ID is invalid."""
        assert _validate_task_id("") is False

    def test_invalid_none_task_id(self):
        """Test None task ID is invalid."""
        assert _validate_task_id(None) is False

    def test_invalid_non_string_task_id(self):
        """Test non-string task ID is invalid."""
        assert _validate_task_id(123) is False
        assert _validate_task_id({"key": "value"}) is False
        assert _validate_task_id(["task"]) is False

    def test_invalid_too_long_task_id(self):
        """Test task ID exceeding max length (50 chars)."""
        too_long_id = "a" * 51
        assert _validate_task_id(too_long_id) is False

        very_long_id = "a" * 100
        assert _validate_task_id(very_long_id) is False

    def test_invalid_special_characters_task_id(self):
        """Test task ID with special characters is invalid."""
        assert _validate_task_id("task@123") is False
        assert _validate_task_id("task#123") is False
        assert _validate_task_id("task$123") is False
        assert _validate_task_id("task%123") is False

    def test_invalid_path_traversal_task_id(self):
        """Test path traversal attempts are invalid."""
        assert _validate_task_id("../etc/passwd") is False
        assert _validate_task_id("..\\..\\windows") is False
        assert _validate_task_id("/etc/passwd") is False
        assert _validate_task_id("C:\\Windows") is False

    def test_invalid_log_injection_task_id(self):
        """Test log injection attempts are invalid."""
        assert _validate_task_id("task\ninjection") is False
        assert _validate_task_id("task\rinjection") is False
        assert _validate_task_id("task\r\ninjection") is False

    def test_invalid_url_manipulation_task_id(self):
        """Test URL manipulation attempts are invalid."""
        assert _validate_task_id("task?id=123") is False
        assert _validate_task_id("task&param=value") is False
        assert _validate_task_id("task;drop") is False


class TestSanitizeForLogging:
    """Test _sanitize_for_logging function for log injection prevention."""

    def test_normal_string_unchanged(self):
        """Test normal strings pass through unchanged."""
        assert _sanitize_for_logging("normal text") == "normal text"
        assert _sanitize_for_logging("task-123") == "task-123"

    def test_removes_carriage_return(self):
        """Test CR characters are removed."""
        assert _sanitize_for_logging("task\rinjection") == "taskinjection"
        # Function removes both CR and LF, so "test\r\n" becomes "test"
        assert _sanitize_for_logging("test\r\n") == "test"

    def test_removes_line_feed(self):
        """Test LF characters are removed."""
        assert _sanitize_for_logging("task\ninjection") == "taskinjection"
        assert _sanitize_for_logging("line1\nline2") == "line1line2"

    def test_removes_null_bytes(self):
        """Test null bytes are removed."""
        assert _sanitize_for_logging("task\x00injection") == "taskinjection"
        assert _sanitize_for_logging("test\x00") == "test"

    def test_removes_all_control_characters(self):
        """Test all CRLF and null bytes are removed."""
        malicious = "task\r\n\x00injection"
        assert _sanitize_for_logging(malicious) == "taskinjection"

    def test_handles_non_string_input(self):
        """Test non-string input is converted to string."""
        assert _sanitize_for_logging(123) == "123"
        assert _sanitize_for_logging(None) == "None"
        assert _sanitize_for_logging(True) == "True"

    def test_empty_string_unchanged(self):
        """Test empty string passes through."""
        assert _sanitize_for_logging("") == ""

    def test_multiple_control_characters(self):
        """Test multiple control characters are all removed."""
        malicious = "line1\r\nline2\r\nline3\x00end"
        assert _sanitize_for_logging(malicious) == "line1line2line3end"

    def test_log_forging_prevention(self):
        """Test log forging attempts are neutralized."""
        # CWE-117: Improper Output Neutralization for Logs
        forged = "12345\r\nINFO - Fake log entry"
        sanitized = _sanitize_for_logging(forged)
        assert "\r" not in sanitized
        assert "\n" not in sanitized
        assert "Fake log entry" in sanitized


class TestResumeGuards:
    def test_guard_resume_not_uploading_raises_for_active_upload(self):
        from web.backend.routers.pipeline import _guard_resume_not_uploading

        redis = MagicMock()
        redis.get.return_value = b"resume-task-1"

        with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "running"}), \
             patch("web.backend.routers.pipeline._latest_resume_upload_uses_task", return_value=True):
            with pytest.raises(HTTPException) as exc_info:
                _guard_resume_not_uploading(redis, "owner-1")

        assert exc_info.value.status_code == 409

    def test_guard_resume_not_uploading_ignores_redis_errors(self):
        from web.backend.routers.pipeline import _guard_resume_not_uploading

        redis = MagicMock()
        redis.get.side_effect = RuntimeError("redis down")

        _guard_resume_not_uploading(redis, "owner-1")

    def test_latest_resume_upload_uses_task_returns_false_when_missing(self):
        from web.backend.routers.pipeline import _latest_resume_upload_uses_task

        repo = MagicMock()
        repo.get_latest_resume_upload.return_value = None
        mock_uow = MagicMock()
        mock_uow.__enter__.return_value = repo
        mock_uow.__exit__.return_value = False

        with patch("web.backend.routers.pipeline.job_uow", return_value=mock_uow):
            assert _latest_resume_upload_uses_task("owner-1", "task-1") is False

    def test_latest_resume_upload_uses_task_returns_true_when_repo_errors(self):
        from web.backend.routers.pipeline import _latest_resume_upload_uses_task

        with patch("web.backend.routers.pipeline.job_uow", side_effect=RuntimeError("db down")):
            assert _latest_resume_upload_uses_task("owner-1", "task-1") is True


class TestMatchingTaskHelpers:
    def test_classify_failed_resume_upload_marks_retryable_when_structured_resume_exists(self):
        from web.backend.routers.pipeline import _classify_failed_resume_upload

        repo = MagicMock()
        repo.get_resume_processing_state.return_value = SimpleNamespace(
            user_safe_message="Retry me",
            last_error="boom",
        )
        repo.get_structured_resume_by_fingerprint.return_value = object()

        status, message, retryable = _classify_failed_resume_upload(repo, "fp-1")

        assert status == "failed_retryable"
        assert message == "Retry me"
        assert retryable is True

    def test_classify_failed_resume_upload_marks_reupload_when_no_structured_resume(self):
        from web.backend.routers.pipeline import _classify_failed_resume_upload

        repo = MagicMock()
        repo.get_resume_processing_state.return_value = SimpleNamespace(
            user_safe_message=None,
            last_error="Need reupload",
        )
        repo.get_structured_resume_by_fingerprint.return_value = None

        status, message, retryable = _classify_failed_resume_upload(repo, "fp-1")

        assert status == "failed_reupload_required"
        assert message == "Need reupload"
        assert retryable is False

    def test_ensure_no_active_matching_task_raises_for_running_state(self):
        from web.backend.routers.pipeline import _ensure_no_active_matching_task

        redis = MagicMock()
        redis.get.return_value = b"match-task-1"

        with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "persisting"}):
            with pytest.raises(HTTPException) as exc_info:
                _ensure_no_active_matching_task(redis, "owner-1")

        assert exc_info.value.status_code == 409

    def test_ensure_no_active_matching_task_ignores_redis_failure(self):
        from web.backend.routers.pipeline import _ensure_no_active_matching_task

        redis = MagicMock()
        redis.get.side_effect = RuntimeError("redis down")

        _ensure_no_active_matching_task(redis, "owner-1")

    def test_enqueue_matching_job_or_500_cleans_up_active_marker(self):
        from web.backend.routers.pipeline import _enqueue_matching_job_or_500

        redis = MagicMock()
        redis.get.return_value = b"match-task-1"

        with patch("web.backend.routers.pipeline.enqueue_job", side_effect=RuntimeError("stream down")), \
             patch("web.backend.routers.pipeline.clear_task_cancellation_requested") as mock_clear_cancel, \
             patch("web.backend.routers.pipeline.set_task_state") as mock_set_state:
            with pytest.raises(HTTPException) as exc_info:
                _enqueue_matching_job_or_500(
                    "match-task-1",
                    "fp-1",
                    "upload-1",
                    "owner-1",
                    redis=redis,
                )

        assert exc_info.value.status_code == 500
        mock_clear_cancel.assert_called_once_with("match-task-1")
        mock_set_state.assert_called_once()
        redis.delete.assert_called_once()


class TestResumeEtlHelpers:
    def test_wait_for_resume_etl_final_state_returns_terminal_state(self):
        from web.backend.routers.pipeline import _wait_for_resume_etl_final_state

        states = [
            {"status": "running"},
            {"status": "completed", "step": "embedding"},
        ]

        class FakeTime:
            def __init__(self):
                self.now = 0

            def time(self):
                self.now += 1
                return self.now

            def sleep(self, _seconds):
                return None

        with patch("web.backend.routers.pipeline.get_task_state", side_effect=states):
            result = _wait_for_resume_etl_final_state("task-1", FakeTime())

        assert result["status"] == "completed"

    def test_wait_for_resume_etl_final_state_times_out(self):
        from web.backend.routers.pipeline import _wait_for_resume_etl_final_state

        class FakeTime:
            def __init__(self):
                self.now = 0

            def time(self):
                self.now += 601
                return self.now

            def sleep(self, _seconds):
                return None

        with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "running"}):
            with pytest.raises(RuntimeError, match="timed out"):
                _wait_for_resume_etl_final_state("task-timeout", FakeTime())

    def test_write_resume_failure_state_updates_repo_and_redis(self):
        from web.backend.routers.pipeline import _write_resume_failure_state

        repo = MagicMock()
        mock_uow = MagicMock()
        mock_uow.__enter__.return_value = repo
        mock_uow.__exit__.return_value = False

        with patch("web.backend.routers.pipeline.job_uow", return_value=mock_uow), \
             patch("web.backend.routers.pipeline._classify_failed_resume_upload", return_value=("failed_retryable", "Retry later", True)), \
             patch("web.backend.routers.pipeline.set_task_state") as mock_set_state:
            _write_resume_failure_state(
                "task-1",
                "upload-1",
                "hash-1",
                "fp-1",
                "owner-1",
                RuntimeError("boom"),
            )

        repo.update_resume_upload.assert_called_once()
        mock_set_state.assert_called_once()
