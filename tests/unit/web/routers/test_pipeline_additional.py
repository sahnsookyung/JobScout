#!/usr/bin/env python3
"""
Additional tests for Pipeline Router - helper functions.
Covers: web/backend/routers/pipeline.py (helper functions)
"""

import pytest
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
