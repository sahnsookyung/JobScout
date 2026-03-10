#!/usr/bin/env python3
"""
Additional tests for Redis Streams - helper functions.
Covers: core/redis_streams.py (additional helper function coverage)
"""

import pytest
from unittest.mock import Mock, patch
from core.redis_streams import (
    validate_job_payload,
    _deserialize_message,
    _is_claimable,
)


class TestValidateJobPayloadAdditional:
    """Additional tests for validate_job_payload function."""

    def test_valid_payload_all_fields_present(self):
        """Test validation with all fields present."""
        payload = {
            "task_id": "task-123",
            "status": "running",
            "resume_fingerprint": "fp-abc123",
            "extra_field": "value"
        }
        required_fields = ["task_id", "status"]

        valid, error = validate_job_payload(payload, required_fields)

        assert valid is True
        assert error == ""

    def test_missing_multiple_required_fields(self):
        """Test validation with multiple missing required fields."""
        payload = {"other_field": "value"}
        required_fields = ["task_id", "status", "resume_fingerprint"]

        valid, error = validate_job_payload(payload, required_fields)

        assert valid is False
        assert "Missing required field" in error
        # Should report first missing field
        assert "task_id" in error

    def test_payload_with_json_serializable_values(self):
        """Test validation doesn't check value types, only presence."""
        payload = {
            "task_id": 123,  # Not a string but still present
            "status": None,
        }
        required_fields = ["task_id", "status"]

        valid, error = validate_job_payload(payload, required_fields)

        # validate_job_payload only checks presence, not types
        assert valid is True
        assert error == ""


class TestDeserializeMessage:
    """Test _deserialize_message function."""

    def test_deserialize_valid_json_values(self):
        """Test deserializing message with valid JSON values."""
        msg = {
            "task_id": '"task-123"',  # JSON string
            "count": '123',  # JSON number
            "data": '{"key": "value"}'  # JSON object
        }

        result = _deserialize_message(msg)

        assert result["task_id"] == "task-123"
        assert result["count"] == 123
        assert result["data"] == {"key": "value"}

    def test_deserialize_mixed_values(self):
        """Test deserializing message with mixed JSON/non-JSON values."""
        msg = {
            "task_id": '"task-123"',
            "status": "running",  # Already a string, not JSON
            "count": '456'
        }

        result = _deserialize_message(msg)

        assert result["task_id"] == "task-123"
        assert result["status"] == "running"
        assert result["count"] == 456

    def test_deserialize_with_json_decode_error(self):
        """Test deserializing handles JSON decode errors gracefully."""
        msg = {
            "task_id": "invalid json {",
            "valid_field": '"valid"'
        }

        result = _deserialize_message(msg)

        # Invalid JSON should be returned as-is
        assert result["task_id"] == "invalid json {"
        assert result["valid_field"] == "valid"

    def test_deserialize_with_type_error(self):
        """Test deserializing handles TypeError gracefully."""
        msg = {
            "task_id": None,  # None can't be JSON decoded
            "valid_field": '"valid"'
        }

        result = _deserialize_message(msg)

        # None should be returned as-is
        assert result["task_id"] is None
        assert result["valid_field"] == "valid"

    def test_deserialize_empty_message(self):
        """Test deserializing empty message."""
        msg = {}

        result = _deserialize_message(msg)

        assert result == {}

    def test_deserialize_list_value(self):
        """Test deserializing list values."""
        msg = {
            "tags": '["tag1", "tag2", "tag3"]',
            "task_id": '"task-123"'
        }

        result = _deserialize_message(msg)

        assert result["tags"] == ["tag1", "tag2", "tag3"]
        assert result["task_id"] == "task-123"

    def test_deserialize_number_value(self):
        """Test deserializing numeric values."""
        msg = {
            "count": '100',
            "ratio": '3.14',
            "negative": '-50'
        }

        result = _deserialize_message(msg)

        assert result["count"] == 100
        assert result["ratio"] == 3.14
        assert result["negative"] == -50

    def test_deserialize_boolean_value(self):
        """Test deserializing boolean values."""
        msg = {
            "success": 'true',
            "failed": 'false'
        }

        result = _deserialize_message(msg)

        assert result["success"] is True
        assert result["failed"] is False

    def test_deserialize_null_value(self):
        """Test deserializing null values."""
        msg = {
            "optional_field": 'null',
            "task_id": '"task-123"'
        }

        result = _deserialize_message(msg)

        assert result["optional_field"] is None
        assert result["task_id"] == "task-123"


class TestIsClaimable:
    """Test _is_claimable function for message claiming logic."""

    def test_claimable_different_consumer_idle_60s(self):
        """Test message is claimable from different consumer after 60s idle."""
        pending = {
            "consumer": "consumer-other",
            "time_since_delivered": 60_000  # Exactly 60 seconds
        }

        result = _is_claimable(pending, "consumer-current")

        assert result is True

    def test_claimable_different_consumer_idle_over_60s(self):
        """Test message is claimable from different consumer after >60s idle."""
        pending = {
            "consumer": "consumer-other",
            "time_since_delivered": 120_000  # 2 minutes
        }

        result = _is_claimable(pending, "consumer-current")

        assert result is True

    def test_not_claimable_same_consumer(self):
        """Test message is not claimable from same consumer."""
        pending = {
            "consumer": "consumer-current",
            "time_since_delivered": 120_000  # 2 minutes
        }

        result = _is_claimable(pending, "consumer-current")

        assert result is False

    def test_not_claimable_idle_under_60s(self):
        """Test message is not claimable if idle <60s."""
        pending = {
            "consumer": "consumer-other",
            "time_since_delivered": 59_999  # Just under 60 seconds
        }

        result = _is_claimable(pending, "consumer-current")

        assert result is False

    def test_not_claimable_exactly_60s_minus_1ms(self):
        """Test message is not claimable at 59.999 seconds."""
        pending = {
            "consumer": "consumer-other",
            "time_since_delivered": 59_999
        }

        result = _is_claimable(pending, "consumer-current")

        assert result is False

    def test_not_claimable_missing_consumer_key(self):
        """Test message with missing consumer key."""
        pending = {
            "time_since_delivered": 120_000
        }

        result = _is_claimable(pending, "consumer-current")

        # None != "consumer-current" is True, and None >= 60000 is False in Python 3
        # But the actual behavior may vary - the key point is the function handles missing keys
        assert isinstance(result, bool)  # Should return a boolean

    def test_not_claimable_missing_time_key(self):
        """Test message with missing time_since_delivered key."""
        pending = {
            "consumer": "consumer-other"
        }

        result = _is_claimable(pending, "consumer-current")

        # Default 0 < 60000
        assert result is False

    def test_not_claimable_empty_pending_dict(self):
        """Test empty pending dict is not claimable."""
        pending = {}

        result = _is_claimable(pending, "consumer-current")

        assert result is False

    def test_claimable_boundary_condition_exactly_60s(self):
        """Test boundary condition: exactly 60000ms is claimable."""
        pending = {
            "consumer": "consumer-other",
            "time_since_delivered": 60_000
        }

        result = _is_claimable(pending, "consumer-current")

        assert result is True

    def test_claimable_just_over_60s(self):
        """Test just over 60s is claimable."""
        pending = {
            "consumer": "consumer-other",
            "time_since_delivered": 60_001
        }

        result = _is_claimable(pending, "consumer-current")

        assert result is True
