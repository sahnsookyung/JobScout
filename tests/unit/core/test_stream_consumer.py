"""
Unit Tests: Core Stream Consumer

Tests the reusable stream consumer functionality.

Usage:
    uv run pytest tests/unit/core/test_stream_consumer.py -v
"""

import asyncio
import logging
import pytest
import threading
from unittest.mock import Mock, patch, AsyncMock, MagicMock

from core.stream_consumer import validate_message, StreamConsumer, StreamConsumerWithCompletion


# ---------------------------------------------------------------------------
# validate_message
# ---------------------------------------------------------------------------

class TestValidateMessage:
    """Test validate_message helper function."""

    def test_valid_message(self):
        """Valid message with all required fields."""
        msg = {"task_id": "t-1", "resume_file": "/app/r.pdf"}
        is_valid, error = validate_message(msg, ["task_id", "resume_file"])
        assert is_valid is True
        assert error == ""

    def test_missing_field(self):
        """Missing required field returns error."""
        msg = {"task_id": "t-1"}
        is_valid, error = validate_message(msg, ["task_id", "resume_file"])
        assert is_valid is False
        assert "resume_file" in error

    def test_empty_required_fields(self):
        """Empty required fields list always valid."""
        msg = {"task_id": "t-1"}
        is_valid, error = validate_message(msg, [])
        assert is_valid is True
        assert error == ""

    def test_extra_fields_ignored(self):
        """Extra fields don't affect validation."""
        msg = {"task_id": "t-1", "resume_file": "/app/r.pdf", "extra": "ignored"}
        is_valid, error = validate_message(msg, ["task_id"])
        assert is_valid is True
        assert error == ""


# ---------------------------------------------------------------------------
# StreamConsumer
# ---------------------------------------------------------------------------

class TestStreamConsumer:
    """Test StreamConsumer base class."""

    def test_initialization(self):
        """Consumer initializes with correct attributes."""
        mock_logger = Mock()
        consumer = StreamConsumer(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            logger=mock_logger,
            block_ms=3000,
            error_backoff_seconds=0.5,
        )

        assert consumer.stream == "test:stream"
        assert consumer.group == "test-group"
        assert consumer.consumer_name == "test-1"
        assert consumer.logger is mock_logger
        assert consumer.block_ms == 3000
        assert consumer.error_backoff_seconds == 0.5
        assert consumer.message_count == 0
        assert consumer.error_count == 0

    def test_get_one_message_returns_tuple(self):
        """get_one_message returns (msg_id, msg) tuple."""
        mock_logger = Mock()
        consumer = StreamConsumer(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            logger=mock_logger,
        )

        mock_msg = ("msg-1", {"task_id": "t-1"})
        with patch("core.stream_consumer.read_stream", return_value=iter([mock_msg])):
            result = consumer.get_one_message()
            assert result == mock_msg

    def test_get_one_message_returns_none_on_empty(self):
        """get_one_message returns None when stream is empty."""
        mock_logger = Mock()
        consumer = StreamConsumer(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            logger=mock_logger,
        )

        with patch("core.stream_consumer.read_stream", return_value=iter([])):
            result = consumer.get_one_message()
            assert result is None

    def test_get_one_message_logs_error_on_exception(self):
        """get_one_message logs error and returns None on exception."""
        mock_logger = Mock()
        consumer = StreamConsumer(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            logger=mock_logger,
        )

        def mock_gen(*args, **kwargs):
            raise Exception("Redis error")
            yield  # Make it a generator

        with patch("core.stream_consumer.read_stream", side_effect=mock_gen):
            result = consumer.get_one_message()
            assert result is None
            mock_logger.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message_not_implemented(self):
        """_process_message raises NotImplementedError in base class."""
        mock_logger = Mock()
        consumer = StreamConsumer(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            logger=mock_logger,
        )

        with pytest.raises(NotImplementedError):
            await consumer._process_message("msg-1", {"task_id": "t-1"})

    @pytest.mark.asyncio
    async def test_consume_loop_processes_messages(self):
        """consume_loop calls _process_message for each message."""
        mock_logger = Mock()
        consumer = StreamConsumer(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            logger=mock_logger,
        )

        call_count = [0]

        def mock_get():
            call_count[0] += 1
            if call_count[0] == 1:
                return ("msg-1", {"task_id": "t-1"})
            raise asyncio.CancelledError()

        with patch.object(consumer, "get_one_message", side_effect=mock_get), \
             patch.object(consumer, "_process_message", AsyncMock(return_value=True)) as mock_process:

            stop_event = threading.Event()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(consumer.consume_loop(stop_event), timeout=2.0)

        mock_process.assert_awaited_once_with("msg-1", {"task_id": "t-1"})
        assert consumer.message_count == 1

    @pytest.mark.asyncio
    async def test_consume_loop_tracks_errors(self):
        """consume_loop increments error_count on processing failure."""
        mock_logger = Mock()
        consumer = StreamConsumer(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            logger=mock_logger,
        )

        call_count = [0]

        def mock_get():
            call_count[0] += 1
            if call_count[0] <= 2:
                return (f"msg-{call_count[0]}", {"task_id": f"t-{call_count[0]}"})
            raise asyncio.CancelledError()

        with patch.object(consumer, "get_one_message", side_effect=mock_get), \
             patch.object(consumer, "_process_message", AsyncMock(return_value=False)):

            stop_event = threading.Event()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(consumer.consume_loop(stop_event), timeout=2.0)

        assert consumer.message_count == 2
        assert consumer.error_count == 2

    @pytest.mark.asyncio
    async def test_consume_loop_backoff_on_error(self):
        """consume_loop sleeps on error before retrying."""
        mock_logger = Mock()
        consumer = StreamConsumer(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            logger=mock_logger,
            error_backoff_seconds=0.01,
        )

        call_count = [0]

        def mock_get():
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("Loop error")
            raise asyncio.CancelledError()

        with patch.object(consumer, "get_one_message", side_effect=mock_get), \
             patch("asyncio.sleep", AsyncMock()) as mock_sleep:

            stop_event = threading.Event()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(consumer.consume_loop(stop_event), timeout=2.0)

        mock_sleep.assert_called_once()
        assert consumer.error_count == 1

    @pytest.mark.asyncio
    async def test_consume_loop_stops_on_stop_event(self):
        """consume_loop exits when stop_event is set."""
        mock_logger = Mock()
        consumer = StreamConsumer(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            logger=mock_logger,
        )

        def mock_get():
            return None  # Keep looping until stop_event

        with patch.object(consumer, "get_one_message", side_effect=mock_get):
            stop_event = threading.Event()
            stop_event.set()  # Set immediately
            await asyncio.wait_for(consumer.consume_loop(stop_event), timeout=2.0)

        # Should exit without processing anything
        assert consumer.message_count == 0


# ---------------------------------------------------------------------------
# StreamConsumerWithCompletion
# ---------------------------------------------------------------------------

class TestStreamConsumerWithCompletion:
    """Test StreamConsumerWithCompletion class."""

    def test_initialization(self):
        """Consumer initializes with completion channel."""
        mock_logger = Mock()
        consumer = StreamConsumerWithCompletion(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            completion_channel="test:completed",
            logger=mock_logger,
        )

        assert consumer.completion_channel == "test:completed"

    @pytest.mark.asyncio
    async def test_do_process_not_implemented(self):
        """_do_process raises NotImplementedError in base class."""
        mock_logger = Mock()
        consumer = StreamConsumerWithCompletion(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            completion_channel="test:completed",
            logger=mock_logger,
        )

        with pytest.raises(NotImplementedError):
            await consumer._do_process("msg-1", {"task_id": "t-1"})

    @pytest.mark.asyncio
    async def test_process_message_success(self):
        """_process_message publishes completion and acks on success."""
        mock_logger = Mock()
        consumer = StreamConsumerWithCompletion(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            completion_channel="test:completed",
            logger=mock_logger,
        )

        async def mock_do_process(msg_id, msg):
            return True, {"status": "completed", "data": "result"}

        with patch.object(consumer, "_do_process", side_effect=mock_do_process), \
             patch("core.stream_consumer.publish_completion") as mock_pub, \
             patch("core.stream_consumer.ack_message") as mock_ack:

            result = await consumer._process_message("msg-1", {"task_id": "t-1"})

        assert result is True
        mock_pub.assert_called_once()
        assert mock_pub.call_args[0][1]["status"] == "completed"
        mock_ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message_failure(self):
        """_process_message publishes failed status and acks on failure."""
        mock_logger = Mock()
        consumer = StreamConsumerWithCompletion(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            completion_channel="test:completed",
            logger=mock_logger,
        )

        async def mock_do_process(msg_id, msg):
            return False, {"status": "failed", "error": "Processing error"}

        with patch.object(consumer, "_do_process", side_effect=mock_do_process), \
             patch("core.stream_consumer.publish_completion") as mock_pub, \
             patch("core.stream_consumer.ack_message") as mock_ack:

            result = await consumer._process_message("msg-1", {"task_id": "t-1"})

        assert result is False
        mock_pub.assert_called_once()
        assert mock_pub.call_args[0][1]["status"] == "failed"
        mock_ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message_exception(self):
        """_process_message handles unexpected exceptions."""
        mock_logger = Mock()
        consumer = StreamConsumerWithCompletion(
            stream="test:stream",
            group="test-group",
            consumer_name="test-1",
            completion_channel="test:completed",
            logger=mock_logger,
        )

        async def mock_do_process(msg_id, msg):
            raise Exception("Unexpected error")

        with patch.object(consumer, "_do_process", side_effect=mock_do_process), \
             patch("core.stream_consumer.publish_completion") as mock_pub, \
             patch("core.stream_consumer.ack_message") as mock_ack:

            result = await consumer._process_message("msg-1", {"task_id": "t-1"})

        assert result is False
        mock_pub.assert_called_once()
        assert mock_pub.call_args[0][1]["status"] == "failed"
        assert "Unexpected error" in mock_pub.call_args[0][1]["error"]
        mock_ack.assert_called_once()
        mock_logger.error.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
