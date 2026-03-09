#!/usr/bin/env python3
"""
Tests for Redis Streams Utilities
Covers: core/redis_streams.py
"""

import pytest
import json
import threading
from unittest.mock import Mock, patch, MagicMock
from redis import Redis

from core.redis_streams import (
    validate_job_payload,
    enqueue_job,
    _claim_stale_messages,
    _is_claimable,
    _try_xclaim,
    _deserialize_message,
    _is_running,
    _yield_claimed_messages,
    _yield_new_messages,
    _read_stream_loop,
    read_stream,
    ack_message,
    publish_completion,
    subscribe,
    listen_for_messages,
    create_consumer_group,
    get_stream_info,
    stream_exists,
    get_task_state,
    set_task_state,
    delete_task_state,
    get_redis_client,
    _get_connection_pool,
    STREAM_EXTRACTION,
    STREAM_EMBEDDINGS,
    STREAM_MATCHING,
    CHANNEL_EXTRACTION_DONE,
    CHANNEL_EMBEDDINGS_DONE,
    CHANNEL_MATCHING_DONE,
)


class TestValidateJobPayload:
    """Test validate_job_payload function."""

    def test_valid_payload(self):
        """Test validation with valid payload."""
        payload = {"task_id": "task-123", "status": "running"}
        required_fields = ["task_id"]

        valid, error = validate_job_payload(payload, required_fields)

        assert valid is True
        assert error == ""

    def test_valid_payload_multiple_fields(self):
        """Test validation with multiple required fields."""
        payload = {"task_id": "task-123", "status": "running", "data": "value"}
        required_fields = ["task_id", "status"]

        valid, error = validate_job_payload(payload, required_fields)

        assert valid is True
        assert error == ""

    def test_missing_required_field(self):
        """Test validation with missing required field."""
        payload = {"status": "running"}
        required_fields = ["task_id"]

        valid, error = validate_job_payload(payload, required_fields)

        assert valid is False
        assert "Missing required field" in error
        assert "task_id" in error

    def test_empty_payload(self):
        """Test validation with empty payload."""
        payload = {}
        required_fields = ["task_id"]

        valid, error = validate_job_payload(payload, required_fields)

        assert valid is False
        assert "Missing required field" in error

    def test_multiple_missing_fields(self):
        """Test validation with multiple missing fields - returns first missing."""
        payload = {}
        required_fields = ["task_id", "status", "data"]

        valid, error = validate_job_payload(payload, required_fields)

        assert valid is False
        assert "task_id" in error


class TestEnqueueJob:
    """Test enqueue_job function."""

    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        with patch('core.redis_streams.get_redis_client') as mock:
            redis_client = Mock()
            redis_client.xadd.return_value = "msg-id-123"
            mock.return_value = redis_client
            yield redis_client

    def test_enqueue_job_success(self, mock_redis):
        """Test successful job enqueue."""
        payload = {"task_id": "task-123", "status": "pending"}

        msg_id = enqueue_job(STREAM_EXTRACTION, payload)

        assert msg_id == "msg-id-123"
        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args[0]
        assert call_args[0] == STREAM_EXTRACTION

    def test_enqueue_job_serializes_values(self, mock_redis):
        """Test that enqueue_job serializes values to JSON."""
        payload = {"task_id": "task-123", "data": {"key": "value"}, "count": 42}

        enqueue_job(STREAM_EXTRACTION, payload)

        serialized = mock_redis.xadd.call_args[0][1]
        assert serialized["task_id"] == '"task-123"'
        assert serialized["data"] == '{"key": "value"}'
        assert serialized["count"] == '42'

    def test_enqueue_job_missing_task_id(self):
        """Test enqueue_job with missing task_id."""
        payload = {"status": "pending"}

        with pytest.raises(ValueError) as exc_info:
            enqueue_job(STREAM_EXTRACTION, payload)

        assert "Invalid job payload" in str(exc_info.value)
        assert "Missing required field: task_id" in str(exc_info.value)

    def test_enqueue_job_non_serializable_value(self, mock_redis):
        """Test enqueue_job with non-serializable value."""
        payload = {"task_id": "task-123", "data": lambda x: x}

        with pytest.raises(ValueError) as exc_info:
            enqueue_job(STREAM_EXTRACTION, payload)

        assert "non-JSON-serializable" in str(exc_info.value)

    def test_enqueue_job_redis_connection_error(self):
        """Test enqueue_job with Redis connection error."""
        with patch('core.redis_streams.get_redis_client') as mock:
            redis_client = Mock()
            redis_client.xadd.side_effect = Exception("Redis connection failed")
            mock.return_value = redis_client

            with pytest.raises(Exception):
                enqueue_job(STREAM_EXTRACTION, {"task_id": "task-123"})


class TestIsClaimable:
    """Test _is_claimable function."""

    def test_claimable_different_consumer_idle_long_enough(self):
        """Test message is claimable from different consumer after idle time."""
        pending = {
            "consumer": "consumer-1",
            "time_since_delivered": 120000  # 2 minutes
        }

        result = _is_claimable(pending, "consumer-2")

        assert result is True

    def test_not_claimable_same_consumer(self):
        """Test message is not claimable from same consumer."""
        pending = {
            "consumer": "consumer-1",
            "time_since_delivered": 120000
        }

        result = _is_claimable(pending, "consumer-1")

        assert result is False

    def test_not_claimable_idle_not_long_enough(self):
        """Test message is not claimable if idle time is too short."""
        pending = {
            "consumer": "consumer-1",
            "time_since_delivered": 30000  # 30 seconds
        }

        result = _is_claimable(pending, "consumer-2")

        assert result is False

    def test_not_claimable_boundary_idle_time(self):
        """Test message is claimable at exactly 60 seconds idle."""
        pending = {
            "consumer": "consumer-1",
            "time_since_delivered": 60000  # Exactly 60 seconds
        }

        result = _is_claimable(pending, "consumer-2")

        assert result is True

    def test_not_claimable_below_boundary_idle_time(self):
        """Test message is not claimable just below 60 seconds."""
        pending = {
            "consumer": "consumer-1",
            "time_since_delivered": 59999  # Just under 60 seconds
        }

        result = _is_claimable(pending, "consumer-2")

        assert result is False


class TestTryXclaim:
    """Test _try_xclaim function."""

    @pytest.fixture
    def mock_redis_client(self):
        """Create mock Redis client."""
        return Mock()

    def test_xclaim_success(self, mock_redis_client):
        """Test successful xclaim."""
        pending = {
            "message_id": "msg-123",
            "consumer": "consumer-1"
        }

        mock_claimed_msg = (b"msg-123", {b"data": b"value"})
        mock_redis_client.xclaim.return_value = [mock_claimed_msg]

        result = _try_xclaim(mock_redis_client, "stream-1", "group-1", "consumer-2", pending)

        assert len(result) == 1
        mock_redis_client.xclaim.assert_called_once_with(
            "stream-1", "group-1", "consumer-2",
            min_idle_time=60000,
            message_ids=["msg-123"]
        )

    def test_xclaim_no_messages(self, mock_redis_client):
        """Test xclaim returns empty list when no messages claimed."""
        pending = {
            "message_id": "msg-123",
            "consumer": "consumer-1"
        }

        mock_redis_client.xclaim.return_value = []

        result = _try_xclaim(mock_redis_client, "stream-1", "group-1", "consumer-2", pending)

        assert result == []

    def test_xclaim_exception_logged(self, mock_redis_client, caplog):
        """Test xclaim exception is logged but not raised."""
        pending = {
            "message_id": "msg-123",
            "consumer": "consumer-1"
        }

        mock_redis_client.xclaim.side_effect = Exception("Claim failed")

        result = _try_xclaim(mock_redis_client, "stream-1", "group-1", "consumer-2", pending)

        assert result == []
        assert "Could not claim message" in caplog.text


class TestDeserializeMessage:
    """Test _deserialize_message function."""

    def test_deserialize_all_json_values(self):
        """Test deserializing message with all JSON values."""
        msg = {
            "task_id": '"task-123"',
            "data": '{"key": "value"}',
            "count": '42'
        }

        result = _deserialize_message(msg)

        assert result["task_id"] == "task-123"
        assert result["data"] == {"key": "value"}
        assert result["count"] == 42

    def test_deserialize_mixed_values(self):
        """Test deserializing message with mixed JSON and non-JSON values."""
        msg = {
            "task_id": '"task-123"',
            "status": "running",  # Already a string, not JSON
            "count": '100'
        }

        result = _deserialize_message(msg)

        assert result["task_id"] == "task-123"
        assert result["status"] == "running"
        assert result["count"] == 100

    def test_deserialize_invalid_json(self):
        """Test deserializing message with invalid JSON keeps original value."""
        msg = {
            "task_id": "not-valid-json",
            "data": '{"incomplete": '
        }

        result = _deserialize_message(msg)

        # Invalid JSON should remain as-is
        assert result["task_id"] == "not-valid-json"

    def test_deserialize_empty_message(self):
        """Test deserializing empty message."""
        msg = {}

        result = _deserialize_message(msg)

        assert result == {}


class TestIsRunning:
    """Test _is_running function."""

    def test_no_shutdown_event(self):
        """Test _is_running with no shutdown event."""
        result = _is_running(None)
        assert result is True

    def test_shutdown_event_not_set(self):
        """Test _is_running with event not set."""
        event = threading.Event()
        result = _is_running(event)
        assert result is True

    def test_shutdown_event_set(self):
        """Test _is_running with event set."""
        event = threading.Event()
        event.set()
        result = _is_running(event)
        assert result is False


class TestYieldClaimedMessages:
    """Test _yield_claimed_messages function."""

    def test_yields_claimed_messages(self):
        """Test generator yields claimed messages."""
        with patch('core.redis_streams._claim_stale_messages') as mock_claim:
            mock_claim.return_value = [
                ("msg-1", {"task_id": '"task-1"'}),
                ("msg-2", {"task_id": '"task-2"'})
            ]

            result = list(_yield_claimed_messages("stream-1", "group-1", "consumer-1", 10))

            assert len(result) == 2
            assert result[0] == ("msg-1", {"task_id": "task-1"})
            assert result[1] == ("msg-2", {"task_id": "task-2"})

    def test_exception_logged(self, caplog):
        """Test exception in claim is logged."""
        with patch('core.redis_streams._claim_stale_messages') as mock_claim:
            mock_claim.side_effect = Exception("Claim failed")

            result = list(_yield_claimed_messages("stream-1", "group-1", "consumer-1", 10))

            assert result == []
            assert "Error claiming stale messages" in caplog.text

    def test_empty_result(self):
        """Test generator with no claimed messages."""
        with patch('core.redis_streams._claim_stale_messages') as mock_claim:
            mock_claim.return_value = []

            result = list(_yield_claimed_messages("stream-1", "group-1", "consumer-1", 10))

            assert result == []


class TestYieldNewMessages:
    """Test _yield_new_messages function."""

    def test_yields_new_messages(self):
        """Test generator yields new messages."""
        messages = [
            ("stream-1", [
                ("msg-1", {"task_id": '"task-1"'}),
                ("msg-2", {"task_id": '"task-2"'})
            ])
        ]

        result = list(_yield_new_messages(messages, "stream-1"))

        assert len(result) == 2
        assert result[0] == ("msg-1", {"task_id": "task-1"})
        assert result[1] == ("msg-2", {"task_id": "task-2"})

    def test_empty_messages(self):
        """Test generator with empty messages."""
        messages = []

        result = list(_yield_new_messages(messages, "stream-1"))

        assert result == []


class TestAckMessage:
    """Test ack_message function."""

    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        with patch('core.redis_streams.get_redis_client') as mock:
            redis_client = Mock()
            mock.return_value = redis_client
            yield redis_client

    def test_ack_success(self, mock_redis):
        """Test successful acknowledgment."""
        mock_redis.xack.return_value = 1

        result = ack_message("stream-1", "group-1", "msg-123")

        assert result is True
        mock_redis.xack.assert_called_once_with("stream-1", "group-1", "msg-123")

    def test_ack_not_found(self, mock_redis, caplog):
        """Test acknowledgment when message not found."""
        mock_redis.xack.return_value = 0

        result = ack_message("stream-1", "group-1", "msg-123")

        assert result is False
        assert "was not acknowledged" in caplog.text


class TestPublishCompletion:
    """Test publish_completion function."""

    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        with patch('core.redis_streams.get_redis_client') as mock:
            redis_client = Mock()
            redis_client.publish.return_value = 2  # 2 subscribers
            mock.return_value = redis_client
            yield redis_client

    def test_publish_success(self, mock_redis):
        """Test successful publish with subscribers."""
        payload = {"task_id": "task-123", "status": "completed"}

        result = publish_completion(CHANNEL_MATCHING_DONE, payload)

        assert result == 2
        mock_redis.publish.assert_called_once()

    def test_publish_no_subscribers(self, mock_redis, caplog):
        """Test publish with no subscribers."""
        mock_redis.publish.return_value = 0

        result = publish_completion(CHANNEL_MATCHING_DONE, {"task_id": "task-123", "status": "completed"})

        assert result == 0
        assert "No subscribers received" in caplog.text

    def test_publish_invalid_payload(self):
        """Test publish with invalid payload."""
        payload = {"status": "completed"}  # Missing task_id

        with pytest.raises(ValueError) as exc_info:
            publish_completion(CHANNEL_MATCHING_DONE, payload)

        assert "Invalid completion payload" in str(exc_info.value)


class TestSubscribe:
    """Test subscribe function."""

    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        with patch('core.redis_streams.get_redis_client') as mock:
            redis_client = Mock()
            pubsub = Mock()
            redis_client.pubsub.return_value = pubsub
            mock.return_value = redis_client
            yield redis_client

    def test_subscribe_success(self, mock_redis):
        """Test successful subscription."""
        result = subscribe([CHANNEL_MATCHING_DONE, CHANNEL_EXTRACTION_DONE])

        mock_redis.pubsub.assert_called_once()
        pubsub = mock_redis.pubsub.return_value
        pubsub.subscribe.assert_called_once_with(CHANNEL_MATCHING_DONE, CHANNEL_EXTRACTION_DONE)


class TestListenForMessages:
    """Test listen_for_messages function."""

    @pytest.fixture
    def mock_pubsub(self):
        """Create mock pubsub."""
        return Mock()

    def test_yields_messages(self, mock_pubsub):
        """Test generator yields messages."""
        # Use itertools.chain to avoid StopIteration issue in Python 3.7+
        import itertools
        messages = [
            {"type": "message", "data": '{"task_id": "task-1"}'},
            {"type": "message", "data": '{"task_id": "task-2"}'},
        ]
        # Return None repeatedly after messages are exhausted (simulates no more messages)
        mock_pubsub.get_message.side_effect = itertools.chain(messages, itertools.repeat(None))

        # Collect first 2 messages then stop
        result = []
        for i, msg in enumerate(listen_for_messages(mock_pubsub)):
            result.append(msg)
            if i >= 1:  # Got 2 messages
                break

        assert len(result) == 2
        assert result[0] == {"task_id": "task-1"}
        assert result[1] == {"task_id": "task-2"}

    def test_skips_subscribe_messages(self, mock_pubsub):
        """Test generator skips subscribe messages."""
        import itertools
        messages = [
            {"type": "subscribe", "channel": "test"},
            {"type": "message", "data": '{"task_id": "task-1"}'},
        ]
        mock_pubsub.get_message.side_effect = itertools.chain(messages, itertools.repeat(None))

        result = []
        for i, msg in enumerate(listen_for_messages(mock_pubsub)):
            result.append(msg)
            if i >= 0:  # Got 1 message
                break

        assert len(result) == 1
        assert result[0] == {"task_id": "task-1"}

    def test_invalid_json_logged(self, mock_pubsub, caplog):
        """Test invalid JSON is logged."""
        import itertools
        messages = [
            {"type": "message", "data": 'invalid-json'},
        ]
        mock_pubsub.get_message.side_effect = itertools.chain(messages, itertools.repeat(None))

        # Run for a bit then stop
        result = []
        for i, msg in enumerate(listen_for_messages(mock_pubsub)):
            result.append(msg)
            if i >= 0:
                break

        assert result == []
        assert "Failed to decode message" in caplog.text

    def test_connection_error_closes_pubsub(self, mock_pubsub, caplog):
        """Test connection error closes pubsub."""
        mock_pubsub.get_message.side_effect = Exception("Connection lost")

        result = list(listen_for_messages(mock_pubsub))

        assert result == []
        mock_pubsub.close.assert_called_once()


class TestCreateConsumerGroup:
    """Test create_consumer_group function."""

    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        with patch('core.redis_streams.get_redis_client') as mock:
            redis_client = Mock()
            mock.return_value = redis_client
            yield redis_client

    def test_create_new_group(self, mock_redis):
        """Test creating new consumer group."""
        create_consumer_group("stream-1", "group-1")

        mock_redis.xgroup_create.assert_called_once_with("stream-1", "group-1", id="0", mkstream=True)

    def test_create_existing_group(self, mock_redis, caplog):
        """Test creating already existing consumer group."""
        import redis
        mock_redis.xgroup_create.side_effect = redis.ResponseError("BUSYGROUP Consumer Group name already exists")

        create_consumer_group("stream-1", "group-1")

        assert "already exists" in caplog.text

    def test_create_group_other_error(self, mock_redis):
        """Test creating consumer group with other error."""
        import redis
        mock_redis.xgroup_create.side_effect = redis.ResponseError("Stream does not exist")

        with pytest.raises(redis.ResponseError):
            create_consumer_group("stream-1", "group-1")


class TestGetStreamInfo:
    """Test get_stream_info function."""

    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        with patch('core.redis_streams.get_redis_client') as mock:
            redis_client = Mock()
            mock.return_value = redis_client
            yield redis_client

    def test_get_info_success(self, mock_redis):
        """Test getting stream info."""
        mock_redis.xinfo_stream.return_value = {"length": 10, "groups": 1}

        result = get_stream_info("stream-1")

        assert result == {"length": 10, "groups": 1}

    def test_get_info_not_found(self, mock_redis):
        """Test getting stream info when not found."""
        import redis
        mock_redis.xinfo_stream.side_effect = redis.ResponseError("no such key")

        result = get_stream_info("stream-1")

        assert result == {}

    def test_get_info_other_error(self, mock_redis):
        """Test getting stream info with other error."""
        import redis
        mock_redis.xinfo_stream.side_effect = redis.ResponseError("Permission denied")

        with pytest.raises(redis.ResponseError):
            get_stream_info("stream-1")


class TestStreamExists:
    """Test stream_exists function."""

    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        with patch('core.redis_streams.get_redis_client') as mock:
            redis_client = Mock()
            mock.return_value = redis_client
            yield redis_client

    def test_stream_exists_true(self, mock_redis):
        """Test stream exists returns True."""
        with patch('core.redis_streams.get_stream_info') as mock_info:
            mock_info.return_value = {"length": 10}

            result = stream_exists("stream-1")

            assert result is True

    def test_stream_exists_false(self, mock_redis):
        """Test stream exists returns False."""
        with patch('core.redis_streams.get_stream_info') as mock_info:
            mock_info.return_value = {}

            result = stream_exists("stream-1")

            assert result is False


class TestTaskState:
    """Test task state functions."""

    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        with patch('core.redis_streams.get_redis_client') as mock:
            redis_client = Mock()
            mock.return_value = redis_client
            yield redis_client

    def test_get_task_state_success(self, mock_redis):
        """Test getting task state."""
        mock_redis.get.return_value = '{"status": "running", "step": "matching"}'

        result = get_task_state("task-123")

        assert result == {"status": "running", "step": "matching"}

    def test_get_task_state_not_found(self, mock_redis):
        """Test getting non-existent task state."""
        mock_redis.get.return_value = None

        result = get_task_state("task-123")

        assert result is None

    def test_get_task_state_invalid_json(self, mock_redis, caplog):
        """Test getting task state with invalid JSON."""
        mock_redis.get.return_value = 'invalid-json'

        result = get_task_state("task-123")

        assert result is None
        assert "Failed to decode" in caplog.text

    def test_set_task_state_success(self, mock_redis):
        """Test setting task state."""
        state = {"status": "running", "step": "matching"}

        set_task_state("task-123", state, ttl=3600)

        mock_redis.setex.assert_called_once()
        call_args = mock_redis.setex.call_args[0]
        assert call_args[0] == "task:task-123:state"
        assert call_args[1] == 3600

    def test_set_task_state_non_serializable(self, mock_redis):
        """Test setting task state with non-serializable value."""
        state = {"callback": lambda x: x}

        with pytest.raises(ValueError) as exc_info:
            set_task_state("task-123", state)

        assert "non-JSON-serializable" in str(exc_info.value)

    def test_delete_task_state_success(self, mock_redis):
        """Test deleting task state."""
        delete_task_state("task-123")

        mock_redis.delete.assert_called_once_with("task:task-123:state")


class TestGetRedisClient:
    """Test get_redis_client function."""

    def test_returns_redis_instance(self):
        """Test get_redis_client returns Redis instance."""
        with patch('core.redis_streams._get_connection_pool') as mock_pool:
            mock_pool.return_value = Mock()

            result = get_redis_client()

            assert isinstance(result, Redis)


class TestGetConnectionPool:
    """Test _get_connection_pool function."""

    def test_creates_pool_once(self):
        """Test connection pool is created only once (singleton)."""
        with patch('redis.ConnectionPool.from_url') as mock_from_url:
            mock_pool = Mock()
            mock_from_url.return_value = mock_pool

            # First call
            result1 = _get_connection_pool()
            assert mock_from_url.call_count == 1

            # Second call should reuse same pool
            result2 = _get_connection_pool()
            assert mock_from_url.call_count == 1
            assert result1 is result2

    def test_pool_configuration(self):
        """Test connection pool is configured correctly."""
        with patch('redis.ConnectionPool.from_url') as mock_from_url:
            mock_pool = Mock()
            mock_from_url.return_value = mock_pool

            _get_connection_pool()

            mock_from_url.assert_called_once()
            call_kwargs = mock_from_url.call_args[1]
            assert call_kwargs["decode_responses"] is True
            assert call_kwargs["max_connections"] == 20
            assert call_kwargs["socket_timeout"] == 10.0
            assert call_kwargs["socket_connect_timeout"] == 5.0
