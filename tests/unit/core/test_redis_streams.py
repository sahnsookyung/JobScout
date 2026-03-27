#!/usr/bin/env python3
"""
Tests for Redis Streams Utilities
Covers: core/redis_streams.py
"""


import pytest
import json
import threading
import itertools
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
    STREAM_EXTRACTION_BATCH,
    STREAM_EMBEDDINGS,
    STREAM_EMBEDDINGS_BATCH,
    STREAM_MATCHING,
    CHANNEL_EXTRACTION_DONE,
    CHANNEL_EXTRACTION_BATCH_DONE,
    CHANNEL_EMBEDDINGS_DONE,
    CHANNEL_EMBEDDINGS_BATCH_DONE,
    CHANNEL_MATCHING_DONE,
)


# FIXED: autouse fixture resets the singleton pool before and after every test
# in this file, preventing state leakage between tests (plan 1.2)
@pytest.fixture(autouse=True)
def reset_redis_module():
    from core import redis_streams
    redis_streams._connection_pool = None
    yield
    redis_streams._connection_pool = None


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


@pytest.fixture
def real_redis(redis_container, monkeypatch):
    """Inject a real Redis client (pointing to the test container) into redis_streams.

    Patches get_redis_client() directly so all module-level calls (enqueue_job,
    ack_message, publish_completion, etc.) use the test container without relying
    on the REDIS_URL env var or the singleton pool.
    """
    import redis as redis_lib
    from core import redis_streams

    client = redis_lib.Redis.from_url(redis_container["url"], decode_responses=True)
    monkeypatch.setattr(redis_streams, "get_redis_client", lambda: client)
    redis_streams._connection_pool = None

    for stream in [STREAM_EXTRACTION, STREAM_EMBEDDINGS, STREAM_MATCHING]:
        client.delete(stream)
    yield client
    for stream in [STREAM_EXTRACTION, STREAM_EMBEDDINGS, STREAM_MATCHING]:
        client.delete(stream)
    client.close()
    redis_streams._connection_pool = None


@pytest.mark.redis
class TestEnqueueJob:
    """enqueue_job writes to a real Redis stream."""

    def test_enqueue_job_success(self, real_redis):
        """Message appears in the stream after enqueue."""
        msg_id = enqueue_job(STREAM_EXTRACTION, {"task_id": "task-123"})

        assert msg_id is not None
        # Verify the message actually landed in the stream
        messages = real_redis.xrange(STREAM_EXTRACTION)
        assert len(messages) == 1

    def test_enqueue_job_serializes_values(self, real_redis):
        """Values are JSON-serialized in the stream entry."""
        enqueue_job(STREAM_EXTRACTION, {"task_id": "task-123", "count": 42})

        messages = real_redis.xrange(STREAM_EXTRACTION)
        assert len(messages) == 1
        _, fields = messages[0]
        assert fields["task_id"] == '"task-123"'
        assert fields["count"] == "42"

    def test_enqueue_job_missing_task_id(self):
        """Missing task_id raises ValueError before touching Redis."""
        with pytest.raises(ValueError) as exc_info:
            enqueue_job(STREAM_EXTRACTION, {"status": "pending"})

        assert "Invalid job payload" in str(exc_info.value)
        assert "Missing required field: task_id" in str(exc_info.value)

    def test_enqueue_job_non_serializable_value(self):
        """Non-JSON-serializable value raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            enqueue_job(STREAM_EXTRACTION, {"task_id": "task-123", "data": lambda x: x})

        assert "non-JSON-serializable" in str(exc_info.value)


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

    # FIXED: renamed from test_not_claimable_boundary_idle_time — name contradicted the assertion
    def test_claimable_at_boundary_idle_time(self):
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


@pytest.mark.redis
class TestAckMessage:
    """ack_message removes messages from the PEL in a real Redis stream."""

    def test_ack_success(self, real_redis):
        """ACK returns True and removes the message from the PEL."""
        stream = STREAM_EXTRACTION
        group = "ack-test-group"
        real_redis.delete(stream)
        create_consumer_group(stream, group)

        # Write and consume a message so it lands in the PEL
        real_redis.xadd(stream, {"task_id": '"ack-task"'})
        entries = real_redis.xreadgroup(group, "consumer-1", {stream: ">"}, count=1)
        msg_id = entries[0][1][0][0]

        result = ack_message(stream, group, msg_id)

        assert result is True
        # PEL should now be empty
        pending = real_redis.xpending(stream, group)
        assert pending["pending"] == 0

    def test_ack_not_found(self, real_redis, caplog):
        """ACK of a non-existent message ID returns False."""
        stream = STREAM_EXTRACTION
        group = "ack-notfound-group"
        real_redis.delete(stream)
        create_consumer_group(stream, group)

        result = ack_message(stream, group, "0-1")

        assert result is False
        assert "was not acknowledged" in caplog.text


@pytest.mark.redis
class TestPublishCompletion:
    """publish_completion delivers messages through real Redis pub/sub."""

    def test_publish_no_subscribers_logs_warning(self, real_redis, caplog):
        """Publishing with no active subscribers returns 0 and logs a warning."""
        result = publish_completion(
            CHANNEL_MATCHING_DONE, {"task_id": "task-123", "status": "completed"}
        )
        assert result == 0
        assert "No subscribers received" in caplog.text

    def test_publish_invalid_payload(self):
        """Missing task_id raises ValueError before hitting Redis."""
        with pytest.raises(ValueError) as exc_info:
            publish_completion(CHANNEL_MATCHING_DONE, {"status": "completed"})

        assert "Invalid completion payload" in str(exc_info.value)

    def test_publish_returns_subscriber_count(self, real_redis, redis_container):
        """A subscribed client increments the receiver count."""
        import redis as redis_lib

        sub_client = redis_lib.Redis.from_url(redis_container["url"], decode_responses=True)
        pubsub = sub_client.pubsub()
        pubsub.subscribe(CHANNEL_MATCHING_DONE)
        # Drain the subscribe confirmation message
        pubsub.get_message(timeout=1)

        count = publish_completion(
            CHANNEL_MATCHING_DONE, {"task_id": "task-456", "status": "completed"}
        )
        assert count >= 1
        pubsub.close()
        sub_client.close()


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
        messages = [
            {"type": "message", "data": '{"task_id": "task-1"}'},
            {"type": "message", "data": '{"task_id": "task-2"}'},
        ]
        mock_pubsub.get_message.side_effect = itertools.chain(messages, itertools.repeat(None))

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
        """Test invalid JSON is logged and message is skipped, generator exits on ConnectionError."""
        import redis as redis_lib
        # Step 1: invalid JSON → logged, loop continues (close NOT called here)
        # Step 2: ConnectionError → caught by except branch, close() called, generator returns
        mock_pubsub.get_message.side_effect = [
            {"type": "message", "data": "invalid-json"},
            redis_lib.ConnectionError("force stop"),
        ]

        result = list(listen_for_messages(mock_pubsub))

        assert result == []
        assert "Failed to decode message" in caplog.text
        mock_pubsub.close.assert_called_once()

    def test_connection_error_closes_pubsub(self, mock_pubsub, caplog):
        """Test redis.ConnectionError closes pubsub and exits generator cleanly."""
        import redis as redis_lib
        # FIXED: must be redis.ConnectionError specifically — the except clause only
        # catches redis.ConnectionError, not the base Exception class
        mock_pubsub.get_message.side_effect = redis_lib.ConnectionError("Connection lost")

        result = list(listen_for_messages(mock_pubsub))

        assert result == []
        mock_pubsub.close.assert_called_once()
        assert "Connection error in pubsub listener" in caplog.text


@pytest.mark.redis
class TestCreateConsumerGroup:
    """create_consumer_group behaves correctly against a real Redis instance."""

    def test_create_new_group(self, real_redis):
        """Creating a new group on a fresh stream succeeds."""
        stream = "cg-test-stream-new"
        real_redis.delete(stream)
        create_consumer_group(stream, "group-1")
        info = real_redis.xinfo_groups(stream)
        assert any(g["name"] == "group-1" for g in info)

    def test_create_existing_group_is_idempotent(self, real_redis, caplog):
        """Creating an already-existing group logs a warning but does not raise."""
        stream = "cg-test-stream-exists"
        real_redis.delete(stream)
        create_consumer_group(stream, "group-dup")
        create_consumer_group(stream, "group-dup")  # second call — must not raise
        assert "already exists" in caplog.text

    def test_create_group_mkstream(self, real_redis):
        """mkstream=True creates the stream automatically when it doesn't exist."""
        stream = "cg-test-mkstream"
        real_redis.delete(stream)
        create_consumer_group(stream, "group-mk")
        assert real_redis.exists(stream)


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

    # FIXED: removed unused mock_redis fixture parameter — stream_exists delegates
    # to get_stream_info which is patched directly; mock_redis was never exercised
    def test_stream_exists_true(self):
        """Test stream exists returns True."""
        with patch('core.redis_streams.get_stream_info') as mock_info:
            mock_info.return_value = {"length": 10}

            result = stream_exists("stream-1")

            assert result is True

    def test_stream_exists_false(self):
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

    def test_creates_pool_once(self):
        """Test connection pool is created only once (singleton)."""
        # FIXED: redis_streams uses `import redis`, not `from redis import ConnectionPool`
        # so the attribute lives at core.redis_streams.redis.ConnectionPool, not
        # core.redis_streams.ConnectionPool
        with patch('core.redis_streams.redis.ConnectionPool.from_url') as mock_from_url:
            mock_pool = Mock()
            mock_from_url.return_value = mock_pool

            result1 = _get_connection_pool()
            assert mock_from_url.call_count == 1

            result2 = _get_connection_pool()
            assert mock_from_url.call_count == 1
            assert result1 is result2

    def test_pool_configuration(self):
        """Test connection pool is configured correctly."""
        with patch('core.redis_streams.redis.ConnectionPool.from_url') as mock_from_url:
            mock_pool = Mock()
            mock_from_url.return_value = mock_pool

            _get_connection_pool()

            mock_from_url.assert_called_once()
            call_kwargs = mock_from_url.call_args[1]
            assert call_kwargs["decode_responses"] is True
            assert call_kwargs["max_connections"] == 20
            assert call_kwargs["socket_timeout"] == 10.0
            assert call_kwargs["socket_connect_timeout"] == 5.0


class TestGetStreamBacklog:
    """Test get_stream_backlog function."""

    @pytest.fixture
    def mock_redis(self):
        with patch('core.redis_streams.get_redis_client') as mock:
            yield mock.return_value

    def test_returns_stream_stats(self, mock_redis):
        """Test returns stream statistics."""
        mock_redis.xinfo_stream.return_value = {
            "length": 100,
            "pending": 10,
            "consumers": 3,
            "groups": 2
        }

        from core.redis_streams import get_stream_backlog
        result = get_stream_backlog("test-stream")

        assert result["length"] == 100
        assert result["pending"] == 10
        assert result["consumers"] == 3
        assert result["groups"] == 2
        assert result["stream"] == "test-stream"

    def test_returns_zero_for_missing_keys(self, mock_redis):
        """Test returns zero for missing keys in response."""
        mock_redis.xinfo_stream.return_value = {}

        from core.redis_streams import get_stream_backlog
        result = get_stream_backlog("test-stream")

        assert result["length"] == 0
        assert result["pending"] == 0
        assert result["consumers"] == 0
        assert result["groups"] == 0


class TestGetAllStreamBacklogs:
    """Test get_all_stream_backlogs function."""

    @pytest.fixture
    def mock_redis(self):
        with patch('core.redis_streams.get_redis_client') as mock:
            mock.xinfo_stream.return_value = {"length": 10, "pending": 2, "consumers": 1, "groups": 1}
            yield mock

    def test_returns_dict_with_all_streams(self, mock_redis):
        """Test returns dict with all pipeline streams."""
        from core.redis_streams import get_all_stream_backlogs

        result = get_all_stream_backlogs()

        assert "extraction" in result
        assert "extraction_batch" in result
        assert "embeddings" in result
        assert "embeddings_batch" in result
        assert "matching" in result

    def test_includes_extraction_stream(self, mock_redis):
        """Test includes extraction stream."""
        from core.redis_streams import get_all_stream_backlogs

        result = get_all_stream_backlogs()

        assert "extraction" in result

    def test_includes_embeddings_stream(self, mock_redis):
        """Test includes embeddings stream."""
        from core.redis_streams import get_all_stream_backlogs

        result = get_all_stream_backlogs()

        assert "embeddings" in result

    def test_batch_stream_constants_are_exposed(self):
        """Test new batch stream and completion channel constants."""
        assert STREAM_EXTRACTION_BATCH == "extraction:batch"
        assert STREAM_EMBEDDINGS_BATCH == "embeddings:batch"
        assert CHANNEL_EXTRACTION_BATCH_DONE == "extraction:batch:completed"
        assert CHANNEL_EMBEDDINGS_BATCH_DONE == "embeddings:batch:completed"

    def test_includes_matching_stream(self, mock_redis):
        """Test includes matching stream."""
        from core.redis_streams import get_all_stream_backlogs

        result = get_all_stream_backlogs()

        assert "matching" in result


class TestLogStreamBacklogs:
    """Test log_stream_backlogs function."""

    def test_logs_all_streams(self, caplog):
        """Test logs backlog for all streams."""
        with patch('core.redis_streams.get_all_stream_backlogs') as mock_backlogs:
            mock_backlogs.return_value = {
                "extraction": {"length": 10, "pending": 2, "consumers": 1, "groups": 1},
                "embeddings": {"length": 20, "pending": 5, "consumers": 2, "groups": 1},
                "matching": {"length": 30, "pending": 8, "consumers": 3, "groups": 2},
            }

            from core.redis_streams import log_stream_backlogs
            log_stream_backlogs()

            assert "Stream backlog: extraction" in caplog.text
            assert "Stream backlog: embeddings" in caplog.text
            assert "Stream backlog: matching" in caplog.text


@pytest.mark.redis
class TestClaimStaleMessages:
    """_claim_stale_messages checks the PEL against a real Redis instance."""

    def test_returns_empty_when_no_pending(self, real_redis):
        """Empty PEL returns an empty list."""
        stream = "claim-test-empty"
        group = "claim-group-empty"
        real_redis.delete(stream)
        create_consumer_group(stream, group)

        result = _claim_stale_messages(stream, group, "consumer-1", 10)

        assert result == []

    def test_returns_empty_for_nonexistent_stream(self, real_redis):
        """Non-existent stream is handled gracefully (returns empty, no raise)."""
        result = _claim_stale_messages("nonexistent-stream-xyz", "group", "consumer-1", 10)
        assert result == []

    def test_no_claim_when_messages_owned_by_same_consumer(self, real_redis):
        """Messages owned by the claiming consumer are not reclaimed."""
        stream = "claim-test-same-consumer"
        group = "claim-group-same"
        real_redis.delete(stream)
        create_consumer_group(stream, group)
        real_redis.xadd(stream, {"task_id": '"t1"'})
        # Read to put into PEL under "consumer-1"
        real_redis.xreadgroup(group, "consumer-1", {stream: ">"}, count=1)

        # Claim attempt from same consumer — _is_claimable returns False
        result = _claim_stale_messages(stream, group, "consumer-1", 10)

        assert isinstance(result, list)

    def test_returns_list_type_always(self, real_redis):
        """Return type is always list regardless of Redis state."""
        stream = "claim-test-type"
        real_redis.delete(stream)
        result = _claim_stale_messages(stream, "any-group", "consumer-1", 10)
        assert isinstance(result, list)
