"""Additional coverage tests for core/redis_streams.py missing branches."""

import pytest
import threading
from unittest.mock import MagicMock, patch, call
import redis


@pytest.fixture(autouse=True)
def reset_redis_module():
    from core import redis_streams
    redis_streams._connection_pool = None
    yield
    redis_streams._connection_pool = None


# ---------------------------------------------------------------------------
# enqueue_job — Redis error paths (lines 73-75)
# ---------------------------------------------------------------------------

class TestEnqueueJobErrors:
    def test_connection_error_is_reraised(self):
        from core.redis_streams import enqueue_job

        mock_client = MagicMock()
        mock_client.xadd.side_effect = redis.ConnectionError("Connection refused")

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            with pytest.raises(redis.ConnectionError):
                enqueue_job("my-stream", {"task_id": "t-1", "job_id": "j-1"})

    def test_timeout_error_is_reraised(self):
        from core.redis_streams import enqueue_job

        mock_client = MagicMock()
        mock_client.xadd.side_effect = redis.TimeoutError("Timeout")

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            with pytest.raises(redis.TimeoutError):
                enqueue_job("my-stream", {"task_id": "t-1", "job_id": "j-1"})


# ---------------------------------------------------------------------------
# _try_xclaim — unexpected return shape (line 134)
# ---------------------------------------------------------------------------

class TestTryXclaimUnexpectedShape:
    def test_unexpected_xclaim_return_shape_reconstructs_tuple(self):
        from core.redis_streams import _try_xclaim

        msg_id = "1234-0"
        # xclaim returns a non-tuple item (unexpected shape)
        weird_msg = {"fields": "value"}
        mock_client = MagicMock()
        mock_client.xclaim.return_value = [weird_msg]

        pending = {"message_id": msg_id, "consumer": "other-consumer"}
        result = _try_xclaim(mock_client, "stream", "group", "consumer", pending)

        assert len(result) == 1
        assert result[0] == (msg_id, weird_msg)

    def test_normal_tuple_shape_returns_tuple(self):
        from core.redis_streams import _try_xclaim

        msg_id = "1234-0"
        mock_client = MagicMock()
        mock_client.xclaim.return_value = [(msg_id, {"task_id": "t-1"})]

        pending = {"message_id": msg_id, "consumer": "other-consumer"}
        result = _try_xclaim(mock_client, "stream", "group", "consumer", pending)

        assert len(result) == 1
        assert result[0] == (msg_id, {"task_id": "t-1"})


# ---------------------------------------------------------------------------
# _claim_stale_messages — claimable message found (line 103)
# ---------------------------------------------------------------------------

class TestClaimStaleMessages:
    def test_claimable_message_is_claimed(self):
        from core.redis_streams import _claim_stale_messages

        mock_client = MagicMock()
        mock_client.xpending.return_value = {"pending": 1}
        pending_entry = {
            "message_id": "msg-1",
            "consumer": "dead-consumer",
            "time_since_delivered": 90_000,
        }
        mock_client.xpending_range.return_value = [pending_entry]
        mock_client.xclaim.return_value = [("msg-1", {"task_id": "t-1"})]

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            result = _claim_stale_messages("stream", "group", "live-consumer", count=1)

        assert len(result) == 1
        assert result[0][0] == "msg-1"

    def test_not_claimable_message_is_skipped(self):
        from core.redis_streams import _claim_stale_messages

        mock_client = MagicMock()
        mock_client.xpending.return_value = {"pending": 1}
        pending_entry = {
            "message_id": "msg-1",
            "consumer": "live-consumer",  # same consumer — not claimable
            "time_since_delivered": 90_000,
        }
        mock_client.xpending_range.return_value = [pending_entry]

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            result = _claim_stale_messages("stream", "group", "live-consumer", count=1)

        assert result == []
        mock_client.xclaim.assert_not_called()


# ---------------------------------------------------------------------------
# _read_stream_loop — exception handling (lines 192-218)
# ---------------------------------------------------------------------------

class TestReadStreamLoopExceptions:
    def _make_client(self):
        return MagicMock()

    def test_connection_error_sleeps_and_continues(self):
        from core.redis_streams import _read_stream_loop

        shutdown = threading.Event()
        mock_client = self._make_client()
        call_count = [0]

        def xreadgroup_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise redis.ConnectionError("Connection refused")
            shutdown.set()
            return []

        mock_client.xreadgroup.side_effect = xreadgroup_side_effect

        with patch("time.sleep"):
            chunks = list(_read_stream_loop(
                mock_client, "stream", "group", "consumer",
                count=1, block=100, shutdown_event=shutdown, read_pending=False
            ))

        assert call_count[0] == 2

    def test_timeout_error_sleeps_and_continues(self):
        from core.redis_streams import _read_stream_loop

        shutdown = threading.Event()
        mock_client = self._make_client()
        call_count = [0]

        def xreadgroup_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise redis.TimeoutError("Timeout")
            shutdown.set()
            return []

        mock_client.xreadgroup.side_effect = xreadgroup_side_effect

        with patch("time.sleep"):
            list(_read_stream_loop(
                mock_client, "stream", "group", "consumer",
                count=1, block=100, shutdown_event=shutdown, read_pending=False
            ))

        assert call_count[0] == 2

    def test_fatal_exception_is_reraised(self):
        from core.redis_streams import _read_stream_loop

        mock_client = self._make_client()
        mock_client.xreadgroup.side_effect = RuntimeError("fatal")

        with pytest.raises(RuntimeError, match="fatal"):
            list(_read_stream_loop(
                mock_client, "stream", "group", "consumer",
                count=1, block=100, shutdown_event=None, read_pending=False
            ))

    def test_no_messages_with_none_block_sleeps(self):
        from core.redis_streams import _read_stream_loop

        shutdown = threading.Event()
        mock_client = self._make_client()
        call_count = [0]

        def xreadgroup_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] >= 2:
                shutdown.set()
            return []

        mock_client.xreadgroup.side_effect = xreadgroup_side_effect

        with patch("time.sleep") as mock_sleep:
            list(_read_stream_loop(
                mock_client, "stream", "group", "consumer",
                count=1, block=None, shutdown_event=shutdown, read_pending=False
            ))

        # With block=None, time.sleep(0.1) should be called on empty response
        mock_sleep.assert_called_with(0.1)


# ---------------------------------------------------------------------------
# read_stream — consumer group creation (lines 244-255)
# ---------------------------------------------------------------------------

class TestReadStream:
    def test_creates_consumer_group_on_first_run(self):
        from core.redis_streams import read_stream

        mock_client = MagicMock()
        mock_client.xgroup_create.return_value = "OK"
        shutdown = threading.Event()
        mock_client.xreadgroup.side_effect = lambda *a, **k: (shutdown.set(), [])[1]

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            list(read_stream("stream", "group", "consumer", shutdown_event=shutdown, read_pending=False))

        mock_client.xgroup_create.assert_called_once()

    def test_busygroup_error_is_ignored(self):
        from core.redis_streams import read_stream

        mock_client = MagicMock()
        mock_client.xgroup_create.side_effect = redis.ResponseError("BUSYGROUP Consumer Group exists")
        shutdown = threading.Event()
        mock_client.xreadgroup.side_effect = lambda *a, **k: (shutdown.set(), [])[1]

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            list(read_stream("stream", "group", "consumer", shutdown_event=shutdown, read_pending=False))

        mock_client.xgroup_create.assert_called_once()

    def test_non_busygroup_error_is_reraised(self):
        from core.redis_streams import read_stream

        mock_client = MagicMock()
        mock_client.xgroup_create.side_effect = redis.ResponseError("ERR some other error")

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            with pytest.raises(redis.ResponseError, match="some other error"):
                list(read_stream("stream", "group", "consumer", read_pending=False))


# ---------------------------------------------------------------------------
# create_consumer_group — non-BUSYGROUP error raises (line 310)
# ---------------------------------------------------------------------------

class TestCreateConsumerGroup:
    def test_non_busygroup_error_reraised(self):
        from core.redis_streams import create_consumer_group

        mock_client = MagicMock()
        mock_client.xgroup_create.side_effect = redis.ResponseError("ERR permission denied")

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            with pytest.raises(redis.ResponseError, match="permission denied"):
                create_consumer_group("stream", "group")

    def test_busygroup_error_silenced(self):
        from core.redis_streams import create_consumer_group

        mock_client = MagicMock()
        mock_client.xgroup_create.side_effect = redis.ResponseError("BUSYGROUP Consumer Group exists")

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            create_consumer_group("stream", "group")  # Should not raise


# ---------------------------------------------------------------------------
# get_stream_info — re-raises non-key-not-found errors (line 332)
# ---------------------------------------------------------------------------

class TestGetStreamInfo:
    def test_non_key_error_reraised(self):
        from core.redis_streams import get_stream_info

        mock_client = MagicMock()
        mock_client.xinfo_stream.side_effect = redis.ResponseError("ERR permission denied")

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            with pytest.raises(redis.ResponseError, match="permission denied"):
                get_stream_info("stream")

    def test_key_not_found_returns_empty_dict(self):
        from core.redis_streams import get_stream_info

        mock_client = MagicMock()
        mock_client.xinfo_stream.side_effect = redis.ResponseError("ERR no such key")

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            result = get_stream_info("stream")
        assert result == {}


# ---------------------------------------------------------------------------
# get_stream_backlog — ResponseError paths (lines 371-374)
# ---------------------------------------------------------------------------

class TestGetStreamBacklog:
    def test_no_such_key_returns_zeros(self):
        from core.redis_streams import get_stream_backlog

        mock_client = MagicMock()
        mock_client.xinfo_stream.side_effect = redis.ResponseError("ERR no such key")

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            result = get_stream_backlog("my-stream")

        assert result == {"stream": "my-stream", "length": 0, "pending": 0, "consumers": 0, "groups": 0}

    def test_other_response_error_reraised(self):
        from core.redis_streams import get_stream_backlog

        mock_client = MagicMock()
        mock_client.xinfo_stream.side_effect = redis.ResponseError("ERR permission denied")

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            with pytest.raises(redis.ResponseError):
                get_stream_backlog("my-stream")

    def test_success_returns_stream_stats(self):
        from core.redis_streams import get_stream_backlog

        mock_client = MagicMock()
        mock_client.xinfo_stream.return_value = {
            "length": 5, "pending": 2, "consumers": 3, "groups": 1
        }

        with patch("core.redis_streams.get_redis_client", return_value=mock_client):
            result = get_stream_backlog("my-stream")

        assert result == {"stream": "my-stream", "length": 5, "pending": 2, "consumers": 3, "groups": 1}
