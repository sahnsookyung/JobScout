import json
import logging
import os
import threading
import time
from typing import Optional, Generator

import redis

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

_connection_pool: Optional[redis.ConnectionPool] = None
_pool_lock = threading.Lock()


def _get_connection_pool() -> redis.ConnectionPool:
    global _connection_pool
    if _connection_pool is None:
        with _pool_lock:
            if _connection_pool is None:
                _connection_pool = redis.ConnectionPool.from_url(
                    REDIS_URL,
                    decode_responses=True,
                    max_connections=20,
                    socket_timeout=10.0,
                    socket_connect_timeout=5.0
                )
    return _connection_pool


def get_redis_client() -> redis.Redis:
    return redis.Redis(connection_pool=_get_connection_pool())


STREAM_EXTRACTION = "extraction:jobs"
STREAM_EMBEDDINGS = "embeddings:jobs"
STREAM_MATCHING = "matching:jobs"

CHANNEL_EXTRACTION_DONE = "extraction:completed"
CHANNEL_EMBEDDINGS_DONE = "embeddings:completed"
CHANNEL_MATCHING_DONE = "matching:completed"


def validate_job_payload(payload: dict, required_fields: list[str]) -> tuple[bool, str]:
    for field in required_fields:
        if field not in payload:
            return False, f"Missing required field: {field}"
    return True, ""


def enqueue_job(stream: str, payload: dict) -> str:
    valid, error = validate_job_payload(payload, ["task_id"])
    if not valid:
        raise ValueError(f"Invalid job payload: {error}")

    client = get_redis_client()
    try:
        serialized = {k: json.dumps(v) for k, v in payload.items()}
    except TypeError as e:
        raise ValueError(f"Payload contains non-JSON-serializable value: {e}")

    try:
        msg_id = client.xadd(stream, serialized)
        logger.info(f"📤 Enqueued job to {stream}: msg_id={msg_id}, task_id={payload.get('task_id')}")
    except (redis.ConnectionError, redis.TimeoutError) as e:
        logger.error(f"❌ Redis error enqueuing job to {stream}: {type(e).__name__}: {e}")
        raise

    return msg_id


def _claim_stale_messages(stream: str, group: str, consumer: str, count: int = 10) -> list[tuple[str, dict]]:
    """Claim stale pending messages from dead consumers.
    
    Uses XCLAIM to claim messages that have been pending for too long (>= 1 minute)
    from consumers that may have died.
    
    Args:
        stream: Stream name
        group: Consumer group name
        consumer: Consumer name to claim messages to
        count: Max messages to claim
        
    Returns:
        List of (message_id, message_data) tuples
    """
    client = get_redis_client()
    claimed = []
    
    try:
        # Get all pending messages summary
        pending_summary = client.xpending(stream, group)
        if isinstance(pending_summary, dict):
            total_pending = pending_summary.get("pending", 0)
        else:
            total_pending = 0
        
        if total_pending == 0:
            return claimed
            
        # Get detailed pending messages
        pending_details = client.xpending_range(
            stream, group,
            min="-", max="+",
            count=min(total_pending, count * 2)
        )
        
        for p in pending_details:
            msg_id = p["message_id"]
            consumer_name = p.get("consumer")
            
            # Skip if already claimed to our consumer
            if consumer_name == consumer:
                continue
                
            # Skip if not stale (less than 60 seconds idle)
            time_since_delivered = p.get("time_since_delivered", 0)
            if time_since_delivered < 60000:  # 60 seconds in milliseconds
                continue
                
            try:
                # Try to claim the message
                claimed_msgs = client.xclaim(
                    stream, group, consumer,
                    min_idle_time=60000,
                    message_ids=[msg_id]
                )
                # xclaim returns list of tuples: [(msg_id, msg_data), ...]
                for claimed_msg in claimed_msgs:
                    if isinstance(claimed_msg, tuple) and len(claimed_msg) == 2:
                        claimed_msg_id, msg_data = claimed_msg
                        claimed.append((claimed_msg_id, msg_data))
                        logger.info(f"Claimed stale message {claimed_msg_id} from consumer {consumer_name}")
                    else:
                        # Fallback for unexpected format
                        claimed.append((msg_id, claimed_msg))
            except Exception as e:
                logger.debug(f"Could not claim message {msg_id}: {e}")
                
    except Exception as e:
        logger.warning("Error claiming stale messages: %s", e)

    return claimed


def _deserialize_message(msg: dict) -> dict:
    """Deserialize message values from JSON strings."""
    deserialized = {}
    for k, v in msg.items():
        try:
            deserialized[k] = json.loads(v)
        except (json.JSONDecodeError, TypeError):
            deserialized[k] = v
    return deserialized


def _read_stream_loop(
    client,
    stream: str,
    group: str,
    consumer: str,
    count: int,
    block: Optional[int],
    shutdown_event: Optional[threading.Event],
    read_pending: bool
) -> Generator[tuple[str, dict], None, None]:
    """Main loop for reading messages from Redis stream."""
    while shutdown_event is None or not shutdown_event.is_set():
        # First, try to claim stale pending messages from dead consumers
        if read_pending:
            try:
                claimed = _claim_stale_messages(stream, group, consumer, count)
                for msg_id, msg in claimed:
                    logger.info("🔄 Claimed stale message %s from %s", msg_id, stream)
                    yield msg_id, _deserialize_message(msg)
            except Exception as e:
                logger.error("❌ Error claiming stale messages: %s: %s", type(e).__name__, e)

        try:
            logger.debug("⏳ Reading from stream %s (blocking for %sms)...", stream, block)
            messages = client.xreadgroup(
                group,
                consumer,
                {stream: ">"},  # ">" = read new messages only
                count=count,
                block=block
            )

            if not messages:
                logger.debug("⏰ No messages from %s (timeout after %sms)", stream, block)
                if block is None:
                    time.sleep(0.1)  # Prevent CPU spin in non-blocking mode
                continue

            for stream_name, msgs in messages:
                for msg_id, msg in msgs:
                    logger.debug("📨 Read new message %s from %s", msg_id, stream)
                    yield msg_id, _deserialize_message(msg)

        except redis.ConnectionError as e:
            logger.error("❌ Connection error reading from stream %s: %s: %s", stream, type(e).__name__, e)
            time.sleep(1)  # Back off on connection errors
            continue
        except redis.TimeoutError as e:
            logger.warning("⚠️ Timeout reading from stream %s: %s: %s", stream, type(e).__name__, e)
            time.sleep(1)  # Brief backoff for transient errors
            continue
        except Exception as e:
            logger.error("❌ Fatal error reading from stream %s: %s: %s", stream, type(e).__name__, e)
            raise


def read_stream(
    stream: str,
    group: str,
    consumer: str,
    count: int = 10,
    block: Optional[int] = 5000,  # Default 5s timeout for graceful shutdown
    shutdown_event: Optional[threading.Event] = None,
    read_pending: bool = True
) -> Generator[tuple[str, dict], None, None]:
    """Read messages from a Redis stream consumer group.

    Args:
        stream: Stream name
        group: Consumer group name
        consumer: Consumer name
        count: Max messages to read per iteration
        block: Milliseconds to block waiting for messages (default 5000)
        shutdown_event: Optional event to signal shutdown
        read_pending: If True, read pending messages first before new ones

    Yields:
        Tuples of (message_id, message_data)
    """
    client = get_redis_client()

    try:
        client.xgroup_create(stream, group, id="0", mkstream=True)
        logger.info("✅ Created/verified consumer group %s for stream %s", group, stream)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            logger.error("❌ Failed to create consumer group: %s: %s", type(e).__name__, e)
            raise
        logger.debug("ℹ️ Consumer group %s already exists for stream %s", group, stream)

    yield from _read_stream_loop(
        client, stream, group, consumer, count, block, shutdown_event, read_pending
    )


def ack_message(stream: str, group: str, msg_id: str) -> bool:
    client = get_redis_client()
    result = client.xack(stream, group, msg_id)
    if result == 0:
        logger.warning(f"⚠️ Message {msg_id} was not acknowledged (already acked or not found) in {stream}")
    else:
        logger.debug(f"✅ Acknowledged message {msg_id} in {stream}")
    return result > 0


def publish_completion(channel: str, payload: dict) -> int:
    valid, error = validate_job_payload(payload, ["task_id", "status"])
    if not valid:
        raise ValueError(f"Invalid completion payload: {error}")

    client = get_redis_client()
    msg = json.dumps(payload)
    result = client.publish(channel, msg)
    if result == 0:
        logger.warning(f"⚠️ No subscribers received completion event on {channel}: task_id={payload.get('task_id')}, status={payload.get('status')}")
    else:
        logger.info(f"📢 Published to {channel}: task_id={payload.get('task_id')}, status={payload.get('status')} (subscribers: {result})")
    return result


def subscribe(channels: list[str]) -> redis.client.PubSub:
    client = get_redis_client()
    pubsub = client.pubsub()
    pubsub.subscribe(*channels)
    logger.info(f"Subscribed to channels: {channels}")
    return pubsub


def listen_for_messages(
    pubsub: redis.client.PubSub,
    shutdown_event: Optional[threading.Event] = None
) -> Generator[dict, None, None]:
    """Listen for messages on pubsub with optional shutdown support.

    Args:
        pubsub: Redis pubsub instance
        shutdown_event: Optional event to signal shutdown

    Yields:
        Decoded message data dicts
    """
    while shutdown_event is None or not shutdown_event.is_set():
        try:
            message = pubsub.get_message(timeout=1.0, ignore_subscribe_messages=True)
            if message is None:
                continue
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    yield data
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
        except redis.ConnectionError as e:
            logger.error(f"Connection error in pubsub listener: {e}")
            pubsub.close()
            return  # Exit generator; caller should handle reconnection


def create_consumer_group(stream: str, group: str) -> None:
    client = get_redis_client()
    try:
        client.xgroup_create(stream, group, id="0", mkstream=True)
        logger.info(f"Created consumer group {group} for stream {stream}")
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"Consumer group {group} already exists for stream {stream}")
        else:
            raise


def get_stream_info(stream: str) -> dict:
    client = get_redis_client()
    try:
        return client.xinfo_stream(stream)
    except redis.ResponseError as e:
        # Only return {} for "stream not found" errors
        if "no such key" in str(e).lower() or "err no such key" in str(e).lower():
            return {}
        # Re-raise other errors (permission issues, misconfiguration, etc.)
        raise


def stream_exists(stream: str) -> bool:
    return bool(get_stream_info(stream))


def get_task_state(task_id: str) -> Optional[dict]:
    client = get_redis_client()
    key = f"task:{task_id}:state"
    data = client.get(key)
    if data:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            logger.warning(f"Failed to decode task state for {task_id}, returning None")
            return None
    return None


def set_task_state(task_id: str, state: dict, ttl: int = 3600) -> None:
    client = get_redis_client()
    key = f"task:{task_id}:state"
    try:
        serialized = json.dumps(state)
    except TypeError as e:
        raise ValueError(f"State contains non-JSON-serializable value: {e}")
    client.setex(key, ttl, serialized)


def delete_task_state(task_id: str) -> None:
    client = get_redis_client()
    key = f"task:{task_id}:state"
    client.delete(key)
