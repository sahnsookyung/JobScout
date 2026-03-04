import json
import logging
import os
from typing import Optional, Generator

import redis

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

_connection_pool: Optional[redis.ConnectionPool] = None


def _get_connection_pool() -> redis.ConnectionPool:
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = redis.ConnectionPool.from_url(
            REDIS_URL,
            decode_responses=True,
            max_connections=20
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
    msg_id = client.xadd(stream, payload)
    logger.info(f"Enqueued job to {stream}: {msg_id}")
    return msg_id


def read_stream(
    stream: str,
    group: str,
    consumer: str,
    count: int = 10,
    block: Optional[int] = None
) -> Generator[tuple[str, dict], None, None]:
    client = get_redis_client()
    
    try:
        client.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
    
    while True:
        try:
            messages = client.xreadgroup(
                group,
                consumer,
                {stream: ">"},
                count=count,
                block=block
            )
            
            if not messages:
                continue
                
            for stream_name, msgs in messages:
                for msg_id, msg in msgs:
                    yield msg_id, msg
                    
        except Exception as e:
            logger.error(f"Error reading from stream {stream}: {e}")
            break


def ack_message(stream: str, group: str, msg_id: str) -> bool:
    client = get_redis_client()
    try:
        client.xack(stream, group, msg_id)
        return True
    except Exception as e:
        logger.error(f"Failed to ack message {msg_id}: {e}")
        return False


def publish_completion(channel: str, payload: dict) -> int:
    valid, error = validate_job_payload(payload, ["task_id", "status"])
    if not valid:
        raise ValueError(f"Invalid completion payload: {error}")
    
    client = get_redis_client()
    msg = json.dumps(payload)
    result = client.publish(channel, msg)
    logger.info(f"Published to {channel}: {payload.get('task_id')}")
    return result


def subscribe(channels: list[str]) -> redis.client.PubSub:
    client = get_redis_client()
    pubsub = client.pubsub()
    pubsub.subscribe(*channels)
    logger.info(f"Subscribed to channels: {channels}")
    return pubsub


def listen_for_messages(pubsub: redis.client.PubSub) -> Generator[dict, None, None]:
    for message in pubsub.listen():
        if message["type"] == "message":
            try:
                data = json.loads(message["data"])
                yield data
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode message: {e}")


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
    except redis.ResponseError:
        return {}


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
            return None
    return None


def set_task_state(task_id: str, state: dict, ttl: int = 3600) -> None:
    client = get_redis_client()
    key = f"task:{task_id}:state"
    client.setex(key, ttl, json.dumps(state))


def delete_task_state(task_id: str) -> None:
    client = get_redis_client()
    key = f"task:{task_id}:state"
    client.delete(key)
