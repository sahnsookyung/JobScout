"""Batch-stage Redis queue adapter for orchestrator use cases."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Optional

import redis.asyncio as redis_async

from core.metrics import (
    record_jobs_embedded,
    record_jobs_embedding_queued,
    record_jobs_extracted,
    record_jobs_extraction_queued,
)
from core.redis_streams import (
    CHANNEL_EMBEDDINGS_BATCH_DONE,
    CHANNEL_EXTRACTION_BATCH_DONE,
    STREAM_EMBEDDINGS_BATCH,
    STREAM_EXTRACTION_BATCH,
    enqueue_job,
)

WaitForTaskMessage = Callable[[Any, str], Awaitable[dict]]
CleanupPubsub = Callable[[Any, Any], Awaitable[None]]


class BatchStageQueueService:
    """Queue and wait for extraction/embedding batch-stage work."""

    def __init__(
        self,
        *,
        redis_url: str,
        batch_stage_timeout_seconds: float,
        logger: logging.Logger,
    ) -> None:
        self.redis_url = redis_url
        self.batch_stage_timeout_seconds = batch_stage_timeout_seconds
        self.logger = logger

    async def wait_for_next_message(self, pubsub: Any) -> dict:
        """Return the next data message from a Redis pubsub stream."""
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue
            payload = message.get("data")
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            if isinstance(payload, str):
                return json.loads(payload)
            if isinstance(payload, dict):
                return payload
        return {}

    async def wait_for_task_message(self, pubsub: Any, task_id: str) -> dict:
        """Return the first completion message matching a task id."""
        while True:
            data = await self.wait_for_next_message(pubsub)
            if not data or data.get("task_id") == task_id:
                return data

    async def cleanup_pubsub_and_client(self, redis_client: Any, pubsub: Any) -> None:
        try:
            await pubsub.unsubscribe()
        finally:
            await pubsub.close()
            await redis_client.aclose()

    async def run_batch_stage_via_queue(
        self,
        *,
        task_id: str,
        stage: str,
        stream: str,
        completion_channel: str,
        limit: int,
        correlation: Optional[dict[str, Any]] = None,
        wait_for_task_message: Optional[WaitForTaskMessage] = None,
        cleanup_pubsub_and_client: Optional[CleanupPubsub] = None,
        redis_factory: Callable[..., Any] = redis_async.from_url,
    ) -> tuple[int, Optional[str]]:
        redis_client = redis_factory(self.redis_url, decode_responses=True)
        pubsub = redis_client.pubsub()
        wait_for_task = wait_for_task_message or self.wait_for_task_message
        cleanup = cleanup_pubsub_and_client or self.cleanup_pubsub_and_client
        try:
            await pubsub.subscribe(completion_channel)
            await asyncio.to_thread(
                enqueue_job,
                stream,
                {"task_id": task_id, "limit": limit, **(correlation or {})},
            )
            if stage == "extract":
                record_jobs_extraction_queued(limit)
            elif stage == "embed":
                record_jobs_embedding_queued(limit)
            try:
                async with asyncio.timeout(self.batch_stage_timeout_seconds):
                    data = await wait_for_task(pubsub, task_id)
            except TimeoutError:
                self.logger.warning(
                    "%s stage timed out waiting for completion for task %s",
                    stage,
                    task_id,
                )
                return 0, (
                    f"{stage} stage timed out waiting for completion "
                    f"after {self.batch_stage_timeout_seconds:g}s"
                )

            if not data:
                return 0, f"{stage} stage did not publish a completion message"

            processed = int(data.get("processed", 0) or 0)
            if data.get("status") != "completed":
                return processed, str(data.get("error", f"{stage} stage failed"))
            if stage == "extract":
                record_jobs_extracted(processed)
            elif stage == "embed":
                record_jobs_embedded(processed)
            return processed, None
        finally:
            await cleanup(redis_client, pubsub)

    async def run_batch_stage(
        self,
        *,
        task_id: str,
        stage: str,
        limit: int,
        correlation: Optional[dict[str, Any]] = None,
        wait_for_task_message: Optional[WaitForTaskMessage] = None,
        cleanup_pubsub_and_client: Optional[CleanupPubsub] = None,
        redis_factory: Callable[..., Any] = redis_async.from_url,
    ) -> tuple[int, Optional[str]]:
        if stage == "extract":
            stream = STREAM_EXTRACTION_BATCH
            channel = CHANNEL_EXTRACTION_BATCH_DONE
        elif stage == "embed":
            stream = STREAM_EMBEDDINGS_BATCH
            channel = CHANNEL_EMBEDDINGS_BATCH_DONE
        else:
            raise ValueError(f"Unsupported batch stage: {stage}")
        return await self.run_batch_stage_via_queue(
            task_id=task_id,
            stage=stage,
            stream=stream,
            completion_channel=channel,
            limit=limit,
            correlation=correlation,
            wait_for_task_message=wait_for_task_message,
            cleanup_pubsub_and_client=cleanup_pubsub_and_client,
            redis_factory=redis_factory,
        )
