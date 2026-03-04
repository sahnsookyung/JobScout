#!/usr/bin/env python3
"""
Orchestrator Service - Coordinates the extraction => embedding => matching pipeline.

This service:
1. Receives match requests via HTTP
2. Enqueues jobs to Redis Streams
3. Subscribes to completion events
4. Triggers next stage on completion
5. Streams status updates to client via SSE
"""

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Optional

import redis
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from core.config_loader import load_config
from core.app_context import AppContext
from core.redis_streams import (
    enqueue_job,
    STREAM_EXTRACTION,
    STREAM_EMBEDDINGS,
    STREAM_MATCHING,
    CHANNEL_EXTRACTION_DONE,
    CHANNEL_EMBEDDINGS_DONE,
    CHANNEL_MATCHING_DONE,
    get_task_state,
    set_task_state,
    delete_task_state,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

ctx: AppContext | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ctx
    logger.info("Starting orchestrator service...")
    config = load_config()
    ctx = AppContext.build(config)
    logger.info("Orchestrator service ready")
    yield
    logger.info("Shutting down orchestrator service...")


app = FastAPI(
    title="Orchestrator Service",
    description="Coordinates extraction => embedding => matching pipeline",
    version="1.0.0",
    lifespan=lifespan
)


class MatchResponse(BaseModel):
    success: bool
    task_id: str
    message: str


class OrchestrationState:
    """Tracks the state of an orchestration task with Redis persistence."""
    
    def __init__(self, task_id: str, load_from_redis: bool = True):
        self.task_id = task_id
        self.status = "pending"
        self.resume_fingerprint: Optional[str] = None
        self.resume_file: Optional[str] = None
        self.matches_count: int = 0
        self.error: Optional[str] = None
        self._subscribers: list[asyncio.Queue] = []
        
        if load_from_redis:
            self._load_from_redis()
    
    def _load_from_redis(self) -> None:
        """Load state from Redis if exists."""
        try:
            data = get_task_state(self.task_id)
            if data:
                self.status = data.get("status", "pending")
                self.resume_fingerprint = data.get("resume_fingerprint")
                self.resume_file = data.get("resume_file")
                self.matches_count = data.get("matches_count", 0)
                self.error = data.get("error")
                logger.info(f"Loaded state from Redis for task {self.task_id}: {self.status}")
        except Exception as e:
            logger.warning(f"Failed to load state from Redis: {e}")
    
    def _save_to_redis(self) -> None:
        """Save state to Redis."""
        try:
            set_task_state(self.task_id, {
                "status": self.status,
                "resume_fingerprint": self.resume_fingerprint,
                "resume_file": self.resume_file,
                "matches_count": self.matches_count,
                "error": self.error,
            })
        except Exception as e:
            logger.warning(f"Failed to save state to Redis: {e}")
    
    def subscribe(self) -> asyncio.Queue:
        queue = asyncio.Queue()
        self._subscribers.append(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        """Remove a subscriber queue to prevent memory leaks."""
        if queue in self._subscribers:
            self._subscribers.remove(queue)
    
    def notify(self, data: dict):
        for queue in self._subscribers:
            try:
                queue.put_nowait(data)
            except Exception as e:
                logger.error(f"Failed to notify subscriber: {e}")
    
    async def close(self):
        for queue in self._subscribers:
            await queue.put(None)
        # Keep completed/failed state with TTL for late-arriving clients
        # Only delete if status is not terminal
        if self.status not in ("completed", "failed"):
            try:
                delete_task_state(self.task_id)
            except Exception as e:
                logger.warning(f"Failed to delete state from Redis: {e}")
        # Remove from in-memory cache
        if self.task_id in orchestrations:
            del orchestrations[self.task_id]


orchestrations: dict[str, OrchestrationState] = {}


def get_or_create_orchestration(task_id: str) -> OrchestrationState:
    """Get or create orchestration state (loads from Redis if exists)."""
    if task_id not in orchestrations:
        orchestrations[task_id] = OrchestrationState(task_id, load_from_redis=True)
    return orchestrations[task_id]


async def _wait_for_task_message(pubsub, task_id: str, timeout: float) -> Optional[dict]:
    """Wait for a message matching the given task_id, skipping messages for other tasks.
    
    Args:
        pubsub: Redis async pubsub instance
        task_id: The task ID we're waiting for
        timeout: Timeout in seconds
        
    Returns:
        The message data dict for the matching task_id
        
    Raises:
        asyncio.TimeoutError: If no matching message received within timeout
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            raise asyncio.TimeoutError(f"Timeout waiting for task {task_id}")
        data = await _wait_for_next_message(pubsub, timeout=remaining)
        if data.get("task_id") == task_id:
            return data
        logger.debug(f"Skipping message for task {data.get('task_id')}, waiting for {task_id}")


async def orchestrate_match(task_id: str, resume_file: str):
    """Run the orchestration flow: extraction -> embeddings -> matching."""
    import redis.asyncio as redis_async

    LISTENER_TIMEOUT = 600.0  # 10 minutes per stage

    state = get_or_create_orchestration(task_id)
    state.status = "extracting"
    state.resume_file = resume_file
    state._save_to_redis()
    state.notify({"task_id": task_id, "status": "extracting", "message": "Starting extraction"})

    redis_client = None
    try:
        enqueue_job(STREAM_EXTRACTION, {
            "task_id": task_id,
            "resume_file": resume_file
        })

        redis_client = redis_async.from_url(REDIS_URL, decode_responses=True)
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(CHANNEL_EXTRACTION_DONE)

        # Wait for extraction completion
        try:
            data = await _wait_for_task_message(pubsub, task_id, LISTENER_TIMEOUT)
        except asyncio.TimeoutError:
            state.status = "failed"
            state.error = "Timeout waiting for extraction completion"
            state._save_to_redis()
            state.notify({"task_id": task_id, "status": "failed", "error": state.error})
            return

        if data.get("status") == "failed":
            state.status = "failed"
            state.error = data.get("error", "Extraction failed")
            state._save_to_redis()
            state.notify({"task_id": task_id, "status": "failed", "error": state.error})
            return

        if data.get("status") in ("skipped", "completed"):
            fp = data.get("resume_fingerprint")
            if not fp:
                state.status = "failed"
                state.error = "No fingerprint in extraction response"
                state._save_to_redis()
                state.notify({"task_id": task_id, "status": "failed", "error": state.error})
                return

            state.resume_fingerprint = fp

            if data.get("status") == "skipped":
                logger.info(f"Resume unchanged, using existing: {fp[:16]}...")
            else:
                logger.info(f"Extraction complete: {fp[:16]}...")

            state.status = "embedding"
            state._save_to_redis()
            state.notify({"task_id": task_id, "status": "embedding", "message": "Starting embeddings"})

            enqueue_job(STREAM_EMBEDDINGS, {
                "task_id": task_id,
                "resume_fingerprint": state.resume_fingerprint
            })

            await pubsub.unsubscribe(CHANNEL_EXTRACTION_DONE)
            await pubsub.subscribe(CHANNEL_EMBEDDINGS_DONE)
        else:
            # Unexpected status - treat as error
            logger.warning(f"Unexpected status in extraction response: {data.get('status')}")
            state.status = "failed"
            state.error = f"Unexpected status from extraction: {data.get('status')}"
            state._save_to_redis()
            state.notify({"task_id": task_id, "status": "failed", "error": state.error})
            return

        # Wait for embeddings completion
        try:
            data = await _wait_for_task_message(pubsub, task_id, LISTENER_TIMEOUT)
        except asyncio.TimeoutError:
            state.status = "failed"
            state.error = "Timeout waiting for embeddings completion"
            state._save_to_redis()
            state.notify({"task_id": task_id, "status": "failed", "error": state.error})
            return

        if data.get("status") == "failed":
            state.status = "failed"
            state.error = data.get("error", "Embeddings failed")
            state._save_to_redis()
            state.notify({"task_id": task_id, "status": "failed", "error": state.error})
            return

        if data.get("status") == "completed":
            fp = state.resume_fingerprint
            if fp:
                logger.info(f"Embeddings complete: {fp[:16]}...")

            state.status = "matching"
            state._save_to_redis()
            state.notify({"task_id": task_id, "status": "matching", "message": "Starting matching"})

            enqueue_job(STREAM_MATCHING, {
                "task_id": task_id,
                "resume_fingerprint": state.resume_fingerprint
            })

            await pubsub.unsubscribe(CHANNEL_EMBEDDINGS_DONE)
            await pubsub.subscribe(CHANNEL_MATCHING_DONE)
        else:
            # Unexpected status - treat as error
            logger.warning(f"Unexpected status in embeddings response: {data.get('status')}")
            state.status = "failed"
            state.error = f"Unexpected status from embeddings: {data.get('status')}"
            state._save_to_redis()
            state.notify({"task_id": task_id, "status": "failed", "error": state.error})
            return

        # Wait for matching completion
        try:
            data = await _wait_for_task_message(pubsub, task_id, LISTENER_TIMEOUT)
        except asyncio.TimeoutError:
            state.status = "failed"
            state.error = "Timeout waiting for matching completion"
            state._save_to_redis()
            state.notify({"task_id": task_id, "status": "failed", "error": state.error})
            return

        if data.get("status") == "failed":
            state.status = "failed"
            state.error = data.get("error", "Matching failed")
            state._save_to_redis()
            state.notify({"task_id": task_id, "status": "failed", "error": state.error})
            return

        if data.get("status") == "completed":
            state.status = "completed"
            state.matches_count = data.get("matches_count", 0)
            state._save_to_redis()
            state.notify({
                "task_id": task_id,
                "status": "completed",
                "matches_count": state.matches_count,
                "message": f"Matching complete, {state.matches_count} matches"
            })
            return
        else:
            # Unexpected status - treat as error
            logger.warning(f"Unexpected status in matching response: {data.get('status')}")
            state.status = "failed"
            state.error = f"Unexpected status from matching: {data.get('status')}"
            state._save_to_redis()
            state.notify({"task_id": task_id, "status": "failed", "error": state.error})
            return

    except asyncio.TimeoutError as e:
        logger.exception(f"Orchestration timeout for task {task_id}")
        state.status = "failed"
        state.error = f"Orchestration timeout: {str(e)}"
        state._save_to_redis()
        state.notify({"task_id": task_id, "status": "failed", "error": state.error})
    except Exception as e:
        logger.exception(f"Orchestration failed for task {task_id}")
        state.status = "failed"
        state.error = str(e)
        state._save_to_redis()
        state.notify({"task_id": task_id, "status": "failed", "error": str(e)})
    finally:
        if redis_client:
            await redis_client.close()
        await state.close()


async def _async_listen(pubsub):
    """Async generator that yields messages from pubsub.
    
    Args:
        pubsub: Redis async pubsub instance
    """
    async for message in pubsub.listen():
        if message["type"] == "message":
            yield message


async def _wait_for_next_message(pubsub, timeout: float = 300.0) -> Optional[dict]:
    """Wait for the next message from pubsub with a timeout.
    
    Args:
        pubsub: Redis async pubsub instance
        timeout: Timeout in seconds (default: 300s)
        
    Returns:
        The message data dict, or None if timeout
        
    Raises:
        asyncio.TimeoutError: If no message received within timeout
    """
    async with asyncio.timeout(timeout):
        async for message in pubsub.listen():
            if message["type"] == "message":
                return json.loads(message["data"])
    return None  # Will raise TimeoutError before reaching here


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "orchestrator"}


@app.get("/metrics")
async def metrics():
    return {"service": "orchestrator", "version": "1.0.0"}


@app.post("/orchestrate/match", response_model=MatchResponse)
async def orchestrate_match_endpoint():
    """Trigger the full pipeline: extraction -> embeddings -> matching."""
    task_id = f"match-{uuid.uuid4().hex[:8]}"
    
    config = load_config()
    resume_file = None
    if config.etl and config.etl.resume:
        resume_file = config.etl.resume.resume_file
    
    if not resume_file:
        return MatchResponse(
            success=False,
            task_id=task_id,
            message="No resume file configured"
        )
    
    asyncio.create_task(orchestrate_match(task_id, resume_file))
    
    return MatchResponse(
        success=True,
        task_id=task_id,
        message="Pipeline started"
    )


@app.get("/orchestrate/status/{task_id}")
async def get_orchestration_status(task_id: str):
    """Get orchestration status via SSE."""

    async def event_generator():
        state = get_or_create_orchestration(task_id)
        queue = state.subscribe()
        try:
            yield f"data: {json.dumps({'task_id': task_id, 'status': state.status})}\n\n"

            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=30.0)
                    if data is None:
                        break
                    yield f"data: {json.dumps(data)}\n\n"

                    if state.status in ["completed", "failed"]:
                        break
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
        finally:
            # Clean up subscriber to prevent memory leak
            state.unsubscribe(queue)
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8084)
