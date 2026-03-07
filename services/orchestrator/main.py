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
import logging
import os
import time
import json
import uuid
from contextlib import asynccontextmanager
from typing import Optional, Tuple
import redis.asyncio as redis_async

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from core.app_context import AppContext
from core.config_loader import load_config
from core.redis_streams import (
    enqueue_job,
    publish_completion,
    ack_message,
    get_task_state,
    set_task_state,
    delete_task_state,
    STREAM_EXTRACTION,
    STREAM_EMBEDDINGS,
    STREAM_MATCHING,
    CHANNEL_EXTRACTION_DONE,
    CHANNEL_EMBEDDINGS_DONE,
    CHANNEL_MATCHING_DONE,
)

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

ctx: AppContext | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ctx
    logger.info("=" * 60)
    logger.info("STARTING ORCHESTRATOR SERVICE")
    logger.info("=" * 60)
    logger.info("🚀 Starting orchestrator service...")
    config = load_config()
    ctx = AppContext.build(config)
    logger.info("✅ Orchestrator service ready")
    logger.info(f"📡 Will subscribe to channels: {CHANNEL_EXTRACTION_DONE}, {CHANNEL_EMBEDDINGS_DONE}, {CHANNEL_MATCHING_DONE}")
    logger.info("=" * 60)

    # Start cleanup task
    cleanup_task = asyncio.create_task(cleanup_stale_orchestrations())

    yield

    # Cancel cleanup task
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        raise  # Re-raise to allow proper cancellation propagation

    logger.info("=" * 60)
    logger.info("SHUTTING DOWN ORCHESTRATOR SERVICE")
    logger.info("=" * 60)
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
        self._subscribers: set[asyncio.Queue] = set()
        
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
                logger.info("Loaded state from Redis for task %s: %s", self.task_id, self.status)
        except Exception as e:
            logger.warning("Failed to load state from Redis: %s", e)

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
            logger.warning("Failed to save state to Redis: %s", e)
    
    def subscribe(self) -> asyncio.Queue:
        queue = asyncio.Queue()
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        """Remove a subscriber queue to prevent memory leaks."""
        self._subscribers.discard(queue)
    
    async def notify(self, data: dict) -> None:
        subscribers = set(self._subscribers)
        for queue in subscribers:
            try:
                await queue.put(data)
            except Exception as e:
                logger.error(f"Failed to notify subscriber: {e}")
                self._subscribers.discard(queue)
    
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
        # Remove from in-memory cache (use lock to avoid race condition)
        async with _orchestration_lock:
            if self.task_id in orchestrations:
                del orchestrations[self.task_id]


orchestrations: dict[str, OrchestrationState] = {}
orchestration_timestamps: dict[str, float] = {}
active_task_ids: set[str] = set()  # Track currently active tasks
orchestration_tasks: dict[str, asyncio.Task] = {}  # Track asyncio.Task objects for cancellation
ORCHESTRATION_TTL = 3600  # 1 hour
LISTENER_TIMEOUT = 300.0  # 5 minutes per stage
_orchestration_lock = asyncio.Lock()


async def cleanup_stale_orchestrations():
    """Periodically clean up old orchestration entries."""
    while True:
        await asyncio.sleep(300)  # Every 5 minutes
        stale_states = []
        async with _orchestration_lock:
            now = time.time()
            stale = [k for k, v in orchestration_timestamps.items()
                     if now - v > ORCHESTRATION_TTL]
            for task_id in stale:
                state = orchestrations.pop(task_id, None)
                if state:
                    stale_states.append(state)
                orchestration_timestamps.pop(task_id, None)
                orchestration_tasks.pop(task_id, None)
                active_task_ids.discard(task_id)
            if stale:
                logger.info(f"Cleaned up {len(stale)} stale orchestrations")

        # Close states outside the lock to avoid deadlock
        for state in stale_states:
            await state.close()


async def get_or_create_orchestration(task_id: str) -> OrchestrationState:
    """Get or create orchestration state (loads from Redis if exists)."""
    async with _orchestration_lock:
        if task_id not in orchestrations:
            orchestrations[task_id] = OrchestrationState(task_id, load_from_redis=True)
        orchestration_timestamps[task_id] = time.time()
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
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise asyncio.TimeoutError(f"Timeout waiting for task {task_id}")
        data = await _wait_for_next_message(pubsub, timeout=remaining)
        if data.get("task_id") == task_id:
            return data
        logger.debug(f"Skipping message for task {data.get('task_id')}, waiting for {task_id}")


async def _run_pipeline_stage(
    state: OrchestrationState,
    pubsub,
    stream: str,
    channel: str,
    job_payload: dict,
    stage_name: str,
    timeout: float
) -> Tuple[bool, Optional[dict]]:
    """Run a single pipeline stage (extract/embed/match).
    
    Args:
        state: Orchestration state
        pubsub: Redis pubsub instance
        stream: Redis stream name
        channel: Completion channel to subscribe to
        job_payload: Payload for the job
        stage_name: Name of stage for logging
        timeout: Timeout in seconds
        
    Returns:
        Tuple of (success, response_data)
    """
    # Subscribe to completion channel
    logger.info("📡 Subscribing to channel: %s", channel)
    await pubsub.subscribe(channel)
    logger.info("📡 Subscribed to channels: %s", pubsub.channels)
    
    # Enqueue the job
    logger.info("📤 Enqueueing %s job to %s", stage_name, stream)
    enqueue_job(stream, job_payload)
    logger.info("✅ %s job enqueued successfully", stage_name.capitalize())
    
    # Wait for completion
    logger.info("⏳ Waiting for %s completion (timeout: %ss)...", stage_name, timeout)
    try:
        data = await _wait_for_task_message(pubsub, state.task_id, timeout)
        logger.info("📨 Received %s response: status=%s", stage_name, data.get("status"))
    except asyncio.TimeoutError:
        logger.error("❌ Timeout waiting for %s completion for task: %s", stage_name, state.task_id)
        state.status = "failed"
        state.error = f"Timeout waiting for {stage_name} completion"
        state._save_to_redis()
        await state.notify({"task_id": state.task_id, "status": "failed", "error": state.error})
        return False, None
    
    # Handle response
    status = data.get("status")
    if status == "failed":
        logger.error("❌ %s failed for task %s: %s", stage_name, state.task_id, data.get("error"))
        state.status = "failed"
        state.error = data.get("error", f"{stage_name.capitalize()} failed")
        state._save_to_redis()
        await state.notify({"task_id": state.task_id, "status": "failed", "error": state.error})
        return False, data
    
    if status not in ("skipped", "completed"):
        logger.warning("❌ Unexpected status in %s response: %s", stage_name, status)
        state.status = "failed"
        state.error = f"Unexpected status from {stage_name}: {status}"
        state._save_to_redis()
        await state.notify({"task_id": state.task_id, "status": "failed", "error": state.error})
        return False, data
    
    return True, data


async def _switch_channel(pubsub, old_channel: str, new_channel: str):
    """Switch pubsub subscription from one channel to another."""
    logger.info("📡 Unsubscribing from %s", old_channel)
    await pubsub.unsubscribe(old_channel)
    logger.info("📡 Subscribing to channel: %s", new_channel)
    await pubsub.subscribe(new_channel)
    logger.info("📡 Now subscribed to channels: %s", pubsub.channels)


async def orchestrate_match(task_id: str, resume_file: str):
    """Run the orchestration flow: extraction -> embeddings -> matching."""
    import redis.asyncio as redis_async
    global active_task_ids
    async with _orchestration_lock:
        active_task_ids.add(task_id)

    state = await get_or_create_orchestration(task_id)
    state.status = "extracting"
    state.resume_file = resume_file
    state._save_to_redis()
    await state.notify({"task_id": task_id, "status": "extracting", "message": "Starting extraction"})

    redis_client = None
    pubsub = None
    try:
        logger.info("🚀 Starting pipeline for task: %s", task_id)

        # Create Redis client and pubsub BEFORE enqueueing job
        redis_client = redis_async.from_url(REDIS_URL, decode_responses=True)
        pubsub = redis_client.pubsub()

        # Stage 1: Extraction
        success, extraction_data = await _run_pipeline_stage(
            state=state,
            pubsub=pubsub,
            stream=STREAM_EXTRACTION,
            channel=CHANNEL_EXTRACTION_DONE,
            job_payload={"task_id": task_id, "resume_file": resume_file},
            stage_name="extraction",
            timeout=LISTENER_TIMEOUT
        )
        if not success:
            return
        
        # Validate extraction response
        fp = extraction_data.get("resume_fingerprint")
        if not fp and extraction_data.get("status") != "skipped":
            logger.error("❌ No fingerprint in extraction response for task: %s", task_id)
            state.status = "failed"
            state.error = "No fingerprint in extraction response"
            state._save_to_redis()
            await state.notify({"task_id": task_id, "status": "failed", "error": state.error})
            return
        
        if fp:
            state.resume_fingerprint = fp
            status_msg = "Resume unchanged, using existing" if extraction_data.get("status") == "skipped" else "Extraction complete"
            logger.info("ℹ️ %s: %s...", status_msg, fp[:16])
        
        # Stage 2: Embeddings
        state.status = "embedding"
        state._save_to_redis()
        await state.notify({"task_id": task_id, "status": "embedding", "message": "Starting embeddings"})
        
        await _switch_channel(pubsub, CHANNEL_EXTRACTION_DONE, CHANNEL_EMBEDDINGS_DONE)
        
        success, embeddings_data = await _run_pipeline_stage(
            state=state,
            pubsub=pubsub,
            stream=STREAM_EMBEDDINGS,
            channel=CHANNEL_EMBEDDINGS_DONE,
            job_payload={"task_id": task_id, "resume_fingerprint": state.resume_fingerprint},
            stage_name="embeddings",
            timeout=LISTENER_TIMEOUT
        )
        if not success:
            return
        
        # Stage 3: Matching
        state.status = "matching"
        state._save_to_redis()
        await state.notify({"task_id": task_id, "status": "matching", "message": "Starting matching"})
        
        await _switch_channel(pubsub, CHANNEL_EMBEDDINGS_DONE, CHANNEL_MATCHING_DONE)
        
        success, matching_data = await _run_pipeline_stage(
            state=state,
            pubsub=pubsub,
            stream=STREAM_MATCHING,
            channel=CHANNEL_MATCHING_DONE,
            job_payload={"task_id": task_id, "resume_fingerprint": state.resume_fingerprint},
            stage_name="matching",
            timeout=LISTENER_TIMEOUT
        )
        if not success:
            return
        
        # Pipeline completed successfully
        state.status = "completed"
        state.matches_count = matching_data.get("matches_count", 0)
        state._save_to_redis()
        logger.info("🎉 Pipeline completed successfully for task %s: %s matches", task_id, state.matches_count)
        await state.notify({
            "task_id": task_id,
            "status": "completed",
            "matches_count": state.matches_count,
            "message": f"Matching complete, {state.matches_count} matches"
        })

    except asyncio.TimeoutError as e:
        logger.exception(f"❌ Orchestration timeout for task {task_id}")
        state.status = "failed"
        state.error = f"Orchestration timeout: {str(e)}"
        state._save_to_redis()
        await state.notify({"task_id": task_id, "status": "failed", "error": state.error})
    except Exception as e:
        logger.exception(f"❌ Orchestration failed for task {task_id}")
        state.status = "failed"
        state.error = str(e)
        state._save_to_redis()
        await state.notify({"task_id": task_id, "status": "failed", "error": str(e)})
    finally:
        if redis_client:
            if pubsub:
                try:
                    logger.info("📡 Unsubscribing from all channels")
                    await pubsub.unsubscribe()
                    await pubsub.close()
                except Exception as e:
                    logger.warning(f"Failed to close pubsub: {e}")
            try:
                await redis_client.close()
            except Exception as e:
                logger.warning(f"Failed to close Redis client: {e}")
        # Clear active task if this was the active one
        async with _orchestration_lock:
            active_task_ids.discard(task_id)
        await state.close()


async def _async_listen(pubsub):
    """Async generator that yields messages from pubsub.
    
    Args:
        pubsub: Redis async pubsub instance
    """
    async for message in pubsub.listen():
        if message["type"] == "message":
            yield message


async def _wait_for_next_message(pubsub, timeout: float = 300.0) -> dict:
    """Wait for the next message from pubsub with a timeout.

    Args:
        pubsub: Redis async pubsub instance
        timeout: Timeout in seconds (default: 300s)

    Returns:
        The message data dict (never None)

    Raises:
        asyncio.TimeoutError: If no message received within timeout
    """
    async with asyncio.timeout(timeout):
        async for message in pubsub.listen():
            if message["type"] == "message":
                return json.loads(message["data"])
    raise asyncio.TimeoutError("No message received")


@app.get("/health")
async def health():
    """Health check endpoint with Redis connectivity verification."""
    from core.redis_streams import get_redis_client
    import redis
    
    redis_status = "unknown"
    try:
        client = get_redis_client()
        client.ping()
        redis_status = "connected"
    except redis.ConnectionError as e:
        redis_status = f"connection_error: {e}"
    except Exception as e:
        redis_status = f"error: {e}"
    
    return {
        "status": "healthy",
        "service": "orchestrator",
        "redis": redis_status,
        "active_tasks": len(active_task_ids)
    }


@app.get("/metrics")
async def metrics():
    return {"service": "orchestrator", "version": "1.0.0"}


async def _handle_task_done(task_id: str, t: asyncio.Task):
    """Async helper to handle task completion/failure with proper locking."""
    if t.cancelled():
        logger.info(f"Orchestration cancelled: {task_id}")
        async with _orchestration_lock:
            state = orchestrations.get(task_id)
            if state:
                if state.status not in ("completed", "failed", "cancelled"):
                    state.status = "cancelled"
                    state.error = "Task cancelled"
                    state._save_to_redis()
    elif t.exception():
        logger.error(f"Orchestration failed: {task_id} - {t.exception()}")
        async with _orchestration_lock:
            state = orchestrations.get(task_id)
            if state:
                if state.status not in ("completed", "failed", "cancelled"):
                    state.status = "failed"
                    state.error = str(t.exception())
                    state._save_to_redis()
                    await state.notify({"task_id": task_id, "status": "failed", "error": state.error})
    else:
        logger.info(f"Orchestration completed successfully: {task_id}")
    
    # Remove from task registry
    async with _orchestration_lock:
        if task_id in orchestration_tasks:
            del orchestration_tasks[task_id]


@app.post("/orchestrate/match", response_model=MatchResponse)
async def orchestrate_match_endpoint():
    """Trigger the full pipeline: extraction -> embeddings -> matching."""
    logger.info("=" * 60)
    logger.info("📨 HTTP POST /orchestrate/match received")
    logger.info("=" * 60)
    
    task_id = f"match-{uuid.uuid4().hex[:8]}"
    logger.info(f"🆔 Created task: {task_id}")

    config = load_config()
    resume_file = None
    if config.etl and config.etl.resume:
        resume_file = config.etl.resume.resume_file

    if not resume_file:
        logger.error("❌ No resume file configured in config")
        return MatchResponse(
            success=False,
            task_id=task_id,
            message="No resume file configured"
        )

    logger.info("📄 Using resume file: %s", resume_file)
    logger.info("🚀 Creating orchestration task...")

    task = asyncio.create_task(orchestrate_match(task_id, resume_file))

    def safe_done_callback(t: asyncio.Task) -> None:
        try:
            cleanup_task = asyncio.create_task(_handle_task_done(task_id, t))
            cleanup_task.add_done_callback(lambda t: None)  # Prevent unhandled exception
        except RuntimeError:
            logger.warning("Could not handle task completion for %s: no running loop", task_id)

    task.add_done_callback(safe_done_callback)

    # Store task reference for potential cancellation
    async with _orchestration_lock:
        orchestration_tasks[task_id] = task

    return MatchResponse(
        success=True,
        task_id=task_id,
        message="Pipeline started"
    )


@app.get("/orchestrate/status/{task_id}")
async def get_orchestration_status(task_id: str):
    """Get orchestration status via SSE."""

    async def event_generator():
        state = await get_or_create_orchestration(task_id)
        queue = state.subscribe()
        try:
            yield f"data: {json.dumps({'task_id': task_id, 'status': state.status})}\n\n"

            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=30.0)
                    if data is None:
                        break
                    yield f"data: {json.dumps(data)}\n\n"

                    if state.status in ["completed", "failed", "cancelled"]:
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


@app.get("/orchestrate/active")
async def get_active_orchestration():
    """Get the currently active orchestration tasks."""
    global active_task_ids
    async with _orchestration_lock:
        task_ids = list(active_task_ids)

    if not task_ids:
        return {"success": False, "message": "No active tasks"}

    states = []
    for task_id in task_ids:
        state = await get_or_create_orchestration(task_id)
        states.append({
            "task_id": task_id,
            "status": state.status,
            "resume_fingerprint": state.resume_fingerprint,
            "matches_count": state.matches_count,
            "error": state.error
        })

    return {"success": True, "tasks": states}


@app.get("/orchestrate/diagnostics")
async def get_diagnostics():
    """Get diagnostics for Redis streams and consumer groups.
    
    This endpoint provides visibility into:
    - Stream lengths and pending messages
    - Consumer group status
    - Active orchestrations
    - Recent task states
    """
    from core.redis_streams import get_redis_client, stream_exists, get_stream_info
    
    redis_client = get_redis_client()
    
    # Get stream info for all streams
    streams_info = {}
    for stream_name in [STREAM_EXTRACTION, STREAM_EMBEDDINGS, STREAM_MATCHING]:
        try:
            if stream_exists(stream_name):
                info = get_stream_info(stream_name)
                streams_info[stream_name] = {
                    "exists": True,
                    "length": info.get("length", 0),
                    "first_entry": info.get("first-entry"),
                    "last_entry": info.get("last-entry"),
                    "groups": info.get("groups", 0),
                }
                
                # Get consumer group info
                try:
                    groups = redis_client.xinfo_groups(stream_name)
                    streams_info[stream_name]["consumer_groups"] = [
                        {
                            "name": g.get("name"),
                            "consumers": g.get("consumers", 0),
                            "pending": g.get("pending", 0),
                            "last_delivered_id": g.get("last-delivered-id"),
                        }
                        for g in groups
                    ]
                except Exception as e:
                    streams_info[stream_name]["consumer_groups_error"] = str(e)
            else:
                streams_info[stream_name] = {"exists": False, "length": 0}
        except Exception as e:
            streams_info[stream_name] = {"error": str(e)}
    
    # Get active orchestrations
    async with _orchestration_lock:
        active_task_ids_list = list(active_task_ids)
        active_states = []
        for task_id in active_task_ids_list:
            if task_id in orchestrations:
                state = orchestrations[task_id]
                active_states.append({
                    "task_id": task_id,
                    "status": state.status,
                    "error": state.error,
                })
    
    # Get recent task states from Redis
    recent_tasks = []
    try:
        keys = redis_client.keys("task:*:state")
        for key in keys[:10]:  # Limit to 10 most recent
            task_id = key.split(":")[1]
            task_data = get_task_state(task_id)
            if task_data:
                recent_tasks.append({
                    "task_id": task_id,
                    "status": task_data.get("status"),
                    "error": task_data.get("error"),
                })
    except Exception as e:
        recent_tasks = {"error": str(e)}
    
    return {
        "success": True,
        "timestamp": time.time(),
        "streams": streams_info,
        "active_orchestrations": active_states,
        "recent_tasks": recent_tasks,
        "active_task_count": len(active_task_ids_list),
    }


@app.post("/orchestrate/stop")
async def stop_orchestration(task_id: str = None):
    """Stop the currently active orchestration task(s)."""
    global active_task_ids
    async with _orchestration_lock:
        if task_id:
            task_ids_to_stop = [task_id] if task_id in active_task_ids else []
        else:
            task_ids_to_stop = list(active_task_ids)
    
    if not task_ids_to_stop:
        return {"success": False, "message": "No active tasks to stop"}
    
    stopped = []
    for task_id in task_ids_to_stop:
        async with _orchestration_lock:
            if task_id in orchestration_tasks:
                task = orchestration_tasks[task_id]
                if not task.done():
                    task.cancel()
                    stopped.append(task_id)
                    continue
        
        state = await get_or_create_orchestration(task_id)
        if state.status not in ("completed", "failed", "cancelled"):
            state.status = "cancelled"
            state.error = "Cancelled by user"
            state._save_to_redis()
            await state.notify({"task_id": task_id, "status": "cancelled", "error": "Cancelled by user"})
            stopped.append(task_id)
    
    return {
        "success": True,
        "stopped": stopped,
        "message": f"Cancelled {len(stopped)} task(s)"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8084)
