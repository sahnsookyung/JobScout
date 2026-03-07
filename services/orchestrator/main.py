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
import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional, Tuple

import redis.asyncio as redis_async
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from core.app_context import AppContext
from core.config_loader import load_config
from core.redis_streams import (
    enqueue_job,
    get_redis_client,
    get_stream_info,
    get_task_state,
    set_task_state,
    delete_task_state,
    stream_exists,
    STREAM_EXTRACTION,
    STREAM_EMBEDDINGS,
    STREAM_MATCHING,
    CHANNEL_EXTRACTION_DONE,
    CHANNEL_EMBEDDINGS_DONE,
    CHANNEL_MATCHING_DONE,
)

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ORCHESTRATION_TTL = 3600  # 1 hour
LISTENER_TIMEOUT = 300.0  # 5 minutes per stage


# ---------------------------------------------------------------------------
# Registry — replaces all module-level mutable globals
# ---------------------------------------------------------------------------

class OrchestratorRegistry:
    """Single container for all mutable orchestration state."""

    def __init__(self) -> None:
        self.orchestrations: dict[str, "OrchestrationState"] = {}
        self.timestamps: dict[str, float] = {}
        self.active_task_ids: set[str] = set()
        self.tasks: dict[str, asyncio.Task] = {}
        self.lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 60)
    logger.info("STARTING ORCHESTRATOR SERVICE")
    logger.info("=" * 60)

    config = load_config()
    app.state.ctx = AppContext.build(config)
    app.state.registry = OrchestratorRegistry()

    logger.info("✅ Orchestrator service ready")
    logger.info(
        "📡 Will subscribe to channels: %s, %s, %s",
        CHANNEL_EXTRACTION_DONE, CHANNEL_EMBEDDINGS_DONE, CHANNEL_MATCHING_DONE,
    )
    logger.info("=" * 60)

    cleanup_task = asyncio.create_task(
        cleanup_stale_orchestrations(app.state.registry)
    )

    yield

    cleanup_task.cancel()
    await asyncio.gather(cleanup_task, return_exceptions=True)

    # Tear down AppContext — try async first, fall back to sync
    ctx: AppContext = app.state.ctx
    if hasattr(ctx, "aclose"):
        await ctx.aclose()
    elif hasattr(ctx, "close"):
        ctx.close()

    logger.info("=" * 60)
    logger.info("SHUTTING DOWN ORCHESTRATOR SERVICE")
    logger.info("=" * 60)


app = FastAPI(
    title="Orchestrator Service",
    description="Coordinates extraction => embedding => matching pipeline",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class MatchResponse(BaseModel):
    success: bool
    task_id: str
    message: str


# ---------------------------------------------------------------------------
# OrchestrationState
# ---------------------------------------------------------------------------

class OrchestrationState:
    """Tracks the state of an orchestration task with Redis persistence."""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        self.status = "pending"
        self.resume_fingerprint: Optional[str] = None
        self.resume_file: Optional[str] = None
        self.matches_count: int = 0
        self.error: Optional[str] = None
        self._subscribers: set[asyncio.Queue] = set()

    @classmethod
    async def create(cls, task_id: str, load_from_redis: bool = True) -> "OrchestrationState":
        """Async factory: construct and optionally hydrate from Redis."""
        instance = cls(task_id)
        if load_from_redis:
            await instance._load_from_redis()
        return instance

    async def _load_from_redis(self) -> None:
        try:
            data = await asyncio.to_thread(get_task_state, self.task_id)
            if data:
                self.status = data.get("status", "pending")
                self.resume_fingerprint = data.get("resume_fingerprint")
                self.resume_file = data.get("resume_file")
                self.matches_count = data.get("matches_count", 0)
                self.error = data.get("error")
                logger.info("Loaded state from Redis for task: %s", self.task_id)
        except Exception:
            logger.warning("Failed to load state from Redis for task: %s", self.task_id)

    async def _save_to_redis(self) -> None:
        try:
            await asyncio.to_thread(set_task_state, self.task_id, {
                "status": self.status,
                "resume_fingerprint": self.resume_fingerprint,
                "resume_file": self.resume_file,
                "matches_count": self.matches_count,
                "error": self.error,
            })
        except Exception as e:
            logger.warning("Failed to save state to Redis: %s", e)

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._subscribers.discard(queue)

    async def notify(self, data: dict) -> None:
        for queue in set(self._subscribers):
            try:
                await queue.put(data)
            except Exception as e:
                logger.error("Failed to notify subscriber: %s", e)
                self._subscribers.discard(queue)

    async def close(self, registry: OrchestratorRegistry) -> None:
        for queue in self._subscribers:
            await queue.put(None)
        if self.status not in ("completed", "failed"):
            try:
                await asyncio.to_thread(delete_task_state, self.task_id)
            except Exception as e:
                logger.warning("Failed to delete state from Redis: %s", e)
        async with registry.lock:
            registry.orchestrations.pop(self.task_id, None)


# ---------------------------------------------------------------------------
# Registry helpers
# ---------------------------------------------------------------------------

async def get_or_create_orchestration(
    registry: OrchestratorRegistry, task_id: str
) -> OrchestrationState:
    """Get or create orchestration state, loading from Redis if not in memory."""
    async with registry.lock:
        if task_id in registry.orchestrations:
            registry.timestamps[task_id] = time.time()
            return registry.orchestrations[task_id]

    # Create outside the lock to avoid blocking the event loop during Redis I/O
    state = await OrchestrationState.create(task_id, load_from_redis=True)

    async with registry.lock:
        # Double-check: another coroutine may have inserted it while we were loading
        if task_id not in registry.orchestrations:
            registry.orchestrations[task_id] = state
        registry.timestamps[task_id] = time.time()
        return registry.orchestrations[task_id]


async def cleanup_stale_orchestrations(registry: OrchestratorRegistry) -> None:
    """Periodically remove orchestrations that have exceeded ORCHESTRATION_TTL."""
    while True:
        await asyncio.sleep(300)
        stale_states: list[OrchestrationState] = []
        async with registry.lock:
            now = time.time()
            stale = [k for k, v in registry.timestamps.items() if now - v > ORCHESTRATION_TTL]
            for task_id in stale:
                state = registry.orchestrations.pop(task_id, None)
                if state:
                    stale_states.append(state)
                registry.timestamps.pop(task_id, None)
                registry.tasks.pop(task_id, None)
                registry.active_task_ids.discard(task_id)
            if stale:
                logger.info("Cleaned up %d stale orchestrations", len(stale))

        # Close states outside the lock to avoid deadlock inside state.close()
        for state in stale_states:
            await state.close(registry)


# ---------------------------------------------------------------------------
# Pipeline helpers
# ---------------------------------------------------------------------------

async def _wait_for_next_message(pubsub) -> dict:
    """Read the next pubsub message. Caller owns the timeout."""
    async for message in pubsub.listen():
        if message["type"] == "message":
            return json.loads(message["data"])


async def _wait_for_task_message(pubsub, task_id: str) -> dict:
    """Skip messages until one matches task_id. Caller owns the timeout."""
    while True:
        data = await _wait_for_next_message(pubsub)
        if data.get("task_id") == task_id:
            return data
        logger.debug(
            "Skipping message for task %s, waiting for %s",
            data.get("task_id"), task_id,
        )


async def _run_pipeline_stage(
    state: OrchestrationState,
    pubsub,
    stream: str,
    job_payload: dict,
    stage_name: str,
) -> Tuple[bool, Optional[dict]]:
    """Enqueue a job and wait for its completion notification.

    The caller is responsible for subscribing pubsub to the correct channel
    and wrapping this call in asyncio.timeout().
    """
    logger.info("📤 Enqueueing %s job to %s", stage_name, stream)
    await asyncio.to_thread(enqueue_job, stream, job_payload)
    logger.info("✅ %s job enqueued", stage_name.capitalize())

    logger.info("⏳ Waiting for %s completion...", stage_name)
    data = await _wait_for_task_message(pubsub, state.task_id)
    logger.info("📨 %s response: status=%s", stage_name, data.get("status"))

    status = data.get("status")
    if status == "failed":
        logger.error("❌ %s failed for task %s: %s", stage_name, state.task_id, data.get("error"))
        state.status = "failed"
        state.error = data.get("error", f"{stage_name.capitalize()} failed")
        await state._save_to_redis()
        await state.notify({"task_id": state.task_id, "status": "failed", "error": state.error})
        return False, data

    if status not in ("skipped", "completed"):
        logger.warning("❌ Unexpected status in %s response: %s", stage_name, status)
        state.status = "failed"
        state.error = f"Unexpected status from {stage_name}: {status}"
        await state._save_to_redis()
        await state.notify({"task_id": state.task_id, "status": "failed", "error": state.error})
        return False, data

    return True, data


async def _handle_extraction_fingerprint(
    state: OrchestrationState, task_id: str, extraction_data: dict
) -> bool:
    """Validate and store the extraction fingerprint. Returns False if pipeline should abort."""
    fp = extraction_data.get("resume_fingerprint")

    if not fp and extraction_data.get("status") != "skipped":
        logger.error("❌ No fingerprint in extraction response for task: %s", task_id)
        state.status = "failed"
        state.error = "No fingerprint in extraction response"
        await state._save_to_redis()
        await state.notify({"task_id": task_id, "status": "failed", "error": state.error})
        return False

    if fp:
        state.resume_fingerprint = fp
        status_msg = (
            "Resume unchanged, using existing"
            if extraction_data.get("status") == "skipped"
            else "Extraction complete"
        )
        logger.info("ℹ️  %s: %s...", status_msg, fp[:16])

    return True


async def _cleanup_pubsub_and_client(redis_client, pubsub) -> None:
    """Close pubsub and Redis client, swallowing errors so finally blocks never raise."""
    if pubsub:
        try:
            await pubsub.unsubscribe()
            await pubsub.close()
        except Exception as e:
            logger.warning("Failed to close pubsub: %s", e)
    try:
        await redis_client.aclose()
    except Exception as e:
        logger.warning("Failed to close Redis client: %s", e)


# ---------------------------------------------------------------------------
# Core orchestration coroutine
# ---------------------------------------------------------------------------

async def orchestrate_match(
    task_id: str, resume_file: str, registry: OrchestratorRegistry
) -> None:
    """Run the full pipeline: extraction -> embeddings -> matching."""
    async with registry.lock:
        registry.active_task_ids.add(task_id)

    state = await get_or_create_orchestration(registry, task_id)
    state.status = "extracting"
    state.resume_file = resume_file
    await state._save_to_redis()
    await state.notify({"task_id": task_id, "status": "extracting", "message": "Starting extraction"})

    redis_client = None
    pubsub = None
    try:
        logger.info("🚀 Starting pipeline for task: %s", task_id)
        redis_client = redis_async.from_url(REDIS_URL, decode_responses=True)
        pubsub = redis_client.pubsub()

        # ── Stage 1: Extraction ──────────────────────────────────────────
        logger.info("📡 Subscribing to %s", CHANNEL_EXTRACTION_DONE)
        await pubsub.subscribe(CHANNEL_EXTRACTION_DONE)

        async with asyncio.timeout(LISTENER_TIMEOUT):
            success, extraction_data = await _run_pipeline_stage(
                state=state, pubsub=pubsub,
                stream=STREAM_EXTRACTION,
                job_payload={"task_id": task_id, "resume_file": resume_file},
                stage_name="extraction",
            )
        if not success:
            return
        if not await _handle_extraction_fingerprint(state, task_id, extraction_data):
            return

        # ── Stage 2: Embeddings ──────────────────────────────────────────
        state.status = "embedding"
        await state._save_to_redis()
        await state.notify({"task_id": task_id, "status": "embedding", "message": "Starting embeddings"})

        await pubsub.unsubscribe(CHANNEL_EXTRACTION_DONE)
        await pubsub.subscribe(CHANNEL_EMBEDDINGS_DONE)
        logger.info("📡 Switched to %s", CHANNEL_EMBEDDINGS_DONE)

        async with asyncio.timeout(LISTENER_TIMEOUT):
            success, _ = await _run_pipeline_stage(
                state=state, pubsub=pubsub,
                stream=STREAM_EMBEDDINGS,
                job_payload={"task_id": task_id, "resume_fingerprint": state.resume_fingerprint},
                stage_name="embeddings",
            )
        if not success:
            return

        # ── Stage 3: Matching ────────────────────────────────────────────
        state.status = "matching"
        await state._save_to_redis()
        await state.notify({"task_id": task_id, "status": "matching", "message": "Starting matching"})

        await pubsub.unsubscribe(CHANNEL_EMBEDDINGS_DONE)
        await pubsub.subscribe(CHANNEL_MATCHING_DONE)
        logger.info("📡 Switched to %s", CHANNEL_MATCHING_DONE)

        async with asyncio.timeout(LISTENER_TIMEOUT):
            success, matching_data = await _run_pipeline_stage(
                state=state, pubsub=pubsub,
                stream=STREAM_MATCHING,
                job_payload={"task_id": task_id, "resume_fingerprint": state.resume_fingerprint},
                stage_name="matching",
            )
        if not success:
            return

        state.status = "completed"
        state.matches_count = matching_data.get("matches_count", 0)
        await state._save_to_redis()
        logger.info("🎉 Pipeline completed for task %s: %d matches", task_id, state.matches_count)
        await state.notify({
            "task_id": task_id,
            "status": "completed",
            "matches_count": state.matches_count,
            "message": f"Matching complete, {state.matches_count} matches",
        })

    except asyncio.TimeoutError:
        # Fired by one of the asyncio.timeout() blocks above
        logger.exception("❌ Orchestration timeout for task %s", task_id)
        state.status = "failed"
        state.error = "Stage timeout"
        await state._save_to_redis()
        await state.notify({"task_id": task_id, "status": "failed", "error": state.error})

    except Exception as e:
        logger.exception("❌ Orchestration failed for task %s", task_id)
        state.status = "failed"
        state.error = str(e)
        await state._save_to_redis()
        await state.notify({"task_id": task_id, "status": "failed", "error": str(e)})

    finally:
        if redis_client:
            await _cleanup_pubsub_and_client(redis_client, pubsub)
        async with registry.lock:
            registry.active_task_ids.discard(task_id)
        await state.close(registry)


# ---------------------------------------------------------------------------
# Task-done callback
# ---------------------------------------------------------------------------

async def _handle_task_done(
    task_id: str, t: asyncio.Task, registry: OrchestratorRegistry
) -> None:
    """Handle asyncio.Task completion: update state and clean up registry."""
    if t.cancelled():
        logger.info("Orchestration cancelled: %s", task_id)
        async with registry.lock:
            state = registry.orchestrations.get(task_id)
        if state and state.status not in ("completed", "failed", "cancelled"):
            state.status = "cancelled"
            state.error = "Task cancelled"
            await state._save_to_redis()
    elif t.exception():
        logger.error("Orchestration failed: %s - %s", task_id, t.exception())
        async with registry.lock:
            state = registry.orchestrations.get(task_id)
        if state and state.status not in ("completed", "failed", "cancelled"):
            state.status = "failed"
            state.error = str(t.exception())
            await state._save_to_redis()
            await state.notify({"task_id": task_id, "status": "failed", "error": state.error})
    else:
        logger.info("Orchestration completed successfully: %s", task_id)

    async with registry.lock:
        registry.tasks.pop(task_id, None)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health(request: Request):
    """Health check endpoint with Redis connectivity verification."""
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

    registry: OrchestratorRegistry = request.app.state.registry
    async with registry.lock:
        active_count = len(registry.active_task_ids)

    return {
        "status": "healthy",
        "service": "orchestrator",
        "redis": redis_status,
        "active_tasks": active_count,
    }


@app.get("/metrics")
async def metrics():
    return {"service": "orchestrator", "version": "1.0.0"}


@app.post("/orchestrate/match", response_model=MatchResponse)
async def orchestrate_match_endpoint(request: Request):
    """Trigger the full pipeline: extraction -> embeddings -> matching."""
    logger.info("=" * 60)
    logger.info("📨 HTTP POST /orchestrate/match received")
    logger.info("=" * 60)

    task_id = f"match-{uuid.uuid4().hex[:8]}"
    logger.info("🆔 Created task: %s", task_id)

    ctx: AppContext = request.app.state.ctx
    registry: OrchestratorRegistry = request.app.state.registry

    resume_file: Optional[str] = None
    if ctx.config.etl and ctx.config.etl.resume:
        resume_file = ctx.config.etl.resume.resume_file

    if not resume_file:
        logger.error("❌ No resume file configured in config")
        return MatchResponse(success=False, task_id=task_id, message="No resume file configured")

    logger.info("📄 Using resume file: %s", resume_file)
    logger.info("🚀 Creating orchestration task...")

    task = asyncio.create_task(orchestrate_match(task_id, resume_file, registry))

    def safe_done_callback(t: asyncio.Task) -> None:
        try:
            cb_task = asyncio.create_task(_handle_task_done(task_id, t, registry))
            cb_task.add_done_callback(lambda _: None)
        except RuntimeError:
            logger.warning("Could not handle task completion for %s: no running loop", task_id)

    task.add_done_callback(safe_done_callback)

    async with registry.lock:
        registry.tasks[task_id] = task

    return MatchResponse(success=True, task_id=task_id, message="Pipeline started")


@app.get("/orchestrate/status/{task_id}")
async def get_orchestration_status(task_id: str, request: Request):
    """Get orchestration status via SSE."""
    registry: OrchestratorRegistry = request.app.state.registry

    async def event_generator():
        state = await get_or_create_orchestration(registry, task_id)
        queue = state.subscribe()
        try:
            yield f"data: {json.dumps({'task_id': task_id, 'status': state.status})}\n\n"
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=30.0)
                    if data is None:
                        break
                    yield f"data: {json.dumps(data)}\n\n"
                    if state.status in ("completed", "failed", "cancelled"):
                        break
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
        finally:
            state.unsubscribe(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/orchestrate/active")
async def get_active_orchestration(request: Request):
    """Get all currently active orchestration tasks."""
    registry: OrchestratorRegistry = request.app.state.registry
    async with registry.lock:
        task_ids = list(registry.active_task_ids)

    if not task_ids:
        return {"success": False, "message": "No active tasks"}

    states = []
    for tid in task_ids:
        state = await get_or_create_orchestration(registry, tid)
        states.append({
            "task_id": tid,
            "status": state.status,
            "resume_fingerprint": state.resume_fingerprint,
            "matches_count": state.matches_count,
            "error": state.error,
        })

    return {"success": True, "tasks": states}


def _get_stream_diagnostic(stream_name: str) -> dict:
    """Return status dict for a single Redis stream."""
    try:
        if not stream_exists(stream_name):
            return {"exists": False, "length": 0}

        info = get_stream_info(stream_name)
        result = {
            "exists": True,
            "length": info.get("length", 0),
            "first_entry": info.get("first-entry"),
        }
        try:
            groups = info.get("groups", [])
            result["consumer_groups"] = [
                {
                    "name": g.get("name"),
                    "consumers": g.get("consumers", 0),
                    "pending": g.get("pending", 0),
                    "last_delivered_id": g.get("last-delivered-id"),
                }
                for g in groups
            ]
        except Exception as e:
            result["consumer_groups_error"] = str(e)

        return result
    except Exception as e:
        return {"error": str(e)}


async def _get_active_orchestration_states(registry: OrchestratorRegistry) -> list:
    """Return status snapshots of all currently active orchestrations."""
    async with registry.lock:
        return [
            {
                "task_id": task_id,
                "status": registry.orchestrations[task_id].status,
                "error": registry.orchestrations[task_id].error,
            }
            for task_id in registry.active_task_ids
            if task_id in registry.orchestrations
        ]


def _get_recent_tasks(redis_client) -> list | dict:
    """Return status snapshots of the 10 most recent tasks from Redis."""
    try:
        keys = redis_client.keys("task:*:state")
        recent = []
        for key in keys[:10]:
            task_id = key.split(":")[1]
            task_data = get_task_state(task_id)
            if task_data:
                recent.append({
                    "task_id": task_id,
                    "status": task_data.get("status"),
                    "error": task_data.get("error"),
                })
        return recent
    except Exception as e:
        return {"error": str(e)}


@app.get("/orchestrate/diagnostics")
async def get_diagnostics(request: Request):
    """Get diagnostics for Redis streams, consumer groups, and active tasks."""
    registry: OrchestratorRegistry = request.app.state.registry
    redis_client = get_redis_client()
    active_states = await _get_active_orchestration_states(registry)

    return {
        "success": True,
        "timestamp": time.time(),
        "streams": {
            stream_name: _get_stream_diagnostic(stream_name)
            for stream_name in [STREAM_EXTRACTION, STREAM_EMBEDDINGS, STREAM_MATCHING]
        },
        "active_orchestrations": active_states,
        "recent_tasks": _get_recent_tasks(redis_client),
        "active_task_count": len(active_states),
    }


@app.post("/orchestrate/stop")
async def stop_orchestration(request: Request, task_id: Optional[str] = None):
    """Stop one or all active orchestration tasks."""
    registry: OrchestratorRegistry = request.app.state.registry

    async with registry.lock:
        if task_id:
            task_ids_to_stop = [task_id] if task_id in registry.active_task_ids else []
        else:
            task_ids_to_stop = list(registry.active_task_ids)

    if not task_ids_to_stop:
        return {"success": False, "message": "No active tasks to stop"}

    stopped = []
    for tid in task_ids_to_stop:
        async with registry.lock:
            task = registry.tasks.get(tid)
        if task and not task.done():
            task.cancel()
            stopped.append(tid)
            continue

        state = await get_or_create_orchestration(registry, tid)
        if state.status not in ("completed", "failed", "cancelled"):
            state.status = "cancelled"
            state.error = "Cancelled by user"
            await state._save_to_redis()
            await state.notify({"task_id": tid, "status": "cancelled", "error": "Cancelled by user"})
            stopped.append(tid)

    return {"success": True, "stopped": stopped, "message": f"Cancelled {len(stopped)} task(s)"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8084)
