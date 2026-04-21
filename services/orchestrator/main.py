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
import inspect
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Annotated, Optional, Tuple, Dict, Any, List

import redis.asyncio as redis_async
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.app_context import AppContext
from core.auth import _auth_mode, _ensure_dev_bypass_allowed, _ensure_dev_user
from core.config_loader import load_config
from core.metrics_router import router as metrics_router
from core.redis_streams import (
    _sanitize_log,
    enqueue_job,
    get_redis_client,
    get_stream_info,
    get_task_state,
    set_task_state,
    delete_task_state,
    stream_exists,
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
from core.resume_selection import evaluate_resume_eligibility, resolve_owner_id
import redis  # used in /health
from database.init_db import init_db

from core.logging_utils import setup_service_logging
logger = logging.getLogger(__name__)
setup_service_logging(logger)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ORCHESTRATION_TTL = 3600  # 1 hour
LISTENER_TIMEOUT = float(os.getenv("LISTENER_TIMEOUT_SECONDS", "300"))  # 5 minutes per stage

# Scraper configuration
SCRAPER_INTERVAL_HOURS = float(os.getenv("SCRAPER_INTERVAL_HOURS", "6"))
SCRAPER_LOCK_TTL_SECONDS = 30 * 60  # 30 minutes
SCRAPER_RETRY_INTERVALS = [1, 6, 60, 600, 6000]  # Exponential backoff in seconds
SCRAPER_EXTRACTION_LIMIT = int(os.getenv("SCRAPER_EXTRACTION_LIMIT", "200"))
SCRAPER_EMBEDDING_LIMIT = int(os.getenv("SCRAPER_EMBEDDING_LIMIT", "100"))
BATCH_STAGE_TIMEOUT_SECONDS = float(os.getenv("BATCH_STAGE_TIMEOUT_SECONDS", "600"))

# Holds references to fire-and-forget background tasks to prevent premature GC.
_etl_tasks: set = set()

# Lua script for safe lock release (verifies ownership before deleting)
RELEASE_LOCK_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


# ---------------------------------------------------------------------------
# Registry — replaces all module-level mutable globals
# ---------------------------------------------------------------------------


class OrchestratorRegistry:
    """Single container for all mutable orchestration state."""

    def __init__(self) -> None:
        self.orchestrations: Dict[str, "OrchestrationState"] = {}
        self.timestamps: Dict[str, float] = {}
        self.active_task_ids: set[str] = set()
        self.tasks: Dict[str, asyncio.Task] = {}
        self.lock = asyncio.Lock()


@lru_cache()
def _session_local():
    config = load_config()
    engine = create_engine(
        config.database.url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )
    return sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    )


def get_current_user():
    """Resolve the current authenticated user for internal orchestrator routes."""
    _ensure_dev_bypass_allowed()
    if _auth_mode() != "dev-bypass":
        raise HTTPException(status_code=401, detail="Authentication required")

    session = _session_local()()
    try:
        user = _ensure_dev_user(session)
        session.expunge(user)
        return user
    finally:
        session.close()


def _scraper_scheduler_disabled() -> bool:
    value = os.getenv("DISABLE_SCRAPER", "").strip().lower()
    return value in {"1", "true", "yes", "on"}


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

async def _log_stream_backlogs_periodically(stop_event: asyncio.Event) -> None:
    """Log stream backlogs every 30 seconds for visibility."""
    from core.redis_streams import log_stream_backlogs

    while not stop_event.is_set():
        try:
            await asyncio.sleep(30)
            log_stream_backlogs()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("Failed to log stream backlogs: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_service_logging(logger)
    logger.info("=" * 60)
    logger.info("STARTING ORCHESTRATOR SERVICE")
    logger.info("=" * 60)
    init_db()

    # startup
    config = load_config()
    app.state.ctx = AppContext.build(config)
    app.state.registry = OrchestratorRegistry()

    logger.info("✅ Orchestrator service ready")
    logger.info(
        "📡 Will subscribe to channels: %s, %s, %s",
        CHANNEL_EXTRACTION_DONE,
        CHANNEL_EMBEDDINGS_DONE,
        CHANNEL_MATCHING_DONE,
    )

    # Start periodic stream backlog logging
    stream_log_stop = asyncio.Event()
    stream_log_task = asyncio.create_task(
        _log_stream_backlogs_periodically(stream_log_stop)
    )
    logger.info("📊 Started periodic stream backlog logging (every 30s)")
    logger.info("=" * 60)

    cleanup_task = asyncio.create_task(
        cleanup_stale_orchestrations(app.state.registry)
    )

    # Start scraper scheduler unless explicitly disabled (e.g. deterministic E2E runs)
    scraper_redis = None
    scraper_stop = asyncio.Event()
    scraper_task = None
    if _scraper_scheduler_disabled():
        logger.info("🛑 Scraper scheduler disabled via DISABLE_SCRAPER")
    else:
        scraper_redis = redis_async.from_url(REDIS_URL)
        scraper_task = asyncio.create_task(
            scraper_scheduler_loop(app.state.ctx, scraper_redis, scraper_stop)
        )

    try:
        yield
    finally:
        # Stop stream logging
        stream_log_stop.set()
        stream_log_task.cancel()
        await asyncio.gather(stream_log_task, return_exceptions=True)

        cleanup_task.cancel()
        await asyncio.gather(cleanup_task, return_exceptions=True)

        # Stop scraper scheduler
        scraper_stop.set()
        if scraper_task is not None:
            scraper_task.cancel()
            await asyncio.gather(scraper_task, return_exceptions=True)
        if scraper_redis is not None:
            await scraper_redis.aclose()

        # Tear down AppContext — try async first, fall back to sync
        ctx: AppContext = app.state.ctx
        aclose = getattr(ctx, "aclose", None)
        if callable(aclose):
            maybe_awaitable = aclose()
            if inspect.isawaitable(maybe_awaitable):
                _ = await maybe_awaitable
        else:
            close = getattr(ctx, "close", None)
            if callable(close):
                close()

        logger.info("=" * 60)
        logger.info("SHUTTING DOWN ORCHESTRATOR SERVICE")
        logger.info("=" * 60)


app = FastAPI(
    title="Orchestrator Service",
    description="Coordinates extraction => embedding => matching pipeline",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(metrics_router)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class MatchResponse(BaseModel):
    success: bool
    task_id: str
    message: str


class StageRequest(BaseModel):
    limit: Optional[int] = None


class ResumeEtlRequest(BaseModel):
    file_path: Optional[str] = None
    task_id: str
    upload_id: Optional[str] = None
    owner_id: str
    resume_fingerprint: Optional[str] = None
    mode: str = "extract_and_embed"


class TaskStatusResponse(BaseModel):
    success: bool
    task_id: str
    status: str
    task_type: Optional[str] = None
    current_stage: Optional[str] = None
    result: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None


class ScrapeResponse(BaseModel):
    success: bool
    total_jobs: int = 0  # Backward compatibility (alias of scraped_jobs)
    scrapers: List[Dict[str, Any]] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)  # Backward compatibility
    scraped_jobs: int = 0
    extracted_count: int = 0
    embedded_count: int = 0
    stage_errors: Dict[str, List[str]] = Field(default_factory=dict)
    message: str


# ---------------------------------------------------------------------------
# OrchestrationState
# ---------------------------------------------------------------------------


class OrchestrationState:
    """Tracks the state of an orchestration task with Redis persistence."""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        self.status = "pending"
        self.task_type: Optional[str] = None
        self.current_stage: Optional[str] = None
        self.resume_fingerprint: Optional[str] = None
        self.resume_file: Optional[str] = None
        self.matches_count: int = 0
        self.result: Dict[str, Any] = {}
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
                self.task_type = data.get("task_type")
                self.current_stage = data.get("current_stage")
                self.resume_fingerprint = data.get("resume_fingerprint")
                self.resume_file = data.get("resume_file")
                self.matches_count = data.get("matches_count", 0)
                self.result = data.get("result", {})
                self.error = data.get("error")
                logger.info("Loaded state from Redis for task: %s", _sanitize_log(self.task_id))
        except Exception:
            logger.warning("Failed to load state from Redis for task: %s", _sanitize_log(self.task_id))

    async def _save_to_redis(self) -> None:
        try:
            await asyncio.to_thread(
                set_task_state,
                self.task_id,
                {
                    "status": self.status,
                    "task_type": self.task_type,
                    "current_stage": self.current_stage,
                    "resume_fingerprint": self.resume_fingerprint,
                    "resume_file": self.resume_file,
                    "matches_count": self.matches_count,
                    "result": self.result,
                    "error": self.error,
                },
            )
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
    while True:  # Infinite loop - tested via integration tests, not unit testable
        await asyncio.sleep(300)
        stale_states: List[OrchestrationState] = []
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
# Scraper functions
# ---------------------------------------------------------------------------


async def acquire_scraper_lock(
    redis_client: redis_async.Redis, scraper_id: str
) -> Optional[str]:
    """Acquire per-scraper distributed lock.
    
    Args:
        redis_client: Async Redis client
        scraper_id: Unique scraper identifier (e.g., "tokyodev")
        
    Returns:
        Owner ID if lock acquired, None if another instance holds the lock.
    """
    lock_key = f"scraper:lock:{scraper_id}"
    owner_id = str(uuid.uuid4())
    acquired = await redis_client.set(
        lock_key, owner_id, nx=True, ex=SCRAPER_LOCK_TTL_SECONDS
    )
    if acquired:
        logger.info(f"Acquired scraper lock for {scraper_id}")
    else:
        logger.info(f"Scraper lock for {scraper_id} held by another instance, skipping")
    return owner_id if acquired else None


async def release_scraper_lock(
    redis_client: redis_async.Redis, lock_key: str, owner_id: str
) -> None:
    """Release scraper lock only if we still own it.
    
    Uses Lua script for atomic ownership verification before deletion.
    """
    await redis_client.eval(RELEASE_LOCK_LUA, 1, lock_key, owner_id)
    logger.info(f"Released scraper lock: {lock_key}")


async def update_scraper_status(
    redis_client: redis_async.Redis,
    scraper_id: str,
    state: str,
    error: str = "",
) -> None:
    """Update scraper status in Redis hash for observability.
    
    Key: scraper:status:{scraper_id}
    Fields: state, started_at, finished_at, last_error
    """
    status_key = f"scraper:status:{scraper_id}"
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    mapping: Dict[str, str] = {"state": state}
    
    if state == "running":
        mapping["started_at"] = timestamp
        mapping["finished_at"] = ""
    elif state in ("idle", "failed"):
        mapping["finished_at"] = timestamp
        mapping["last_error"] = error
    
    await redis_client.hset(status_key, mapping=mapping)


async def _wait_for_scrape_with_retry(
    jobspy_client,
    task_id: str,
    scraper_cfg,
    max_retries: int = 5,
) -> List[Dict[str, Any]]:
    """Wait for scrape result with exponential backoff + jitter.
    
    Args:
        jobspy_client: JobSpyClient instance
        task_id: Task ID from submit_scrape
        scraper_cfg: ScraperConfig for the scraper
        max_retries: Maximum retry attempts
        
    Returns:
        List of job dicts from JobSpy
        
    Raises:
        Exception: If all retries exhausted
    """
    import random
    
    for attempt in range(max_retries):
        try:
            request_timeout = getattr(scraper_cfg, "request_timeout", None)
            result = jobspy_client.wait_for_result(
                task_id,
                request_timeout_s=request_timeout,
            )
            if result is not None:
                return result
            return []
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(
                    f"Scraper retry exhausted for task {task_id}: {e}"
                )
                raise
            wait_time = SCRAPER_RETRY_INTERVALS[attempt] * random.uniform(0.5, 1.5)
            logger.warning(
                f"Scraper attempt {attempt + 1}/{max_retries} failed for "
                f"{scraper_cfg.site_type}: {e}. Retrying in {wait_time:.1f}s..."
            )
            await asyncio.sleep(wait_time)
    
    return []


async def _scrape_single_scraper(
    ctx: AppContext,
    redis_client: redis_async.Redis,
    scraper_cfg,
) -> Dict[str, Any]:
    """Scrape jobs from a single scraper configuration.
    
    Implements:
    - Distributed lock per scraper
    - Status tracking in Redis
    - Retry with exponential backoff
    
    Args:
        ctx: AppContext with jobspy_client and job_etl_service
        redis_client: Async Redis client for locking
        scraper_cfg: ScraperConfig from config.yaml
        
    Returns:
        Dict with: scraper_id, jobs_scraped, error
    """
    scraper_id = str(scraper_cfg.site_type[0])
    lock_key = f"scraper:lock:{scraper_id}"
    
    owner_id = await acquire_scraper_lock(redis_client, scraper_id)
    if not owner_id:
        return {"scraper_id": scraper_id, "jobs_scraped": 0, "error": "skipped: lock held"}
    
    try:
        await update_scraper_status(redis_client, scraper_id, "running")
        
        task_id = ctx.jobspy_client.submit_scrape(scraper_cfg)
        if not task_id:
            logger.warning(f"No task_id from scraper {scraper_id}")
            return {"scraper_id": scraper_id, "jobs_scraped": 0, "error": "no task_id"}
        
        jobs = await _wait_for_scrape_with_retry(
            ctx.jobspy_client, task_id, scraper_cfg
        )
        
        if jobs:
            from database.uow import job_uow
            for job in jobs:
                try:
                    with job_uow() as repo:
                        ctx.job_etl_service.ingest_one(repo, job, scraper_id)
                except Exception as e:
                    logger.error(f"Ingest failed for {scraper_id}: {e}")
        
        logger.info(f"Scraped {len(jobs)} jobs from {scraper_id}")
        return {"scraper_id": scraper_id, "jobs_scraped": len(jobs), "error": None}
        
    except Exception as e:
        logger.error(f"Scraper {scraper_id} failed: {e}")
        return {"scraper_id": scraper_id, "jobs_scraped": 0, "error": str(e)}
        
    finally:
        await release_scraper_lock(redis_client, lock_key, owner_id)
        error = ""
        await update_scraper_status(redis_client, scraper_id, "idle", error)


async def run_all_scrapers(
    ctx: AppContext,
    redis_client: redis_async.Redis,
) -> Dict[str, Any]:
    """Run scraping for all configured scrapers.
    
    Args:
        ctx: AppContext with config.scrapers
        redis_client: Async Redis client
        
    Returns:
        Summary dict with: total_jobs, results_by_scraper, errors
    """
    total_jobs = 0
    results_by_scraper: List[Dict[str, Any]] = []
    errors: List[str] = []
    
    for scraper_cfg in ctx.config.scrapers:
        result = await _scrape_single_scraper(ctx, redis_client, scraper_cfg)
        results_by_scraper.append(result)
        
        if result["error"]:
            errors.append(f"{result['scraper_id']}: {result['error']}")
        else:
            total_jobs += result["jobs_scraped"]
    
    return {
        "total_jobs": total_jobs,
        "results_by_scraper": results_by_scraper,
        "errors": errors,
    }


def _get_downstream_config_errors() -> Dict[str, str]:
    """Return configuration errors for stage execution."""
    return {}


async def _run_batch_stage_via_queue(
    *,
    task_id: str,
    stage: str,
    stream: str,
    completion_channel: str,
    limit: int,
) -> tuple[int, Optional[str]]:
    """Trigger batch work through Redis streams and wait for completion."""
    redis_client = redis_async.from_url(REDIS_URL, decode_responses=True)
    pubsub = redis_client.pubsub()
    try:
        await pubsub.subscribe(completion_channel)
        await asyncio.to_thread(
            enqueue_job,
            stream,
            {"task_id": task_id, "limit": limit},
        )
        async with asyncio.timeout(BATCH_STAGE_TIMEOUT_SECONDS):
            data = await _wait_for_task_message(pubsub, task_id)

        if not data:
            return 0, f"{stage} stage did not publish a completion message"

        processed = int(data.get("processed", 0) or 0)
        if data.get("status") != "completed":
            return processed, str(data.get("error", f"{stage} stage failed"))
        return processed, None
    finally:
        await _cleanup_pubsub_and_client(redis_client, pubsub)


async def run_batch_stage(
    _ctx: AppContext,
    *,
    task_id: str,
    stage: str,
    limit: int,
) -> tuple[int, Optional[str]]:
    """Run a batch stage via Redis streams."""
    stream = STREAM_EXTRACTION_BATCH if stage == "extract" else STREAM_EMBEDDINGS_BATCH
    channel = (
        CHANNEL_EXTRACTION_BATCH_DONE
        if stage == "extract"
        else CHANNEL_EMBEDDINGS_BATCH_DONE
    )
    return await _run_batch_stage_via_queue(
        task_id=task_id,
        stage=stage,
        stream=stream,
        completion_channel=channel,
        limit=limit,
    )


async def run_post_scrape_job_pipeline(
    ctx: AppContext,
    task_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Run extraction + embedding after scraping using the stage adapter."""
    stage_errors: Dict[str, List[str]] = {}
    pipeline_task_id = task_id or f"scrape-batch-{uuid.uuid4().hex[:8]}"

    extracted, extract_error = await run_batch_stage(
        ctx,
        task_id=pipeline_task_id,
        stage="extract",
        limit=SCRAPER_EXTRACTION_LIMIT,
    )
    if extract_error:
        stage_errors.setdefault("extract", []).append(extract_error)

    embedded, embed_error = await run_batch_stage(
        ctx,
        task_id=pipeline_task_id,
        stage="embed",
        limit=SCRAPER_EMBEDDING_LIMIT,
    )
    if embed_error:
        stage_errors.setdefault("embed", []).append(embed_error)

    return {
        "extracted": extracted,
        "embedded": embedded,
        "stage_errors": stage_errors,
    }


async def scraper_scheduler_loop(
    ctx: AppContext,
    redis_client: redis_async.Redis,
    stop_event: asyncio.Event,
) -> None:
    """Main scheduler loop - runs scraping on fixed interval.
    
    Args:
        ctx: AppContext with scraper config
        redis_client: Async Redis client
        stop_event: Event to signal shutdown
    """
    logger.info(
        f"Scraper scheduler started (interval: {SCRAPER_INTERVAL_HOURS}h)"
    )
    
    while not stop_event.is_set():
        interval_seconds = SCRAPER_INTERVAL_HOURS * 3600
        
        try:
            logger.info("Starting scheduled scrape cycle")
            result = await run_all_scrapers(ctx, redis_client)
            
            if result["errors"]:
                logger.warning(
                    f"Scheduled scrape completed with errors: {result['errors']}"
                )
            else:
                logger.info(
                    f"Scheduled scrape completed: {result['total_jobs']} jobs "
                    f"from {len(result['results_by_scraper'])} scrapers"
                )

            pipeline_result = await run_post_scrape_job_pipeline(ctx)
            logger.info(
                "Scheduled scrape post-processing complete: extracted=%d embedded=%d",
                pipeline_result["extracted"],
                pipeline_result["embedded"],
            )
            if pipeline_result["stage_errors"]:
                logger.warning(
                    "Scheduled post-processing stage errors: %s",
                    pipeline_result["stage_errors"],
                )
                
        except Exception as e:
            logger.error(f"Scheduled scrape failed: {e}")
        
        await asyncio.sleep(interval_seconds)
    
    logger.info("Scraper scheduler stopped")


# ---------------------------------------------------------------------------
# Pipeline helpers
# ---------------------------------------------------------------------------


async def _wait_for_next_message(pubsub) -> dict:
    """Read the next pubsub message. Caller owns the timeout."""
    async for message in pubsub.listen():
        msg_type = message.get("type")
        if msg_type != "message":
            logger.debug("Received non-data pubsub message type: %s", msg_type)
            continue
        data = json.loads(message["data"])
        logger.debug(
            "📬 PubSub message received: channel=%s, task_id=%s, status=%s",
            message.get("channel"),
            data.get("task_id"),
            data.get("status"),
        )
        return data
    # exhausted iterator — surface as an error sentinel
    logger.debug("PubSub listen() iterator exhausted with no message")
    return {}


async def _wait_for_task_message(pubsub, task_id: str) -> dict:
    """Skip messages until one matches task_id. Caller owns the timeout."""
    while True:
        data = await _wait_for_next_message(pubsub)
        if not data:
            # exhausted or invalid data; let caller's timeout handle it
            logger.debug("No data received while waiting for completion message")
            return {}
        logger.debug("Received completion candidate message; checking task id match")
        if data.get("task_id") == task_id:
            logger.info("Found matching completion message")
            return data
        logger.debug("Skipping completion message for a different task")


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
    # Map stage name to completion channel
    channel_map = {
        "extraction": CHANNEL_EXTRACTION_DONE,
        "embeddings": CHANNEL_EMBEDDINGS_DONE,
        "matching": CHANNEL_MATCHING_DONE,
    }

    completion_channel = channel_map.get(stage_name, "unknown")

    logger.info(
        "📤 Enqueueing %s job to %s: task_id=%s",
        stage_name,
        stream,
        job_payload.get("task_id"),
    )
    logger.debug(" Payload: %s", json.dumps(job_payload))
    await asyncio.to_thread(enqueue_job, stream, job_payload)
    logger.info(
        "✅ %s job enqueued: task_id=%s",
        stage_name.capitalize(),
        job_payload.get("task_id"),
    )

    logger.info("⏳ Waiting for %s completion on %s...", stage_name, completion_channel)
    data = await _wait_for_task_message(pubsub, state.task_id)
    if not data:
        logger.error("❌ No completion message received for stage %s (task_id=%s)", stage_name, state.task_id)
        state.status = "failed"
        state.error = f"No completion message from {stage_name}"
        await state._save_to_redis()
        await state.notify(
            {"task_id": state.task_id, "status": "failed", "error": state.error}
        )
        return False, None

    logger.info(
        "📨 Received %s completion: task_id=%s, status=%s, channel=%s",
        stage_name,
        data.get("task_id"),
        data.get("status"),
        completion_channel,
    )

    status = data.get("status")
    if status == "failed":
        logger.error(
            "❌ %s failed for task %s: %s",
            stage_name,
            state.task_id,
            data.get("error"),
        )
        state.status = "failed"
        state.error = data.get("error", f"{stage_name.capitalize()} failed")
        await state._save_to_redis()
        await state.notify(
            {"task_id": state.task_id, "status": "failed", "error": state.error}
        )
        return False, data

    if status not in ("skipped", "completed"):
        logger.warning("❌ Unexpected status in %s response: %s", stage_name, status)
        state.status = "failed"
        state.error = f"Unexpected status from {stage_name}: {status}"
        await state._save_to_redis()
        await state.notify(
            {"task_id": state.task_id, "status": "failed", "error": state.error}
        )
        return False, data

    return True, data


async def _handle_extraction_fingerprint(
    state: OrchestrationState, task_id: str, extraction_data: dict
) -> bool:
    """Validate and store the extraction fingerprint.

    Returns False if pipeline should abort.
    """
    fp = extraction_data.get("resume_fingerprint")
    status = extraction_data.get("status")

    # Always require fingerprint unless the stage was explicitly skipped
    if not fp:
        if status != "skipped":
            logger.error("❌ No fingerprint in extraction response for task: %s", task_id)
            state.status = "failed"
            state.error = "No fingerprint in extraction response"
            await state._save_to_redis()
            await state.notify(
                {"task_id": task_id, "status": "failed", "error": state.error}
            )
            return False
        # skipped + no fingerprint: rely on previously persisted state
        logger.info(
            "ℹ️ Extraction skipped with no new fingerprint for task %s; using existing: %s",
            task_id,
            (state.resume_fingerprint or "")[:16],
        )
        return True

    state.resume_fingerprint = fp
    status_msg = (
        "Resume unchanged, using existing"
        if status == "skipped"
        else "Extraction complete"
    )
    logger.info("ℹ️ %s: %s...", status_msg, fp[:16])

    return True


async def _cleanup_pubsub_and_client(redis_client, pubsub) -> None:
    """Close pubsub and Redis client, swallowing errors so finally blocks never raise."""
    if pubsub:
        try:
            await pubsub.unsubscribe()
            await pubsub.close()
        except Exception as e:
            logger.warning("Failed to close pubsub: %s", e)
    if redis_client:
        try:
            await redis_client.aclose()
        except Exception as e:
            logger.warning("Failed to close Redis client: %s", e)


# ---------------------------------------------------------------------------
# Core orchestration coroutine
# ---------------------------------------------------------------------------


async def _run_extraction_stage(
    state: OrchestrationState,
    task_id: str,
    pubsub: redis_async.client.PubSub,
) -> bool:
    """Run extraction stage. Returns True on success."""
    state.status = "extracting"
    state.current_stage = "extract"
    await state._save_to_redis()
    await state.notify({
        "task_id": task_id,
        "status": "extracting",
        "message": "Starting extraction",
    })

    await pubsub.subscribe(CHANNEL_EXTRACTION_DONE)

    async with asyncio.timeout(LISTENER_TIMEOUT):
        success, extraction_data = await _run_pipeline_stage(
            state=state,
            pubsub=pubsub,
            stream=STREAM_EXTRACTION,
            job_payload={"task_id": task_id},
            stage_name="extraction",
        )

    if not success:
        return False
    return await _handle_extraction_fingerprint(state, task_id, extraction_data)


async def _run_embeddings_stage(
    state: OrchestrationState,
    task_id: str,
    pubsub: redis_async.client.PubSub,
) -> bool:
    """Run embeddings stage. Returns True on success."""
    state.status = "embedding"
    state.current_stage = "embed"
    await state._save_to_redis()
    await state.notify({
        "task_id": task_id,
        "status": "embedding",
        "message": "Starting embeddings",
    })

    await pubsub.unsubscribe(CHANNEL_EXTRACTION_DONE)
    await pubsub.subscribe(CHANNEL_EMBEDDINGS_DONE)

    async with asyncio.timeout(LISTENER_TIMEOUT):
        success, _ = await _run_pipeline_stage(
            state=state,
            pubsub=pubsub,
            stream=STREAM_EMBEDDINGS,
            job_payload={
                "task_id": task_id,
                "resume_fingerprint": state.resume_fingerprint,
            },
            stage_name="embeddings",
        )

    return success


async def _run_matching_stage(
    state: OrchestrationState,
    task_id: str,
    pubsub: redis_async.client.PubSub,
    channel_done: str,
) -> tuple[bool, Optional[dict]]:
    """Run matching stage. Returns (success, matching_data)."""
    state.status = "matching"
    state.current_stage = "match"
    await state._save_to_redis()
    await state.notify({
        "task_id": task_id,
        "status": "matching",
        "message": "Starting matching",
    })

    await pubsub.unsubscribe(channel_done)
    await pubsub.subscribe(CHANNEL_MATCHING_DONE)

    async with asyncio.timeout(LISTENER_TIMEOUT):
        success, matching_data = await _run_pipeline_stage(
            state=state,
            pubsub=pubsub,
            stream=STREAM_MATCHING,
            job_payload={
                "task_id": task_id,
                "resume_fingerprint": state.resume_fingerprint,
            },
            stage_name="matching",
        )

    return success, matching_data


async def _run_matching_fast_path(
    state: OrchestrationState,
    task_id: str,
) -> tuple[redis_async.Redis, redis_async.client.PubSub, bool, Optional[dict]]:
    """Run only the matching stage for an already-processed resume."""
    redis_client = redis_async.from_url(REDIS_URL, decode_responses=True)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(CHANNEL_MATCHING_DONE)
    logger.info("📡 Subscribed to %s for matching", CHANNEL_MATCHING_DONE)

    async with asyncio.timeout(LISTENER_TIMEOUT):
        success, matching_data = await _run_pipeline_stage(
            state=state,
            pubsub=pubsub,
            stream=STREAM_MATCHING,
            job_payload={
                "task_id": task_id,
                "resume_fingerprint": state.resume_fingerprint,
            },
            stage_name="matching",
        )

    return redis_client, pubsub, success, matching_data


async def _run_full_match_pipeline(
    state: OrchestrationState,
    task_id: str,
) -> tuple[redis_async.Redis, redis_async.client.PubSub, bool, Optional[dict]]:
    """Run extraction, embeddings, and matching for a new resume."""
    redis_client = redis_async.from_url(REDIS_URL, decode_responses=True)
    pubsub = redis_client.pubsub()

    if not await _run_extraction_stage(state, task_id, pubsub):
        return redis_client, pubsub, False, None

    if not await _run_embeddings_stage(state, task_id, pubsub):
        return redis_client, pubsub, False, None

    success, matching_data = await _run_matching_stage(
        state, task_id, pubsub, CHANNEL_EMBEDDINGS_DONE
    )
    return redis_client, pubsub, success, matching_data


async def _complete_match_task(
    state: OrchestrationState,
    task_id: str,
    matching_data: Optional[dict],
) -> None:
    """Persist final orchestration success state and notify subscribers."""
    state.status = "completed"
    state.current_stage = "match"
    state.matches_count = (matching_data or {}).get("matches_count", 0)
    state.result = {"matches_count": state.matches_count}
    await state._save_to_redis()
    logger.info(
        "🎉 Pipeline completed for task %s: %d matches",
        task_id,
        state.matches_count,
    )
    await state.notify(
        {
            "task_id": task_id,
            "status": "completed",
            "matches_count": state.matches_count,
            "message": f"Matching complete, {state.matches_count} matches",
        }
    )


async def orchestrate_match(
    task_id: str,
    registry: OrchestratorRegistry,
    resume_fingerprint: Optional[str] = None,
) -> None:
    """Run the full pipeline: extraction -> embeddings -> matching.

    If resume_fingerprint is provided, extraction and embedding stages are skipped
    and matching is run directly using the existing stored data.
    """
    async with registry.lock:
        registry.active_task_ids.add(task_id)

    state = await get_or_create_orchestration(registry, task_id)
    state.task_type = "match"

    if resume_fingerprint:
        state.resume_fingerprint = resume_fingerprint
        logger.info("🔄 Resume already processed, skipping extraction/embedding")
    else:
        state.status = "extracting"
        state.current_stage = "extract"

    await state._save_to_redis()

    redis_client = None
    pubsub = None
    try:
        logger.info("🚀 Starting pipeline for task: %s", task_id)

        if resume_fingerprint:
            # Skip directly to matching
            await state.notify(
                {
                    "task_id": task_id,
                    "status": "matching",
                    "message": "Resume already processed, starting matching",
                }
            )
            logger.info("⏭️ Skipping extraction and embedding stages")
            redis_client, pubsub, success, matching_data = await _run_matching_fast_path(
                state, task_id
            )
        else:
            redis_client, pubsub, success, matching_data = await _run_full_match_pipeline(
                state, task_id
            )

        if not success:
            return

        await _complete_match_task(state, task_id, matching_data)

    except asyncio.TimeoutError:
        logger.error(
            "❌ Orchestration timeout for task %s: %s",
            task_id,
            "stage timeout exceeded",
            exc_info=True,
        )
        state.status = "failed"
        state.error = "Stage timeout"
        await state._save_to_redis()
        await state.notify(
            {"task_id": task_id, "status": "failed", "error": state.error}
        )

    except Exception as e:  # generic safety net
        logger.error(
            "❌ Orchestration failed for task %s: %s: %s",
            task_id,
            type(e).__name__,
            e,
            exc_info=True,
        )
        state.status = "failed"
        state.error = str(e)
        await state._save_to_redis()
        await state.notify(
            {"task_id": task_id, "status": "failed", "error": str(e)}
        )

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
                await state.notify(
                    {
                        "task_id": task_id,
                        "status": "failed",
                        "error": state.error,
                    }
                )
    else:
        logger.info("Orchestration completed successfully: %s", task_id)

    async with registry.lock:
        registry.tasks.pop(task_id, None)


def _task_snapshot(state: OrchestrationState) -> Dict[str, Any]:
    """Build a JSON-safe task snapshot."""
    result = dict(state.result)
    if state.matches_count and "matches_count" not in result:
        result["matches_count"] = state.matches_count
    return {
        "success": True,
        "task_id": state.task_id,
        "status": state.status,
        "task_type": state.task_type,
        "current_stage": state.current_stage,
        "result": result,
        "error": state.error,
    }


def _task_status_response(snapshot: Dict[str, Any]) -> TaskStatusResponse:
    """Convert task snapshot dict to response model."""
    return TaskStatusResponse(
        success=bool(snapshot.get("success", True)),
        task_id=str(snapshot.get("task_id", "")),
        status=str(snapshot.get("status", "unknown")),
        task_type=snapshot.get("task_type"),
        current_stage=snapshot.get("current_stage"),
        result=snapshot.get("result", {}) or {},
        error=snapshot.get("error"),
    )


async def _spawn_background_task(
    registry: OrchestratorRegistry,
    task_id: str,
    task_type: str,
    coroutine: "asyncio.Future[None]",
    message: str,
    current_stage: Optional[str] = None,
    initial_result: Optional[Dict[str, Any]] = None,
) -> MatchResponse:
    """Register and start a background task."""
    state = await get_or_create_orchestration(registry, task_id)
    state.status = "queued"
    state.task_type = task_type
    state.current_stage = current_stage
    state.result = initial_result or {}
    state.error = None
    await state._save_to_redis()

    task = asyncio.create_task(coroutine)

    def safe_done_callback(t: asyncio.Task) -> None:
        try:
            cb_task = asyncio.create_task(_handle_task_done(task_id, t, registry))
            cb_task.add_done_callback(lambda _: None)
        except RuntimeError:
            logger.warning(
                "Could not handle task completion for %s: no running loop", task_id
            )

    task.add_done_callback(safe_done_callback)

    async with registry.lock:
        registry.tasks[task_id] = task

    return MatchResponse(success=True, task_id=task_id, message=message)


async def _run_stage_task(
    task_id: str,
    registry: OrchestratorRegistry,
    ctx: AppContext,
    stage: str,
    limit: int,
) -> None:
    """Run a single scrape/extract/embed stage as a managed task."""
    async with registry.lock:
        registry.active_task_ids.add(task_id)

    state = await get_or_create_orchestration(registry, task_id)
    state.task_type = "stage"
    state.current_stage = stage
    state.status = "running"
    state.result = {"stage": stage, "limit": limit}
    await state._save_to_redis()
    await state.notify(
        {
            "task_id": task_id,
            "status": "running",
            "current_stage": stage,
            "message": f"Starting {stage} stage",
        }
    )

    redis_client = None
    try:
        if stage == "scrape":
            redis_client = redis_async.from_url(REDIS_URL)
            scrape_result = await run_all_scrapers(ctx, redis_client)
            state.result = {
                "stage": stage,
                "scraped_jobs": scrape_result["total_jobs"],
                "scrapers": scrape_result["results_by_scraper"],
                "errors": scrape_result["errors"],
            }
            if scrape_result["errors"]:
                raise RuntimeError("; ".join(scrape_result["errors"]))
        elif stage in {"extract", "embed"}:
            processed, error = await run_batch_stage(
                ctx, task_id=task_id, stage=stage, limit=limit
            )
            state.result = {"stage": stage, "processed": processed, "limit": limit}
            if error:
                raise RuntimeError(error)
        else:
            raise RuntimeError(f"Unsupported stage: {stage}")

        state.status = "completed"
        await state._save_to_redis()
        await state.notify(
            {
                "task_id": task_id,
                "status": "completed",
                "current_stage": stage,
                "result": state.result,
            }
        )
    except Exception as exc:
        state.status = "failed"
        state.error = str(exc)
        await state._save_to_redis()
        await state.notify(
            {
                "task_id": task_id,
                "status": "failed",
                "current_stage": stage,
                "error": state.error,
                "result": state.result,
            }
        )
    finally:
        if redis_client is not None:
            await redis_client.aclose()
        async with registry.lock:
            registry.active_task_ids.discard(task_id)
        await state.close(registry)


async def _run_scrape_extract_embed_pipeline_task(
    task_id: str,
    registry: OrchestratorRegistry,
    ctx: AppContext,
) -> None:
    """Run scrape -> extract -> embed as a managed task."""
    async with registry.lock:
        registry.active_task_ids.add(task_id)

    state = await get_or_create_orchestration(registry, task_id)
    state.task_type = "pipeline"
    state.status = "running"
    state.result = {
        "scraped_jobs": 0,
        "scrapers": [],
        "errors": [],
        "extracted_count": 0,
        "embedded_count": 0,
        "stage_errors": {},
    }
    await state._save_to_redis()

    redis_client = redis_async.from_url(REDIS_URL)
    try:
        state.current_stage = "scrape"
        await state._save_to_redis()
        await state.notify(
            {
                "task_id": task_id,
                "status": "running",
                "current_stage": "scrape",
                "message": "Starting scrape stage",
            }
        )
        scrape_result = await run_all_scrapers(ctx, redis_client)
        state.result["scraped_jobs"] = scrape_result["total_jobs"]
        state.result["scrapers"] = scrape_result["results_by_scraper"]
        if scrape_result["errors"]:
            state.result["errors"] = list(scrape_result["errors"])
            state.result["stage_errors"]["scrape"] = list(scrape_result["errors"])

        state.current_stage = "extract"
        await state._save_to_redis()
        extracted, extract_error = await run_batch_stage(
            ctx,
            task_id=task_id,
            stage="extract",
            limit=SCRAPER_EXTRACTION_LIMIT,
        )
        state.result["extracted_count"] = extracted
        if extract_error:
            state.result["stage_errors"].setdefault("extract", []).append(extract_error)

        state.current_stage = "embed"
        await state._save_to_redis()
        embedded, embed_error = await run_batch_stage(
            ctx,
            task_id=task_id,
            stage="embed",
            limit=SCRAPER_EMBEDDING_LIMIT,
        )
        state.result["embedded_count"] = embedded
        if embed_error:
            state.result["stage_errors"].setdefault("embed", []).append(embed_error)

        flat_errors = []
        for errors in state.result["stage_errors"].values():
            flat_errors.extend(errors)
        state.result["errors"] = flat_errors

        if flat_errors:
            raise RuntimeError("; ".join(flat_errors))

        state.status = "completed"
        await state._save_to_redis()
        await state.notify(
            {
                "task_id": task_id,
                "status": "completed",
                "current_stage": "embed",
                "result": state.result,
            }
        )
    except Exception as exc:
        state.status = "failed"
        state.error = str(exc)
        await state._save_to_redis()
        await state.notify(
            {
                "task_id": task_id,
                "status": "failed",
                "current_stage": state.current_stage,
                "error": state.error,
                "result": state.result,
            }
        )
    finally:
        await redis_client.aclose()
        async with registry.lock:
            registry.active_task_ids.discard(task_id)
        await state.close(registry)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
async def health(request: Request):
    """Health check endpoint with Redis connectivity verification."""
    try:
        client = get_redis_client()
        client.ping()
        redis_status = "connected"
    except redis.ConnectionError as e:
        logger.error("Redis connection error in health check: %s", e)
        redis_status = "connection_error"
    except Exception as e:
        logger.error("Redis error in health check: %s", e)
        redis_status = "error"

    registry: OrchestratorRegistry = request.app.state.registry
    async with registry.lock:
        active_count = len(registry.active_task_ids)
    downstream_config_errors = _get_downstream_config_errors()

    return {
        "status": "healthy",
        "service": "orchestrator",
        "redis": redis_status,
        "active_tasks": active_count,
        "downstream_config_errors": downstream_config_errors,
        "downstream_ready": not downstream_config_errors,
    }


async def _get_existing_task_snapshot(
    registry: OrchestratorRegistry,
    task_id: str,
) -> Optional[Dict[str, Any]]:
    """Return a snapshot for an existing task without creating a new one."""
    async with registry.lock:
        state = registry.orchestrations.get(task_id)
        if state is not None:
            return _task_snapshot(state)

    persisted = await asyncio.to_thread(get_task_state, task_id)
    if not persisted:
        return None

    state = await OrchestrationState.create(task_id, load_from_redis=True)
    return _task_snapshot(state)


@app.post(
    "/orchestrate/stages/{stage}",
    response_model=TaskStatusResponse,
    responses={404: {"description": "Unknown stage"}},
)
async def orchestrate_stage(stage: str, request: Request, body: StageRequest = StageRequest()):
    """Canonical stage trigger surface for scrape/extract/embed."""
    if stage not in {"scrape", "extract", "embed"}:
        raise HTTPException(status_code=404, detail=f"Unknown stage: {stage}")

    registry: OrchestratorRegistry = request.app.state.registry
    ctx: AppContext = request.app.state.ctx
    task_id = f"{stage}-{uuid.uuid4().hex[:8]}"
    default_limit = 200 if stage in {"scrape", "extract"} else 100
    limit = body.limit or default_limit

    response = await _spawn_background_task(
        registry,
        task_id,
        "stage",
        _run_stage_task(task_id, registry, ctx, stage, limit),
        f"{stage} stage started",
    )
    return TaskStatusResponse(
        success=response.success,
        task_id=response.task_id,
        status="queued",
        task_type="stage",
        current_stage=stage,
        result={"stage": stage, "limit": limit},
    )


@app.post("/orchestrate/pipelines/scrape-extract-embed", response_model=TaskStatusResponse)
async def orchestrate_scrape_extract_embed_pipeline(request: Request):
    """Canonical trigger for scrape -> extract -> embed."""
    registry: OrchestratorRegistry = request.app.state.registry
    ctx: AppContext = request.app.state.ctx
    task_id = f"pipeline-{uuid.uuid4().hex[:8]}"
    response = await _spawn_background_task(
        registry,
        task_id,
        "pipeline",
        _run_scrape_extract_embed_pipeline_task(task_id, registry, ctx),
        "scrape-extract-embed pipeline started",
    )
    return TaskStatusResponse(
        success=response.success,
        task_id=response.task_id,
        status="queued",
        task_type="pipeline",
        current_stage="scrape",
        result={
            "scraped_jobs": 0,
            "scrapers": [],
            "errors": [],
            "extracted_count": 0,
            "embedded_count": 0,
            "stage_errors": {},
        },
    )


@app.get(
    "/orchestrate/tasks/{task_id}",
    response_model=TaskStatusResponse,
    responses={404: {"description": "Task not found"}},
)
async def get_task_status(task_id: str, request: Request):
    """Canonical JSON task status endpoint."""
    registry: OrchestratorRegistry = request.app.state.registry
    snapshot = await _get_existing_task_snapshot(registry, task_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return snapshot


@app.post("/orchestrate/match", response_model=MatchResponse)
async def orchestrate_match_endpoint(
    request: Request,
    user: Annotated[Any, Depends(get_current_user)],
):
    """Trigger the full pipeline: extraction -> embeddings -> matching.

    If a resume has already been uploaded and processed, this will skip
    extraction and embedding stages and go straight to matching.

    Resume must be uploaded via the web UI - no config file fallback.
    """
    logger.info("=" * 60)
    logger.info("📨 HTTP POST /orchestrate/match received")
    logger.info("=" * 60)

    task_id = f"match-{uuid.uuid4().hex[:8]}"
    logger.info("🆔 Created task: %s", task_id)

    registry: OrchestratorRegistry = request.app.state.registry

    eligibility = evaluate_resume_eligibility(resolve_owner_id(user))
    if not eligibility.can_run or not eligibility.resume_fingerprint:
        logger.warning("⚠️ Blocking match: %s", eligibility.message)
        return MatchResponse(
            success=False,
            task_id=task_id,
            message=eligibility.message,
        )

    resume_fingerprint: Optional[str] = eligibility.resume_fingerprint

    logger.info(
        "🚀 Using existing resume from DB (fingerprint: %s...)",
        resume_fingerprint[:16],
    )

    return await _spawn_background_task(
        registry,
        task_id,
        "match",
        orchestrate_match(task_id, registry, resume_fingerprint),
        "Pipeline started",
    )


@app.post("/orchestrate/resume-etl")
async def orchestrate_resume_etl(payload: ResumeEtlRequest, request: Request):
    """Sequence extraction → embedding for a resume file.

    The caller (web-backend) provides its own task_id so progress is written
    to task:{task_id}:state and can be polled directly without an orchestrator
    status proxy.

    Returns 202 immediately; processing happens in a background asyncio task.
    """
    task_id = payload.task_id
    file_path = payload.file_path

    logger.info("Received /orchestrate/resume-etl request")

    initial_step = "embedding" if payload.mode == "embed_only" else "extracting"
    initial_state = {"status": "running", "step": initial_step}
    if payload.upload_id:
        initial_state["upload_id"] = payload.upload_id
    if payload.resume_fingerprint:
        initial_state["resume_fingerprint"] = payload.resume_fingerprint
    set_task_state(task_id, initial_state, ttl=3600)

    etl_task = asyncio.create_task(
        _run_resume_etl(
            task_id,
            file_path,
            upload_id=payload.upload_id,
            owner_id=payload.owner_id,
            resume_fingerprint=payload.resume_fingerprint,
            mode=payload.mode,
        )
    )
    _etl_tasks.add(etl_task)

    def _etl_done(t: asyncio.Task) -> None:
        _etl_tasks.discard(t)
        if not t.cancelled() and t.exception() is not None:
            logger.error("_run_resume_etl background task raised an unhandled exception")

    etl_task.add_done_callback(_etl_done)

    from fastapi.responses import JSONResponse as _JSONResponse
    return _JSONResponse(status_code=202, content={"task_id": task_id, "success": True})


async def _run_resume_etl(
    task_id: str,
    file_path: Optional[str],
    *,
    upload_id: Optional[str] = None,
    owner_id: str,
    resume_fingerprint: Optional[str] = None,
    mode: str = "extract_and_embed",
) -> None:
    """Background task: extraction → embeddings for a single resume.

    Writes progress to task:{task_id}:state so the web-backend can poll
    Redis directly (no orchestrator status proxy needed).
    """
    redis_client = None
    pubsub = None
    try:
        redis_client = redis_async.from_url(REDIS_URL, decode_responses=True)
        pubsub = redis_client.pubsub()

        fingerprint = resume_fingerprint
        if mode != "embed_only":
            await pubsub.subscribe(CHANNEL_EXTRACTION_DONE)
            await asyncio.to_thread(
                enqueue_job,
                STREAM_EXTRACTION,
                {
                    "task_id": task_id,
                    "resume_file": file_path,
                    "known_fingerprint": resume_fingerprint,
                    "resume_upload_id": upload_id,
                    "owner_id": owner_id,
                },
            )
            logger.info("Enqueued extraction stage for resume ETL")

            async with asyncio.timeout(LISTENER_TIMEOUT):
                extraction_data = await _wait_for_task_message(pubsub, task_id)

            await pubsub.unsubscribe(CHANNEL_EXTRACTION_DONE)

            if not extraction_data or extraction_data.get("status") == "failed":
                err = (extraction_data or {}).get("error", "Extraction failed")
                logger.error("Resume extraction stage failed")
                set_task_state(task_id, {"status": "failed", "step": "extracting", "error": err}, ttl=3600)
                return

            fingerprint = extraction_data.get("resume_fingerprint")
            if not fingerprint:
                logger.error("Extraction stage returned no resume fingerprint")
                set_task_state(
                    task_id,
                    {
                        "status": "failed",
                        "step": "extracting",
                        "upload_id": upload_id,
                        "owner_id": owner_id,
                        "error": "No fingerprint in extraction response",
                    },
                    ttl=3600,
                )
                return

            logger.info("Extraction stage completed")
            set_task_state(
                task_id,
                {
                    "status": "running",
                    "step": "embedding",
                    "upload_id": upload_id,
                    "owner_id": owner_id,
                    "resume_fingerprint": fingerprint,
                },
                ttl=3600,
            )
        elif not fingerprint:
            set_task_state(
                task_id,
                {
                    "status": "failed",
                    "step": "embedding",
                    "upload_id": upload_id,
                    "owner_id": owner_id,
                    "error": "Missing resume fingerprint for embed-only retry",
                },
                ttl=3600,
            )
            return

        # ---- Stage 2: embeddings ------------------------------------------------
        await pubsub.subscribe(CHANNEL_EMBEDDINGS_DONE)
        await asyncio.to_thread(
            enqueue_job,
            STREAM_EMBEDDINGS,
            {
                "task_id": task_id,
                "resume_fingerprint": fingerprint,
                "resume_upload_id": upload_id,
                "owner_id": owner_id,
            },
        )
        logger.info("Enqueued embedding stage for resume ETL")

        async with asyncio.timeout(LISTENER_TIMEOUT):
            embeddings_data = await _wait_for_task_message(pubsub, task_id)

        await pubsub.unsubscribe(CHANNEL_EMBEDDINGS_DONE)

        if not embeddings_data or embeddings_data.get("status") == "failed":
            err = (embeddings_data or {}).get("error", "Embeddings failed")
            logger.error("Embedding stage failed")
            set_task_state(
                task_id,
                {
                    "status": "failed",
                    "step": "embedding",
                    "upload_id": upload_id,
                    "owner_id": owner_id,
                    "resume_fingerprint": fingerprint,
                    "error": err,
                },
                ttl=3600,
            )
            return

        logger.info("Resume ETL completed successfully")
        set_task_state(
            task_id,
            {
                "status": "completed",
                "upload_id": upload_id,
                "owner_id": owner_id,
                "resume_fingerprint": fingerprint,
            },
            ttl=3600,
        )

    except asyncio.TimeoutError:
        logger.error("Timeout during resume ETL")
        set_task_state(
            task_id,
            {
                "status": "failed",
                "upload_id": upload_id,
                "owner_id": owner_id,
                "resume_fingerprint": resume_fingerprint,
                "error": "Stage timeout",
            },
            ttl=3600,
        )
    except Exception as exc:
        logger.exception("Resume ETL failed due to an unhandled exception")
        set_task_state(
            task_id,
            {
                "status": "failed",
                "upload_id": upload_id,
                "owner_id": owner_id,
                "resume_fingerprint": resume_fingerprint,
                "error": str(exc),
            },
            ttl=3600,
        )
    finally:
        if redis_client:
            await _cleanup_pubsub_and_client(redis_client, pubsub)


@app.get("/orchestrate/status/{task_id}")
async def get_orchestration_status(task_id: str, request: Request):
    """Get orchestration status via SSE."""
    registry: OrchestratorRegistry = request.app.state.registry

    async def event_generator():
        state = await get_or_create_orchestration(registry, task_id)
        queue = state.subscribe()
        try:
            # initial snapshot
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
                    # heartbeat
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
        states.append(
            {
                "task_id": tid,
                "status": state.status,
                "task_type": state.task_type,
                "current_stage": state.current_stage,
                "resume_fingerprint": state.resume_fingerprint,
                "matches_count": state.matches_count,
                "result": state.result,
                "error": state.error,
            }
        )

    return {"success": True, "tasks": states}


def _get_stream_diagnostic(stream_name: str) -> dict:
    """Return status dict for a single Redis stream."""
    try:
        if not stream_exists(stream_name):
            return {"exists": False, "length": 0}

        info = get_stream_info(stream_name)
        result: Dict[str, Any] = {
            "exists": True,
            "length": info.get("length", 0),
            "first_entry": info.get("first-entry"),
        }

        try:
            groups = info.get("groups", []) or []
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
            logger.error("Consumer groups error for stream %s: %s", stream_name, e)
            result["consumer_groups_error"] = "Failed to retrieve consumer groups"

        return result
    except Exception as e:
        logger.error("Stream diagnostic error for %s: %s", stream_name, e)
        return {"error": "Failed to retrieve stream info"}


async def _get_active_orchestration_states(
    registry: OrchestratorRegistry,
) -> list:
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
            # expected format: task:<task_id>:state
            task_id = key.removeprefix("task:").removesuffix(":state")
            task_data = get_task_state(task_id)
            if task_data:
                recent.append(
                    {
                        "task_id": task_id,
                        "status": task_data.get("status"),
                        "error": task_data.get("error"),
                    }
                )
        return recent
    except Exception as e:
        logger.error("Failed to retrieve recent tasks from Redis: %s", e)
        return {"error": "Failed to retrieve recent tasks"}


@app.get("/orchestrate/diagnostics")
async def get_diagnostics(request: Request):
    """Get diagnostics for Redis streams, consumer groups, and active tasks."""
    registry: OrchestratorRegistry = request.app.state.registry
    redis_client = get_redis_client()
    active_states = await _get_active_orchestration_states(registry)
    downstream_config_errors = _get_downstream_config_errors()

    return {
        "success": True,
        "timestamp": time.time(),
        "streams": {
            stream_name: _get_stream_diagnostic(stream_name)
            for stream_name in [
                STREAM_EXTRACTION,
                STREAM_EXTRACTION_BATCH,
                STREAM_EMBEDDINGS,
                STREAM_EMBEDDINGS_BATCH,
                STREAM_MATCHING,
            ]
        },
        "active_orchestrations": active_states,
        "recent_tasks": _get_recent_tasks(redis_client),
        "active_task_count": len(active_states),
        "downstream_config_errors": downstream_config_errors,
        "downstream_ready": not downstream_config_errors,
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

    stopped: list[str] = []
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
            await state.notify(
                {
                    "task_id": tid,
                    "status": "cancelled",
                    "error": "Cancelled by user",
                }
            )
            stopped.append(tid)

    return {
        "success": True,
        "stopped": stopped,
        "message": f"Cancelled {len(stopped)} task(s)",
    }


@app.post("/orchestrate/scrape-extract-embed", response_model=ScrapeResponse)
@app.post("/orchestrate/scrape", response_model=ScrapeResponse, include_in_schema=False)
async def trigger_scrape(request: Request):
    """Manually trigger scrape + extract + embed for all configured scrapers.
    
    This endpoint is intended for ops/admin use. The scheduler normally runs
    automatically on a schedule (controlled by SCRAPER_INTERVAL_HOURS env var).
    """
    ctx: AppContext = request.app.state.ctx
    
    redis_client = redis_async.from_url(REDIS_URL)
    try:
        result = await run_all_scrapers(ctx, redis_client)
        pipeline_result = await run_post_scrape_job_pipeline(ctx)
        extracted = pipeline_result["extracted"]
        embedded = pipeline_result["embedded"]
        stage_errors: Dict[str, List[str]] = {}

        if result["errors"]:
            stage_errors["scrape"] = list(result["errors"])
        for stage, errors in pipeline_result["stage_errors"].items():
            stage_errors.setdefault(stage, []).extend(errors)

        flat_errors = [err for errs in stage_errors.values() for err in errs]
        success = len(flat_errors) == 0
        
        return ScrapeResponse(
            success=success,
            total_jobs=result["total_jobs"],
            scrapers=result["results_by_scraper"],
            errors=flat_errors,
            scraped_jobs=result["total_jobs"],
            extracted_count=extracted,
            embedded_count=embedded,
            stage_errors=stage_errors,
            message=(
                f"Scraped {result['total_jobs']} jobs from "
                f"{len([s for s in result['results_by_scraper'] if not s.get('error')])} scrapers; "
                f"extracted={extracted}, embedded={embedded}, "
                f"stage_errors={len(flat_errors)}"
            ),
        )
    except Exception as e:
        logger.exception("Manual scrape failed: %s", e)
        stage_errors = {"scrape": ["Scrape failed unexpectedly"]}
        return ScrapeResponse(
            success=False,
            total_jobs=0,
            scrapers=[],
            errors=["Scrape failed unexpectedly"],
            scraped_jobs=0,
            extracted_count=0,
            embedded_count=0,
            stage_errors=stage_errors,
            message="Scrape failed with error",
        )
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    # Entry point guard - cannot be unit tested
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8084)
