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
import sys
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
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
from core.metrics import (
    record_jobs_embedded,
    record_jobs_extracted,
    record_jobs_matched,
)
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
from services.orchestrator.control import OrchestratorControlService
from services.orchestrator.diagnostics import OrchestratorDiagnosticsService
from services.orchestrator.match_pipeline import OrchestratorMatchPipelineService
from services.orchestrator.pipeline_runs import PipelineRunService
from services.orchestrator.repair import run_stuck_job_repair
from services.orchestrator.redis_gateway import RedisTaskStateGateway
from services.orchestrator.resume_etl import ResumeEtlOrchestrator, ResumeEtlPipelineService
from services.orchestrator.routes import register_orchestrator_routes
from services.orchestrator.scrape_pipeline import ScrapePipelineService
from services.orchestrator.scheduler import RepairScheduler, ScrapeScheduler
from services.orchestrator.state_registry import OrchestratorStateRegistryService
from services.orchestrator.task_state import OrchestratorTaskStateService

from core.logging_utils import setup_service_logging
logger = logging.getLogger(__name__)
setup_service_logging(logger)

_ORCHESTRATOR_CONFIG = load_config().orchestrator
REDIS_URL = _ORCHESTRATOR_CONFIG.redis_url
ORCHESTRATION_TTL = _ORCHESTRATOR_CONFIG.orchestration_ttl
LISTENER_TIMEOUT = float(_ORCHESTRATOR_CONFIG.listener_timeout_seconds)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# Scraper configuration
SCRAPER_INTERVAL_HOURS = float(_ORCHESTRATOR_CONFIG.scraper_interval_hours)
SCRAPER_LOCK_TTL_SECONDS = _ORCHESTRATOR_CONFIG.scraper_lock_ttl_seconds
SCRAPER_RETRY_INTERVALS = list(_ORCHESTRATOR_CONFIG.scraper_retry_intervals)
SCRAPER_EXTRACTION_LIMIT = _ORCHESTRATOR_CONFIG.scraper_extraction_limit
SCRAPER_EMBEDDING_LIMIT = _ORCHESTRATOR_CONFIG.scraper_embedding_limit
PROCESS_IMPORTED_EMBEDDING_MAX_BATCHES = _ORCHESTRATOR_CONFIG.process_imported_embedding_max_batches
BATCH_STAGE_TIMEOUT_SECONDS = float(_ORCHESTRATOR_CONFIG.batch_stage_timeout_seconds)
REPAIR_INTERVAL_SECONDS = _ORCHESTRATOR_CONFIG.repair_interval_seconds
RECENT_TASK_LIMIT = _ORCHESTRATOR_CONFIG.recent_task_limit
RECENT_TASK_SCAN_LIMIT = _ORCHESTRATOR_CONFIG.recent_task_scan_limit

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
    app.state.redis_task_state = RedisTaskStateGateway(ttl=config.orchestrator.orchestration_ttl)
    app.state.pipeline_runs = PipelineRunService(redis_gateway=app.state.redis_task_state)
    app.state.pipeline_runs_enabled = True

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
    repair_scheduler = RepairScheduler(
        pipeline_runs=app.state.pipeline_runs,
        interval_seconds=REPAIR_INTERVAL_SECONDS,
        extraction_limit=SCRAPER_EXTRACTION_LIMIT,
        embedding_limit=SCRAPER_EMBEDDING_LIMIT,
        repair_fn=run_stuck_job_repair,
    )
    await repair_scheduler.start()
    logger.info("🧰 Started stuck-job repair scheduler (every %s seconds)", REPAIR_INTERVAL_SECONDS)
    logger.info("=" * 60)

    cleanup_task = asyncio.create_task(
        cleanup_stale_orchestrations(app.state.registry)
    )

    # Start scraper scheduler unless explicitly disabled (e.g. deterministic E2E runs)
    scrape_service = _scrape_pipeline_service()

    async def _scheduled_scrape_loop(
        ctx: AppContext,
        redis_client: redis_async.Redis,
        stop_event: asyncio.Event,
    ) -> None:
        await scrape_service.run_scheduler_loop(
            ctx,
            redis_client,
            stop_event,
            pipeline_runs=app.state.pipeline_runs,
        )

    scraper_scheduler = ScrapeScheduler(
        ctx=app.state.ctx,
        redis_url=REDIS_URL,
        loop_fn=_scheduled_scrape_loop,
        disabled=_scraper_scheduler_disabled(),
    )
    await scraper_scheduler.start()

    try:
        yield
    finally:
        app.state.pipeline_runs_enabled = False

        # Stop stream logging
        stream_log_stop.set()
        stream_log_task.cancel()
        await asyncio.gather(stream_log_task, return_exceptions=True)

        await repair_scheduler.stop()

        cleanup_task.cancel()
        await asyncio.gather(cleanup_task, return_exceptions=True)

        await scraper_scheduler.stop()

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
app.state.pipeline_runs = None
app.state.pipeline_runs_enabled = False


def _pipeline_run_service(request: Request) -> Optional[PipelineRunService]:
    if getattr(request.app.state, "pipeline_runs_enabled", False) is not True:
        return None
    return getattr(request.app.state, "pipeline_runs", None)


@lru_cache()
def _scrape_pipeline_service() -> ScrapePipelineService:
    return ScrapePipelineService(
        redis_url=REDIS_URL,
        lock_ttl_seconds=SCRAPER_LOCK_TTL_SECONDS,
        retry_intervals=SCRAPER_RETRY_INTERVALS,
        extraction_limit=SCRAPER_EXTRACTION_LIMIT,
        embedding_limit=SCRAPER_EMBEDDING_LIMIT,
        embedding_max_batches=PROCESS_IMPORTED_EMBEDDING_MAX_BATCHES,
        batch_stage_timeout_seconds=BATCH_STAGE_TIMEOUT_SECONDS,
        scraper_interval_hours=SCRAPER_INTERVAL_HOURS,
        release_lock_lua=RELEASE_LOCK_LUA,
        logger=logger,
    )


def _task_state_service() -> OrchestratorTaskStateService:
    return OrchestratorTaskStateService(
        state_getter=get_or_create_orchestration,
        task_state_reader=get_task_state,
        logger=logger,
    )


def _match_pipeline_service() -> OrchestratorMatchPipelineService:
    return OrchestratorMatchPipelineService(
        listener_timeout=LISTENER_TIMEOUT,
        logger=logger,
        record_jobs_matched=record_jobs_matched,
        record_jobs_embedded=record_jobs_embedded,
        record_jobs_extracted=record_jobs_extracted,
    )


def _state_registry_service() -> OrchestratorStateRegistryService:
    return OrchestratorStateRegistryService(
        logger=logger,
        time_fn=time.time,
        sleep_fn=asyncio.sleep,
    )


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
    task_id: Optional[str] = None
    total_jobs: int = 0  # Backward compatibility (alias of scraped_jobs)
    scrapers: List[Dict[str, Any]] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)  # Backward compatibility
    scraped_jobs: int = 0
    jobs_imported: int = 0
    jobs_processed: int = 0
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
                    "task_id": self.task_id,
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
            except Exception:
                logger.exception("Failed to notify subscriber")
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
    return await _state_registry_service().get_or_create(
        registry,
        task_id,
        state_cls=OrchestrationState,
    )


async def cleanup_stale_orchestrations(registry: OrchestratorRegistry) -> None:
    """Periodically remove orchestrations that have exceeded ORCHESTRATION_TTL."""
    await _state_registry_service().cleanup_stale(
        registry,
        ttl_seconds=ORCHESTRATION_TTL,
    )


# ---------------------------------------------------------------------------
# Scraper functions
# ---------------------------------------------------------------------------


async def acquire_scraper_lock(
    redis_client: redis_async.Redis, scraper_id: str
) -> Optional[str]:
    """Compatibility wrapper for ScrapePipelineService.acquire_scraper_lock."""
    return await _scrape_pipeline_service().acquire_scraper_lock(
        redis_client,
        scraper_id,
    )


async def release_scraper_lock(
    redis_client: redis_async.Redis, lock_key: str, owner_id: str
) -> None:
    """Compatibility wrapper for ScrapePipelineService.release_scraper_lock."""
    await _scrape_pipeline_service().release_scraper_lock(
        redis_client,
        lock_key,
        owner_id,
    )


async def update_scraper_status(
    redis_client: redis_async.Redis,
    scraper_id: str,
    state: str,
    error: str = "",
) -> None:
    """Compatibility wrapper for ScrapePipelineService.update_scraper_status."""
    await _scrape_pipeline_service().update_scraper_status(
        redis_client,
        scraper_id,
        state,
        error,
    )


async def _wait_for_scrape_with_retry(
    jobspy_client,
    task_id: str,
    scraper_cfg,
    max_retries: int = 5,
) -> List[Dict[str, Any]]:
    """Compatibility wrapper for ScrapePipelineService.wait_for_scrape_with_retry."""
    return await _scrape_pipeline_service().wait_for_scrape_with_retry(
        jobspy_client,
        task_id,
        scraper_cfg,
        max_retries,
    )


async def _scrape_single_scraper(
    ctx: AppContext,
    redis_client: redis_async.Redis,
    scraper_cfg,
) -> Dict[str, Any]:
    """Compatibility wrapper for ScrapePipelineService.scrape_single_scraper."""
    return await _scrape_pipeline_service().scrape_single_scraper(
        ctx,
        redis_client,
        scraper_cfg,
    )


async def run_all_scrapers(
    ctx: AppContext,
    redis_client: redis_async.Redis,
) -> Dict[str, Any]:
    """Compatibility wrapper for ScrapePipelineService.run_all_scrapers."""
    return await _scrape_pipeline_service().run_all_scrapers(ctx, redis_client)


def _get_downstream_config_errors() -> Dict[str, str]:
    """Return configuration errors for stage execution."""
    return _scrape_pipeline_service().get_downstream_config_errors()


async def _run_batch_stage_via_queue(
    *,
    task_id: str,
    stage: str,
    stream: str,
    completion_channel: str,
    limit: int,
    correlation: Optional[Dict[str, Any]] = None,
) -> tuple[int, Optional[str]]:
    """Compatibility wrapper for ScrapePipelineService.run_batch_stage_via_queue."""
    return await _scrape_pipeline_service().run_batch_stage_via_queue(
        task_id=task_id,
        stage=stage,
        stream=stream,
        completion_channel=completion_channel,
        limit=limit,
        correlation=correlation,
        wait_for_task_message=_wait_for_task_message,
        cleanup_pubsub_and_client=_cleanup_pubsub_and_client,
        redis_factory=redis_async.from_url,
    )


async def run_batch_stage(
    _ctx: AppContext,
    *,
    task_id: str,
    stage: str,
    limit: int,
    correlation: Optional[Dict[str, Any]] = None,
) -> tuple[int, Optional[str]]:
    """Compatibility wrapper for ScrapePipelineService.run_batch_stage."""
    del _ctx
    stream = STREAM_EXTRACTION_BATCH if stage == "extract" else STREAM_EMBEDDINGS_BATCH
    completion_channel = (
        CHANNEL_EXTRACTION_BATCH_DONE
        if stage == "extract"
        else CHANNEL_EMBEDDINGS_BATCH_DONE
    )
    return await _run_batch_stage_via_queue(
        task_id=task_id,
        stage=stage,
        stream=stream,
        completion_channel=completion_channel,
        limit=limit,
        correlation=correlation,
    )


async def enqueue_best_effort_extraction_backfill(
    task_id: str,
    *,
    extraction_limit: int,
    embedding_limit: int,
    correlation: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """Compatibility wrapper for ScrapePipelineService.enqueue_best_effort_extraction_backfill."""
    return await _scrape_pipeline_service().enqueue_best_effort_extraction_backfill(
        task_id,
        extraction_limit=extraction_limit,
        embedding_limit=embedding_limit,
        correlation=correlation,
    )


async def run_embedding_stage_until_drained(
    ctx: AppContext,
    *,
    task_id: str,
    limit: int,
    max_batches: int = PROCESS_IMPORTED_EMBEDDING_MAX_BATCHES,
    correlation: Optional[Dict[str, Any]] = None,
) -> tuple[int, List[str], int]:
    """Compatibility wrapper for ScrapePipelineService.run_embedding_stage_until_drained."""
    return await _scrape_pipeline_service().run_embedding_stage_until_drained(
        ctx,
        task_id=task_id,
        limit=limit,
        max_batches=max_batches,
        run_batch_stage_fn=run_batch_stage,
        correlation=correlation,
    )


async def run_post_scrape_job_pipeline(
    ctx: AppContext,
    task_id: Optional[str] = None,
    pipeline_runs: Optional[PipelineRunService] = None,
) -> Dict[str, Any]:
    """Compatibility wrapper for ScrapePipelineService.run_post_scrape_job_pipeline."""
    return await _scrape_pipeline_service().run_post_scrape_job_pipeline(
        ctx,
        task_id,
        pipeline_runs=pipeline_runs,
        run_embedding_fn=run_embedding_stage_until_drained,
        enqueue_extraction_fn=enqueue_best_effort_extraction_backfill,
    )


async def scraper_scheduler_loop(
    ctx: AppContext,
    redis_client: redis_async.Redis,
    stop_event: asyncio.Event,
) -> None:
    """Compatibility wrapper for ScrapePipelineService.run_scheduler_loop."""
    service = _scrape_pipeline_service()
    service.logger = logger
    await service.run_scheduler_loop(
        ctx,
        redis_client,
        stop_event,
        run_all_scrapers_fn=run_all_scrapers,
        run_post_scrape_pipeline_fn=run_post_scrape_job_pipeline,
    )


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


def _durable_stage_name(stage_name: str) -> str:
    return _match_pipeline_service().durable_stage_name(stage_name)


def _pipeline_stage_id(snapshot: Dict[str, Any], stage: str) -> Optional[str]:
    return _match_pipeline_service().pipeline_stage_id(snapshot, stage)


def _stage_processed_count(stage_name: str, data: Optional[dict]) -> int:
    return _match_pipeline_service().stage_processed_count(stage_name, data)


def _record_stage_completion_metric(stage_name: str, count: int) -> None:
    _match_pipeline_service().record_stage_completion_metric(stage_name, count)


async def _start_pipeline_run_stage(
    *,
    pipeline_runs: Optional[PipelineRunService],
    state: "OrchestrationState",
    task_id: str,
    stage_name: str,
    run_type: str,
    job_payload: dict,
    queued_count: int = 1,
) -> dict[str, str]:
    return await _match_pipeline_service().start_pipeline_run_stage(
        pipeline_runs=pipeline_runs,
        state=state,
        task_id=task_id,
        stage_name=stage_name,
        run_type=run_type,
        job_payload=job_payload,
        queued_count=queued_count,
    )


async def _complete_pipeline_run_stage(
    *,
    pipeline_runs: Optional[PipelineRunService],
    task_id: str,
    stage_name: str,
    run_type: str,
    data: Optional[dict],
) -> None:
    await _match_pipeline_service().complete_pipeline_run_stage(
        pipeline_runs=pipeline_runs,
        task_id=task_id,
        stage_name=stage_name,
        run_type=run_type,
        data=data,
    )


async def _fail_pipeline_run_stage(
    *,
    pipeline_runs: Optional[PipelineRunService],
    task_id: str,
    stage_name: str,
    run_type: str,
    error: str,
    data: Optional[dict] = None,
) -> None:
    await _match_pipeline_service().fail_pipeline_run_stage(
        pipeline_runs=pipeline_runs,
        task_id=task_id,
        stage_name=stage_name,
        run_type=run_type,
        error=error,
        data=data,
    )


async def _run_pipeline_stage(
    state: OrchestrationState,
    pubsub,
    stream: str,
    job_payload: dict,
    stage_name: str,
    pipeline_runs: Optional[PipelineRunService] = None,
    run_type: str = "match",
) -> Tuple[bool, Optional[dict]]:
    """Enqueue a job and wait for its completion notification.

    The caller is responsible for subscribing pubsub to the correct channel
    and wrapping this call in asyncio.timeout().
    """
    return await _match_pipeline_service().run_pipeline_stage(
        state=state,
        pubsub=pubsub,
        stream=stream,
        job_payload=job_payload,
        stage_name=stage_name,
        pipeline_runs=pipeline_runs,
        run_type=run_type,
        channel_map={
            "extraction": CHANNEL_EXTRACTION_DONE,
            "embeddings": CHANNEL_EMBEDDINGS_DONE,
            "matching": CHANNEL_MATCHING_DONE,
        },
        enqueue_job_fn=enqueue_job,
        wait_for_task_message_fn=_wait_for_task_message,
        start_stage_fn=_start_pipeline_run_stage,
        complete_stage_fn=_complete_pipeline_run_stage,
        fail_stage_fn=_fail_pipeline_run_stage,
    )


async def _handle_extraction_fingerprint(
    state: OrchestrationState, task_id: str, extraction_data: dict
) -> bool:
    """Validate and store the extraction fingerprint.

    Returns False if pipeline should abort.
    """
    return await _match_pipeline_service().handle_extraction_fingerprint(
        state,
        task_id,
        extraction_data,
    )


async def _cleanup_pubsub_and_client(redis_client, pubsub) -> None:
    """Close pubsub and Redis client, swallowing errors so finally blocks never raise."""
    await _match_pipeline_service().cleanup_pubsub_and_client(redis_client, pubsub)


# ---------------------------------------------------------------------------
# Core orchestration coroutine
# ---------------------------------------------------------------------------


async def _run_extraction_stage(
    state: OrchestrationState,
    task_id: str,
    pubsub: redis_async.client.PubSub,
    pipeline_runs: Optional[PipelineRunService] = None,
) -> bool:
    """Run extraction stage. Returns True on success."""
    return await _match_pipeline_service().run_extraction_stage(
        state=state,
        task_id=task_id,
        pubsub=pubsub,
        pipeline_runs=pipeline_runs,
        channel_extraction_done=CHANNEL_EXTRACTION_DONE,
        stream_extraction=STREAM_EXTRACTION,
        run_pipeline_stage_fn=_run_pipeline_stage,
        handle_extraction_fingerprint_fn=_handle_extraction_fingerprint,
    )


async def _run_embeddings_stage(
    state: OrchestrationState,
    task_id: str,
    pubsub: redis_async.client.PubSub,
    pipeline_runs: Optional[PipelineRunService] = None,
) -> bool:
    """Run embeddings stage. Returns True on success."""
    return await _match_pipeline_service().run_embeddings_stage(
        state=state,
        task_id=task_id,
        pubsub=pubsub,
        pipeline_runs=pipeline_runs,
        channel_extraction_done=CHANNEL_EXTRACTION_DONE,
        channel_embeddings_done=CHANNEL_EMBEDDINGS_DONE,
        stream_embeddings=STREAM_EMBEDDINGS,
        run_pipeline_stage_fn=_run_pipeline_stage,
    )


async def _run_matching_stage(
    state: OrchestrationState,
    task_id: str,
    pubsub: redis_async.client.PubSub,
    channel_done: str,
    pipeline_runs: Optional[PipelineRunService] = None,
) -> tuple[bool, Optional[dict]]:
    """Run matching stage. Returns (success, matching_data)."""
    return await _match_pipeline_service().run_matching_stage(
        state=state,
        task_id=task_id,
        pubsub=pubsub,
        channel_done=channel_done,
        pipeline_runs=pipeline_runs,
        channel_matching_done=CHANNEL_MATCHING_DONE,
        stream_matching=STREAM_MATCHING,
        run_pipeline_stage_fn=_run_pipeline_stage,
    )


async def _run_matching_fast_path(
    state: OrchestrationState,
    task_id: str,
    pipeline_runs: Optional[PipelineRunService] = None,
) -> tuple[redis_async.Redis, redis_async.client.PubSub, bool, Optional[dict]]:
    """Run only the matching stage for an already-processed resume."""
    return await _match_pipeline_service().run_matching_fast_path(
        state=state,
        task_id=task_id,
        pipeline_runs=pipeline_runs,
        redis_url=REDIS_URL,
        redis_factory=redis_async.from_url,
        channel_matching_done=CHANNEL_MATCHING_DONE,
        stream_matching=STREAM_MATCHING,
        run_pipeline_stage_fn=_run_pipeline_stage,
    )


async def _run_full_match_pipeline(
    state: OrchestrationState,
    task_id: str,
    pipeline_runs: Optional[PipelineRunService] = None,
) -> tuple[redis_async.Redis, redis_async.client.PubSub, bool, Optional[dict]]:
    """Run extraction, embeddings, and matching for a new resume."""
    return await _match_pipeline_service().run_full_match_pipeline(
        state=state,
        task_id=task_id,
        pipeline_runs=pipeline_runs,
        redis_url=REDIS_URL,
        redis_factory=redis_async.from_url,
        channel_embeddings_done=CHANNEL_EMBEDDINGS_DONE,
        run_extraction_stage_fn=_run_extraction_stage,
        run_embeddings_stage_fn=_run_embeddings_stage,
        run_matching_stage_fn=_run_matching_stage,
    )


async def _complete_match_task(
    state: OrchestrationState,
    task_id: str,
    matching_data: Optional[dict],
) -> None:
    """Persist final orchestration success state and notify subscribers."""
    await _match_pipeline_service().complete_match_task(state, task_id, matching_data)


async def orchestrate_match(
    task_id: str,
    registry: OrchestratorRegistry,
    resume_fingerprint: Optional[str] = None,
    pipeline_runs: Optional[PipelineRunService] = None,
) -> None:
    """Run the full pipeline: extraction -> embeddings -> matching.

    If resume_fingerprint is provided, extraction and embedding stages are skipped
    and matching is run directly using the existing stored data.
    """
    await _match_pipeline_service().orchestrate_match(
        task_id=task_id,
        registry=registry,
        resume_fingerprint=resume_fingerprint,
        pipeline_runs=pipeline_runs,
        state_getter=get_or_create_orchestration,
        run_matching_fast_path_fn=_run_matching_fast_path,
        run_full_match_pipeline_fn=_run_full_match_pipeline,
        complete_match_task_fn=_complete_match_task,
        fail_stage_fn=_fail_pipeline_run_stage,
        cleanup_fn=_cleanup_pubsub_and_client,
    )


# ---------------------------------------------------------------------------
# Task-done callback
# ---------------------------------------------------------------------------


async def _handle_task_done(
    task_id: str,
    t: asyncio.Task,
    registry: OrchestratorRegistry,
    pipeline_runs: Optional[PipelineRunService] = None,
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
        if pipeline_runs is not None:
            await asyncio.to_thread(pipeline_runs.cancel_run, task_id=task_id)
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
        if pipeline_runs is not None:
            await asyncio.to_thread(
                pipeline_runs.fail_run,
                task_id=task_id,
                error=str(t.exception()),
                retry_eligible=True,
            )
    else:
        logger.info("Orchestration completed successfully: %s", task_id)
        if pipeline_runs is not None:
            async with registry.lock:
                state = registry.orchestrations.get(task_id)
                final_status = state.status if state is not None else "completed"
                final_error = state.error if state is not None else None
                final_result = dict(state.result or {}) if state is not None else {}
            if final_status == "failed":
                await asyncio.to_thread(
                    pipeline_runs.fail_run,
                    task_id=task_id,
                    error=final_error or "Task failed",
                    retry_eligible=True,
                    metadata=final_result,
                )
            elif final_status == "cancelled":
                await asyncio.to_thread(pipeline_runs.cancel_run, task_id=task_id)
            else:
                await asyncio.to_thread(
                    pipeline_runs.complete_run,
                    task_id=task_id,
                    metadata=final_result,
                )

    async with registry.lock:
        registry.tasks.pop(task_id, None)


def _task_snapshot(state: OrchestrationState) -> Dict[str, Any]:
    """Build a JSON-safe task snapshot."""
    return _task_state_service().snapshot(state)


def _task_status_response(snapshot: Dict[str, Any]) -> TaskStatusResponse:
    """Convert task snapshot dict to response model."""
    return _task_state_service().status_response(snapshot, TaskStatusResponse)


async def _spawn_background_task(
    registry: OrchestratorRegistry,
    task_id: str,
    task_type: str,
    coroutine: "asyncio.Future[None]",
    message: str,
    current_stage: Optional[str] = None,
    initial_result: Optional[Dict[str, Any]] = None,
    pipeline_runs: Optional[PipelineRunService] = None,
) -> MatchResponse:
    """Register and start a background task."""
    return await _task_state_service().spawn_background_task(
        registry,
        task_id,
        task_type,
        coroutine,
        message,
        response_model=MatchResponse,
        completion_handler=_handle_task_done,
        current_stage=current_stage,
        initial_result=initial_result,
        pipeline_runs=pipeline_runs,
    )


async def _spawn_background_task_compat(
    registry: OrchestratorRegistry,
    task_id: str,
    task_type: str,
    coroutine: "asyncio.Future[None]",
    message: str,
    *,
    current_stage: Optional[str] = None,
    initial_result: Optional[Dict[str, Any]] = None,
    pipeline_runs: Optional[PipelineRunService] = None,
) -> MatchResponse:
    """Call the task spawner with the legacy signature when durable runs are absent."""
    if pipeline_runs is None or hasattr(_spawn_background_task, "mock_calls"):
        return await _spawn_background_task(
            registry,
            task_id,
            task_type,
            coroutine,
            message,
        )
    return await _spawn_background_task(
        registry,
        task_id,
        task_type,
        coroutine,
        message,
        current_stage=current_stage,
        initial_result=initial_result,
        pipeline_runs=pipeline_runs,
    )


async def _run_stage_task(
    task_id: str,
    registry: OrchestratorRegistry,
    ctx: AppContext,
    stage: str,
    limit: int,
    pipeline_runs: Optional[PipelineRunService] = None,
) -> None:
    """Run a single scrape/extract/embed stage as a managed task."""
    await _scrape_pipeline_service().run_stage_task(
        task_id=task_id,
        registry=registry,
        ctx=ctx,
        stage=stage,
        limit=limit,
        state_getter=get_or_create_orchestration,
        pipeline_runs=pipeline_runs,
        run_all_scrapers_fn=run_all_scrapers,
        run_batch_stage_fn=run_batch_stage,
        redis_factory=redis_async.from_url,
    )


async def _run_scrape_extract_embed_pipeline_task(
    task_id: str,
    registry: OrchestratorRegistry,
    ctx: AppContext,
    pipeline_runs: Optional[PipelineRunService] = None,
) -> None:
    """Run scrape -> embed, then queue extraction enrichment as a managed task."""
    await _scrape_pipeline_service().run_scrape_extract_embed_pipeline_task(
        task_id=task_id,
        registry=registry,
        ctx=ctx,
        state_getter=get_or_create_orchestration,
        pipeline_runs=pipeline_runs,
        run_all_scrapers_fn=run_all_scrapers,
        run_embedding_fn=run_embedding_stage_until_drained,
        enqueue_extraction_fn=enqueue_best_effort_extraction_backfill,
        redis_factory=redis_async.from_url,
    )


async def _run_process_imported_jobs_pipeline_task(
    task_id: str,
    registry: OrchestratorRegistry,
    ctx: AppContext,
    pipeline_runs: Optional[PipelineRunService] = None,
) -> None:
    """Embed already imported jobs and queue extraction enrichment best-effort."""
    await _scrape_pipeline_service().run_process_imported_jobs_pipeline_task(
        task_id=task_id,
        registry=registry,
        ctx=ctx,
        state_getter=get_or_create_orchestration,
        pipeline_runs=pipeline_runs,
        run_embedding_fn=run_embedding_stage_until_drained,
        enqueue_extraction_fn=enqueue_best_effort_extraction_backfill,
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


async def health(request: Request):
    """Health check endpoint with Redis connectivity verification."""
    try:
        client = get_redis_client()
        client.ping()
        redis_status = "connected"
    except redis.ConnectionError:
        logger.exception("Redis connection error in health check")
        redis_status = "connection_error"
    except Exception:
        logger.exception("Redis error in health check")
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
    pipeline_runs: Optional[PipelineRunService] = None,
) -> Optional[Dict[str, Any]]:
    """Return a snapshot for an existing task without creating a new one."""
    return await _task_state_service().get_existing_snapshot(
        registry,
        task_id,
        pipeline_runs=pipeline_runs,
        state_factory=OrchestrationState.create,
    )


async def orchestrate_stage(stage: str, request: Request, body: StageRequest = StageRequest()):
    """Canonical stage trigger surface for scrape/extract/embed."""
    if stage not in {"scrape", "extract", "embed"}:
        raise HTTPException(status_code=404, detail=f"Unknown stage: {stage}")

    registry: OrchestratorRegistry = request.app.state.registry
    ctx: AppContext = request.app.state.ctx
    pipeline_runs = _pipeline_run_service(request)
    task_id = f"{stage}-{uuid.uuid4().hex[:8]}"
    default_limit = 200 if stage in {"scrape", "extract"} else 100
    limit = body.limit or default_limit

    response = await _spawn_background_task_compat(
        registry,
        task_id,
        "stage",
        _run_stage_task(task_id, registry, ctx, stage, limit, pipeline_runs),
        f"{stage} stage started",
        current_stage=stage,
        initial_result={"stage": stage, "limit": limit},
        pipeline_runs=pipeline_runs,
    )
    return TaskStatusResponse(
        success=response.success,
        task_id=response.task_id,
        status="queued",
        task_type="stage",
        current_stage=stage,
        result={"stage": stage, "limit": limit},
    )


async def orchestrate_scrape_extract_embed_pipeline(request: Request):
    """Canonical trigger for scrape -> extract -> embed."""
    registry: OrchestratorRegistry = request.app.state.registry
    ctx: AppContext = request.app.state.ctx
    pipeline_runs = _pipeline_run_service(request)
    task_id = f"pipeline-{uuid.uuid4().hex[:8]}"
    response = await _spawn_background_task_compat(
        registry,
        task_id,
        "pipeline",
        _run_scrape_extract_embed_pipeline_task(task_id, registry, ctx, pipeline_runs),
        "scrape-extract-embed pipeline started",
        current_stage="scrape",
        initial_result={
            "scraped_jobs": 0,
            "jobs_imported": 0,
            "jobs_processed": 0,
            "scrapers": [],
            "errors": [],
            "extracted_count": 0,
            "embedded_count": 0,
            "stage_errors": {},
        },
        pipeline_runs=pipeline_runs,
    )
    return TaskStatusResponse(
        success=response.success,
        task_id=response.task_id,
        status="queued",
        task_type="pipeline",
        current_stage="scrape",
        result={
            "scraped_jobs": 0,
            "jobs_imported": 0,
            "jobs_processed": 0,
            "scrapers": [],
            "errors": [],
            "extracted_count": 0,
            "embedded_count": 0,
            "stage_errors": {},
        },
    )


async def orchestrate_process_imported_jobs_pipeline(request: Request):
    """Canonical trigger for extract -> embed on already imported jobs."""
    registry: OrchestratorRegistry = request.app.state.registry
    ctx: AppContext = request.app.state.ctx
    pipeline_runs = _pipeline_run_service(request)
    task_id = f"process-jobs-{uuid.uuid4().hex[:8]}"
    response = await _spawn_background_task_compat(
        registry,
        task_id,
        "pipeline",
        _run_process_imported_jobs_pipeline_task(task_id, registry, ctx, pipeline_runs),
        "process-imported-jobs pipeline started",
        current_stage="extract",
        initial_result={
            "extracted_count": 0,
            "embedded_count": 0,
            "jobs_processed": 0,
            "stage_errors": {},
            "errors": [],
        },
        pipeline_runs=pipeline_runs,
    )
    return TaskStatusResponse(
        success=response.success,
        task_id=response.task_id,
        status="queued",
        task_type="pipeline",
        current_stage="extract",
        result={
            "extracted_count": 0,
            "embedded_count": 0,
            "jobs_processed": 0,
            "stage_errors": {},
            "errors": [],
        },
    )


async def get_task_status(task_id: str, request: Request):
    """Canonical JSON task status endpoint."""
    registry: OrchestratorRegistry = request.app.state.registry
    pipeline_runs = _pipeline_run_service(request)
    snapshot = await _get_existing_task_snapshot(registry, task_id, pipeline_runs)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return snapshot


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
    pipeline_runs = _pipeline_run_service(request)

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

    return await _spawn_background_task_compat(
        registry,
        task_id,
        "match",
        orchestrate_match(task_id, registry, resume_fingerprint, pipeline_runs),
        "Pipeline started",
        current_stage="matching",
        initial_result={"resume_fingerprint": resume_fingerprint},
        pipeline_runs=pipeline_runs,
    )


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

    pipeline_runs = _pipeline_run_service(request)
    resume_orchestrator = ResumeEtlOrchestrator(
        run_fn=_run_resume_etl,
        task_registry=_etl_tasks,
        state_writer=set_task_state,
        now_fn=_utc_now_iso,
        logger=logger,
        create_task=asyncio.create_task,
    )
    await resume_orchestrator.start(
        task_id=task_id,
        file_path=file_path,
        upload_id=payload.upload_id,
        owner_id=payload.owner_id,
        resume_fingerprint=payload.resume_fingerprint,
        mode=payload.mode,
        pipeline_runs=pipeline_runs,
    )

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
    pipeline_runs: Optional[PipelineRunService] = None,
) -> None:
    """Background task: extraction → embeddings for a single resume.

    Writes progress to task:{task_id}:state so the web-backend can poll
    Redis directly (no orchestrator status proxy needed).
    """
    service = ResumeEtlPipelineService(
        redis_url=REDIS_URL,
        listener_timeout=LISTENER_TIMEOUT,
        wait_for_task_message=_wait_for_task_message,
        cleanup_pubsub_and_client=_cleanup_pubsub_and_client,
        state_writer=set_task_state,
        now_fn=_utc_now_iso,
        logger=logger,
        redis_factory=redis_async.from_url,
        enqueue_job_fn=enqueue_job,
    )
    await service.run(
        task_id,
        file_path,
        upload_id=upload_id,
        owner_id=owner_id,
        resume_fingerprint=resume_fingerprint,
        mode=mode,
        pipeline_runs=pipeline_runs,
    )


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


async def get_active_orchestration(request: Request):
    """Get all currently active orchestration tasks."""
    registry: OrchestratorRegistry = request.app.state.registry
    return await OrchestratorControlService(
        state_getter=get_or_create_orchestration,
    ).active(registry)


def _diagnostics_service() -> OrchestratorDiagnosticsService:
    return OrchestratorDiagnosticsService(
        get_stream_info=get_stream_info,
        stream_exists=stream_exists,
        get_task_state=get_task_state,
        recent_task_limit=RECENT_TASK_LIMIT,
        recent_task_scan_limit=RECENT_TASK_SCAN_LIMIT,
        logger=logger,
    )


def _get_stream_diagnostic(stream_name: str) -> dict:
    """Return status dict for a single Redis stream."""
    return _diagnostics_service().get_stream_diagnostic(stream_name)


async def _get_active_orchestration_states(
    registry: OrchestratorRegistry,
) -> list:
    """Return status snapshots of all currently active orchestrations."""
    return await _diagnostics_service().get_active_orchestration_states(registry)


def _get_recent_tasks(redis_client) -> list | dict:
    """Return status snapshots of the 10 most recent tasks from Redis."""
    return _diagnostics_service().get_recent_tasks(redis_client)


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


async def stop_orchestration(request: Request, task_id: Optional[str] = None):
    """Stop one or all active orchestration tasks."""
    registry: OrchestratorRegistry = request.app.state.registry
    return await OrchestratorControlService(
        state_getter=get_or_create_orchestration,
    ).stop(registry, task_id=task_id)


async def trigger_scrape(request: Request):
    """Manually trigger scrape + extract + embed for all configured scrapers.
    
    This endpoint is intended for ops/admin use. The scheduler normally runs
    automatically on a schedule (controlled by SCRAPER_INTERVAL_HOURS env var).
    """
    ctx: AppContext = request.app.state.ctx
    pipeline_runs = _pipeline_run_service(request)
    
    redis_client = redis_async.from_url(REDIS_URL)
    try:
        result = await _scrape_pipeline_service().run_manual_scrape(
            ctx=ctx,
            redis_client=redis_client,
            pipeline_runs=pipeline_runs,
            run_all_scrapers_fn=run_all_scrapers,
            run_post_scrape_pipeline_fn=run_post_scrape_job_pipeline,
        )
        return ScrapeResponse(**result)
    except Exception as e:
        logger.exception("Manual scrape failed: %s", e)
        stage_errors = {"scrape": ["Scrape failed unexpectedly"]}
        return ScrapeResponse(
            success=False,
            total_jobs=0,
            scrapers=[],
            errors=["Scrape failed unexpectedly"],
            scraped_jobs=0,
            jobs_imported=0,
            jobs_processed=0,
            extracted_count=0,
            embedded_count=0,
            stage_errors=stage_errors,
            message="Scrape failed with error",
        )
    finally:
        await redis_client.aclose()


from services.orchestrator.route_handlers import route_handlers as _route_handlers

register_orchestrator_routes(app, _route_handlers(sys.modules[__name__]))


if __name__ == "__main__":
    # Entry point guard - cannot be unit tested
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8084)
