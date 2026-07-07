#!/usr/bin/env python3
"""
Matcher Service - Handles vector matching and scoring.

This service:
- Consumes from Redis Streams (matching:jobs)
- Runs the matching pipeline
- Publishes completion events

Note: Extraction and embeddings are now separate services.
"""

import asyncio
import json
import logging
import os
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Callable, Optional

from fastapi import FastAPI, Request
from pydantic import BaseModel
from sqlalchemy import func, select

from core.config_loader import load_config
from core.app_context import AppContext
from core.metrics import bind_worker_running
from core.metrics_router import router as metrics_router
from core.stream_consumer import StreamConsumerWithCompletion, validate_message
from core.redis_streams import (
    CHANNEL_MATCHING_DONE,
    STREAM_EMBEDDINGS_BATCH,
    STREAM_EXTRACTION_BATCH,
    STREAM_MATCHING,
    clear_task_cancellation_requested,
    enqueue_job,
    get_redis_client,
    is_task_cancellation_requested,
    set_task_state,
)
from database.models import JobPost
from database.uow import job_uow
from services.scorer_matcher.pipeline import run_matching_pipeline
from database.init_db import init_db

logger = logging.getLogger(__name__)

CONSUMER_GROUP = os.getenv("MATCHER_CONSUMER_GROUP", "matcher-service")
CONSUMER_NAME = os.getenv("HOSTNAME", "matcher-1")
PREPARATION_BACKFILL_LOCK_KEY = "matching:preparation_backfill:lock"
PREPARATION_BACKFILL_LOCK_TTL_SECONDS = int(os.getenv("MATCHER_PREP_BACKFILL_LOCK_TTL_SECONDS", "300"))
PREPARATION_BACKFILL_BATCH_SIZE = int(
    os.getenv(
        "MATCHER_PREP_BACKFILL_BATCH_SIZE",
        os.getenv("MATCHER_PREP_BACKFILL_LIMIT", "50"),
    )
)
PREPARATION_BACKFILL_MAX_BATCHES = int(os.getenv("MATCHER_PREP_BACKFILL_MAX_BATCHES", "10"))
MATCHING_PAGE_LOCK_TTL_SECONDS = int(os.getenv("MATCHER_PAGE_LOCK_TTL_SECONDS", "900"))


def _serialize_task_state(state: dict) -> dict:
    """Coerce terminal task state into JSON-safe primitives for Redis."""
    return json.loads(json.dumps(state, default=str))


def _compute_stale_result_metadata(
    owner_id: Optional[str],
    upload_id: Optional[str],
) -> dict:
    if not owner_id or not upload_id:
        return {}

    try:
        owner_uuid = uuid.UUID(str(owner_id))
    except (ValueError, TypeError, AttributeError):
        logger.warning("Invalid owner_id for stale-result check: %s", owner_id)
        return {}

    try:
        with job_uow() as repo:
            latest_upload = repo.get_latest_resume_upload(owner_uuid)
            if latest_upload is None:
                return {}
            latest_upload_id = str(latest_upload.id)
            latest_resume_fingerprint = latest_upload.resume_fingerprint
    except Exception:
        logger.warning("Failed to load latest upload for stale-result check", exc_info=True)
        return {}

    if latest_upload_id == str(upload_id):
        return {
            "stale_due_to_newer_upload": False,
            "latest_upload_id": latest_upload_id,
            "latest_resume_fingerprint": latest_resume_fingerprint,
        }

    return {
        "stale_due_to_newer_upload": True,
        "latest_upload_id": latest_upload_id,
        "latest_resume_fingerprint": latest_resume_fingerprint,
        "stale_message": (
            "These results were generated from an older resume upload. "
            "Run matching again to use your latest resume."
        ),
    }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _matching_page_size(config) -> int:
    matcher_config = getattr(getattr(config, "matching", None), "matcher", None)
    raw = getattr(matcher_config, "batch_size", None)
    try:
        return max(1, int(raw or 0))
    except (TypeError, ValueError):
        return 0


def _job_preparation_stats(
    resume_fingerprint: Optional[str] = None,
    *,
    matching_page: Optional[int] = None,
    matching_page_size: Optional[int] = None,
) -> dict:
    """Return user-safe active job preparation counts for pipeline visibility."""
    try:
        with job_uow() as repo:
            active_filter = JobPost.status == "active"
            jobs_seen = repo.db.scalar(
                select(func.count(JobPost.id)).where(active_filter)
            ) or 0
            jobs_ready = repo.db.scalar(
                select(func.count(JobPost.id)).where(
                    active_filter,
                    JobPost.is_extracted.is_(True),
                    JobPost.is_embedded.is_(True),
                    JobPost.summary_embedding.isnot(None),
                )
            ) or 0
            pending_extraction = repo.db.scalar(
                select(func.count(JobPost.id)).where(
                    active_filter,
                    JobPost.is_extracted.is_(False),
                )
            ) or 0
            pending_embedding = repo.db.scalar(
                select(func.count(JobPost.id)).where(
                    active_filter,
                    JobPost.is_extracted.is_(True),
                    JobPost.is_embedded.is_(False),
                )
            ) or 0
            stats = {
                "jobs_seen": int(jobs_seen),
                "jobs_ready_to_score": int(jobs_ready),
                "jobs_pending_extraction": int(pending_extraction),
                "jobs_pending_embedding": int(pending_embedding),
            }
            if resume_fingerprint:
                processed_fresh = repo.count_reusable_matches_for_resume(resume_fingerprint)
                pending_matching = repo.count_pending_matching_jobs(resume_fingerprint)
                stats.update(
                    {
                        "jobs_matching_processed_fresh": int(processed_fresh),
                        "jobs_pending_matching": int(pending_matching),
                        "matching_backlog_complete": int(pending_matching) == 0,
                    }
                )
            if matching_page_size is not None:
                stats["matching_page_size"] = int(matching_page_size)
            if matching_page is not None:
                stats["matching_pages_completed"] = max(0, int(matching_page) - 1)
            return stats
    except Exception:
        logger.warning("Failed to load job preparation counts", exc_info=True)
        return {}


def _maybe_enqueue_preparation_backfill(task_id: str, stats: dict) -> list[dict[str, str]]:
    """Enqueue bounded existing preparation batches when matching finds a backlog."""
    pending_extraction = int(stats.get("jobs_pending_extraction") or 0)
    pending_embedding = int(stats.get("jobs_pending_embedding") or 0)
    if pending_extraction <= 0 and pending_embedding <= 0:
        return []

    try:
        batch_size = max(1, PREPARATION_BACKFILL_BATCH_SIZE)
        max_batches = max(1, PREPARATION_BACKFILL_MAX_BATCHES)
        redis_client = get_redis_client()
        if not redis_client.set(
            PREPARATION_BACKFILL_LOCK_KEY,
            task_id,
            nx=True,
            ex=PREPARATION_BACKFILL_LOCK_TTL_SECONDS,
        ):
            return [{
                "code": "jobs_preparing",
                "pending_extraction": str(pending_extraction),
                "pending_embedding": str(pending_embedding),
                "backfill_state": "locked",
            }]

        queued_extraction = 0
        if pending_extraction > 0:
            extraction_batches = min(max_batches, (pending_extraction + batch_size - 1) // batch_size)
            for index in range(extraction_batches):
                limit = min(batch_size, pending_extraction - queued_extraction)
                if limit <= 0:
                    break
                enqueue_job(
                    STREAM_EXTRACTION_BATCH,
                    {
                        "task_id": f"{task_id}-prep-extract-{index + 1}",
                        "limit": limit,
                        "trigger": "matching_backfill",
                    },
                )
                queued_extraction += limit
        queued_embedding = 0
        if pending_embedding > 0:
            embedding_batches = min(max_batches, (pending_embedding + batch_size - 1) // batch_size)
            for index in range(embedding_batches):
                limit = min(batch_size, pending_embedding - queued_embedding)
                if limit <= 0:
                    break
                enqueue_job(
                    STREAM_EMBEDDINGS_BATCH,
                    {
                        "task_id": f"{task_id}-prep-embed-{index + 1}",
                        "limit": limit,
                        "trigger": "matching_backfill",
                    },
                )
                queued_embedding += limit
        return [{
            "code": "jobs_preparing",
            "pending_extraction": str(pending_extraction),
            "pending_embedding": str(pending_embedding),
            "queued_extraction": str(queued_extraction),
            "queued_embedding": str(queued_embedding),
            "batch_size": str(batch_size),
            "max_batches": str(max_batches),
        }]
    except Exception:
        logger.warning("Failed to enqueue preparation backfill", exc_info=True)
        return []


def _maybe_enqueue_next_matching_page(
    *,
    parent_task_id: str,
    current_page: int,
    resume_fingerprint: Optional[str],
    owner_id: Optional[str],
    result: Optional[object],
    stats: dict,
) -> list[dict[str, str]]:
    pending_matching = int(stats.get("jobs_pending_matching") or 0)
    if not resume_fingerprint or pending_matching <= 0:
        return []
    if not result or not getattr(result, "success", False) or getattr(result, "cancelled", False):
        return []

    saved_count = int(getattr(result, "saved_count", 0) or 0)
    if saved_count <= 0:
        return [{
            "code": "matching_backlog_no_progress",
            "pending_matching": str(pending_matching),
        }]

    next_page = max(1, int(current_page)) + 1
    next_task_id = f"{parent_task_id}-match-page-{next_page}"
    lock_key = f"matching:page:{parent_task_id}:{next_page}:lock"
    try:
        redis_client = get_redis_client()
        if not redis_client.set(
            lock_key,
            next_task_id,
            nx=True,
            ex=MATCHING_PAGE_LOCK_TTL_SECONDS,
        ):
            return [{
                "code": "matching_backlog_page_queued",
                "pending_matching": str(pending_matching),
                "next_task_id": next_task_id,
            }]

        payload = {
            "task_id": next_task_id,
            "resume_fingerprint": resume_fingerprint,
            "parent_task_id": parent_task_id,
            "matching_page": next_page,
        }
        if owner_id is not None:
            payload["owner_id"] = owner_id
        enqueue_job(STREAM_MATCHING, payload)
        return [{
            "code": "matching_backlog_page_enqueued",
            "pending_matching": str(pending_matching),
            "next_task_id": next_task_id,
            "next_page": str(next_page),
        }]
    except Exception:
        logger.warning("Failed to enqueue next matching page", exc_info=True)
        return [{
            "code": "matching_backlog_enqueue_failed",
            "pending_matching": str(pending_matching),
        }]


# ---------------------------------------------------------------------------
# Stream consumer for matcher service
# ---------------------------------------------------------------------------

class MatcherConsumer(StreamConsumerWithCompletion):
    """Consumer for matching jobs from Redis Streams."""

    def __init__(self, ctx: AppContext) -> None:
        super().__init__(
            stream=STREAM_MATCHING,
            group=CONSUMER_GROUP,
            consumer_name=CONSUMER_NAME,
            completion_channel=CHANNEL_MATCHING_DONE,
            logger=logger,
        )
        self.ctx = ctx
        self.stop_event = threading.Event()

    @staticmethod
    def _read_cancellation_status(
        task_id: str,
        step: str,
        task_stop_event: threading.Event,
    ) -> str:
        try:
            requested = is_task_cancellation_requested(task_id)
        except Exception:
            logger.warning("Failed to read cancellation state for %s", task_id)
            return "running"

        if not requested:
            return "running"

        task_stop_event.set()
        return "persisting" if step == "saving_results" else "cancellation_requested"

    @staticmethod
    def _write_task_state(task_id: str, state: dict, *, warning_message: str) -> None:
        try:
            set_task_state(task_id, _serialize_task_state(state), ttl=3600)
        except Exception:
            logger.warning(warning_message, task_id, exc_info=True)

    @staticmethod
    def _terminal_task_state(
        *,
        final_status: str,
        last_step: str,
        owner_id: Optional[str],
        upload_id: Optional[str],
        resume_fingerprint: Optional[str],
        result: Optional[object],
        stats: Optional[dict] = None,
        extra_warnings: Optional[list[dict[str, str]]] = None,
    ) -> dict:
        matches_count = result.matches_count if result else 0
        saved_count = result.saved_count if result else 0
        notified_count = result.notified_count if result else 0
        execution_time = result.execution_time if result else 0.0
        stale_metadata = _compute_stale_result_metadata(owner_id, upload_id)
        resolved_stats = {
            **(stats or _job_preparation_stats(resume_fingerprint)),
            "candidates_considered": matches_count,
            "matches_selected": matches_count,
            "matches_saved": saved_count,
            "notifications_sent": notified_count,
        }
        warnings = list(extra_warnings or [])
        if (
            final_status == "completed"
            and saved_count == 0
            and resolved_stats.get("jobs_ready_to_score", 0) == 0
        ):
            warnings.append({"code": "no_jobs_ready"})
        return {
            "status": final_status,
            "step": last_step,
            "task_type": "matching",
            "owner_id": owner_id,
            "upload_id": upload_id,
            "resume_fingerprint": resume_fingerprint,
            "updated_at": _utc_now_iso(),
            "stats": resolved_stats,
            "warnings": warnings,
            "result": {
                "matches_count": matches_count,
                "saved_count": saved_count,
                "notified_count": notified_count,
                "execution_time": execution_time,
            },
            "error": result.error if result and (result.cancelled or not result.success) else None,
            **stale_metadata,
        }

    @staticmethod
    def _failed_task_state(
        *,
        last_step: str,
        owner_id: Optional[str],
        upload_id: Optional[str],
        resume_fingerprint: Optional[str],
        error: Exception,
    ) -> dict:
        return {
            "status": "failed",
            "step": last_step,
            "task_type": "matching",
            "owner_id": owner_id,
            "upload_id": upload_id,
            "resume_fingerprint": resume_fingerprint,
            "error": str(error),
            "updated_at": _utc_now_iso(),
            "stats": _job_preparation_stats(resume_fingerprint),
        }

    async def _do_process(self, msg_id: str, msg: dict) -> tuple[bool, dict]:
        """Process a matching job.

        Args:
            msg_id: Redis Stream message ID
            msg: Message data dict with task_id and resume_fingerprint

        Returns:
            Tuple of (success, result_data)
        """
        task_id = msg.get("task_id")
        resume_fingerprint = msg.get("resume_fingerprint")
        upload_id = msg.get("resume_upload_id")
        owner_id = msg.get("owner_id")
        parent_task_id = msg.get("parent_task_id") or task_id
        state_task_id = parent_task_id or task_id
        try:
            matching_page = max(1, int(msg.get("matching_page") or 1))
        except (TypeError, ValueError):
            matching_page = 1
        matching_page_size = _matching_page_size(self.ctx.config)

        # Validate required fields
        is_valid, error = validate_message(msg, ["task_id", "resume_fingerprint"])
        if not is_valid:
            logger.error("❌ Invalid matching job: %s", error)
            return False, {"status": "failed", "error": error}

        fp_preview = (resume_fingerprint or "")[:16]
        logger.info(
            "⚙️ Processing matching job: task_id=%s, fingerprint=%s...",
            task_id, fp_preview,
        )

        last_step = "initializing"
        task_stop_event = threading.Event()
        initial_stats = _job_preparation_stats(
            resume_fingerprint,
            matching_page=matching_page,
            matching_page_size=matching_page_size,
        )
        backfill_warnings = _maybe_enqueue_preparation_backfill(task_id, initial_stats)

        def _update_task_state(step: str) -> None:
            nonlocal last_step
            last_step = step
            stats = _job_preparation_stats(
                resume_fingerprint,
                matching_page=matching_page,
                matching_page_size=matching_page_size,
            )
            self._write_task_state(
                state_task_id,
                {
                    "status": self._read_cancellation_status(
                        state_task_id,
                        step,
                        task_stop_event,
                    ),
                    "step": step,
                    "task_type": "matching",
                    "task_id": task_id,
                    "parent_task_id": parent_task_id,
                    "owner_id": owner_id,
                    "upload_id": upload_id,
                    "resume_fingerprint": resume_fingerprint,
                    "updated_at": _utc_now_iso(),
                    "stats": stats,
                    "warnings": backfill_warnings,
                },
                warning_message="Failed to write running task state for %s",
            )

        try:
            _update_task_state(last_step)
            run_kwargs = {}
            if owner_id is not None:
                run_kwargs["owner_id"] = owner_id
                if task_id is not None:
                    run_kwargs["task_id"] = task_id

            result = await asyncio.to_thread(
                _run_matching_pipeline_sync,
                self.ctx,
                task_stop_event,
                resume_fingerprint,
                _update_task_state,
                **run_kwargs,
            )

            saved_count = result.saved_count if result else 0
            logger.info(
                "✅ Matching job done: task_id=%s, matches=%d",
                task_id, saved_count,
            )

            if result and result.cancelled:
                final_status = "cancelled"
            elif result and not result.success:
                final_status = "failed"
            else:
                final_status = "completed"
            final_stats = _job_preparation_stats(
                resume_fingerprint,
                matching_page=matching_page + 1,
                matching_page_size=matching_page_size,
            )
            matching_page_warnings = _maybe_enqueue_next_matching_page(
                parent_task_id=parent_task_id,
                current_page=matching_page,
                resume_fingerprint=resume_fingerprint,
                owner_id=owner_id,
                result=result,
                stats=final_stats,
            )
            terminal_state = self._terminal_task_state(
                final_status=final_status,
                last_step=last_step,
                owner_id=owner_id,
                upload_id=upload_id,
                resume_fingerprint=resume_fingerprint,
                result=result,
                stats=final_stats,
                extra_warnings=backfill_warnings + matching_page_warnings,
            )
            terminal_state["task_id"] = task_id
            terminal_state["parent_task_id"] = parent_task_id
            self._write_task_state(
                state_task_id,
                terminal_state,
                warning_message="Failed to write completed task state for %s",
            )

            clear_task_cancellation_requested(task_id)
            if state_task_id != task_id:
                clear_task_cancellation_requested(state_task_id)
            success = bool(result and result.success and not result.cancelled) if result else True
            return success, {
                "status": final_status,
                "resume_fingerprint": resume_fingerprint,
                "matches_count": saved_count,
            }
        except Exception as e:
            logger.exception("❌ Matching failed: task_id=%s, error=%s", task_id, type(e).__name__)
            failed_state = self._failed_task_state(
                last_step=last_step,
                owner_id=owner_id,
                upload_id=upload_id,
                resume_fingerprint=resume_fingerprint,
                error=e,
            )
            failed_state["task_id"] = task_id
            failed_state["parent_task_id"] = parent_task_id
            self._write_task_state(
                state_task_id,
                failed_state,
                warning_message="Failed to write failed task state for %s",
            )
            clear_task_cancellation_requested(task_id)
            if state_task_id != task_id:
                clear_task_cancellation_requested(state_task_id)
            return False, {"status": "failed", "error": str(e)}


# ---------------------------------------------------------------------------
# App state container — replaces module-level globals
# ---------------------------------------------------------------------------

class MatcherState:
    """Holds all mutable service-level state."""

    def __init__(self, ctx: AppContext, consumer: MatcherConsumer) -> None:
        self.ctx = ctx
        self.consumer = consumer
        self.stop_event = threading.Event()
        self.consumer_task: Optional[asyncio.Task] = None


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

def _setup_logging() -> None:
    from core.logging_utils import setup_logging, is_nul_filter_active
    setup_logging()
    logger.debug("NUL log sanitization active=%s", is_nul_filter_active())


def _warm_up_cross_encoder(config) -> None:
    """Boot-time cross-encoder warm-up.

    Surfaces local-runtime deploy bugs immediately instead of letting them hide
    as "every match scored by heuristic". Controlled by MATCHER_STRICT_WARMUP
    (default true); when false, a failure is logged but the service still
    starts — useful for local dev without the model cache.
    """
    if os.getenv("MATCHER_SKIP_WARMUP", "false").lower() in {"1", "true", "yes"}:
        logger.info("Cross-encoder warm-up skipped via MATCHER_SKIP_WARMUP")
        return

    from core.scorer.semantic_fit import get_shared_local_cross_encoder_provider

    semantic_fit = getattr(getattr(config, "matching", None), "scorer", None)
    semantic_fit = getattr(semantic_fit, "semantic_fit", None) if semantic_fit else None
    if not semantic_fit or not semantic_fit.cross_encoder.local.enabled:
        logger.info("Cross-encoder warm-up skipped: local provider disabled in config")
        return

    local_cfg = semantic_fit.cross_encoder.local
    try:
        max_batch_size = int(local_cfg.max_batch_size)
    except (TypeError, ValueError):
        logger.info("Cross-encoder warm-up skipped: local config is not fully materialized")
        return
    provider = get_shared_local_cross_encoder_provider(
        model_name=local_cfg.model_name,
        cache_path=local_cfg.model_cache_path,
        runtime=local_cfg.runtime,
        max_batch_size=max_batch_size,
        trust_remote_code=local_cfg.trust_remote_code,
    )
    strict = os.getenv("MATCHER_STRICT_WARMUP", "true").lower() in {"1", "true", "yes"}
    try:
        diag = provider.warm_up()
        logger.info(
            "Cross-encoder warm-up succeeded: route=%s canary_score=%.3f",
            diag.get("provider_route"),
            diag.get("canary_score", 0.0),
        )
    # Warm-up is the one place we catch broadly, to decide strict-vs-lenient.
    except Exception:  # noqa: BLE001
        msg = (
            "Cross-encoder warm-up failed. The service cannot score requirements "
            "with the configured local model. "
            "If this is intentional (local dev without the model cache), set "
            "MATCHER_STRICT_WARMUP=false."
        )
        if strict:
            logger.exception("%s Error", msg)
            raise
        logger.exception("%s Error (continuing because MATCHER_STRICT_WARMUP=false)", msg)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _setup_logging()
    logger.info("Starting matcher service...")
    init_db()

    config = load_config()
    _warm_up_cross_encoder(config)
    ctx = AppContext.build(config)
    consumer = MatcherConsumer(ctx)
    state = MatcherState(ctx=ctx, consumer=consumer)
    app.state.matcher = state

    logger.info("Matcher service ready")
    state.consumer_task = asyncio.create_task(
        consumer.consume_loop(state.stop_event)
    )

    yield

    logger.info("Shutting down matcher service...")
    state.stop_event.set()

    if state.consumer_task:
        state.consumer_task.cancel()
        await asyncio.gather(state.consumer_task, return_exceptions=True)

    ctx: AppContext = state.ctx
    if hasattr(ctx, "aclose"):
        await ctx.aclose()
    elif hasattr(ctx, "close"):
        ctx.close()

    logger.info("Matcher service shutdown complete")


app = FastAPI(
    title="Matcher Service",
    description="Vector matching and scoring for jobs and resumes",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(metrics_router)


def _task_running(task: Optional[asyncio.Task]) -> bool:
    return task is not None and not task.done()


def _worker_running() -> bool:
    state = getattr(app.state, "matcher", None)
    if state is None:
        return False
    return _task_running(getattr(state, "consumer_task", None))


bind_worker_running("matcher", "consumer", _worker_running)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class MatchResumeRequest(BaseModel):
    resume_fingerprint: Optional[str] = None


class MatchJobRequest(BaseModel):
    job_ids: Optional[list[str]] = None


class MatchResponse(BaseModel):
    success: bool
    message: str
    matches: int = 0
    task_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    consumer_running = _worker_running()
    return {
        "status": "healthy" if consumer_running else "degraded",
        "service": "matcher",
        "consumer_running": consumer_running,
    }


@app.post("/match/resume", response_model=MatchResponse)
async def match_resume(request: Request, body: MatchResumeRequest):
    """Run matching for a resume."""
    state: MatcherState = request.app.state.matcher
    logger.info("Running matching for resume request")

    fp = body.resume_fingerprint
    task_id = f"match-{fp[:8] if fp else 'none'}"

    try:
        result = await asyncio.to_thread(
            _run_matching_pipeline_sync, state.ctx, state.stop_event
        )
        matches = result.saved_count if result else 0
        msg = f"Matching complete, {matches} matches saved" if matches > 0 else "No matches found"
        return MatchResponse(success=True, message=msg, matches=matches, task_id=task_id)

    except Exception:
        logger.exception("Matching failed")
        return MatchResponse(
            success=False,
            message="Matching failed",
            matches=0,
            task_id=task_id,
        )


@app.post("/match/jobs", response_model=MatchResponse)
async def match_jobs(request: Request, body: MatchJobRequest):
    """Run matching for specific jobs.

    Currently returns a stub response.
    """
    job_count = len(body.job_ids) if body.job_ids else 0
    logger.info("Matching %d jobs", job_count)

    if not body.job_ids:
        return MatchResponse(success=True, message="No job IDs provided", matches=0)

    # Job matching not yet implemented
    return MatchResponse(success=False, message="Job matching not yet implemented", matches=0)


@app.post("/match/stop")
async def stop_matching(request: Request):
    """Signal any in-progress pipeline run to stop gracefully."""
    state: MatcherState = request.app.state.matcher
    state.stop_event.set()
    return {"success": True, "message": "Stop signal sent"}


# ---------------------------------------------------------------------------
# Pipeline helpers
# ---------------------------------------------------------------------------

def _run_matching_pipeline_sync(
    ctx: AppContext,
    stop_event: threading.Event,
    resume_fingerprint: Optional[str] = None,
    status_callback: Optional[Callable[[str], None]] = None,
    owner_id: Optional[str] = None,
    task_id: Optional[str] = None,
):
    """Run the matching pipeline synchronously — safe to call via asyncio.to_thread."""
    run_kwargs = {
        "status_callback": status_callback,
        "resume_fingerprint": resume_fingerprint,
    }
    if owner_id is not None:
        run_kwargs["owner_id"] = owner_id
    if task_id is not None:
        run_kwargs["task_id"] = task_id

    return run_matching_pipeline(
        ctx,
        stop_event,
        **run_kwargs,
    )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8083)
