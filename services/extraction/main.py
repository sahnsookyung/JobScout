#!/usr/bin/env python3
"""
Extraction Service - Handles job and resume ETL.

This service processes:
- Job scraping and extraction from job boards
- Resume parsing and profiling
- Consumes from Redis Streams (extraction:jobs)
"""

import asyncio
import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request
from pydantic import BaseModel

from core.config_loader import load_config
from core.app_context import AppContext
from core.logging_utils import setup_service_logging
from core.metrics import bind_worker_running
from core.metrics_router import router as metrics_router
from services.base.service_state import BaseServiceState
from core.stream_consumer import StreamConsumerWithCompletion, validate_message
from core.redis_streams import (
    CHANNEL_EXTRACTION_BATCH_DONE,
    CHANNEL_EXTRACTION_DONE,
    STREAM_EMBEDDINGS_BATCH,
    STREAM_EXTRACTION_BATCH,
    STREAM_EXTRACTION,
    enqueue_job,
)
from services.base.extraction import (
    ProviderQuotaExceeded,
    run_job_extraction,
    extract_resume as extract_resume_file,
)
from database.init_db import init_db
from database.models import SYSTEM_OWNER_ID
from database.uow import job_uow

logger = logging.getLogger(__name__)

CONSUMER_GROUP = os.getenv("EXTRACTION_CONSUMER_GROUP", "extraction-service")
CONSUMER_NAME = os.getenv("HOSTNAME", "extraction-1")
DEFAULT_BATCH_MAX_RETRIES = 3
BATCH_RETRY_COUNT_FIELD = "batch_retry_count"
BATCH_QUOTA_RETRY_COUNT_FIELD = "batch_quota_retry_count"
DEFAULT_BATCH_QUOTA_BACKOFF_SECONDS = 120
DEFAULT_BATCH_DAILY_QUOTA_BACKOFF_SECONDS = 900


def _batch_max_retries() -> int:
    try:
        return max(0, int(os.getenv("EXTRACTION_BATCH_MAX_RETRIES", DEFAULT_BATCH_MAX_RETRIES)))
    except (TypeError, ValueError):
        return DEFAULT_BATCH_MAX_RETRIES


def _env_int(name: str, default: int) -> int:
    try:
        return max(0, int(os.getenv(name, default)))
    except (TypeError, ValueError):
        return default


def _batch_quota_backoff_seconds(exc: Exception) -> int:
    message = str(exc).lower()
    daily_quota = any(token in message for token in ("per day", "tpd", "daily"))
    if daily_quota:
        return _env_int(
            "EXTRACTION_BATCH_DAILY_QUOTA_BACKOFF_SECONDS",
            DEFAULT_BATCH_DAILY_QUOTA_BACKOFF_SECONDS,
        )
    return _env_int(
        "EXTRACTION_BATCH_QUOTA_BACKOFF_SECONDS",
        DEFAULT_BATCH_QUOTA_BACKOFF_SECONDS,
    )


def _enqueue_followup_embeddings_batch(msg: dict) -> dict | None:
    payload = msg.get("enqueue_embeddings_batch")
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise ValueError("enqueue_embeddings_batch must be an object")

    enqueue_job(STREAM_EMBEDDINGS_BATCH, payload)
    logger.info(
        "Enqueued follow-up embeddings batch: task_id=%s",
        payload.get("task_id"),
    )
    return payload


def _requeue_extraction_batch(
    msg: dict,
    *,
    count_against_retry_cap: bool = True,
) -> tuple[bool, int, int]:
    retry_count = int(msg.get(BATCH_RETRY_COUNT_FIELD, 0) or 0)
    quota_retry_count = int(msg.get(BATCH_QUOTA_RETRY_COUNT_FIELD, 0) or 0)
    next_retry_count = retry_count + 1 if count_against_retry_cap else retry_count
    next_quota_retry_count = (
        quota_retry_count if count_against_retry_cap else quota_retry_count + 1
    )
    if count_against_retry_cap and retry_count >= _batch_max_retries():
        return False, retry_count, quota_retry_count

    payload = dict(msg)
    if count_against_retry_cap:
        payload[BATCH_RETRY_COUNT_FIELD] = next_retry_count
    else:
        payload[BATCH_QUOTA_RETRY_COUNT_FIELD] = next_quota_retry_count
    enqueue_job(STREAM_EXTRACTION_BATCH, payload)
    logger.info(
        "Requeued extraction batch: task_id=%s, retry_count=%d, quota_retry_count=%d",
        payload.get("task_id"),
        next_retry_count,
        next_quota_retry_count,
    )
    return True, next_retry_count, next_quota_retry_count


# ---------------------------------------------------------------------------
# Path validation — must be above all callers
# ---------------------------------------------------------------------------

def _validate_resume_path(resume_file: str) -> tuple[bool, str]:
    """Validate resume file path against allowed directories.

    Returns:
        Tuple of (is_valid, resolved_path or error_message)
    """
    resume_path = os.path.realpath(resume_file)
    allowed_dirs = [
        os.path.realpath("/app"),
        os.path.realpath("/data"),
        os.path.realpath(os.getcwd()),
    ]
    if not any(
        resume_path == d or resume_path.startswith(d + os.sep)
        for d in allowed_dirs
    ):
        return False, "Invalid resume file path"
    return True, resume_path


# ---------------------------------------------------------------------------
# Stream consumer for extraction service
# ---------------------------------------------------------------------------

class ExtractionConsumer(StreamConsumerWithCompletion):
    """Consumer for extraction jobs from Redis Streams."""

    def __init__(self, ctx: AppContext) -> None:
        super().__init__(
            stream=STREAM_EXTRACTION,
            group=CONSUMER_GROUP,
            consumer_name=CONSUMER_NAME,
            completion_channel=CHANNEL_EXTRACTION_DONE,
            logger=logger,
        )
        self.ctx = ctx

    async def _do_process(self, msg_id: str, msg: dict) -> tuple[bool, dict]:
        """Process an extraction job.

        Args:
            msg_id: Redis Stream message ID
            msg: Message data dict with task_id and resume_file

        Returns:
            Tuple of (success, result_data)
        """
        task_id = msg.get("task_id")
        resume_file = msg.get("resume_file")
        known_fingerprint = msg.get("known_fingerprint")

        # Validate required fields
        is_valid, error = validate_message(msg, ["task_id", "resume_file"])
        if not is_valid:
            logger.error("❌ Invalid extraction job: %s", error)
            return False, {"status": "failed", "error": error}
        owner_id = msg.get("owner_id") or SYSTEM_OWNER_ID

        # Validate resume path
        is_valid, result = _validate_resume_path(resume_file)
        if not is_valid:
            logger.error(
                "❌ Invalid path in extraction job: task_id=%s, file=%s",
                task_id, resume_file,
            )
            return False, {"status": "failed", "error": "Invalid resume file path"}

        resume_path = result
        logger.info(
            "⚙️ Processing extraction job: task_id=%s, file=%s",
            task_id, resume_path,
        )

        changed, fingerprint = await asyncio.to_thread(
            extract_resume_file,
            self.ctx,
            resume_path,
            known_fingerprint,
            False,
            owner_id,
        )

        status = "skipped" if not changed else "completed"
        if not changed:
            if not fingerprint:
                logger.error("❌ Extraction produced no fingerprint: task_id=%s", task_id)
                return False, {
                    "status": "failed",
                    "error": "Resume extraction did not produce a fingerprint",
                }

            with job_uow() as repo:
                structured_resume = repo.get_structured_resume_by_fingerprint(fingerprint)

            if structured_resume is None or not structured_resume.extracted_data:
                logger.error(
                    "❌ Extraction produced no structured resume: task_id=%s, fingerprint=%s...",
                    task_id,
                    fingerprint[:16],
                )
                return False, {
                    "status": "failed",
                    "error": "Resume extraction failed to produce structured data",
                    "resume_fingerprint": fingerprint,
                    "resume_upload_id": msg.get("resume_upload_id"),
                    "owner_id": owner_id,
                }

        logger.info(
            "✅ Extraction job done: task_id=%s, status=%s, fingerprint=%s...",
            task_id, status, (fingerprint or "")[:16],
        )

        return True, {
            "status": status,
            "resume_fingerprint": fingerprint or "",
            "resume_upload_id": msg.get("resume_upload_id"),
            "owner_id": owner_id,
        }


class ExtractionBatchConsumer(StreamConsumerWithCompletion):
    """Consumer for queued extraction batch jobs."""

    def __init__(self, ctx: AppContext, stop_event: threading.Event) -> None:
        super().__init__(
            stream=STREAM_EXTRACTION_BATCH,
            group=CONSUMER_GROUP,
            consumer_name=f"{CONSUMER_NAME}-batch",
            completion_channel=CHANNEL_EXTRACTION_BATCH_DONE,
            logger=logger,
        )
        self.ctx = ctx
        self.stop_event = stop_event

    async def _do_process(self, msg_id: str, msg: dict) -> tuple[bool, dict]:
        del msg_id

        is_valid, error = validate_message(msg, ["task_id"])
        if not is_valid:
            logger.error("❌ Invalid extraction batch job: %s", error)
            return False, {"status": "failed", "error": error}

        limit = int(msg.get("limit", 200) or 200)
        logger.info(
            "⚙️ Processing extraction batch: task_id=%s, limit=%d",
            msg["task_id"],
            limit,
        )

        try:
            processed = await asyncio.to_thread(
                run_job_extraction, self.ctx, self.stop_event, limit
            )
        except ProviderQuotaExceeded as exc:
            backoff_seconds = _batch_quota_backoff_seconds(exc)
            if backoff_seconds:
                logger.warning(
                    "Extraction provider quota exhausted; delaying batch retry for %ds: task_id=%s",
                    backoff_seconds,
                    msg["task_id"],
                )
                await asyncio.sleep(backoff_seconds)
            retry_enqueued, retry_count, quota_retry_count = _requeue_extraction_batch(
                msg,
                count_against_retry_cap=False,
            )
            logger.warning(
                "Extraction batch quota failure: task_id=%s, retry_enqueued=%s, retry_count=%d, quota_retry_count=%d",
                msg["task_id"],
                retry_enqueued,
                retry_count,
                quota_retry_count,
                exc_info=True,
            )
            return False, {
                "status": "failed",
                "error": str(exc),
                "retry_enqueued": retry_enqueued,
                "batch_retry_count": retry_count,
                "batch_quota_retry_count": quota_retry_count,
                "provider_quota_backoff_seconds": backoff_seconds,
            }
        except Exception as exc:
            retry_enqueued, retry_count, quota_retry_count = _requeue_extraction_batch(msg)
            logger.warning(
                "Extraction batch failed: task_id=%s, retry_enqueued=%s, retry_count=%d, quota_retry_count=%d",
                msg["task_id"],
                retry_enqueued,
                retry_count,
                quota_retry_count,
                exc_info=True,
            )
            return False, {
                "status": "failed",
                "error": str(exc),
                "retry_enqueued": retry_enqueued,
                "batch_retry_count": retry_count,
                "batch_quota_retry_count": quota_retry_count,
            }

        result = {"status": "completed", "processed": processed}
        try:
            followup_payload = _enqueue_followup_embeddings_batch(msg)
        except Exception as exc:
            logger.warning(
                "Unable to enqueue follow-up embeddings batch for extraction task %s",
                msg["task_id"],
                exc_info=True,
            )
            return False, {
                **result,
                "status": "failed",
                "error": f"Unable to enqueue follow-up embeddings batch: {exc}",
                "followup_stage": "embeddings",
            }

        if followup_payload is not None:
            result["followup_embeddings_batch_enqueued"] = True
            result["followup_embeddings_task_id"] = followup_payload.get("task_id")
        return True, result


# ---------------------------------------------------------------------------
# App state container — replaces module-level globals
# ---------------------------------------------------------------------------

class ExtractionState(BaseServiceState):
    """Holds all mutable service-level state."""


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_service_logging(logger)
    logger.info("Starting extraction service...")
    init_db()

    config = load_config()
    ctx = AppContext.build(config)
    stop_event = threading.Event()
    consumer = ExtractionConsumer(ctx)
    batch_consumer = ExtractionBatchConsumer(ctx, stop_event)
    state = ExtractionState(
        ctx=ctx,
        consumer=consumer,
        batch_consumer=batch_consumer,
        stop_event=stop_event,
    )
    app.state.extraction = state

    logger.info("Extraction service ready")
    state.consumer_task = asyncio.create_task(
        consumer.consume_loop(state.stop_event)
    )
    state.batch_consumer_task = asyncio.create_task(
        batch_consumer.consume_loop(state.stop_event)
    )

    yield

    logger.info("Shutting down extraction service...")
    state.stop_event.set()

    if state.consumer_task:
        state.consumer_task.cancel()
        await asyncio.gather(state.consumer_task, return_exceptions=True)
    if state.batch_consumer_task:
        state.batch_consumer_task.cancel()
        await asyncio.gather(state.batch_consumer_task, return_exceptions=True)

    ctx: AppContext = state.ctx
    if hasattr(ctx, "aclose"):
        await ctx.aclose()
    elif hasattr(ctx, "close"):
        ctx.close()

    logger.info("Extraction service shutdown complete")


app = FastAPI(
    title="Extraction Service",
    description="Job and resume ETL processing",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(metrics_router)


def _task_running(task: Optional[asyncio.Task]) -> bool:
    return task is not None and not task.done()


def _worker_running(worker_attr: str) -> bool:
    state = getattr(app.state, "extraction", None)
    if state is None:
        return False
    return _task_running(getattr(state, worker_attr, None))


bind_worker_running("extraction", "consumer", lambda: _worker_running("consumer_task"))
bind_worker_running("extraction", "batch_consumer", lambda: _worker_running("batch_consumer_task"))


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class ExtractResumeRequest(BaseModel):
    resume_file: str
    force_re_extraction: bool = False
    owner_id: str = SYSTEM_OWNER_ID


class ExtractResponse(BaseModel):
    success: bool
    message: str
    processed: int = 0
    fingerprint: Optional[str] = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    consumer_running = _worker_running("consumer_task")
    batch_consumer_running = _worker_running("batch_consumer_task")
    return {
        "status": "healthy" if consumer_running and batch_consumer_running else "degraded",
        "service": "extraction",
        "consumer_running": consumer_running,
        "batch_consumer_running": batch_consumer_running,
    }


@app.post("/extract/resume", response_model=ExtractResponse)
async def extract_resume(request: Request, body: ExtractResumeRequest):
    """Extract resume data from file."""
    state: ExtractionState = request.app.state.extraction
    logger.info("Processing resume extraction request")

    is_valid, result = _validate_resume_path(body.resume_file)
    if not is_valid:
        return ExtractResponse(success=False, message=result, processed=0)

    resume_path = result
    try:
        changed, fingerprint = await asyncio.to_thread(
            extract_resume_file,
            state.ctx,
            resume_path,
            None,
            body.force_re_extraction,
            body.owner_id,
        )
        if changed:
            return ExtractResponse(
                success=True,
                message="Resume processed successfully",
                processed=1,
                fingerprint=fingerprint,
            )
        return ExtractResponse(
            success=True,
            message="Resume unchanged, no processing needed",
            processed=0,
            fingerprint=fingerprint,
        )
    except Exception:
        logger.exception("Resume extraction failed")
        return ExtractResponse(
            success=False,
            message="Resume extraction failed",
            processed=0,
        )


@app.post("/extract/stop")
async def stop_extraction(request: Request):
    """Signal any in-progress extraction run to stop gracefully."""
    state: ExtractionState = request.app.state.extraction
    state.stop_event.set()
    return {"success": True, "message": "Stop signal sent"}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8081)
