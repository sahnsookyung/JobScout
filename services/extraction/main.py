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
from core.stream_consumer import StreamConsumerWithCompletion, validate_message
from core.redis_streams import (
    CHANNEL_EXTRACTION_DONE,
    STREAM_EXTRACTION,
)
from services.base.extraction import run_job_extraction, extract_resume as extract_resume_file

logger = logging.getLogger(__name__)

CONSUMER_GROUP = os.getenv("EXTRACTION_CONSUMER_GROUP", "extraction-service")
CONSUMER_NAME = os.getenv("HOSTNAME", "extraction-1")


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

        # Validate required fields
        is_valid, error = validate_message(msg, ["task_id", "resume_file"])
        if not is_valid:
            logger.error("❌ Invalid extraction job: %s", error)
            return False, {"status": "failed", "error": error}

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
            extract_resume_file, self.ctx, resume_path
        )

        status = "skipped" if not changed else "completed"
        logger.info(
            "✅ Extraction job done: task_id=%s, status=%s, fingerprint=%s...",
            task_id, status, (fingerprint or "")[:16],
        )

        return True, {
            "status": status,
            "resume_fingerprint": fingerprint or "",
        }


# ---------------------------------------------------------------------------
# App state container — replaces module-level globals
# ---------------------------------------------------------------------------

class ExtractionState:
    """Holds all mutable service-level state."""

    def __init__(self, ctx: AppContext, consumer: ExtractionConsumer) -> None:
        self.ctx = ctx
        self.consumer = consumer
        self.stop_event = threading.Event()
        self.consumer_task: Optional[asyncio.Task] = None


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    _setup_logging()
    logger.info("Starting extraction service...")

    config = load_config()
    ctx = AppContext.build(config)
    consumer = ExtractionConsumer(ctx)
    state = ExtractionState(ctx=ctx, consumer=consumer)
    app.state.extraction = state

    logger.info("Extraction service ready")
    state.consumer_task = asyncio.create_task(
        consumer.consume_loop(state.stop_event)
    )

    yield

    logger.info("Shutting down extraction service...")
    state.stop_event.set()

    if state.consumer_task:
        state.consumer_task.cancel()
        await asyncio.gather(state.consumer_task, return_exceptions=True)

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


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class ExtractJobRequest(BaseModel):
    limit: int = 200


class ExtractResumeRequest(BaseModel):
    resume_file: str


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
    return {"status": "healthy", "service": "extraction"}


@app.get("/metrics")
async def metrics(request: Request):
    state: ExtractionState = request.app.state.extraction
    return {
        "service": "extraction",
        "version": "1.0.0",
        "consumer_running": (
            state.consumer_task is not None and not state.consumer_task.done()
        ),
    }


@app.post("/extract/jobs", response_model=ExtractResponse)
async def extract_jobs(request: Request, body: ExtractJobRequest = ExtractJobRequest()):
    """Extract job data from job boards."""
    state: ExtractionState = request.app.state.extraction
    logger.info("Processing job extraction (limit: %d)", body.limit)

    try:
        processed = await asyncio.to_thread(
            run_job_extraction, state.ctx, state.stop_event, body.limit
        )
        return ExtractResponse(
            success=True,
            message="Job extraction completed",
            processed=processed,
        )
    except Exception:
        logger.error("Job extraction failed", exc_info=True)
        return ExtractResponse(
            success=False,
            message="Job extraction failed",
            processed=0,
        )


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
            extract_resume_file, state.ctx, resume_path
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
        logger.error("Resume extraction failed", exc_info=True)
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8081)
