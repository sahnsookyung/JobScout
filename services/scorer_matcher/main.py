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
import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request
from pydantic import BaseModel

from core.config_loader import load_config
from core.app_context import AppContext
from core.redis_streams import (
    read_stream,
    ack_message,
    publish_completion,
    CHANNEL_MATCHING_DONE,
    STREAM_MATCHING,
)
from pipeline.runner import run_matching_pipeline

logger = logging.getLogger(__name__)

CONSUMER_GROUP = os.getenv("MATCHER_CONSUMER_GROUP", "matcher-service")
CONSUMER_NAME = os.getenv("HOSTNAME", "matcher-1")


# ---------------------------------------------------------------------------
# App state container — replaces module-level globals
# ---------------------------------------------------------------------------

class MatcherState:
    """Holds all mutable service-level state."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
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
    logger.info("Starting matcher service...")

    config = load_config()
    state = MatcherState(ctx=AppContext.build(config))
    app.state.matcher = state

    logger.info("Matcher service ready")
    state.consumer_task = asyncio.create_task(consume_matching_jobs(state))

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
    return {"status": "healthy", "service": "matcher"}


@app.get("/metrics")
async def metrics(request: Request):
    state: MatcherState = request.app.state.matcher
    return {
        "service": "matcher",
        "version": "1.0.0",
        "consumer_running": (
            state.consumer_task is not None and not state.consumer_task.done()
        ),
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
    ctx: AppContext, stop_event: threading.Event
):
    """Run the matching pipeline synchronously — safe to call via asyncio.to_thread."""
    return run_matching_pipeline(ctx, stop_event)


async def _process_matching_message(
    state: MatcherState, msg_id: str, msg: dict
) -> bool:
    """Process a single matching job. Returns True if successful."""
    task_id = msg.get("task_id")
    resume_fingerprint = msg.get("resume_fingerprint")
    fp_preview = (resume_fingerprint or "")[:16]

    logger.info(
        "📨 Received matching job: msg_id=%s, task_id=%s, fingerprint=%s...",
        msg_id, task_id, fp_preview,
    )

    try:
        result = await asyncio.to_thread(
            _run_matching_pipeline_sync, state.ctx, state.stop_event
        )
        matches_count = result.saved_count if result else 0
        await asyncio.to_thread(
            publish_completion,
            CHANNEL_MATCHING_DONE,
            {
                "task_id": task_id,
                "status": "completed",
                "resume_fingerprint": resume_fingerprint,
                "matches_count": matches_count,
            },
        )
        await asyncio.to_thread(ack_message, STREAM_MATCHING, CONSUMER_GROUP, msg_id)
        logger.info("✅ Matching job done: task_id=%s, matches=%d", task_id, matches_count)
        return True

    except Exception as e:
        logger.exception(
            "❌ Matching failed: task_id=%s, error=%s: %s", task_id, type(e).__name__, e
        )
        await asyncio.to_thread(
            publish_completion,
            CHANNEL_MATCHING_DONE,
            {"task_id": task_id, "status": "failed", "error": str(e)},
        )
        await asyncio.to_thread(ack_message, STREAM_MATCHING, CONSUMER_GROUP, msg_id)
        logger.info("✅ Acknowledged failed job: msg_id=%s", msg_id)
        return False


async def consume_matching_jobs(state: MatcherState) -> None:
    """Background task that consumes matching jobs from Redis Streams."""
    logger.info(
        "Starting matching consumer: %s (group: %s)", CONSUMER_NAME, CONSUMER_GROUP
    )
    message_count = 0
    error_count = 0

    while True:
        try:
            logger.debug("Waiting for matching job from Redis stream...")
            messages = await asyncio.to_thread(
                lambda: list(
                    read_stream(STREAM_MATCHING, CONSUMER_GROUP, CONSUMER_NAME, count=1, block=5000)
                )
            )

            if not messages:
                logger.debug("No messages received (timeout), continuing...")
                continue

            for msg_id, msg in messages:
                message_count += 1
                success = await _process_matching_message(state, msg_id, msg)
                if not success:
                    error_count += 1

        except asyncio.CancelledError:
            logger.info(
                "🛑 Matching consumer cancelled (processed: %d, errors: %d)",
                message_count, error_count,
            )
            raise

        except Exception as e:
            error_count += 1
            logger.exception(
                "❌ Error in matching consumer: %s: %s", type(e).__name__, e
            )
            await asyncio.sleep(1)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8083)
