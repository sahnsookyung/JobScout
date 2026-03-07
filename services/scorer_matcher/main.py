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

from fastapi import FastAPI
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CONSUMER_GROUP = os.getenv("MATCHER_CONSUMER_GROUP", "matcher-service")
CONSUMER_NAME = os.getenv("HOSTNAME", "matcher-1")

ctx: AppContext | None = None
consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ctx, consumer_task
    logger.info("Starting matcher service...")
    config = load_config()
    ctx = AppContext.build(config)
    logger.info("Matcher service ready")
    
    consumer_task = asyncio.create_task(consume_matching_jobs())
    
    yield
    
    logger.info("Shutting down matcher service...")
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            raise


app = FastAPI(
    title="Matcher Service",
    description="Vector matching and scoring for jobs and resumes",
    version="1.0.0",
    lifespan=lifespan
)


class MatchResumeRequest(BaseModel):
    resume_fingerprint: str | None = None


class MatchJobRequest(BaseModel):
    job_ids: list[str] | None = None


class MatchResponse(BaseModel):
    success: bool
    message: str
    matches: int = 0
    task_id: str | None = None


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "matcher"}


@app.get("/metrics")
async def metrics():
    return {"service": "matcher", "version": "1.0.0"}


@app.post("/match/resume", response_model=MatchResponse)
async def match_resume(request: MatchResumeRequest):
    """Run matching for a resume."""
    global ctx
    logger.info("Running matching for resume request")

    if ctx is None:
        return MatchResponse(
            success=False,
            message="Service not initialized",
            matches=0
        )
    
    try:
        loop = asyncio.get_running_loop()

        def run_match():
            stop_event = threading.Event()
            result = run_matching_pipeline(ctx, stop_event)
            return result

        result = await loop.run_in_executor(None, run_match)
        
        if result and result.saved_count > 0:
            return MatchResponse(
                success=True,
                message=f"Matching complete, {result.saved_count} matches saved",
                matches=result.saved_count,
                task_id=f"match-{request.resume_fingerprint[:8] if request.resume_fingerprint else 'none'}"
            )
        else:
            return MatchResponse(
                success=True,
                message="No matches found",
                matches=0,
                task_id=f"match-{request.resume_fingerprint[:8] if request.resume_fingerprint else 'none'}"
            )
    except Exception as e:
        logger.exception("Matching failed")
        return MatchResponse(
            success=False,
            message=f"Matching failed: {str(e)}",
            matches=0
        )


@app.post("/match/jobs", response_model=MatchResponse)
async def match_jobs(request: MatchJobRequest):
    """Run matching for specific jobs.
    
    TODO: Implement actual job matching logic.
    Currently returns a stub response.
    """
    global ctx
    logger.info(f"Matching {len(request.job_ids) if request.job_ids else 0} jobs")

    if ctx is None:
        return MatchResponse(
            success=False,
            message="Service not initialized",
            matches=0
        )

    if not request.job_ids:
        return MatchResponse(
            success=True,
            message="No job IDs provided",
            matches=0
        )

    # TODO: Implement actual job matching logic using run_matching_pipeline
    return MatchResponse(
        success=False,
        message="Job matching not yet implemented",
        matches=0
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8083)


async def consume_matching_jobs():
    """Background task that consumes matching jobs from Redis Streams."""
    logger.info(f"Starting matching consumer: {CONSUMER_NAME} (group: {CONSUMER_GROUP})")
    loop = asyncio.get_running_loop()
    message_count = 0
    error_count = 0

    while True:
        try:
            logger.debug("Waiting for matching job from Redis stream...")
            messages = await loop.run_in_executor(
                None,
                lambda: list(read_stream(STREAM_MATCHING, CONSUMER_GROUP, CONSUMER_NAME, count=1, block=5000))
            )
            
            if not messages:
                logger.debug("No messages received (timeout), continuing...")
                continue

            for msg_id, msg in messages:
                message_count += 1
                task_id = msg.get("task_id")
                resume_fingerprint = msg.get("resume_fingerprint")

                logger.info(f"📨 Received matching job: msg_id={msg_id}, task_id={task_id}, fingerprint={(resume_fingerprint or '')[:16]}...")

                try:
                    def run_pipeline():
                        if ctx is None:
                            raise RuntimeError("AppContext not initialized")
                        stop_event = threading.Event()
                        return run_matching_pipeline(ctx, stop_event)

                    result = await loop.run_in_executor(None, run_pipeline)

                    matches_count = result.saved_count if result else 0

                    publish_completion(CHANNEL_MATCHING_DONE, {
                        "task_id": task_id,
                        "status": "completed",
                        "resume_fingerprint": resume_fingerprint,
                        "matches_count": matches_count
                    })

                    ack_message(STREAM_MATCHING, CONSUMER_GROUP, msg_id)
                    logger.info(f"✅ Matching job done: task_id={task_id}, matches={matches_count}")

                except Exception as e:
                    error_count += 1
                    logger.exception(f"❌ Matching failed: task_id={task_id}, error={type(e).__name__}: {e}")
                    publish_completion(CHANNEL_MATCHING_DONE, {
                        "task_id": task_id,
                        "status": "failed",
                        "error": str(e)
                    })
                    # Ack to prevent infinite redelivery; failure is recorded in completion event
                    ack_message(STREAM_MATCHING, CONSUMER_GROUP, msg_id)
                    logger.info(f"✅ Acknowledged failed job: msg_id={msg_id}")

        except asyncio.CancelledError:
            logger.info("🛑 Matching consumer cancelled (processed: %d, errors: %d)", message_count, error_count)
            raise
        except Exception as e:
            error_count += 1
            logger.exception(f"❌ Error in matching consumer: {type(e).__name__}: {e}")
            await asyncio.sleep(1)
