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

from fastapi import FastAPI
from pydantic import BaseModel

from core.config_loader import load_config
from core.app_context import AppContext
from core.redis_streams import (
    read_stream,
    ack_message,
    publish_completion,
    CHANNEL_EXTRACTION_DONE,
    STREAM_EXTRACTION,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CONSUMER_GROUP = os.getenv("EXTRACTION_CONSUMER_GROUP", "extraction-service")
CONSUMER_NAME = os.getenv("HOSTNAME", "extraction-1")

ctx: AppContext | None = None
consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ctx, consumer_task
    logger.info("Starting extraction service...")
    config = load_config()
    ctx = AppContext.build(config)
    logger.info("Extraction service ready")

    consumer_task = asyncio.create_task(consume_extraction_jobs())

    yield

    logger.info("Shutting down extraction service...")
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            raise
    logger.info("Extraction service shutdown complete")


app = FastAPI(
    title="Extraction Service",
    description="Job and resume ETL processing",
    version="1.0.0",
    lifespan=lifespan
)


class ExtractJobRequest(BaseModel):
    limit: int = 200


class ExtractResumeRequest(BaseModel):
    resume_file: str


class ExtractResponse(BaseModel):
    success: bool
    message: str
    processed: int = 0
    fingerprint: str | None = None


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "extraction"}


@app.get("/metrics")
async def metrics():
    return {"service": "extraction", "version": "1.0.0"}


@app.post("/extract/jobs", response_model=ExtractResponse)
async def extract_jobs(request: ExtractJobRequest = ExtractJobRequest(limit=200)):
    """Extract job data from job boards."""
    global ctx
    logger.info(f"Processing job extraction (limit: {request.limit})")

    if ctx is None:
        return ExtractResponse(
            success=False,
            message="Service not initialized",
            processed=0
        )

    try:
        from services.base.extraction import run_job_extraction
        stop_event = threading.Event()
        loop = asyncio.get_running_loop()
        processed = await loop.run_in_executor(
            None, run_job_extraction, ctx, stop_event, request.limit
        )
        return ExtractResponse(
            success=True,
            message="Job extraction completed",
            processed=processed
        )
    except Exception as e:
        logger.exception("Job extraction failed")
        return ExtractResponse(
            success=False,
            message=f"Job extraction failed: {str(e)}",
            processed=0
        )


@app.post("/extract/resume", response_model=ExtractResponse)
async def extract_resume(request: ExtractResumeRequest):
    """Extract resume data from file."""
    global ctx
    logger.info("Processing resume extraction request")

    if ctx is None:
        return ExtractResponse(
            success=False,
            message="Service not initialized",
            processed=0
        )

    # Validate file path to prevent path traversal
    is_valid, result = _validate_resume_path(request.resume_file)
    if not is_valid:
        return ExtractResponse(
            success=False,
            message=result,
            processed=0
        )
    resume_path = result

    try:
        from services.base.extraction import process_resume
        loop = asyncio.get_running_loop()
        changed, fingerprint = await loop.run_in_executor(None, process_resume, ctx, resume_path)

        if changed:
            return ExtractResponse(
                success=True,
                message="Resume processed successfully",
                processed=1,
                fingerprint=fingerprint
            )
        else:
            return ExtractResponse(
                success=True,
                message="Resume already exists, no processing needed",
                processed=0,
                fingerprint=fingerprint
            )
    except Exception as e:
        logger.exception("Resume extraction failed")
        return ExtractResponse(
            success=False,
            message=f"Resume extraction failed: {str(e)}",
            processed=0
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8081)


def _validate_resume_path(resume_file: str) -> tuple[bool, str]:
    """Validate resume file path against allowed directories.

    Returns:
        Tuple of (is_valid, resolved_path or error_message)
    """
    resume_path = os.path.realpath(resume_file)
    ALLOWED_DIRS = [
        os.path.realpath('/app'),
        os.path.realpath('/data'),
        os.path.realpath(os.getcwd()),
    ]
    # Ensure proper directory boundary check
    if not any(
        resume_path == allowed_dir or resume_path.startswith(allowed_dir + os.sep)
        for allowed_dir in ALLOWED_DIRS
    ):
        return False, "Invalid resume file path"
    return True, resume_path


async def consume_extraction_jobs():
    """Background task that consumes extraction jobs from Redis Streams."""
    from services.base.extraction import process_resume

    logger.info(f"Starting extraction consumer: {CONSUMER_NAME} (group: {CONSUMER_GROUP})")
    loop = asyncio.get_running_loop()
    message_count = 0
    error_count = 0

    while True:
        try:
            # Get messages one at a time using next() on the generator
            # The generator will block for up to block=5000ms waiting for messages
            # Use run_in_executor with a timeout to avoid blocking forever
            def get_one_message():
                gen = read_stream(STREAM_EXTRACTION, CONSUMER_GROUP, CONSUMER_NAME, count=1, block=5000)
                try:
                    return next(gen)
                except StopIteration:
                    return None

            logger.debug("Waiting for extraction job from Redis stream...")
            result = await asyncio.wait_for(loop.run_in_executor(None, get_one_message), timeout=10.0)

            if result is None:
                logger.debug("No messages received (timeout), continuing...")
                continue

            msg_id, msg = result
            message_count += 1
            task_id = msg.get("task_id")
            resume_file = msg.get("resume_file")

            logger.info(f"📨 Received extraction job: msg_id={msg_id}, task_id={task_id}, file={resume_file}")

            # Validate path before processing
            is_valid, result = _validate_resume_path(resume_file)
            if not is_valid:
                logger.error(f"❌ Invalid path in extraction job: task_id={task_id}, file={resume_file}")
                publish_completion(CHANNEL_EXTRACTION_DONE, {
                    "task_id": task_id,
                    "status": "failed",
                    "error": "Invalid resume file path"
                })
                ack_message(STREAM_EXTRACTION, CONSUMER_GROUP, msg_id)
                logger.info(f"✅ Acknowledged failed job: msg_id={msg_id}")
                error_count += 1
                continue

            logger.info(f"⚙️ Processing extraction job: task_id={task_id}, file={result}")

            try:
                if ctx is None:
                    raise RuntimeError("AppContext not initialized")
                # process_resume now returns (changed, fingerprint)
                changed, fingerprint = await loop.run_in_executor(None, process_resume, ctx, result)

                status = "skipped" if not changed else "completed"
                publish_completion(CHANNEL_EXTRACTION_DONE, {
                    "task_id": task_id,
                    "status": status,
                    "resume_fingerprint": fingerprint or ""
                })

                ack_message(STREAM_EXTRACTION, CONSUMER_GROUP, msg_id)
                logger.info(f"✅ Extraction job done: task_id={task_id}, status={status}, fingerprint={(fingerprint or '')[:16]}...")

            except Exception as e:
                error_count += 1
                logger.exception(f"❌ Extraction failed: task_id={task_id}, error={type(e).__name__}: {e}")
                publish_completion(CHANNEL_EXTRACTION_DONE, {
                    "task_id": task_id,
                    "status": "failed",
                    "error": str(e)
                })
                # Ack to prevent infinite redelivery; failure is recorded in completion event
                ack_message(STREAM_EXTRACTION, CONSUMER_GROUP, msg_id)
                logger.info(f"✅ Acknowledged failed job: msg_id={msg_id}")

        except asyncio.CancelledError:
            logger.info("🛑 Extraction consumer cancelled (processed: %d, errors: %d)", message_count, error_count)
            raise
        except Exception as e:
            error_count += 1
            logger.exception(f"❌ Error in extraction consumer: {type(e).__name__}: {e}")
            await asyncio.sleep(1)
