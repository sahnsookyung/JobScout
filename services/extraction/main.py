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
            pass


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
            message=f"Job extraction completed",
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
    logger.info(f"Processing resume: {request.resume_file}")

    if ctx is None:
        return ExtractResponse(
            success=False,
            message="Service not initialized",
            processed=0
        )

    # Validate file path to prevent path traversal
    import os
    resume_path = os.path.realpath(request.resume_file)
    # Define allowed directories as absolute paths
    ALLOWED_DIRS = [
        os.path.realpath('/app/'),
        os.path.realpath('/data/'),
        os.path.realpath(os.getcwd()),  # Current working directory
    ]
    if not any(resume_path.startswith(allowed_dir) for allowed_dir in ALLOWED_DIRS):
        return ExtractResponse(
            success=False,
            message="Invalid resume file path",
            processed=0
        )

    try:
        from services.base.extraction import process_resume
        loop = asyncio.get_running_loop()
        changed = await loop.run_in_executor(None, process_resume, ctx, request.resume_file)

        if changed:
            return ExtractResponse(
                success=True,
                message="Resume processed successfully",
                processed=1
            )
        else:
            return ExtractResponse(
                success=True,
                message="Resume already exists, no processing needed",
                processed=0
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
    uvicorn.run(app, host="0.0.0.0", port=8081)


async def consume_extraction_jobs():
    """Background task that consumes extraction jobs from Redis Streams."""
    from services.base.extraction import process_resume

    logger.info(f"Starting extraction consumer: {CONSUMER_NAME}")
    loop = asyncio.get_running_loop()

    while True:
        try:
            messages = await loop.run_in_executor(
                None,
                lambda: list(read_stream(STREAM_EXTRACTION, CONSUMER_GROUP, CONSUMER_NAME, count=1, block=5000))
            )
            for msg_id, msg in messages:
                task_id = msg.get("task_id")
                resume_file = msg.get("resume_file")

                logger.info(f"Processing extraction job: task_id={task_id}, file={resume_file}")

                try:
                    changed = await loop.run_in_executor(None, process_resume, ctx, resume_file)

                    from database.uow import job_uow
                    with job_uow() as repo:
                        resume = repo.resume.get_latest_stored_resume_fingerprint()

                    status = "skipped" if not changed else "completed"
                    publish_completion(CHANNEL_EXTRACTION_DONE, {
                        "task_id": task_id,
                        "status": status,
                        "resume_fingerprint": resume or ""
                    })

                    ack_message(STREAM_EXTRACTION, CONSUMER_GROUP, msg_id)
                    logger.info(f"Extraction job done: task_id={task_id}, status={status}")

                except Exception as e:
                    logger.exception(f"Extraction failed: task_id={task_id}")
                    publish_completion(CHANNEL_EXTRACTION_DONE, {
                        "task_id": task_id,
                        "status": "failed",
                        "error": str(e)
                    })
                    # Ack to prevent infinite redelivery; failure is recorded in completion event
                    ack_message(STREAM_EXTRACTION, CONSUMER_GROUP, msg_id)
                    
        except asyncio.CancelledError:
            logger.info("Extraction consumer cancelled")
            break
        except Exception as e:
            logger.error(f"Error in extraction consumer: {e}")
            await asyncio.sleep(1)
