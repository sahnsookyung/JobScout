#!/usr/bin/env python3
"""
Embeddings Service - Handles vector generation for jobs and resumes.

This service processes:
- Job embedding generation
- Resume embedding generation
- Consumes from Redis Streams (embeddings:jobs)
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
    CHANNEL_EMBEDDINGS_DONE,
    STREAM_EMBEDDINGS,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CONSUMER_GROUP = os.getenv("EMBEDDINGS_CONSUMER_GROUP", "embeddings-service")
CONSUMER_NAME = os.getenv("HOSTNAME", "embeddings-1")

ctx: AppContext | None = None
consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ctx, consumer_task
    logger.info("Starting embeddings service...")
    config = load_config()
    ctx = AppContext.build(config)
    logger.info("Embeddings service ready")

    consumer_task = asyncio.create_task(consume_embeddings_jobs())

    yield

    logger.info("Shutting down embeddings service...")
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            raise  # Re-raise after cleanup to allow proper cancellation
    logger.info("Embeddings service shutdown complete")


app = FastAPI(
    title="Embeddings Service",
    description="Vector embedding generation for jobs and resumes",
    version="1.0.0",
    lifespan=lifespan
)


class EmbedJobRequest(BaseModel):
    limit: int = 100


class EmbedResumeRequest(BaseModel):
    resume_fingerprint: str


class EmbedResponse(BaseModel):
    success: bool
    message: str
    processed: int = 0


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "embeddings"}


@app.get("/metrics")
async def metrics():
    return {"service": "embeddings", "version": "1.0.0"}


@app.post("/embed/jobs", response_model=EmbedResponse)
async def embed_jobs(request: EmbedJobRequest = EmbedJobRequest(limit=100)):
    """Generate embeddings for jobs."""
    global ctx
    logger.info(f"Processing job embeddings (limit: {request.limit})")

    if ctx is None:
        return EmbedResponse(
            success=False,
            message="Service not initialized",
            processed=0
        )

    try:
        from services.base.embeddings import run_embedding_extraction
        stop_event = threading.Event()
        loop = asyncio.get_running_loop()
        processed = await loop.run_in_executor(
            None, run_embedding_extraction, ctx, stop_event, request.limit
        )
        return EmbedResponse(
            success=True,
            message="Job embedding completed",
            processed=processed
        )
    except Exception as e:
        logger.exception("Job embedding failed")
        return EmbedResponse(
            success=False,
            message=f"Job embedding failed: {str(e)}",
            processed=0
        )


@app.post("/embed/resume", response_model=EmbedResponse)
async def embed_resume(request: EmbedResumeRequest):
    """Generate embeddings for resume."""
    global ctx
    logger.info("Processing resume embedding request")

    if ctx is None:
        return EmbedResponse(
            success=False,
            message="Service not initialized",
            processed=0
        )

    try:
        from services.base.embeddings import generate_resume_embedding
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, generate_resume_embedding, ctx, request.resume_fingerprint)
        return EmbedResponse(
            success=True,
            message="Resume embedding completed",
            processed=1
        )
    except Exception as e:
        logger.exception("Resume embedding failed")
        return EmbedResponse(
            success=False,
            message=f"Resume embedding failed: {str(e)}",
            processed=0
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8082)


# pylint: disable=too-many-branches
async def consume_embeddings_jobs():
    """Background task that consumes embeddings jobs from Redis Streams."""
    from services.base.embeddings import generate_resume_embedding

    logger.info(f"Starting embeddings consumer: {CONSUMER_NAME} (group: {CONSUMER_GROUP})")
    loop = asyncio.get_running_loop()
    message_count = 0
    error_count = 0

    while True:
        try:
            logger.debug("Waiting for embeddings job from Redis stream...")
            messages = await loop.run_in_executor(
                None,
                lambda: list(read_stream(STREAM_EMBEDDINGS, CONSUMER_GROUP, CONSUMER_NAME, count=1, block=5000))
            )
            
            if not messages:
                logger.debug("No messages received (timeout), continuing...")
                continue

            for msg_id, msg in messages:
                message_count += 1
                task_id = msg.get("task_id")
                resume_fingerprint = msg.get("resume_fingerprint")

                logger.info(f"📨 Received embeddings job: msg_id={msg_id}, task_id={task_id}, fingerprint={(resume_fingerprint or '')[:16]}...")

                try:
                    if ctx is None:
                        raise RuntimeError("AppContext not initialized")
                    await loop.run_in_executor(None, generate_resume_embedding, ctx, resume_fingerprint)

                    publish_completion(CHANNEL_EMBEDDINGS_DONE, {
                        "task_id": task_id,
                        "status": "completed",
                        "resume_fingerprint": resume_fingerprint
                    })

                    ack_message(STREAM_EMBEDDINGS, CONSUMER_GROUP, msg_id)
                    logger.info(f"✅ Embeddings job done: task_id={task_id}, fingerprint={(resume_fingerprint or '')[:16]}...")

                except Exception as e:
                    error_count += 1
                    logger.exception(f"❌ Embeddings failed: task_id={task_id}, error={type(e).__name__}: {e}")
                    publish_completion(CHANNEL_EMBEDDINGS_DONE, {
                        "task_id": task_id,
                        "status": "failed",
                        "error": str(e)
                    })
                    # Ack to prevent infinite redelivery; failure is recorded in completion event
                    ack_message(STREAM_EMBEDDINGS, CONSUMER_GROUP, msg_id)
                    logger.info(f"✅ Acknowledged failed job: msg_id={msg_id}")

        except asyncio.CancelledError:
            logger.info("🛑 Embeddings consumer cancelled (processed: %d, errors: %d)", message_count, error_count)
            raise
        except Exception as e:
            error_count += 1
            logger.exception(f"❌ Error in embeddings consumer: {type(e).__name__}: {e}")
            await asyncio.sleep(1)
