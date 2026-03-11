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
from typing import Optional

from fastapi import FastAPI, Request
from pydantic import BaseModel

from core.config_loader import load_config
from core.app_context import AppContext
from core.stream_consumer import StreamConsumerWithCompletion, validate_message
from core.redis_streams import (
    CHANNEL_EMBEDDINGS_DONE,
    STREAM_EMBEDDINGS,
)
from services.base.embeddings import run_embedding_extraction, generate_resume_embedding

logger = logging.getLogger(__name__)

CONSUMER_GROUP = os.getenv("EMBEDDINGS_CONSUMER_GROUP", "embeddings-service")
CONSUMER_NAME = os.getenv("HOSTNAME", "embeddings-1")


# ---------------------------------------------------------------------------
# Stream consumer for embeddings service
# ---------------------------------------------------------------------------

class EmbeddingsConsumer(StreamConsumerWithCompletion):
    """Consumer for embeddings jobs from Redis Streams."""

    def __init__(self, ctx: AppContext) -> None:
        super().__init__(
            stream=STREAM_EMBEDDINGS,
            group=CONSUMER_GROUP,
            consumer_name=CONSUMER_NAME,
            completion_channel=CHANNEL_EMBEDDINGS_DONE,
            logger=logger,
        )
        self.ctx = ctx

    async def _do_process(self, msg_id: str, msg: dict) -> tuple[bool, dict]:
        """Process an embeddings job.

        Args:
            msg_id: Redis Stream message ID
            msg: Message data dict with task_id and resume_fingerprint

        Returns:
            Tuple of (success, result_data)
        """
        task_id = msg.get("task_id")
        resume_fingerprint = msg.get("resume_fingerprint")

        # Validate required fields
        is_valid, error = validate_message(msg, ["task_id", "resume_fingerprint"])
        if not is_valid:
            logger.error("❌ Invalid embeddings job: %s", error)
            return False, {"status": "failed", "error": error}

        fp_preview = (resume_fingerprint or "")[:16]
        logger.info(
            "⚙️ Processing embeddings job: task_id=%s, fingerprint=%s...",
            task_id, fp_preview,
        )

        try:
            await asyncio.to_thread(generate_resume_embedding, self.ctx, resume_fingerprint)

            logger.info(
                "✅ Embeddings job done: task_id=%s, fingerprint=%s...",
                task_id, fp_preview,
            )

            return True, {
                "status": "completed",
                "resume_fingerprint": resume_fingerprint,
            }
        except Exception as e:
            logger.error(
                "❌ Embeddings failed: task_id=%s, error=%s: %s",
                task_id, type(e).__name__, e, exc_info=True,
            )
            return False, {"status": "failed", "error": str(e)}


# ---------------------------------------------------------------------------
# App state container — replaces module-level globals
# ---------------------------------------------------------------------------

class EmbeddingsState:
    """Holds all mutable service-level state."""

    def __init__(self, ctx: AppContext, consumer: EmbeddingsConsumer) -> None:
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
    logger.info("Starting embeddings service...")

    config = load_config()
    ctx = AppContext.build(config)
    consumer = EmbeddingsConsumer(ctx)
    state = EmbeddingsState(ctx=ctx, consumer=consumer)
    app.state.embeddings = state

    logger.info("Embeddings service ready")
    state.consumer_task = asyncio.create_task(
        consumer.consume_loop(state.stop_event)
    )

    yield

    logger.info("Shutting down embeddings service...")
    state.stop_event.set()

    if state.consumer_task:
        state.consumer_task.cancel()
        await asyncio.gather(state.consumer_task, return_exceptions=True)

    ctx: AppContext = state.ctx
    if hasattr(ctx, "aclose"):
        await ctx.aclose()
    elif hasattr(ctx, "close"):
        ctx.close()

    logger.info("Embeddings service shutdown complete")


app = FastAPI(
    title="Embeddings Service",
    description="Vector embedding generation for jobs and resumes",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class EmbedJobRequest(BaseModel):
    limit: int = 100


class EmbedResumeRequest(BaseModel):
    resume_fingerprint: str


class EmbedResponse(BaseModel):
    success: bool
    message: str
    processed: int = 0


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "embeddings"}


@app.get("/metrics")
async def metrics(request: Request):
    state: EmbeddingsState = request.app.state.embeddings
    return {
        "service": "embeddings",
        "version": "1.0.0",
        "consumer_running": (
            state.consumer_task is not None and not state.consumer_task.done()
        ),
    }


@app.post("/embed/jobs", response_model=EmbedResponse)
async def embed_jobs(request: Request, body: EmbedJobRequest = EmbedJobRequest()):
    """Generate embeddings for jobs."""
    state: EmbeddingsState = request.app.state.embeddings
    logger.info("Processing job embeddings (limit: %d)", body.limit)

    try:
        processed = await asyncio.to_thread(
            run_embedding_extraction, state.ctx, state.stop_event, body.limit
        )
        return EmbedResponse(
            success=True,
            message="Job embedding completed",
            processed=processed,
        )
    except Exception:
        logger.error("Job embedding failed", exc_info=True)
        return EmbedResponse(success=False, message="Job embedding failed", processed=0)


@app.post("/embed/resume", response_model=EmbedResponse)
async def embed_resume(request: Request, body: EmbedResumeRequest):
    """Generate embeddings for resume."""
    state: EmbeddingsState = request.app.state.embeddings
    logger.info("Processing resume embedding request")

    try:
        await asyncio.to_thread(generate_resume_embedding, state.ctx, body.resume_fingerprint)
        return EmbedResponse(success=True, message="Resume embedding completed", processed=1)
    except Exception:
        logger.error("Resume embedding failed", exc_info=True)
        return EmbedResponse(success=False, message="Resume embedding failed", processed=0)


@app.post("/embed/stop")
async def stop_embeddings(request: Request):
    """Signal any in-progress embedding run to stop gracefully."""
    state: EmbeddingsState = request.app.state.embeddings
    state.stop_event.set()
    return {"success": True, "message": "Stop signal sent"}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8082)