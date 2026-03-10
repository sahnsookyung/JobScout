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
from core.redis_streams import (
    read_stream,
    ack_message,
    publish_completion,
    CHANNEL_EMBEDDINGS_DONE,
    STREAM_EMBEDDINGS,
)
from services.base.embeddings import run_embedding_extraction, generate_resume_embedding

logger = logging.getLogger(__name__)

CONSUMER_GROUP = os.getenv("EMBEDDINGS_CONSUMER_GROUP", "embeddings-service")
CONSUMER_NAME = os.getenv("HOSTNAME", "embeddings-1")


# ---------------------------------------------------------------------------
# App state container — replaces module-level globals
# ---------------------------------------------------------------------------

class EmbeddingsState:
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
    logger.info("Starting embeddings service...")

    config = load_config()
    state = EmbeddingsState(ctx=AppContext.build(config))
    app.state.embeddings = state

    logger.info("Embeddings service ready")
    state.consumer_task = asyncio.create_task(consume_embeddings_jobs(state))

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
        logger.exception("Job embedding failed")
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
        logger.exception("Resume embedding failed")
        return EmbedResponse(success=False, message="Resume embedding failed", processed=0)


@app.post("/embed/stop")
async def stop_embeddings(request: Request):
    """Signal any in-progress embedding run to stop gracefully."""
    state: EmbeddingsState = request.app.state.embeddings
    state.stop_event.set()
    return {"success": True, "message": "Stop signal sent"}


# ---------------------------------------------------------------------------
# Stream consumer helpers
# ---------------------------------------------------------------------------

async def _process_embedding_message(
    state: EmbeddingsState, msg_id: str, msg: dict
) -> bool:
    """Process a single embedding job. Returns True if successful."""
    task_id = msg.get("task_id")
    resume_fingerprint = msg.get("resume_fingerprint")
    fp_preview = (resume_fingerprint or "")[:16]

    logger.info(
        "📨 Received embeddings job: msg_id=%s, task_id=%s, fingerprint=%s...",
        msg_id, task_id, fp_preview,
    )

    try:
        await asyncio.to_thread(generate_resume_embedding, state.ctx, resume_fingerprint)
        await asyncio.to_thread(
            publish_completion,
            CHANNEL_EMBEDDINGS_DONE,
            {
                "task_id": task_id,
                "status": "completed",
                "resume_fingerprint": resume_fingerprint,
            },
        )
        await asyncio.to_thread(ack_message, STREAM_EMBEDDINGS, CONSUMER_GROUP, msg_id)
        logger.info(
            "✅ Embeddings job done: task_id=%s, fingerprint=%s...", task_id, fp_preview
        )
        return True

    except Exception as e:
        logger.exception(
            "❌ Embeddings failed: task_id=%s, error=%s: %s", task_id, type(e).__name__, e
        )
        await asyncio.to_thread(
            publish_completion,
            CHANNEL_EMBEDDINGS_DONE,
            {"task_id": task_id, "status": "failed", "error": str(e)},
        )
        await asyncio.to_thread(ack_message, STREAM_EMBEDDINGS, CONSUMER_GROUP, msg_id)
        logger.info("✅ Acknowledged failed job: msg_id=%s", msg_id)
        return False


async def consume_embeddings_jobs(state: EmbeddingsState) -> None:
    """Background task that consumes embeddings jobs from Redis Streams."""
    logger.info(
        "Starting embeddings consumer: %s (group: %s)", CONSUMER_NAME, CONSUMER_GROUP
    )
    message_count = 0
    error_count = 0

    while not state.stop_event.is_set():
        try:
            logger.debug("Waiting for embeddings job from Redis stream...")
            messages = await asyncio.to_thread(
                lambda: list(
                    read_stream(
                        STREAM_EMBEDDINGS, CONSUMER_GROUP, CONSUMER_NAME, count=1, block=5000
                    )
                )
            )

            if not messages:
                logger.debug("No messages received (timeout), continuing...")
                continue

            for msg_id, msg in messages:
                message_count += 1
                success = await _process_embedding_message(state, msg_id, msg)
                if not success:
                    error_count += 1

        except asyncio.CancelledError:
            logger.info(
                "🛑 Embeddings consumer cancelled (processed: %d, errors: %d)",
                message_count, error_count,
            )
            raise

        except Exception as e:
            error_count += 1
            logger.exception(
                "❌ Error in embeddings consumer: %s: %s", type(e).__name__, e
            )
            await asyncio.sleep(1)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8082)