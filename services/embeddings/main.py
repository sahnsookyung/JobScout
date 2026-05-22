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
from core.logging_utils import setup_service_logging
from core.metrics import bind_worker_running
from core.metrics_router import router as metrics_router
from services.base.service_state import BaseServiceState
from core.stream_consumer import StreamConsumerWithCompletion, validate_message
from core.redis_streams import (
    CHANNEL_EMBEDDINGS_BATCH_DONE,
    CHANNEL_EMBEDDINGS_DONE,
    STREAM_EMBEDDINGS_BATCH,
    STREAM_EMBEDDINGS,
)
from services.base.embeddings import run_embedding_extraction, generate_resume_embedding
from database.init_db import init_db
from database.models import SYSTEM_OWNER_ID

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
        owner_id = msg.get("owner_id") or SYSTEM_OWNER_ID

        fp_preview = (resume_fingerprint or "")[:16]
        logger.info(
            "⚙️ Processing embeddings job: task_id=%s, fingerprint=%s...",
            task_id, fp_preview,
        )

        try:
            await asyncio.to_thread(
                generate_resume_embedding,
                self.ctx,
                resume_fingerprint,
                owner_id,
            )

            logger.info(
                "✅ Embeddings job done: task_id=%s, fingerprint=%s...",
                task_id, fp_preview,
            )

            return True, {
                "status": "completed",
                "resume_fingerprint": resume_fingerprint,
                "resume_upload_id": msg.get("resume_upload_id"),
                "owner_id": owner_id,
            }
        except Exception as e:
            logger.exception("❌ Embeddings failed: task_id=%s", task_id)
            return False, {"status": "failed", "error": str(e)}


class EmbeddingsBatchConsumer(StreamConsumerWithCompletion):
    """Consumer for queued embedding batch jobs."""

    def __init__(self, ctx: AppContext, stop_event: threading.Event) -> None:
        super().__init__(
            stream=STREAM_EMBEDDINGS_BATCH,
            group=CONSUMER_GROUP,
            consumer_name=f"{CONSUMER_NAME}-batch",
            completion_channel=CHANNEL_EMBEDDINGS_BATCH_DONE,
            logger=logger,
        )
        self.ctx = ctx
        self.stop_event = stop_event

    async def _do_process(self, msg_id: str, msg: dict) -> tuple[bool, dict]:
        del msg_id

        is_valid, error = validate_message(msg, ["task_id"])
        if not is_valid:
            logger.error("❌ Invalid embeddings batch job: %s", error)
            return False, {"status": "failed", "error": error}

        limit = int(msg.get("limit", 100) or 100)
        logger.info(
            "⚙️ Processing embeddings batch: task_id=%s, limit=%d",
            msg["task_id"],
            limit,
        )
        processed = await asyncio.to_thread(
            run_embedding_extraction, self.ctx, self.stop_event, limit
        )
        return True, {"status": "completed", "processed": processed}


# ---------------------------------------------------------------------------
# App state container — replaces module-level globals
# ---------------------------------------------------------------------------

class EmbeddingsState(BaseServiceState):
    """Holds all mutable service-level state."""


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_service_logging(logger)
    logger.info("Starting embeddings service...")
    init_db()

    config = load_config()
    ctx = AppContext.build(config)
    stop_event = threading.Event()
    consumer = EmbeddingsConsumer(ctx)
    batch_consumer = EmbeddingsBatchConsumer(ctx, stop_event)
    state = EmbeddingsState(
        ctx=ctx,
        consumer=consumer,
        batch_consumer=batch_consumer,
        stop_event=stop_event,
    )
    app.state.embeddings = state

    logger.info("Embeddings service ready")
    state.consumer_task = asyncio.create_task(
        consumer.consume_loop(state.stop_event)
    )
    state.batch_consumer_task = asyncio.create_task(
        batch_consumer.consume_loop(state.stop_event)
    )

    yield

    logger.info("Shutting down embeddings service...")
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

    logger.info("Embeddings service shutdown complete")


app = FastAPI(
    title="Embeddings Service",
    description="Vector embedding generation for jobs and resumes",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(metrics_router)


def _task_running(task: Optional[asyncio.Task]) -> bool:
    return task is not None and not task.done()


def _worker_running(worker_attr: str) -> bool:
    state = getattr(app.state, "embeddings", None)
    if state is None:
        return False
    return _task_running(getattr(state, worker_attr, None))


bind_worker_running("embeddings", "consumer", lambda: _worker_running("consumer_task"))
bind_worker_running("embeddings", "batch_consumer", lambda: _worker_running("batch_consumer_task"))


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class EmbedResumeRequest(BaseModel):
    resume_fingerprint: str
    owner_id: str = SYSTEM_OWNER_ID


class EmbedResponse(BaseModel):
    success: bool
    message: str
    processed: int = 0


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    consumer_running = _worker_running("consumer_task")
    batch_consumer_running = _worker_running("batch_consumer_task")
    return {
        "status": "healthy" if consumer_running and batch_consumer_running else "degraded",
        "service": "embeddings",
        "consumer_running": consumer_running,
        "batch_consumer_running": batch_consumer_running,
    }


@app.post("/embed/resume", response_model=EmbedResponse)
async def embed_resume(request: Request, body: EmbedResumeRequest):
    """Generate embeddings for resume."""
    state: EmbeddingsState = request.app.state.embeddings
    logger.info("Processing resume embedding request")

    try:
        await asyncio.to_thread(
            generate_resume_embedding,
            state.ctx,
            body.resume_fingerprint,
            body.owner_id,
        )
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
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8082)
