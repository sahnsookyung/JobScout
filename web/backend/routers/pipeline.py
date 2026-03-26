#!/usr/bin/env python3
"""
Pipeline endpoints - trigger and monitor matching pipeline.
"""

# Constants
TASK_NOT_FOUND_DETAIL = "Task not found"
ACTIVE_TASK_ID_KEY = "pipeline:active_task_id"
STOP_PIPELINE_ERROR = "Failed to stop pipeline"

import json
import os
import asyncio
import logging
import re
from pathlib import Path
from typing import Annotated, Optional
from urllib.parse import quote

from fastapi import APIRouter, UploadFile, File, HTTPException, Form, Request, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from sqlalchemy.orm import Session

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from ..services.pipeline_service import get_pipeline_manager
from core.redis_streams import (
    get_redis_client,
    set_task_state,
    get_task_state,
    STREAM_MATCHING,
    enqueue_job,
)
from database.uow import job_uow
from ..dependencies import get_db, get_current_user
from ..models.responses import (
    PipelineTaskResponse,
    PipelineStatusResponse,
    ResumeHashCheckResponse,
    ResumeUploadResponse,
    ResumeStatusResponse,
)
from ..models.requests import ResumeHashCheckRequest
from ..exceptions import PipelineLockedException
from etl.resume import ResumeParser
from web.shared.constants import RESUME_MAX_SIZE

logger = logging.getLogger(__name__)

# Strong references to fire-and-forget upload tasks — prevents GC on Python 3.12+
# where the event loop only keeps weak refs to asyncio.Task objects.
_upload_tasks: set = set()

limiter = Limiter(key_func=get_remote_address)

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

# Pre-compiled pattern for task_id validation
# Format used by orchestrator: "match-{8 hex chars}" e.g., "match-a1b2c3d4"
TASK_ID_PATTERN = re.compile(r'^[a-zA-Z0-9-]{1,50}$')


def _sanitize_for_logging(value: str) -> str:
    """Remove log injection characters (CRLF) from user input.

    Prevents CWE-117: Improper Output Neutralization for Logs.
    """
    if not isinstance(value, str):
        return str(value)
    # Remove CR, LF, and null bytes to prevent log forging
    # Using chr(13), chr(10), chr(0) to ensure actual control characters
    return value.replace(chr(13), '').replace(chr(10), '').replace(chr(0), '')


def _validate_task_id(task_id: str) -> bool:
    """Validate task_id using allowlist validation.

    Prevents CWE-952: URL manipulation and path injection attacks.

    Args:
        task_id: The task identifier from user input

    Returns:
        True if valid, False otherwise
    """
    if not task_id or not isinstance(task_id, str):
        return False
    if len(task_id) > 50:
        return False
    return bool(TASK_ID_PATTERN.match(task_id))


def add_rate_limit_handlers(app):
    """Add rate limit exception handlers to the FastAPI app."""
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


async def _rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": str(exc)}
    )


@router.post(
    "/run-matching",
    response_model=PipelineTaskResponse,
    responses={
        400: {"description": "No resume found"},
        409: {"description": "Pipeline is already running"},
        500: {"description": "Internal server error"},
    }
)
def run_matching_pipeline_endpoint(user: Annotated[None, Depends(get_current_user)] = None):
    """
    Trigger the full matching pipeline in the background.

    Returns immediately with a task_id that can be used to poll for status via SSE.
    The pipeline will:
    - Extract resume (if changed)
    - Generate embeddings
    - Run vector-based job matching
    - Calculate fit/want scores
    - Save results to database
    - Send notifications (if configured)

    Raises:
        409: Pipeline is already running.
        500: Internal error starting the pipeline.
    """
    return _start_matching()


def _guard_resume_not_uploading(redis) -> None:
    """Raise 409 if a resume upload is currently in progress.

    No-ops silently when Redis is unavailable.
    """
    try:
        if not redis:
            return
        latest_task_id = redis.get("resume:upload:latest_task_id")
        if not latest_task_id:
            return
        state = get_task_state(latest_task_id)
        if state and state.get("status") in ("processing", "running"):
            # "processing" = web-backend initial write; "running" = orchestrator stage active.
            # Both mean extraction/embedding is in progress; matching against the old
            # fingerprint would produce stale results.
            raise HTTPException(
                status_code=409,
                detail="Resume is currently being processed. Please wait and try again.",
            )
    except HTTPException:
        raise
    except Exception:
        pass  # Redis unavailable — proceed without guard


def _get_matching_redis_client():
    """Return a Redis client for matching orchestration, or None on failure."""
    try:
        return get_redis_client()
    except Exception:
        logger.warning("Redis unavailable — proceeding without active task check")
        return None


def _decode_redis_value(value) -> str:
    """Normalize Redis values to strings."""
    return value if isinstance(value, str) else value.decode()


def _normalize_matching_step(
    step: Optional[str],
    *,
    default: Optional[str] = None,
) -> Optional[str]:
    """Map raw backend stage names to the canonical matching step vocabulary."""
    if not step:
        return default

    normalized = step.strip().lower().replace(" ", "_").replace("-", "_")
    canonical_steps = {
        "initializing",
        "loading_resume",
        "vector_matching",
        "scoring",
        "saving_results",
        "notifying",
    }
    if normalized in canonical_steps:
        return normalized

    aliases = {
        "start": "initializing",
        "starting": "initializing",
        "queued": "initializing",
        "loading": "loading_resume",
        "resume_loading": "loading_resume",
        "resume_loaded": "loading_resume",
        "extracting": "loading_resume",
        "embedding": "loading_resume",
        "matching": "vector_matching",
        "match": "vector_matching",
        "vector_match": "vector_matching",
        "saving": "saving_results",
        "save": "saving_results",
        "saved": "saving_results",
        "notification": "notifying",
        "notify": "notifying",
    }
    return aliases.get(normalized, default)


def _build_pipeline_status_response(task_id: str, state: dict) -> PipelineStatusResponse:
    """Convert Redis task state into the shared pipeline status response shape."""
    result_data = state.get("result", {}) or {}
    status = state.get("status", "unknown")
    default_step = "initializing" if status in ("pending", "running") else None
    return PipelineStatusResponse(
        task_id=task_id,
        status=status,
        step=_normalize_matching_step(state.get("step"), default=default_step),
        matches_count=result_data.get("matches_count"),
        saved_count=result_data.get("saved_count"),
        notified_count=result_data.get("notified_count"),
        execution_time=result_data.get("execution_time"),
        error=state.get("error"),
    )


def _ensure_no_active_matching_task(redis) -> None:
    """Raise 409 if a matching task is already pending/running."""
    if not redis:
        return

    try:
        active_id_raw = redis.get(ACTIVE_TASK_ID_KEY)
        if not active_id_raw:
            return

        active_id = _decode_redis_value(active_id_raw)
        state = get_task_state(active_id)
        if state and state.get("status") in ("pending", "running"):
            raise HTTPException(status_code=409, detail="Matching pipeline is already running")
    except HTTPException:
        raise
    except Exception:
        logger.warning("Failed to check active task state in Redis")


def _get_latest_resume_fingerprint_or_400() -> str:
    """Return latest ready resume fingerprint or raise 400 with state context."""
    with job_uow() as repo:
        fingerprint = repo.get_latest_ready_resume_fingerprint()
        latest_state = (
            repo.get_resume_processing_state(fingerprint)
            if fingerprint
            else repo.resume.get_latest_resume_processing_state()
        )

    if fingerprint:
        return fingerprint

    if latest_state and latest_state.processing_status in {"extracting", "extracted", "embedding"}:
        raise HTTPException(
            status_code=400,
            detail=(
                "Latest resume upload is still processing "
                f"({latest_state.processing_status}). Please wait and try again."
            ),
        )
    raise HTTPException(
        status_code=400,
        detail="No ready resume found. Please upload and process a resume via the web UI first.",
    )


def _set_initial_matching_task_state(task_id: str) -> None:
    """Write initial pending state for matching tasks."""
    try:
        set_task_state(task_id, {"status": "pending", "step": "initializing"}, ttl=3600)
    except Exception:
        logger.warning("Failed to set initial Redis task state for %s", task_id)


def _store_active_task_id(redis, task_id: str) -> None:
    """Store active matching task ID in Redis when available."""
    if not redis:
        return

    try:
        redis.set(ACTIVE_TASK_ID_KEY, task_id, ex=3600)
    except Exception:
        logger.warning("Failed to store active_task_id in Redis for %s", task_id)


def _enqueue_matching_job_or_500(task_id: str, fingerprint: str) -> None:
    """Enqueue matching work or raise 500 if enqueue fails."""
    try:
        enqueue_job(STREAM_MATCHING, {
            "task_id": task_id,
            "resume_fingerprint": fingerprint,
            "correlation_id": task_id,
        })
    except Exception:
        logger.exception("Failed to enqueue matching job to stream")
        raise HTTPException(status_code=500, detail="Failed to start matching pipeline")


def _start_matching() -> PipelineTaskResponse:
    """Enqueue a matching job to the Redis stream for the scorer-matcher consumer."""
    import uuid as _uuid

    redis = _get_matching_redis_client()
    _ensure_no_active_matching_task(redis)
    _guard_resume_not_uploading(redis)
    fingerprint = _get_latest_resume_fingerprint_or_400()

    task_id = str(_uuid.uuid4())
    _set_initial_matching_task_state(task_id)
    _store_active_task_id(redis, task_id)
    _enqueue_matching_job_or_500(task_id, fingerprint)

    return PipelineTaskResponse(
        success=True,
        task_id=task_id,
        message="Matching pipeline started. Use SSE /api/pipeline/events/{task_id} to track progress.",
    )


def _stop_matching() -> PipelineTaskResponse:
    """Cancel the active matching task by marking it cancelled in Redis."""
    try:
        redis = get_redis_client()
        active_id_raw = redis.get(ACTIVE_TASK_ID_KEY)
        if not active_id_raw:
            raise HTTPException(status_code=404, detail="No active pipeline to stop")

        task_id = active_id_raw if isinstance(active_id_raw, str) else active_id_raw.decode()
        state = get_task_state(task_id)
        if not state or state.get("status") not in ("pending", "running"):
            raise HTTPException(status_code=404, detail="No active pipeline to stop")

        cancelled_state = {"status": "cancelled"}
        normalized_step = _normalize_matching_step(state.get("step"), default="initializing")
        if normalized_step:
            cancelled_state["step"] = normalized_step
        set_task_state(task_id, cancelled_state, ttl=3600)
        redis.delete(ACTIVE_TASK_ID_KEY)

        return PipelineTaskResponse(
            success=True,
            task_id=task_id,
            message="Pipeline cancellation requested.",
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to stop pipeline")
        raise HTTPException(status_code=500, detail=STOP_PIPELINE_ERROR)


@router.get("/status/{task_id}", response_model=PipelineStatusResponse, responses={500: {"description": "Internal server error"}, 404: {"description": "Task not found"}})
def get_pipeline_status(task_id: str):
    """
    Get the status of a pipeline task.

    Status values:
    - pending: Task created but not yet started
    - running: Pipeline is currently executing
    - completed: Pipeline finished successfully
    - failed: Pipeline encountered an error
    """
    # Check Redis first — task state is written by the scorer-matcher consumer
    try:
        state = get_task_state(task_id)
        if state:
            return _build_pipeline_status_response(task_id, state)
    except Exception:
        pass  # fall through to orchestrator

    # Proxy to orchestrator for tasks not yet reflected in Redis
    try:
        from web.backend.services.clients import orchestrator_client
        result = orchestrator_client.get_task_status(task_id)

        if not result.get("success"):
            raise HTTPException(status_code=404, detail=TASK_NOT_FOUND_DETAIL)

        return PipelineStatusResponse(
            task_id=task_id,
            status=result.get("status", "unknown"),
            step=_normalize_matching_step(
                result.get("current_stage"),
                default="initializing" if result.get("status") in ("pending", "running") else None,
            ),
            matches_count=result.get("result", {}).get("matches_count"),
            saved_count=result.get("result", {}).get("saved_count"),
            notified_count=result.get("result", {}).get("notified_count"),
            execution_time=result.get("result", {}).get("execution_time"),
            error=result.get("error")
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to get pipeline status")
        raise HTTPException(status_code=500, detail="Failed to get pipeline status")


@router.get("/active", response_model=Optional[PipelineStatusResponse])
def get_active_pipeline_task():
    """
    Get the currently running pipeline task, if any.

    Useful for frontend recovery on page refresh.
    """
    return _get_active_task()


def _get_active_task() -> Optional[PipelineStatusResponse]:
    """Return the active matching task from Redis, or None if nothing is running."""
    try:
        redis = get_redis_client()
        task_id_raw = redis.get(ACTIVE_TASK_ID_KEY)
        if not task_id_raw:
            return None
        task_id = task_id_raw if isinstance(task_id_raw, str) else task_id_raw.decode()
        state = get_task_state(task_id)
        if not state or state.get("status") not in ("running", "pending"):
            return None
        return _build_pipeline_status_response(task_id, state)
    except Exception:
        return None


@router.post(
    "/stop",
    response_model=PipelineTaskResponse,
    responses={
        404: {"description": "No active pipeline to stop"},
        500: {"description": "Internal server error"},
    }
)
def stop_matching_pipeline(user: Annotated[None, Depends(get_current_user)] = None):
    """
    Stop the currently running pipeline task.

    Raises:
        404: No active pipeline is running.
        500: Internal error stopping the pipeline.
    """
    return _stop_matching()


async def _stream_orchestrator_sse(orchestrator_url: str, task_id: str):
    """Async generator that proxies SSE bytes from the orchestrator."""
    import httpx

    # URL-encode task_id to prevent path injection (CWE-952)
    encoded_task_id = quote(task_id, safe='')

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=10.0)) as client:
            async with client.stream(
                "GET",
                f"{orchestrator_url}/orchestrate/status/{encoded_task_id}"
            ) as response:
                if response.status_code == 404:
                    logger.error("Orchestrator: task not found")
                    yield f"data: {json.dumps({'error': TASK_NOT_FOUND_DETAIL, 'status': 'failed'})}\n\n"
                    return
                if response.is_error:
                    logger.error("Orchestrator returned %s", response.status_code)
                    yield f"data: {json.dumps({'error': 'Failed to get pipeline status'})}\n\n"
                    return
                async for chunk in response.aiter_raw():
                    if chunk:
                        yield chunk
    except Exception:
        logger.exception("Failed to connect to orchestrator")
        yield f"data: {json.dumps({'error': 'Failed to connect to pipeline service'})}\n\n"


async def _preflight_task_check(orchestrator_url: str, _task_id: str) -> None:
    """Quick probe to verify task existence before opening a long-lived SSE stream.

    Raises HTTPException(404) only if the orchestrator definitively reports
    the task does not exist. Failures during the probe are logged and ignored
    so the SSE stream can surface errors itself.

    Note: _task_id is reserved for future use when the orchestrator supports
    task-specific status endpoints.
    """
    import httpx

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as probe:
            probe_resp = await probe.get(f"{orchestrator_url}/orchestrate/active")
            if probe_resp.status_code == 404:
                logger.warning("Task not found")
                raise HTTPException(status_code=404, detail=TASK_NOT_FOUND_DETAIL)
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("Pre-flight check failed: %s", type(e).__name__)


async def _stream_local_task_sse(task_id: str):
    """Poll Redis task state and emit SSE events for a running task."""
    timeout = 600.0
    poll_interval = 1.5
    elapsed = 0.0

    while elapsed < timeout:
        try:
            state = await asyncio.to_thread(get_task_state, task_id)
        except Exception:
            state = None

        if state is None:
            yield f"data: {json.dumps({'task_id': task_id, 'status': 'failed', 'error': 'Task not found'})}\n\n"
            return

        # Flatten result sub-dict to match PipelineStatusResponse shape expected by the frontend
        response = _build_pipeline_status_response(task_id, state)
        event_data = response.model_dump(exclude_none=True)

        yield f"data: {json.dumps(event_data)}\n\n"

        if state.get("status") in ("completed", "failed", "cancelled"):
            return

        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    yield f"data: {json.dumps({'status': 'failed', 'error': 'Timeout waiting for pipeline'})}\n\n"


@router.get(
    "/events/{task_id}",
    responses={
        404: {"description": TASK_NOT_FOUND_DETAIL},
        400: {"description": "Invalid task_id format"},
    }
)
async def pipeline_events(task_id: str, db: Annotated[Session, Depends(get_db)] = None):
    """
    Server-Sent Events endpoint for real-time pipeline status updates.

    Checks Redis first (task state written by scorer-matcher consumer), then
    falls back to proxying the orchestrator's SSE stream for tasks not yet
    reflected in Redis.

    Raises:
        404: Task does not exist.
        400: Invalid task_id format.
    """
    # Validate task_id BEFORE any use (OWASP: Validate Early)
    if not _validate_task_id(task_id):
        raise HTTPException(
            status_code=400,
            detail="Invalid task_id format. Must be alphanumeric with hyphens, max 50 characters."
        )

    # Check Redis first — task state is written by the scorer-matcher consumer
    try:
        state = await asyncio.to_thread(get_task_state, task_id)
    except Exception:
        state = None

    if state is not None:
        return StreamingResponse(
            _stream_local_task_sse(task_id),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    # Fall back to orchestrator SSE for tasks not managed via Redis
    orchestrator_url = os.getenv("ORCHESTRATOR_URL", "").strip()
    if not orchestrator_url:
        raise HTTPException(status_code=404, detail=TASK_NOT_FOUND_DETAIL)

    await _preflight_task_check(orchestrator_url, task_id)

    return StreamingResponse(
        _stream_orchestrator_sse(orchestrator_url, task_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
    )


@router.get(
    "/resume-status/{task_id}",
    response_model=ResumeStatusResponse,
    responses={
        400: {"description": "Invalid task_id format"},
        404: {"description": "Task not found"},
    }
)
def get_resume_status(task_id: str):
    """
    Poll the status of a background resume processing task.

    Status values:
    - processing: Resume ETL is currently running
    - completed: Resume was extracted and embedded successfully
    - failed: Resume processing encountered an error
    """
    if not _validate_task_id(task_id):
        raise HTTPException(
            status_code=400,
            detail="Invalid task_id format. Must be alphanumeric with hyphens, max 50 characters."
        )
    state = get_task_state(task_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Task not found or expired")
    return ResumeStatusResponse(
        task_id=task_id,
        status=state.get("status", "unknown"),
        step=state.get("step"),
        error=state.get("error")
    )


@router.post("/check-resume-hash", response_model=ResumeHashCheckResponse)
@limiter.limit("10/minute")
def check_resume_hash_endpoint(request: Request, body: ResumeHashCheckRequest, user: Annotated[None, Depends(get_current_user)] = None):
    """
    Check if a resume with the given hash already exists in the database.

    Used for deduplication - if the hash exists, the frontend can skip
    uploading the same file again. The frontend stores the file in IndexedDB.
    """
    from database.uow import job_uow

    with job_uow() as repo:
        exists = repo.resume.resume_hash_exists(body.resume_hash)

    return ResumeHashCheckResponse(
        exists=exists,
        resume_hash=body.resume_hash
    )


@router.post(
    "/upload-resume",
    response_model=ResumeUploadResponse,
    responses={
        400: {"description": "Invalid file, empty file, or hash mismatch"},
        413: {"description": "File size exceeds 2MB limit"},
        415: {"description": "Unsupported file format"},
        429: {"description": "Rate limit exceeded"},
    }
)
@limiter.limit("5/minute")
async def upload_resume_endpoint(
    request: Request,
    file: Annotated[UploadFile, File(...)],
    resume_hash: Annotated[Optional[str], Form()] = None,
    user: Annotated[None, Depends(get_current_user)] = None,
):
    """
    Upload a resume file.
    Supports: .json, .yaml, .yml, .txt, .docx, .pdf

    If resume_hash is provided, checks if it already exists first.
    If the hash exists, returns success without re-processing.

    The file is written to a temporary file for ETL processing, then cleaned up.
    Returns only the hash for frontend verification and IndexedDB storage.
    """
    from database.uow import job_uow

    # Validate file
    content = await _validate_resume_file(file)

    # Compute and verify hash
    resume_hash = _compute_and_verify_hash(content, resume_hash)

    # Check if resume already exists in DB
    with job_uow() as repo:
        if repo.is_resume_ready(resume_hash):
            return ResumeUploadResponse(
                success=True,
                resume_hash=resume_hash,
                message="Resume already processed and ready for matching.",
                task_id=None,
            )

        existing_state = repo.get_resume_processing_state(resume_hash)
        if existing_state and existing_state.processing_status in {
            "extracting",
            "embedding",
        }:
            return ResumeUploadResponse(
                success=True,
                resume_hash=resume_hash,
                message=f"Resume is already processing ({existing_state.processing_status}).",
                task_id=None,
            )
        if existing_state and existing_state.processing_status == "extracted":
            return ResumeUploadResponse(
                success=True,
                resume_hash=resume_hash,
                message="Resume is already processing (embedding).",
                task_id=None,
            )

    # Create task and process in background
    manager = get_pipeline_manager()
    task_id = manager.create_resume_task()

    # Advertise task_id so the orchestrator can check upload state cross-process
    try:
        redis = get_redis_client()
        redis.set("resume:upload:latest_task_id", task_id, ex=3600)
    except Exception:
        logger.warning("Failed to set resume:upload:latest_task_id in Redis — guard will not work")

    # Fire and forget - return immediately while processing continues in background
    # Store task reference to prevent premature garbage collection
    background_task = asyncio.create_task(asyncio.to_thread(
        _process_resume_background, content, file.filename, task_id, manager, resume_hash
    ))
    _upload_tasks.add(background_task)

    def _upload_done(t: asyncio.Task) -> None:
        _upload_tasks.discard(t)
        if not t.cancelled() and t.exception() is not None:
            logger.error("Upload background task raised unhandled: %s", t.exception())

    background_task.add_done_callback(_upload_done)

    return ResumeUploadResponse(
        success=True,
        resume_hash=resume_hash,
        message="Resume uploaded. Processing in background...",
        task_id=task_id
    )


async def _validate_resume_file(file: UploadFile) -> bytes:
    """Validate resume file and return its content."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    # Validate file format — 415 Unsupported Media Type
    parser = ResumeParser()
    if not parser.is_supported(file.filename):
        supported = ', '.join(ResumeParser.get_supported_formats())
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported file format. Supported formats: {supported}"
        )

    content = await file.read()

    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Empty file")

    # File too large — 413 Payload Too Large
    if len(content) > RESUME_MAX_SIZE:
        limit_mb = RESUME_MAX_SIZE / (1024 * 1024)
        limit_str = f"{limit_mb:.1f}MB" if limit_mb >= 1 else f"{RESUME_MAX_SIZE // 1024}KB"
        raise HTTPException(status_code=413, detail=f"File size exceeds {limit_str} limit")

    return content


def _compute_and_verify_hash(content: bytes, provided_hash: Optional[str]) -> str:
    """Compute file fingerprint and verify against provided hash."""
    from database.models.resume import generate_file_fingerprint

    computed_hash = generate_file_fingerprint(content)
    logger.debug(f"Hash check - frontend: {provided_hash}, backend: {computed_hash}, len: {len(computed_hash)}")

    # If client provided a hash, verify it matches
    if provided_hash and provided_hash != computed_hash:
        logger.debug(f"Hash mismatch - provided: {provided_hash}, computed: {computed_hash}")
        raise HTTPException(
            status_code=400,
            detail="File hash mismatch. The provided hash does not match the file content."
        )

    return computed_hash


def _process_resume_background(
    file_content: bytes,
    filename: str,
    task_id: str,
    manager,
    known_fingerprint: str
) -> None:
    """Run ETL processing in background thread with status updates.

    Args:
        file_content: Raw file bytes
        filename: Original filename
        task_id: Task identifier
        manager: Pipeline manager
        known_fingerprint: Pre-computed fingerprint from raw file bytes
    """
    import time as _time
    from web.backend.services.clients import orchestrator_client

    _ = known_fingerprint

    # Update task status to running
    task = manager.get_task(task_id)
    _mark_resume_task_running(task)

    # Signal to cross-process listeners (e.g. orchestrator) that upload is in progress
    try:
        set_task_state(task_id, {"status": "processing", "step": "extracting"}, ttl=3600)
    except Exception:
        logger.warning("Failed to write Redis processing state for task %s", task_id)

    tmp_path = _write_resume_file_to_shared_volume(file_content, filename, task_id)

    try:
        _mark_resume_phase_extracting(task)

        orchestrator_client.process_resume(tmp_path, task_id)
        final_state = _wait_for_resume_etl_final_state(task_id, task, _time)
        if final_state.get("status") == "failed":
            raise RuntimeError(final_state.get("error") or "Resume ETL failed")

        # Mark complete
        _mark_resume_task_completed(task)
    except Exception as exc:
        logger.exception("Background resume processing failed")
        _mark_resume_task_failed(task)
        _write_resume_failure_state(task_id, exc)
    finally:
        _remove_temporary_resume_file(tmp_path)


def _mark_resume_task_running(task) -> None:
    """Set in-memory task state to running."""
    if not task:
        return

    task.status = "running"
    task.message = "Processing resume..."
    task.phases = {"resume_etl": {"status": "running", "progress": 0}}


def _mark_resume_phase_extracting(task) -> None:
    """Update in-memory progress for extraction phase."""
    if not task:
        return

    task.phases = {"resume_etl": {"status": "running", "progress": 30}}
    task.message = "Extracting resume data..."


def _mark_resume_task_completed(task) -> None:
    """Mark in-memory task as successfully completed."""
    if not task:
        return

    task.status = "completed"
    task.message = "Resume processed successfully"
    task.phases = {"resume_etl": {"status": "completed", "progress": 100}}


def _mark_resume_task_failed(task) -> None:
    """Mark in-memory task as failed."""
    if not task:
        return

    task.status = "failed"
    task.message = "Resume processing failed. Please try again or contact support."
    task.phases = {"resume_etl": {"status": "failed", "progress": 0}}


def _write_resume_file_to_shared_volume(file_content: bytes, filename: str, task_id: str) -> str:
    """Write uploaded resume to shared volume for orchestrator processing."""
    shared_dir = Path("/data/resume_uploads")
    shared_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = shared_dir / f"{task_id}{Path(filename).suffix}"
    tmp_path.write_bytes(file_content)
    return str(tmp_path)


def _wait_for_resume_etl_final_state(task_id: str, task, time_module) -> dict:
    """Poll Redis for resume ETL completion and return the final state."""
    poll_timeout_seconds = 600
    deadline = time_module.time() + poll_timeout_seconds

    while time_module.time() < deadline:
        state = get_task_state(task_id)
        if state:
            _sync_resume_task_message_from_state(task, state)
            if state.get("status") in ("completed", "failed"):
                return state
        time_module.sleep(1)

    raise RuntimeError(
        f"Resume ETL timed out after {poll_timeout_seconds}s waiting for orchestrator"
    )


def _sync_resume_task_message_from_state(task, state: dict) -> None:
    """Reflect Redis stage progress into in-memory task status message."""
    if not task:
        return

    step = state.get("step")
    if not step:
        return

    task.message = "Extracting resume data..." if step == "extracting" else "Generating resume vectors..."


def _write_resume_failure_state(task_id: str, error: Exception) -> None:
    """Persist failed state to Redis for resume ETL tasks."""
    try:
        set_task_state(task_id, {"status": "failed", "error": str(error)}, ttl=3600)
    except Exception:
        logger.warning("Failed to write Redis failed state for task %s", task_id)


def _remove_temporary_resume_file(tmp_path: str) -> None:
    """Best-effort cleanup of temporary resume file."""
    try:
        os.unlink(tmp_path)
    except Exception:
        pass
