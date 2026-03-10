#!/usr/bin/env python3
"""
Pipeline endpoints - trigger and monitor matching pipeline.
"""

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
from core.config_loader import load_config
from ..dependencies import get_db
from ..models.responses import (
    PipelineTaskResponse,
    PipelineStatusResponse,
    ResumeHashCheckResponse,
    ResumeUploadResponse
)
from ..models.requests import ResumeHashCheckRequest
from ..exceptions import PipelineLockedException
from etl.resume import ResumeParser
from web.shared.constants import RESUME_MAX_SIZE

logger = logging.getLogger(__name__)

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
        409: {"description": "Pipeline is already running"},
        500: {"description": "Internal server error"},
    }
)
def run_matching_pipeline_endpoint():
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
    from web.backend.services.clients import orchestrator_client
    try:
        result = orchestrator_client.start_matching()
    except PipelineLockedException as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception:
        logger.exception("Failed to start matching pipeline")
        raise HTTPException(status_code=500, detail="Failed to start matching pipeline")

    task_id = result.get("task_id", "")
    message = result.get("message", "")

    if not result.get("success"):
        # Orchestrator reports already-running state
        if "already" in message.lower() or "running" in message.lower():
            raise HTTPException(status_code=409, detail=message or "Pipeline is already running")
        raise HTTPException(status_code=500, detail=message or "Failed to start pipeline")

    return PipelineTaskResponse(
        success=True,
        task_id=task_id,
        message="Matching pipeline started. Use SSE /api/pipeline/events/{task_id} to track progress."
    )


@router.get("/status/{task_id}", response_model=PipelineStatusResponse, responses={500: {"description": "Internal server error"}})
def get_pipeline_status(task_id: str):
    """
    Get the status of a pipeline task.

    Status values:
    - pending: Task created but not yet started
    - running: Pipeline is currently executing
    - completed: Pipeline finished successfully
    - failed: Pipeline encountered an error
    """
    try:
        from web.backend.services.clients import orchestrator_client
        result = orchestrator_client.get_task_status(task_id)

        if not result.get("success"):
            raise HTTPException(status_code=404, detail="Task not found")

        # Map orchestrator response to PipelineStatusResponse
        status_data = result.get("status", {})
        return PipelineStatusResponse(
            task_id=task_id,
            status=status_data.get("status", "unknown"),
            step=status_data.get("step"),
            matches_count=status_data.get("matches_count"),
            saved_count=status_data.get("saved_count"),
            notified_count=status_data.get("notified_count"),
            execution_time=status_data.get("execution_time"),
            error=status_data.get("error")
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
    try:
        from web.backend.services.clients import orchestrator_client
        result = orchestrator_client.get_active_task()

        if not result or not result.get("success"):
            return None

        # Map orchestrator response to PipelineStatusResponse
        status_data = result.get("status", {})
        return PipelineStatusResponse(
            task_id=status_data.get("task_id", ""),
            status=status_data.get("status", "unknown"),
            step=status_data.get("step")
        )
    except Exception:
        logger.exception("Failed to get active pipeline task")
        return None


@router.post(
    "/stop",
    response_model=PipelineTaskResponse,
    responses={
        404: {"description": "No active pipeline to stop"},
        500: {"description": "Internal server error"},
    }
)
def stop_matching_pipeline():
    """
    Stop the currently running pipeline task.

    Raises:
        404: No active pipeline is running.
        500: Internal error stopping the pipeline.
    """
    try:
        from web.backend.services.clients import orchestrator_client
        result = orchestrator_client.stop_task()
    except Exception:
        logger.exception("Failed to stop pipeline")
        raise HTTPException(status_code=500, detail="Failed to stop pipeline")

    if not result or not result.get("success"):
        raise HTTPException(status_code=404, detail="No active pipeline to stop")

    return PipelineTaskResponse(
        success=True,
        task_id=result.get("task_id", ""),
        message="Pipeline cancellation requested."
    )



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
                    yield f"data: {json.dumps({'error': 'Task not found', 'status': 'failed'})}\n\n"
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


async def _preflight_task_check(orchestrator_url: str, task_id: str) -> None:
    """Quick probe to verify task existence before opening a long-lived SSE stream.

    Raises HTTPException(404) only if the orchestrator definitively reports
    the task does not exist. Failures during the probe are logged and ignored
    so the SSE stream can surface errors itself.
    """
    import httpx

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as probe:
            probe_resp = await probe.get(f"{orchestrator_url}/orchestrate/active")
            if probe_resp.status_code == 404:
                logger.warning("Task not found")
                raise HTTPException(status_code=404, detail="Task not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("Pre-flight check failed: %s", type(e).__name__)


@router.get(
    "/events/{task_id}",
    responses={
        404: {"description": "Task not found"},
        400: {"description": "Invalid task_id format"},
    }
)
async def pipeline_events(task_id: str, db: Annotated[Session, Depends(get_db)] = None):
    """
    Server-Sent Events endpoint for real-time pipeline status updates.

    Validates task existence before opening the stream.
    Proxies to the orchestrator service.

    Raises:
        404: Task does not exist in the orchestrator.
        400: Invalid task_id format.
    """
    # Validate task_id BEFORE any use (OWASP: Validate Early)
    if not _validate_task_id(task_id):
        raise HTTPException(
            status_code=400, 
            detail="Invalid task_id format. Must be alphanumeric with hyphens, max 50 characters."
        )
    
    orchestrator_url = os.getenv("ORCHESTRATOR_URL", "http://localhost:8084")
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


@router.post("/check-resume-hash", response_model=ResumeHashCheckResponse)
@limiter.limit("10/minute")
def check_resume_hash_endpoint(request: Request, body: ResumeHashCheckRequest):
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
        if repo.resume.resume_hash_exists(resume_hash):
            # Resume already processed - skip ETL, just return success
            return ResumeUploadResponse(
                success=True,
                resume_hash=resume_hash,
                message="Resume already processed",
                task_id=None
            )

    # Create task and process in background
    manager = get_pipeline_manager()
    task_id = manager.create_task()

    # Fire and forget - return immediately while processing continues in background
    # Store task reference to prevent premature garbage collection
    background_task = asyncio.create_task(asyncio.to_thread(
        _process_resume_background, content, file.filename, task_id, manager, resume_hash
    ))
    background_task.add_done_callback(lambda t: None)  # Suppress unhandled exception warnings

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
    import tempfile
    from database.uow import job_uow
    from core.app_context import AppContext

    # Update task status to running
    task = manager.get_task(task_id)
    if task:
        task.status = "running"
        task.message = "Processing resume..."
        task.phases = {"resume_etl": {"status": "running", "progress": 0}}

    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(filename).suffix) as tmp:
        tmp.write(file_content)
        tmp_path = tmp.name

    try:
        full_config = load_config()
        ctx = AppContext.build(full_config)

        # Update progress
        if task:
            task.phases = {"resume_etl": {"status": "running", "progress": 50}}
            task.message = "Extracting resume data..."

        with job_uow() as repo:
            # Pass known_fingerprint to avoid re-computing
            ctx.job_etl_service.extract_and_embed_resume(
                repo, tmp_path, known_fingerprint=known_fingerprint
            )

        # Mark complete
        if task:
            task.status = "completed"
            task.message = "Resume processed successfully"
            task.phases = {"resume_etl": {"status": "completed", "progress": 100}}
    except Exception:
        logger.exception("Background resume processing failed")
        if task:
            task.status = "failed"
            task.message = "Resume processing failed. Please try again or contact support."
            task.phases = {"resume_etl": {"status": "failed", "progress": 0}}
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
