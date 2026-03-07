#!/usr/bin/env python3
"""
Pipeline endpoints - trigger and monitor matching pipeline.
"""

import json
import os
import asyncio
import logging
from pathlib import Path
from typing import Annotated, Optional

from fastapi import APIRouter, UploadFile, File, HTTPException, Form, Request
from fastapi.responses import StreamingResponse, JSONResponse

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from ..services.pipeline_service import get_pipeline_manager
from core.config_loader import load_config
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


def add_rate_limit_handlers(app):
    """Add rate limit exception handlers to the FastAPI app."""
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


async def _rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": str(exc)}
    )


@router.post("/run-matching", response_model=PipelineTaskResponse)
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
    """
    # Call orchestrator service - it will handle the full pipeline
    try:
        from web.backend.services.clients import orchestrator_client
        result = orchestrator_client.start_matching()
        
        task_id = result.get("task_id", "")
        
        if result.get("success"):
            return PipelineTaskResponse(
                success=True,
                task_id=task_id,
                message="Matching pipeline started. Use SSE /api/pipeline/events/{task_id} to track progress."
            )
        else:
            return PipelineTaskResponse(
                success=False,
                task_id=task_id,
                message=result.get("message", "Failed to start pipeline")
            )
    except Exception as e:
        logger.exception("Failed to start matching pipeline")
        return PipelineTaskResponse(
            success=False,
            task_id="",
            message="Failed to start matching pipeline. Please try again or check server logs."
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
        logger.exception("Failed to get pipeline status for task %s", task_id)
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
    except Exception as e:
        logger.exception("Failed to get active pipeline task")
        return None


@router.post("/stop", response_model=PipelineTaskResponse)
def stop_matching_pipeline():
    """
    Stop the currently running pipeline task.
    """
    try:
        from web.backend.services.clients import orchestrator_client
        result = orchestrator_client.stop_task()

        if not result or not result.get("success"):
            return PipelineTaskResponse(
                success=False,
                task_id="",
                message="No active pipeline to stop."
            )

        return PipelineTaskResponse(
            success=True,
            task_id=result.get("task_id", ""),
            message="Pipeline cancellation requested."
        )
    except Exception as e:
        logger.exception("Failed to stop pipeline")
        return PipelineTaskResponse(
            success=False,
            task_id="",
            message="Failed to stop pipeline. Please try again."
        )


@router.get("/events/{task_id}")
async def pipeline_events(task_id: str):
    """
    Server-Sent Events endpoint for real-time pipeline status updates.
    
    Streams status updates for the specified task in real-time.
    Proxies to the orchestrator service.
    """
    import httpx

    orchestrator_url = os.getenv("ORCHESTRATOR_URL", "http://localhost:8084")
    
    async def event_generator():
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=10.0)) as client:
                async with client.stream(
                    "GET",
                    f"{orchestrator_url}/orchestrate/status/{task_id}"
                ) as response:
                    if response.is_error:
                        logger.error(f"Orchestrator returned {response.status_code} for task {task_id}")
                        yield f"data: {json.dumps({'error': 'Failed to get pipeline status'})}\n\n"
                        return
                    async for chunk in response.aiter_raw():
                        if chunk:
                            yield chunk
        except Exception as e:
            logger.exception(f"Failed to connect to orchestrator: {e}")
            yield f"data: {json.dumps({'error': 'Failed to connect to pipeline service'})}\n\n"
    
    return StreamingResponse(
        event_generator(),
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


@router.post("/upload-resume", response_model=ResumeUploadResponse)
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
    # Validate file
    content = await _validate_resume_file(file)
    
    # Compute and verify hash
    resume_hash = _compute_and_verify_hash(content, resume_hash)
    
    # Create task and process in background
    manager = get_pipeline_manager()
    task_id = manager.create_task()

    # Fire and forget - return immediately while processing continues in background
    background_task = asyncio.create_task(asyncio.to_thread(
        _process_resume_background, content, file.filename, task_id, manager
    ))

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

    # Validate file format
    parser = ResumeParser()
    if not parser.is_supported(file.filename):
        supported = ', '.join(ResumeParser.get_supported_formats())
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file format. Supported formats: {supported}"
        )

    content = await file.read()

    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Empty file")

    if len(content) > RESUME_MAX_SIZE:
        limit_mb = RESUME_MAX_SIZE / (1024 * 1024)
        limit_str = f"{limit_mb:.1f}MB" if limit_mb >= 1 else f"{RESUME_MAX_SIZE // 1024}KB"
        raise HTTPException(status_code=400, detail=f"File size exceeds {limit_str} limit")

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
    manager
) -> None:
    """Run ETL processing in background thread with status updates."""
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
            ctx.job_etl_service.process_resume(repo, tmp_path)

        # Mark complete
        if task:
            task.status = "completed"
            task.message = "Resume processed successfully"
            task.phases = {"resume_etl": {"status": "completed", "progress": 100}}
    except Exception as e:
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
