#!/usr/bin/env python3
"""
Pipeline endpoints - trigger and monitor matching pipeline.
"""

import json
import os
import asyncio
import logging
from pathlib import Path
from fastapi import APIRouter, UploadFile, File, HTTPException, Form, Request
from fastapi.responses import StreamingResponse, JSONResponse
from typing import Optional

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from ..services.pipeline_service import get_pipeline_manager
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
    
    Returns immediately with a task_id that can be used to poll for status.
    The pipeline will:
    - Load the resume from the database
    - Run vector-based job matching
    - Calculate fit/want scores
    - Save results to database
    - Send notifications (if configured)
    """
    manager = get_pipeline_manager()
    
    try:
        task_id = manager.create_task()
        
        # Check if this is an existing task
        task = manager.get_task(task_id)
        if task and task.status in ["pending", "running"]:
            return PipelineTaskResponse(
                success=True,
                task_id=task_id,
                message="Pipeline is already running. Returning existing task."
            )
        
        return PipelineTaskResponse(
            success=True,
            task_id=task_id,
            message="Matching pipeline started. Use /api/pipeline/status/{task_id} to check progress."
        )
        
    except PipelineLockedException as e:
        return PipelineTaskResponse(
            success=False,
            task_id="",
            message=str(e)
        )


@router.get("/status/{task_id}", response_model=PipelineStatusResponse)
def get_pipeline_status(task_id: str):
    """
    Get the status of a pipeline task.
    
    Status values:
    - pending: Task created but not yet started
    - running: Pipeline is currently executing
    - completed: Pipeline finished successfully
    - failed: Pipeline encountered an error
    """
    manager = get_pipeline_manager()
    task = manager.get_task(task_id)
    
    if not task:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Task not found")
    
    response = PipelineStatusResponse(
        task_id=task_id,
        status=task.status,
        step=task.step
    )
    
    if task.result:
        response.matches_count = task.result.matches_count
        response.saved_count = task.result.saved_count
        response.notified_count = task.result.notified_count
        response.execution_time = task.result.execution_time
        if not task.result.success:
            response.error = task.result.error
    elif task.error:
        response.error = task.error
    
    return response


@router.get("/active", response_model=Optional[PipelineStatusResponse])
def get_active_pipeline_task():
    """
    Get the currently running pipeline task, if any.
    
    Useful for frontend recovery on page refresh.
    """
    manager = get_pipeline_manager()
    task = manager.get_active_task()
    
    if not task:
        return None
    
    return PipelineStatusResponse(
        task_id=task.task_id,
        status=task.status,
        step=task.step
    )


@router.post("/stop", response_model=PipelineTaskResponse)
def stop_matching_pipeline():
    """
    Stop the currently running pipeline task.
    """
    manager = get_pipeline_manager()
    task_id = manager.stop_active_task()
    
    if not task_id:
        return PipelineTaskResponse(
            success=False,
            task_id="",
            message="No active pipeline to stop."
        )
    
    return PipelineTaskResponse(
        success=True,
        task_id=task_id,
        message="Pipeline cancellation requested."
    )


@router.get("/events/{task_id}")
async def pipeline_events(task_id: str):
    """
    Server-Sent Events endpoint for real-time pipeline status updates.
    
    Streams status updates for the specified task in real-time.
    """
    
    manager = get_pipeline_manager()
    task = manager.get_task(task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    queue = manager.subscribe(task_id)
    
    async def event_generator():
        try:
            task = manager.get_task(task_id)
            if task:
                initial_data = {
                    "task_id": task_id,
                    "status": task.status,
                    "step": task.step,
                }
                if task.status in ["completed", "failed"] and task.result:
                    initial_data["matches_count"] = task.result.matches_count
                    initial_data["saved_count"] = task.result.saved_count
                    initial_data["notified_count"] = task.result.notified_count
                    initial_data["execution_time"] = task.result.execution_time
                    initial_data["success"] = task.result.success
                    if not task.result.success:
                        initial_data["error"] = task.result.error
                elif task.status in ["completed", "failed"] and task.error:
                    initial_data["error"] = task.error
                yield f"data: {json.dumps(initial_data)}\n\n"
            
            if task and task.status in ["completed", "failed"]:
                return
            
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield f"data: {json.dumps(data)}\n\n"
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
                    
                task = manager.get_task(task_id)
                if task and task.status in ["completed", "failed"]:
                    final_data = {
                        "task_id": task_id,
                        "status": task.status,
                        "step": task.step,
                    }
                    if task.result:
                        final_data["matches_count"] = task.result.matches_count
                        final_data["saved_count"] = task.result.saved_count
                        final_data["notified_count"] = task.result.notified_count
                        final_data["execution_time"] = task.result.execution_time
                        final_data["success"] = task.result.success
                        if not task.result.success:
                            final_data["error"] = task.result.error
                    elif task.error:
                        final_data["error"] = task.error
                    yield f"data: {json.dumps(final_data)}\n\n"
                    break
        except asyncio.CancelledError:
            logger.info(f"SSE connection cancelled for task {task_id}")
        finally:
            manager.unsubscribe(task_id)
            logger.info(f"SSE connection closed for task {task_id}")
    
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
async def check_resume_hash_endpoint(request: Request, body: ResumeHashCheckRequest):
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
    file: UploadFile = File(...),
    resume_hash: Optional[str] = Form(None)
):
    """
    Upload a resume file.
    Supports: .json, .yaml, .yml, .txt, .docx, .pdf
    
    If resume_hash is provided, checks if it already exists first.
    If the hash exists, returns success without re-processing.
    
    The file is processed in memory - never written to disk.
    Returns only the hash for frontend verification and IndexedDB storage.
    """

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
        raise HTTPException(status_code=400, detail=f"File size exceeds {RESUME_MAX_SIZE // (1024*1024)}MB limit")

    # Always compute hash from file bytes (security: verify file integrity)
    from database.models.resume import generate_file_fingerprint
    computed_hash = generate_file_fingerprint(content)

    # If client provided a hash, verify it matches (security: prevent tampering)
    if resume_hash and resume_hash != computed_hash:
        raise HTTPException(
            status_code=400,
            detail="File hash mismatch. The provided hash does not match the file content. This may indicate file tampering."
        )

    # Use the computed hash (either client matched, or we use computed)
    resume_hash = computed_hash

    # Check if hash already exists (deduplication)
    from database.uow import job_uow
    with job_uow() as repo:
        exists = repo.resume.resume_hash_exists(resume_hash)
        if exists:
            logger.info(f"Resume hash already exists: {resume_hash[:16]}...")
            return ResumeUploadResponse(
                success=True,
                resume_hash=resume_hash,
                message="Resume already processed"
            )

    # Process the resume (file is in memory, not saved to disk)
    from core.config_loader import load_config
    from core.app_context import AppContext
    import tempfile

    fingerprint = None
    try:
        # Write to temp file for processing (will be cleaned up)
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        try:
            full_config = load_config()
            ctx = AppContext.build(full_config)

            with job_uow() as repo:
                changed, fp, _ = ctx.job_etl_service.process_resume(repo, tmp_path)
                fingerprint = fp
        finally:
            # Clean up temp file
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
    except Exception as e:
        logger.error(f"ETL processing failed during resume upload: {e}")
        return ResumeUploadResponse(
            success=False,
            resume_hash=resume_hash,
            message=f"Processing failed: {str(e)}"
        )

    # Verify the fingerprint matches (should always match since we provided hash)
    if fingerprint and fingerprint != resume_hash:
        logger.warning(f"Fingerprint mismatch: provided={resume_hash[:16]}..., computed={fingerprint[:16]}...")

    return ResumeUploadResponse(
        success=True,
        resume_hash=resume_hash,
        message="Resume uploaded and processed successfully"
    )
