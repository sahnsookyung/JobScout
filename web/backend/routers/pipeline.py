#!/usr/bin/env python3
"""
Pipeline endpoints - trigger and monitor matching pipeline.
"""

import json
import os
from fastapi import APIRouter, UploadFile, File, HTTPException
from typing import Optional

from ..services.pipeline_service import get_pipeline_manager
from ..models.responses import (
    PipelineTaskResponse,
    PipelineStatusResponse
)
from ..exceptions import PipelineLockedException

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


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


@router.post("/upload-resume", response_model=PipelineTaskResponse)
async def upload_resume_endpoint(file: UploadFile = File(...)):
    """
    Upload a resume JSON file.
    Saves to configured resume file path and triggers ETL processing.
    """
    import logging
    logger = logging.getLogger(__name__)

    if not file.filename or not file.filename.endswith('.json'):
        raise HTTPException(status_code=400, detail="Only JSON files are allowed")

    content = await file.read()

    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File size exceeds 10MB limit")

    try:
        json.loads(content.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON file: {str(e)}")

    from core.config_loader import load_config
    config = load_config()
    resume_file = config.etl.resume_file if config.etl and config.etl.resume_file else "resume.json"
    if not os.path.isabs(resume_file):
        resume_file = os.path.join(os.getcwd(), resume_file)

    try:
        with open(resume_file, 'wb') as f:
            f.write(content)
    except IOError as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

    fingerprint = None
    try:
        from database.uow import job_uow
        from core.app_context import AppContext

        full_config = load_config()
        ctx = AppContext.build(full_config)

        with job_uow() as repo:
            changed, fp, _ = ctx.job_etl_service.process_resume(repo, resume_file)
            fingerprint = fp
    except Exception as e:
        logger.error(f"ETL processing failed during resume upload: {e}")

    return PipelineTaskResponse(
        success=True,
        task_id="",
        message=f"Resume uploaded successfully{f' (fingerprint: {fingerprint[:16]}...)' if fingerprint else ''}"
    )
