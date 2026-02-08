#!/usr/bin/env python3
"""
Pipeline endpoints - trigger and monitor matching pipeline.
"""

from fastapi import APIRouter
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
