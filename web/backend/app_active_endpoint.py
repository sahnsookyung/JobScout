
@app.get("/api/pipeline/active", response_model=Optional[PipelineStatusResponse], tags=["pipeline"])
def get_active_pipeline_task():
    """
    Get the currently running pipeline task, if any.
    Useful for frontend recovery on page refresh.
    """
    with _pipeline_tasks_lock:
        for tid, task in _pipeline_tasks.items():
            if task["status"] in ["pending", "running"]:
                return PipelineStatusResponse(
                    task_id=tid,
                    status=task["status"],
                    step=task.get("step")
                )
    return None
