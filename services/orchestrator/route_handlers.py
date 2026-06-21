"""HTTP handler adapters for the orchestrator service.

These functions are the FastAPI-facing adapters. They delegate to the
compatibility functions in ``main`` so existing tests/imports can move over
gradually while route registration no longer binds directly to ``main.py``.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import Body, Depends, Request

_main_module: Any | None = None


def _main():
    if _main_module is None:
        raise RuntimeError("Orchestrator route handlers have not been configured")
    return _main_module


def _configure(main_module: Any) -> None:
    global _main_module
    _main_module = main_module


def _get_current_user_dependency():
    return _main().get_current_user()


async def health(request: Request):
    return await _main().health(request)

async def orchestrate_stage(
    stage: str,
    request: Request,
    body: Any = Body(default=None),
):
    main = _main()
    stage_body = body if body is not None else main.StageRequest()
    if isinstance(stage_body, dict):
        stage_body = main.StageRequest(**stage_body)
    return await main.orchestrate_stage(stage, request, stage_body)

async def orchestrate_scrape_extract_embed_pipeline(request: Request):
    return await _main().orchestrate_scrape_extract_embed_pipeline(request)

async def orchestrate_process_imported_jobs_pipeline(request: Request):
    return await _main().orchestrate_process_imported_jobs_pipeline(request)

async def get_task_status(task_id: str, request: Request):
    return await _main().get_task_status(task_id, request)

async def orchestrate_match_endpoint(
    request: Request,
    user: Any = Depends(_get_current_user_dependency),
):
    return await _main().orchestrate_match_endpoint(request, user)

async def orchestrate_resume_etl(request: Request, payload: Any = Body(...)):
    main = _main()
    resume_payload = payload
    if isinstance(resume_payload, dict):
        resume_payload = main.ResumeEtlRequest(**resume_payload)
    return await main.orchestrate_resume_etl(resume_payload, request)

async def get_orchestration_status(task_id: str, request: Request):
    return await _main().get_orchestration_status(task_id, request)

async def get_active_orchestration(request: Request):
    return await _main().get_active_orchestration(request)

async def get_diagnostics(request: Request):
    return await _main().get_diagnostics(request)

async def stop_orchestration(request: Request, task_id: Optional[str] = None):
    return await _main().stop_orchestration(request, task_id)

async def trigger_scrape(request: Request):
    return await _main().trigger_scrape(request)

def route_handlers(main_module: Any) -> dict[str, Any]:
    _configure(main_module)
    main = _main()

    async def _orchestrate_match_endpoint(
        request: Request,
        user: Any = Depends(main.get_current_user),
    ):
        return await main.orchestrate_match_endpoint(request, user)

    _orchestrate_match_endpoint.__name__ = "orchestrate_match_endpoint"

    return {
        "health": health,
        "orchestrate_stage": orchestrate_stage,
        "orchestrate_scrape_extract_embed_pipeline": orchestrate_scrape_extract_embed_pipeline,
        "orchestrate_process_imported_jobs_pipeline": orchestrate_process_imported_jobs_pipeline,
        "get_task_status": get_task_status,
        "orchestrate_match_endpoint": _orchestrate_match_endpoint,
        "orchestrate_resume_etl": orchestrate_resume_etl,
        "get_orchestration_status": get_orchestration_status,
        "get_active_orchestration": get_active_orchestration,
        "get_diagnostics": get_diagnostics,
        "stop_orchestration": stop_orchestration,
        "trigger_scrape": trigger_scrape,
        "TaskStatusResponse": main.TaskStatusResponse,
        "MatchResponse": main.MatchResponse,
        "ScrapeResponse": main.ScrapeResponse,
    }
