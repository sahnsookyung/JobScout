"""HTTP handler adapters for the orchestrator service.

These functions are the FastAPI-facing adapters. They delegate to the
compatibility functions in ``main`` so existing tests/imports can move over
gradually while route registration no longer binds directly to ``main.py``.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import Depends, Request

from services.orchestrator import main as _main_mod

def _main():
    return _main_mod

async def health(request: Request):
    return await _main().health(request)

async def orchestrate_stage(
    stage: str,
    request: Request,
    body: _main_mod.StageRequest = _main_mod.StageRequest(),
):
    return await _main_mod.orchestrate_stage(stage, request, body)

async def orchestrate_scrape_extract_embed_pipeline(request: Request):
    return await _main().orchestrate_scrape_extract_embed_pipeline(request)

async def orchestrate_process_imported_jobs_pipeline(request: Request):
    return await _main().orchestrate_process_imported_jobs_pipeline(request)

async def get_task_status(task_id: str, request: Request):
    return await _main().get_task_status(task_id, request)

async def orchestrate_match_endpoint(
    request: Request,
    user: Any = Depends(_main_mod.get_current_user),
):
    return await _main().orchestrate_match_endpoint(request, user)

async def orchestrate_resume_etl(payload: _main_mod.ResumeEtlRequest, request: Request):
    return await _main().orchestrate_resume_etl(payload, request)

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

def route_handlers() -> dict[str, Any]:
    main = _main()
    return {
        "health": health,
        "orchestrate_stage": orchestrate_stage,
        "orchestrate_scrape_extract_embed_pipeline": orchestrate_scrape_extract_embed_pipeline,
        "orchestrate_process_imported_jobs_pipeline": orchestrate_process_imported_jobs_pipeline,
        "get_task_status": get_task_status,
        "orchestrate_match_endpoint": orchestrate_match_endpoint,
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
