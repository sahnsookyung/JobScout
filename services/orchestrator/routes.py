"""HTTP route registration for the orchestrator service."""

from __future__ import annotations

from typing import Any, Mapping

from fastapi import FastAPI


def _handler(handlers: Mapping[str, Any], name: str) -> Any:
    return handlers[name]


def register_orchestrator_routes(app: FastAPI, handlers: Mapping[str, Any]) -> None:
    """Register orchestrator HTTP routes against compatibility handlers."""

    app.get("/health")(_handler(handlers, "health"))
    app.post(
        "/orchestrate/stages/{stage}",
        response_model=_handler(handlers, "TaskStatusResponse"),
        responses={404: {"description": "Unknown stage"}},
    )(_handler(handlers, "orchestrate_stage"))
    app.post(
        "/orchestrate/pipelines/scrape-extract-embed",
        response_model=_handler(handlers, "TaskStatusResponse"),
    )(_handler(handlers, "orchestrate_scrape_extract_embed_pipeline"))
    app.post(
        "/orchestrate/pipelines/process-imported-jobs",
        response_model=_handler(handlers, "TaskStatusResponse"),
    )(_handler(handlers, "orchestrate_process_imported_jobs_pipeline"))
    app.get(
        "/orchestrate/tasks/{task_id}",
        response_model=_handler(handlers, "TaskStatusResponse"),
        responses={404: {"description": "Task not found"}},
    )(_handler(handlers, "get_task_status"))
    app.post(
        "/orchestrate/match",
        response_model=_handler(handlers, "MatchResponse"),
    )(_handler(handlers, "orchestrate_match_endpoint"))
    app.post("/orchestrate/resume-etl")(_handler(handlers, "orchestrate_resume_etl"))
    app.get("/orchestrate/status/{task_id}")(
        _handler(handlers, "get_orchestration_status")
    )
    app.get("/orchestrate/active")(_handler(handlers, "get_active_orchestration"))
    app.get("/orchestrate/diagnostics")(_handler(handlers, "get_diagnostics"))
    app.post("/orchestrate/stop")(_handler(handlers, "stop_orchestration"))

    trigger_scrape = _handler(handlers, "trigger_scrape")
    scrape_response = _handler(handlers, "ScrapeResponse")
    app.post("/orchestrate/scrape-extract-embed", response_model=scrape_response)(
        trigger_scrape
    )
    app.post(
        "/orchestrate/scrape",
        response_model=scrape_response,
        include_in_schema=False,
    )(trigger_scrape)
