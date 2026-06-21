from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.backend.dependencies import get_current_user, get_db
from web.backend.models.responses import PipelineRunSummary
from web.backend.routers.pipeline_runs import router

def _current_user():
    return SimpleNamespace(id="user-123")

def _db_session():
    return Mock()

def _client():
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_current_user] = _current_user
    app.dependency_overrides[get_db] = _db_session
    return TestClient(app, raise_server_exceptions=True)

def _run_summary() -> PipelineRunSummary:
    return PipelineRunSummary(
        id="run-1",
        task_id="task-1",
        run_type="pipeline",
        status="completed",
        current_stage="embedding",
        queued_count=10,
        processed_count=8,
        succeeded_count=8,
        failed_count=1,
        skipped_count=1,
        retry_eligible=False,
        metadata={"expensive": "payload"},
        stages=[
            {
                "id": "stage-1",
                "stage": "matching",
                "status": "completed",
                "metadata": {"detail": "payload"},
            }
        ],
        allowed_actions=[],
    )

def test_get_pipeline_runs_returns_service_payload():
    client = _client()
    run = _run_summary()

    with patch(
        "web.backend.routers.pipeline_runs.pipeline_run_read_service.list_runs",
        return_value=([run], 1),
    ) as list_runs:
        response = client.get(
            "/api/pipeline-runs",
            params={"status": "completed", "run_type": "pipeline", "limit": 5},
            headers={"X-Tenant-Id": "00000000-0000-4000-8000-000000000201"},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["count"] == 1
    assert data["runs"][0]["task_id"] == "task-1"
    kwargs = list_runs.call_args.kwargs
    assert str(kwargs["tenant_id"]) == "00000000-0000-4000-8000-000000000201"
    assert kwargs["status"] == "completed"
    assert kwargs["run_type"] == "pipeline"
    assert kwargs["limit"] == 5

def test_get_pipeline_runs_returns_cursor_metadata():
    client = _client()
    run = _run_summary()

    with patch(
        "web.backend.routers.pipeline_runs.pipeline_run_read_service.list_runs",
        return_value=([run], 2, "next-run-cursor", True, "cursor", 0),
    ) as list_runs:
        response = client.get(
            "/api/pipeline-runs",
            params={"limit": 1, "cursor": "run-cursor"},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["page_mode"] == "cursor"
    assert data["next_cursor"] == "next-run-cursor"
    assert data["has_more"] is True
    assert list_runs.call_args.kwargs["cursor"] == "run-cursor"

def test_get_pipeline_runs_accepts_cursor_page_mode_without_cursor():
    client = _client()
    run = _run_summary()

    with patch(
        "web.backend.routers.pipeline_runs.pipeline_run_read_service.list_runs",
        return_value=([run], 2, "next-run-cursor", True, "cursor", 0),
    ) as list_runs:
        response = client.get(
            "/api/pipeline-runs",
            params={"limit": 1, "page_mode": "cursor"},
        )

    assert response.status_code == 200
    assert response.json()["next_cursor"] == "next-run-cursor"
    assert list_runs.call_args.kwargs["page_mode"] == "cursor"

def test_get_pipeline_runs_compact_view_strips_stage_and_metadata_payloads():
    client = _client()
    run = _run_summary()

    with patch(
        "web.backend.routers.pipeline_runs.pipeline_run_read_service.list_runs",
        return_value=([run], 1, None, False, "offset", 0),
    ):
        response = client.get("/api/pipeline-runs", params={"view": "compact"})

    assert response.status_code == 200
    data = response.json()
    assert data["view"] == "compact"
    assert data["runs"][0]["metadata"] == {}
    assert data["runs"][0]["stages"] == []

def test_get_pipeline_runs_rejects_invalid_view():
    client = _client()

    response = client.get("/api/pipeline-runs", params={"view": "tiny"})

    assert response.status_code == 422
    assert "Invalid view" in response.json()["detail"]

def test_get_pipeline_run_returns_404_when_missing():
    client = _client()

    with patch(
        "web.backend.routers.pipeline_runs.pipeline_run_read_service.get_run",
        return_value=None,
    ):
        response = client.get("/api/pipeline-runs/run-missing")

    assert response.status_code == 404
    assert response.json()["detail"] == "Pipeline run not found"

def test_get_pipeline_runs_rejects_invalid_status():
    client = _client()

    response = client.get("/api/pipeline-runs", params={"status": "bogus"})

    assert response.status_code == 422
    assert "Invalid status" in response.json()["detail"]

def test_llm_queue_status_returns_operational_payload():
    client = _client()

    with patch(
        "web.backend.routers.pipeline_runs.pipeline_run_ops_service.llm_queue_status",
        return_value={
            "success": True,
            "ready": True,
            "queue": "llm_evaluations",
            "queued": 2,
            "started": 1,
            "deferred": 0,
            "scheduled": 3,
            "failed": 4,
        },
    ):
        response = client.get("/api/pipeline-runs/llm-evaluations/queue")

    assert response.status_code == 200
    data = response.json()
    assert data["ready"] is True
    assert data["queued"] == 2
    assert data["failed"] == 4

def test_llm_queue_status_preserves_degraded_error_metadata():
    client = _client()

    with patch(
        "web.backend.routers.pipeline_runs.pipeline_run_ops_service.llm_queue_status",
        return_value={
            "success": False,
            "ready": False,
            "queue": "llm_evaluations",
            "queued": 0,
            "started": 0,
            "deferred": 0,
            "scheduled": 0,
            "failed": 0,
            "error": "redis unavailable",
        },
    ):
        response = client.get("/api/pipeline-runs/llm-evaluations/queue")

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is False
    assert data["ready"] is False
    assert data["error"] == "redis unavailable"

def test_cancel_pipeline_run_returns_operation_response():
    client = _client()
    run = _run_summary()

    with patch(
        "web.backend.routers.pipeline_runs.pipeline_run_ops_service.cancel_run",
        return_value=run,
    ) as cancel_run:
        response = client.post(
            "/api/pipeline-runs/run-1/cancel",
            headers={"X-Tenant-Id": "00000000-0000-4000-8000-000000000201"},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["action"] == "cancel"
    assert data["run"]["task_id"] == "task-1"
    assert str(cancel_run.call_args.kwargs["tenant_id"]) == "00000000-0000-4000-8000-000000000201"

def test_requeue_pipeline_run_returns_enqueued_task_id():
    client = _client()
    run = _run_summary()

    with patch(
        "web.backend.routers.pipeline_runs.pipeline_run_ops_service.requeue_run",
        return_value=(run, "task-1-requeue-abc123"),
    ):
        response = client.post("/api/pipeline-runs/run-1/requeue")

    assert response.status_code == 200
    data = response.json()
    assert data["action"] == "requeue"
    assert data["source_run_id"] == "run-1"
    assert data["enqueued_task_id"] == "task-1-requeue-abc123"

def test_retry_pipeline_run_rejects_disallowed_action():
    client = _client()

    with patch(
        "web.backend.routers.pipeline_runs.pipeline_run_ops_service.retry_run",
        side_effect=ValueError("Pipeline run cannot be retried from its current state."),
    ):
        response = client.post("/api/pipeline-runs/run-1/retry")

    assert response.status_code == 400
    assert "cannot be retried" in response.json()["detail"]
