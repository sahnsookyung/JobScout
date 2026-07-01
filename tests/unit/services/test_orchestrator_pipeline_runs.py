import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from services.orchestrator.pipeline_runs import PipelineRunService, _coerce_uuid


class _SessionScope:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, exc, tb):
        return False


def _stage():
    now = datetime.now(timezone.utc)
    return SimpleNamespace(
        id=uuid.uuid4(),
        stage="matching",
        status="completed",
        queued_count=5,
        processed_count=4,
        succeeded_count=4,
        failed_count=0,
        skipped_count=1,
        retry_count=0,
        retry_eligible=False,
        last_error=None,
        started_at=now,
        completed_at=now,
        metadata_json={"stage": "metadata"},
    )


def _run(**overrides):
    now = datetime.now(timezone.utc)
    values = {
        "id": uuid.uuid4(),
        "task_id": "task-1",
        "run_type": "pipeline",
        "status": "running",
        "current_stage": "matching",
        "owner_id": uuid.uuid4(),
        "tenant_id": uuid.uuid4(),
        "resume_fingerprint": "resume-fp",
        "retry_eligible": False,
        "last_error": None,
        "started_at": now,
        "completed_at": None,
        "heartbeat_at": now,
        "queued_count": 5,
        "processed_count": 4,
        "succeeded_count": 4,
        "failed_count": 0,
        "skipped_count": 1,
        "metadata_json": {
            "step": "matching",
            "upload_id": "upload-1",
            "matches_count": 4,
        },
        "stages": [_stage()],
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _service_with_repo(run=None):
    redis_gateway = Mock()
    service = PipelineRunService(redis_gateway=redis_gateway)
    db = Mock()
    repo = Mock()
    repo.get_or_create_run.return_value = run or _run()
    return service, redis_gateway, db, repo


def test_coerce_uuid_accepts_none_uuid_and_string():
    value = uuid.uuid4()

    assert _coerce_uuid(None) is None
    assert _coerce_uuid(value) is value
    assert _coerce_uuid(str(value)) == value
    with pytest.raises(ValueError):
        _coerce_uuid("not-a-uuid")


def test_snapshot_helpers_include_stage_metadata_and_passthrough_fields():
    run = _run()

    snapshot = PipelineRunService._run_snapshot(run)

    assert snapshot["pipeline_run_id"] == str(run.id)
    assert snapshot["owner_id"] == str(run.owner_id)
    assert snapshot["metadata"]["upload_id"] == "upload-1"
    assert snapshot["upload_id"] == "upload-1"
    assert snapshot["matches_count"] == 4
    assert snapshot["result"]["stages"][0]["metadata"] == {"stage": "metadata"}


def test_project_logs_redis_projection_failure(caplog):
    redis_gateway = Mock()
    redis_gateway.set_task_state.side_effect = RuntimeError("redis down")
    service = PipelineRunService(redis_gateway=redis_gateway)

    service._project({"task_id": "task-1"})

    assert "Failed to project pipeline run task-1 to Redis" in caplog.text


def test_get_snapshot_returns_none_when_run_is_missing():
    service, _redis_gateway, db, repo = _service_with_repo()
    repo.get_by_task_id.return_value = None

    with patch("services.orchestrator.pipeline_runs.db_session_scope", return_value=_SessionScope(db)), patch(
        "services.orchestrator.pipeline_runs.PipelineRunRepository",
        return_value=repo,
    ):
        assert service.get_snapshot("missing-task") is None


def test_get_snapshot_returns_projectable_snapshot():
    run = _run()
    service, _redis_gateway, db, repo = _service_with_repo(run)
    repo.get_by_task_id.return_value = run

    with patch("services.orchestrator.pipeline_runs.db_session_scope", return_value=_SessionScope(db)), patch(
        "services.orchestrator.pipeline_runs.PipelineRunRepository",
        return_value=repo,
    ):
        snapshot = service.get_snapshot("task-1")

    assert snapshot["task_id"] == "task-1"
    assert snapshot["result"]["processed_count"] == 4


def test_start_run_creates_run_and_projects_snapshot():
    run = _run()
    service, redis_gateway, db, repo = _service_with_repo(run)

    with patch("services.orchestrator.pipeline_runs.db_session_scope", return_value=_SessionScope(db)), patch(
        "services.orchestrator.pipeline_runs.PipelineRunRepository",
        return_value=repo,
    ):
        snapshot = service.start_run(
            task_id="task-1",
            run_type="pipeline",
            owner_id=str(run.owner_id),
            tenant_id=str(run.tenant_id),
            resume_fingerprint="resume-fp",
            current_stage="matching",
            metadata={"trigger": "manual"},
        )

    assert snapshot["task_id"] == "task-1"
    redis_gateway.set_task_state.assert_called_once()
    assert repo.get_or_create_run.call_args.kwargs["owner_id"] == run.owner_id
    assert repo.get_or_create_run.call_args.kwargs["tenant_id"] == run.tenant_id


def test_touch_run_updates_and_projects_existing_run():
    run = _run()
    service, redis_gateway, db, repo = _service_with_repo(run)

    with patch("services.orchestrator.pipeline_runs.db_session_scope", return_value=_SessionScope(db)), patch(
        "services.orchestrator.pipeline_runs.PipelineRunRepository",
        return_value=repo,
    ):
        snapshot = service.touch_run(
            task_id="task-1",
            current_stage="embedding",
            metadata={"heartbeat": True},
        )

    repo.touch_run.assert_called_once_with(
        run,
        current_stage="embedding",
        metadata={"heartbeat": True},
    )
    redis_gateway.set_task_state.assert_called_once()
    assert snapshot["task_id"] == "task-1"


def test_stage_and_run_lifecycle_methods_delegate_to_repository_and_project():
    run = _run()
    service, redis_gateway, db, repo = _service_with_repo(run)

    with patch("services.orchestrator.pipeline_runs.db_session_scope", return_value=_SessionScope(db)), patch(
        "services.orchestrator.pipeline_runs.PipelineRunRepository",
        return_value=repo,
    ):
        service.start_stage(task_id="task-1", stage="extract", queued_count=10)
        service.complete_stage(task_id="task-1", stage="extract", processed_count=10)
        service.fail_stage(task_id="task-1", stage="matching", error="boom", retry_eligible=True)
        service.complete_run(task_id="task-1", metadata={"done": True})
        service.fail_run(task_id="task-1", error="fatal", retry_eligible=True)
        service.cancel_run(task_id="task-1")

    repo.start_stage.assert_called_once()
    repo.complete_stage.assert_called_once()
    repo.fail_stage.assert_called_once()
    repo.complete_run.assert_called_once()
    repo.fail_run.assert_called_once()
    repo.cancel_run.assert_called_once_with(run)
    assert redis_gateway.set_task_state.call_count == 6
