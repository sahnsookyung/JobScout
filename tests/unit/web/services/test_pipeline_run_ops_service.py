import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from database.models import PIPELINE_RUN_FAILED, PIPELINE_RUN_RUNNING
from web.backend.services.pipeline_run_ops_service import (
    PipelineRunOpsService,
    _coerce_run_id,
    _stage_limit,
    _target_stage,
)


def _stage(stage: str = "matching", status: str = PIPELINE_RUN_FAILED):
    return SimpleNamespace(
        id=uuid.uuid4(),
        stage=stage,
        status=status,
        queued_count=7,
        processed_count=0,
        succeeded_count=0,
        failed_count=1,
        skipped_count=0,
        retry_count=0,
        retry_eligible=True,
        last_error="worker failed",
        started_at=None,
        completed_at=None,
        metadata_json={},
    )


def _run(
    *,
    status: str = PIPELINE_RUN_RUNNING,
    retry_eligible: bool = False,
    stage: str = "matching",
    resume_fingerprint: str | None = "resume-fp",
):
    now = datetime.now(timezone.utc)
    stages = [_stage(stage=stage)]
    return SimpleNamespace(
        id=uuid.uuid4(),
        task_id="match-1234",
        run_type="match",
        owner_id=None,
        tenant_id=None,
        resume_fingerprint=resume_fingerprint,
        status=status,
        current_stage=stage,
        queued_count=7,
        processed_count=0,
        succeeded_count=0,
        failed_count=1,
        skipped_count=0,
        retry_eligible=retry_eligible,
        last_error="worker failed",
        started_at=now,
        completed_at=None,
        heartbeat_at=now,
        created_at=now,
        updated_at=now,
        metadata_json={},
        stages=stages,
    )


def test_cancel_run_sets_runtime_cancellation_flag_before_durable_cancel():
    service = PipelineRunOpsService()
    db = Mock()
    run = _run()
    repo = Mock()

    def cancel_side_effect(target, *, metadata):
        target.status = "cancelled"
        target.completed_at = datetime.now(timezone.utc)
        target.metadata_json = metadata
        return target

    repo.cancel_run.side_effect = cancel_side_effect

    with patch.object(service, "get_run_model", return_value=run), patch(
        "web.backend.services.pipeline_run_ops_service.PipelineRunRepository",
        return_value=repo,
    ), patch(
        "web.backend.services.pipeline_run_ops_service.set_task_cancellation_requested",
    ) as set_cancel:
        summary = service.cancel_run(db, tenant_id=None, run_id=str(run.id))

    set_cancel.assert_called_once_with("match-1234", ttl=3600)
    repo.cancel_run.assert_called_once()
    assert repo.cancel_run.call_args.kwargs["metadata"]["runtime_cancel_requested"] is True
    db.commit.assert_called_once()
    assert summary.status == "cancelled"


def test_cancel_run_records_degraded_metadata_when_runtime_cancel_fails():
    service = PipelineRunOpsService()
    db = Mock()
    run = _run()
    repo = Mock()

    def cancel_side_effect(target, *, metadata):
        target.status = "cancelled"
        target.metadata_json = metadata
        return target

    repo.cancel_run.side_effect = cancel_side_effect

    with patch.object(service, "get_run_model", return_value=run), patch(
        "web.backend.services.pipeline_run_ops_service.PipelineRunRepository",
        return_value=repo,
    ), patch(
        "web.backend.services.pipeline_run_ops_service.set_task_cancellation_requested",
        side_effect=RuntimeError("redis down"),
    ):
        summary = service.cancel_run(db, tenant_id=None, run_id=str(run.id))

    metadata = summary.metadata
    assert metadata["runtime_cancel_requested"] is False
    assert metadata["runtime_cancel_error"] == "redis down"
    db.commit.assert_called_once()


def test_requeue_run_enqueues_matching_with_durable_correlation():
    service = PipelineRunOpsService()
    db = Mock()
    source = _run(status=PIPELINE_RUN_FAILED, retry_eligible=True)
    retry_run = _run(status=PIPELINE_RUN_RUNNING, retry_eligible=False)
    retry_run.id = uuid.uuid4()
    retry_run.task_id = "match-1234-requeue-abcd"
    retry_run.stages = []
    stage_row = _stage(stage="matching", status=PIPELINE_RUN_RUNNING)
    stage_row.retry_eligible = False
    repo = Mock()
    repo.create_run.return_value = retry_run

    def start_stage_side_effect(target, **_kwargs):
        target.stages = [stage_row]
        return stage_row

    repo.start_stage.side_effect = start_stage_side_effect

    with patch.object(service, "get_run_model", return_value=source), patch(
        "web.backend.services.pipeline_run_ops_service.PipelineRunRepository",
        return_value=repo,
    ), patch(
        "web.backend.services.pipeline_run_ops_service.enqueue_job",
    ) as enqueue_job:
        summary, task_id = service.requeue_run(db, tenant_id=None, run_id=str(source.id))

    assert task_id.startswith("match-1234-requeue-")
    enqueue_job.assert_called_once()
    stream, payload = enqueue_job.call_args.args
    assert stream == "matching:jobs"
    assert payload["resume_fingerprint"] == "resume-fp"
    assert payload["pipeline_run_id"] == str(retry_run.id)
    assert payload["pipeline_stage_id"] == str(stage_row.id)
    assert summary.id == str(retry_run.id)
    db.commit.assert_called_once()


def test_requeue_run_validates_matching_resume_before_creating_retry_run():
    service = PipelineRunOpsService()
    db = Mock()
    source = _run(
        status=PIPELINE_RUN_FAILED,
        retry_eligible=True,
        resume_fingerprint=None,
    )

    with patch.object(service, "get_run_model", return_value=source), patch(
        "web.backend.services.pipeline_run_ops_service.PipelineRunRepository",
    ) as repo_cls, patch(
        "web.backend.services.pipeline_run_ops_service.enqueue_job",
    ) as enqueue_job:
        with pytest.raises(ValueError, match="Matching requeue requires a resume fingerprint"):
            service.requeue_run(db, tenant_id=None, run_id=str(source.id))

    repo_cls.assert_not_called()
    enqueue_job.assert_not_called()
    db.commit.assert_not_called()


def test_helpers_handle_invalid_ids_stage_fallbacks_and_limits():
    run = _run(stage="custom")
    run.stages = [
        _stage(stage="extraction", status="completed"),
        _stage(stage="embedding", status="running"),
    ]
    run.stages[0].retry_eligible = False
    run.stages[1].retry_eligible = False
    run.stages[1].queued_count = 0
    run.current_stage = "custom"
    run.queued_count = 0

    assert _coerce_run_id("not-a-uuid") is None
    assert _target_stage(run) == "custom"
    assert _stage_limit(run, "embedding") == 1

    run.stages[0].retry_eligible = True
    assert _target_stage(run) == "extraction"
    assert _stage_limit(run, "extraction") == 7


def test_get_run_model_returns_none_for_invalid_id_without_querying_db():
    service = PipelineRunOpsService()
    db = Mock()

    assert service.get_run_model(db, tenant_id=None, run_id="not-a-uuid") is None
    db.execute.assert_not_called()


def test_cancel_run_rejects_missing_or_non_cancelable_run():
    service = PipelineRunOpsService()
    db = Mock()

    with patch.object(service, "get_run_model", return_value=None):
        with pytest.raises(LookupError, match="not found"):
            service.cancel_run(db, tenant_id=None, run_id="missing")

    completed = _run(status="completed", retry_eligible=False)
    with patch.object(service, "get_run_model", return_value=completed):
        with pytest.raises(ValueError, match="cannot be cancelled"):
            service.cancel_run(db, tenant_id=None, run_id=str(completed.id))


def test_requeue_run_rejects_missing_disallowed_and_non_requeueable_runs():
    service = PipelineRunOpsService()
    db = Mock()

    with patch.object(service, "get_run_model", return_value=None):
        with pytest.raises(LookupError, match="not found"):
            service.requeue_run(db, tenant_id=None, run_id="missing")

    running = _run(status=PIPELINE_RUN_RUNNING, retry_eligible=False)
    with patch.object(service, "get_run_model", return_value=running):
        with pytest.raises(ValueError, match="cannot be requeued"):
            service.requeue_run(db, tenant_id=None, run_id=str(running.id))

    failed = _run(status=PIPELINE_RUN_FAILED, retry_eligible=True, stage="extraction")
    with patch.object(service, "get_run_model", return_value=failed), patch.dict(
        "web.backend.services.pipeline_run_ops_service.REQUEUE_STAGE_STREAMS",
        {},
        clear=True,
    ):
        with pytest.raises(ValueError, match="does not have a requeueable"):
            service.requeue_run(db, tenant_id=None, run_id=str(failed.id))


def test_requeue_run_enqueues_extraction_with_stage_limit():
    service = PipelineRunOpsService()
    db = Mock()
    source = _run(status=PIPELINE_RUN_FAILED, retry_eligible=True, stage="extraction")
    retry_run = _run(status=PIPELINE_RUN_RUNNING, retry_eligible=False, stage="extraction")
    retry_run.stages = []
    stage_row = _stage(stage="extraction", status=PIPELINE_RUN_RUNNING)
    repo = Mock()
    repo.create_run.return_value = retry_run
    repo.start_stage.return_value = stage_row

    with patch.object(service, "get_run_model", return_value=source), patch(
        "web.backend.services.pipeline_run_ops_service.PipelineRunRepository",
        return_value=repo,
    ), patch("web.backend.services.pipeline_run_ops_service.enqueue_job") as enqueue_job:
        summary, task_id = service.requeue_run(db, tenant_id=None, run_id=str(source.id))

    stream, payload = enqueue_job.call_args.args
    assert stream == "extraction:batch"
    assert payload["limit"] == 7
    assert summary.id == str(retry_run.id)
    assert task_id.startswith("match-1234-requeue-")
    db.commit.assert_called_once()


def test_retry_run_delegates_to_requeue_action():
    service = PipelineRunOpsService()
    db = Mock()

    with patch.object(
        service,
        "requeue_run",
        return_value=("summary", "task-retry"),
    ) as requeue:
        assert service.retry_run(db, tenant_id=None, run_id="run-1") == (
            "summary",
            "task-retry",
        )

    requeue.assert_called_once_with(
        db,
        tenant_id=None,
        run_id="run-1",
        action="retry",
    )


def test_llm_queue_status_wraps_success_and_degraded_error():
    service = PipelineRunOpsService()

    with patch(
        "web.backend.services.pipeline_run_ops_service.check_llm_evaluation_queue_readiness",
        return_value={"ready": True, "queue": "llm_evaluations", "queued": 2},
    ):
        assert service.llm_queue_status() == {
            "success": True,
            "ready": True,
            "queue": "llm_evaluations",
            "queued": 2,
        }

    with patch(
        "web.backend.services.pipeline_run_ops_service.check_llm_evaluation_queue_readiness",
        side_effect=RuntimeError("redis unavailable"),
    ):
        status = service.llm_queue_status()

    assert status["success"] is False
    assert status["ready"] is False
    assert status["queue"] == "llm_evaluations"
    assert status["queued"] == 0
    assert status["error"] == "redis unavailable"

def test_llm_queue_operator_methods_delegate_to_queue_helpers():
    service = PipelineRunOpsService()

    with patch(
        "web.backend.services.pipeline_run_ops_service.set_llm_evaluation_queue_paused",
    ) as pause, patch.object(
        service,
        "llm_queue_status",
        return_value={"success": True, "ready": True, "queue": "llm_evaluations"},
    ) as status:
        assert service.pause_llm_queue(reason="maintenance", ttl_seconds=60) == status.return_value

    pause.assert_called_once_with(reason="maintenance", ttl_seconds=60)

    with patch(
        "web.backend.services.pipeline_run_ops_service.resume_llm_evaluation_queue",
    ) as resume, patch.object(
        service,
        "llm_queue_status",
        return_value={"success": True, "ready": True, "queue": "llm_evaluations"},
    ) as status:
        assert service.resume_llm_queue() == status.return_value

    resume.assert_called_once_with()

def test_retry_llm_queue_returns_enqueued_count_and_status():
    service = PipelineRunOpsService()

    with patch(
        "web.backend.services.pipeline_run_ops_service.enqueue_stale_or_retryable_evaluations",
        return_value=7,
    ) as enqueue, patch.object(
        service,
        "llm_queue_status",
        return_value={"success": True, "ready": True, "queue": "llm_evaluations"},
    ):
        status, enqueued = service.retry_llm_queue(limit=25)

    enqueue.assert_called_once_with(limit=25)
    assert enqueued == 7
    assert status["queue"] == "llm_evaluations"

def test_llm_provider_operator_methods_delegate_to_provider_health():
    service = PipelineRunOpsService()

    with patch(
        "web.backend.services.pipeline_run_ops_service.configured_llm_provider_status",
        return_value={"success": True, "count": 0, "providers": []},
    ) as status:
        assert service.llm_provider_status()["providers"] == []
    status.assert_called_once_with()

    with patch(
        "web.backend.services.pipeline_run_ops_service.run_llm_provider_canaries",
        return_value={"success": True, "count": 0, "results": []},
    ) as canaries:
        assert service.run_llm_provider_canaries()["results"] == []
    canaries.assert_called_once_with()

    with patch(
        "web.backend.services.pipeline_run_ops_service.reset_llm_provider_circuit",
        return_value={"success": True, "provider": "nvidia", "model": "model"},
    ) as reset:
        assert service.reset_llm_provider_circuit(provider="nvidia", model="model")["provider"] == "nvidia"
    reset.assert_called_once_with(provider="nvidia", model="model")
