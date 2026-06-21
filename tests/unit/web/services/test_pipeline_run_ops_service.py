import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from database.models import PIPELINE_RUN_FAILED, PIPELINE_RUN_RUNNING
from web.backend.services.pipeline_run_ops_service import PipelineRunOpsService


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
