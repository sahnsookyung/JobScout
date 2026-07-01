import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from web.backend.services.cursors import MatchCursorCodec
from web.backend.services.pipeline_run_service import (
    PipelineRunReadService,
    _latest_failed_or_retryable_stage,
    pipeline_run_allowed_actions,
    pipeline_run_summary,
)


def _stage(
    *,
    stage: str = "matching",
    status: str = "failed",
    retry_eligible: bool = True,
):
    return SimpleNamespace(
        id=uuid.uuid4(),
        stage=stage,
        status=status,
        queued_count=7,
        processed_count=3,
        succeeded_count=2,
        failed_count=1,
        skipped_count=0,
        retry_count=1,
        retry_eligible=retry_eligible,
        last_error="stage failed",
        started_at=None,
        completed_at=None,
        metadata_json={"stage_meta": True},
    )


def _run(
    *,
    status: str = "failed",
    retry_eligible: bool = True,
    current_stage: str = "matching",
    stages=None,
    tenant_id=None,
    created_at=None,
):
    now = created_at or datetime.now(timezone.utc)
    return SimpleNamespace(
        id=uuid.uuid4(),
        task_id="task-1",
        run_type="pipeline",
        status=status,
        current_stage=current_stage,
        queued_count=7,
        processed_count=3,
        succeeded_count=2,
        failed_count=1,
        skipped_count=0,
        retry_eligible=retry_eligible,
        last_error="run failed",
        owner_id=uuid.uuid4(),
        tenant_id=tenant_id,
        resume_fingerprint="resume-fp",
        started_at=now,
        completed_at=None,
        heartbeat_at=now,
        created_at=now,
        updated_at=now,
        metadata_json={"step": "matching", "upload_id": "upload-1"},
        stages=stages if stages is not None else [_stage(stage=current_stage)],
    )


def _scalar_result(value):
    result = Mock()
    result.scalar_one.return_value = value
    return result


def _rows_result(rows):
    result = Mock()
    result.scalars.return_value.all.return_value = rows
    return result


def _one_or_none_result(value):
    result = Mock()
    result.scalar_one_or_none.return_value = value
    return result


def test_pipeline_run_summary_includes_stages_and_allowed_actions():
    run = _run(status="failed", retry_eligible=True, current_stage="matching")

    summary = pipeline_run_summary(run)

    assert summary.task_id == "task-1"
    assert summary.metadata["upload_id"] == "upload-1"
    assert summary.stages[0].metadata == {"stage_meta": True}
    assert summary.allowed_actions == ["retry", "requeue"]


def test_latest_failed_or_retryable_stage_falls_back_to_current_stage():
    run = _run(
        status="failed",
        retry_eligible=True,
        current_stage="custom",
        stages=[_stage(stage="extract", status="completed", retry_eligible=False)],
    )

    assert _latest_failed_or_retryable_stage(run) == "custom"


def test_allowed_actions_include_cancel_for_active_run_and_requeue_for_retryable_stage():
    running = _run(status="running", retry_eligible=False, current_stage="matching")
    failed = _run(status="failed", retry_eligible=True, current_stage="embedding")

    assert pipeline_run_allowed_actions(running) == ["cancel"]
    assert pipeline_run_allowed_actions(failed) == ["retry", "requeue"]


def test_allowed_actions_retry_without_requeue_for_non_requeueable_stage():
    failed = _run(
        status="failed",
        retry_eligible=True,
        current_stage="custom",
        stages=[_stage(stage="custom", retry_eligible=True)],
    )

    assert pipeline_run_allowed_actions(failed) == ["retry"]


def test_list_runs_offset_mode_returns_summaries_and_total():
    service = PipelineRunReadService()
    db = Mock()
    run = _run(status="completed", retry_eligible=False, stages=[])
    db.execute.side_effect = [_scalar_result(1), _rows_result([run])]

    runs, total, next_cursor, has_more, page_mode, offset = service.list_runs(
        db,
        tenant_id=None,
        status="completed",
        run_type="pipeline",
        limit=10,
        offset=5,
    )

    assert total == 1
    assert runs[0].id == str(run.id)
    assert next_cursor is None
    assert has_more is False
    assert page_mode == "offset"
    assert offset == 5


def test_list_runs_cursor_mode_applies_cursor_and_returns_next_cursor():
    service = PipelineRunReadService()
    db = Mock()
    now = datetime.now(timezone.utc)
    first = _run(created_at=now)
    second = _run(created_at=now - timedelta(minutes=1))
    cursor = MatchCursorCodec.encode(
        "pipeline_runs",
        created_at=now.isoformat(),
        id=str(first.id),
    )
    db.execute.side_effect = [_scalar_result(3), _rows_result([first, second])]

    runs, total, next_cursor, has_more, page_mode, offset = service.list_runs(
        db,
        tenant_id=uuid.uuid4(),
        status="all",
        run_type="all",
        limit=1,
        offset=99,
        cursor=cursor,
    )

    assert total == 3
    assert [run.id for run in runs] == [str(first.id)]
    assert has_more is True
    assert page_mode == "cursor"
    assert offset == 0
    assert MatchCursorCodec.decode(next_cursor, expected_kind="pipeline_runs")["id"] == str(first.id)


def test_list_runs_rejects_invalid_cursor_payload():
    service = PipelineRunReadService()
    db = Mock()
    cursor = MatchCursorCodec.encode(
        "pipeline_runs",
        created_at="not-a-date",
        id="not-a-uuid",
    )
    db.execute.return_value = _scalar_result(0)

    with pytest.raises(ValueError, match="Invalid pipeline run cursor"):
        service.list_runs(
            db,
            tenant_id=None,
            status="all",
            run_type="all",
            limit=10,
            offset=0,
            cursor=cursor,
        )


def test_get_run_rejects_invalid_id_and_returns_summary_for_visible_run():
    service = PipelineRunReadService()
    db = Mock()
    run = _run(stages=[])

    assert service.get_run(db, tenant_id=None, run_id="not-a-uuid") is None

    db.execute.return_value = _one_or_none_result(run)
    summary = service.get_run(db, tenant_id=None, run_id=str(run.id))

    assert summary.id == str(run.id)


def test_get_run_returns_none_when_missing():
    service = PipelineRunReadService()
    db = Mock()
    db.execute.return_value = _one_or_none_result(None)

    assert service.get_run(db, tenant_id=uuid.uuid4(), run_id=str(uuid.uuid4())) is None
