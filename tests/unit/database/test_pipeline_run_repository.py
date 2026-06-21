import uuid
from types import SimpleNamespace

from database.models import (
    PIPELINE_RUN_CANCELLED,
    PIPELINE_RUN_COMPLETED,
    PIPELINE_RUN_FAILED,
    PIPELINE_RUN_RUNNING,
    PipelineRunStage,
)
from database.repositories.pipeline_run import (
    PipelineRunRepository,
    normalize_pipeline_stage,
)

class _MemoryDb:
    def __init__(self, repo):
        self.repo = repo

    def add(self, value):
        if isinstance(value, PipelineRunStage):
            self.repo.stages.append(value)

    def flush(self):
        return None

class _MemoryPipelineRunRepository(PipelineRunRepository):
    def __init__(self):
        self.stages = []
        super().__init__(_MemoryDb(self))

    def get_stage(self, run_id, stage):
        stage = normalize_pipeline_stage(stage)
        return next(
            (
                stage_row
                for stage_row in self.stages
                if stage_row.run_id == run_id and stage_row.stage == stage
            ),
            None,
        )

    def _recompute_counts(self, run):
        stages = [stage for stage in self.stages if stage.run_id == run.id]
        run.queued_count = sum(int(stage.queued_count or 0) for stage in stages)
        run.processed_count = sum(int(stage.processed_count or 0) for stage in stages)
        run.succeeded_count = sum(int(stage.succeeded_count or 0) for stage in stages)
        run.failed_count = sum(int(stage.failed_count or 0) for stage in stages)
        run.skipped_count = sum(int(stage.skipped_count or 0) for stage in stages)

def _run(status=PIPELINE_RUN_RUNNING):
    return SimpleNamespace(
        id=uuid.uuid4(),
        status=status,
        current_stage=None,
        queued_count=0,
        processed_count=0,
        succeeded_count=0,
        failed_count=0,
        skipped_count=0,
        retry_eligible=False,
        last_error=None,
        started_at=None,
        completed_at=None,
        heartbeat_at=None,
        metadata_json={},
    )

def test_complete_stage_is_idempotent_for_aggregate_counts():
    repo = _MemoryPipelineRunRepository()
    run = _run()

    repo.complete_stage(run, stage="embed", processed_count=3)
    repo.complete_stage(run, stage="embed", processed_count=3)

    assert repo.stages[0].stage == "embedding"
    assert run.processed_count == 3
    assert run.succeeded_count == 3
    assert run.failed_count == 0

def test_failed_run_cannot_be_accidentally_completed():
    repo = _MemoryPipelineRunRepository()
    run = _run()

    repo.fail_stage(run, stage="extraction", error="worker failed", retry_eligible=True)
    repo.complete_run(run, metadata={"done_callback": True})

    assert run.status == PIPELINE_RUN_FAILED
    assert run.retry_eligible is True
    assert run.last_error == "worker failed"

def test_cancelled_run_cannot_be_overwritten_by_failure():
    repo = _MemoryPipelineRunRepository()
    run = _run()

    repo.cancel_run(run)
    repo.fail_run(run, error="late failure", retry_eligible=True)

    assert run.status == PIPELINE_RUN_CANCELLED
    assert run.last_error is None
    assert run.retry_eligible is False

def test_completed_run_cannot_be_cancelled_later():
    repo = _MemoryPipelineRunRepository()
    run = _run()

    repo.complete_run(run)
    repo.cancel_run(run)

    assert run.status == PIPELINE_RUN_COMPLETED

def test_resume_stage_aliases_normalize_to_canonical_names():
    assert normalize_pipeline_stage("extracting") == "resume_extraction"
    assert normalize_pipeline_stage("resume-embedding") == "resume_embedding"
    assert normalize_pipeline_stage("match") == "matching"

def test_unknown_stage_normalization_preserves_value():
    assert normalize_pipeline_stage("custom_stage") == "custom_stage"
