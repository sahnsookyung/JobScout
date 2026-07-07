from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock, patch

from services.orchestrator.repair import run_stuck_job_repair


class _FakePipelineRuns:
    def __init__(self):
        self.calls = []

    def start_run(self, **kwargs):
        self.calls.append(("start_run", kwargs))
        return {"pipeline_run_id": "repair-run-1", "result": {"stages": []}}

    def start_stage(self, **kwargs):
        self.calls.append(("start_stage", kwargs))
        return {
            "pipeline_run_id": "repair-run-1",
            "result": {
                "stages": [
                    {"id": "repair-stage-1", "stage": kwargs["stage"]},
                ],
            },
        }

    def complete_stage(self, **kwargs):
        self.calls.append(("complete_stage", kwargs))

    def complete_run(self, **kwargs):
        self.calls.append(("complete_run", kwargs))

    def fail_stage(self, **kwargs):
        self.calls.append(("fail_stage", kwargs))

    def fail_run(self, **kwargs):
        self.calls.append(("fail_run", kwargs))


def test_repair_claims_jobs_before_enqueueing_batches():
    job_post_repo = Mock()
    job_post_repo.claim_unextracted_jobs_for_queue.return_value = [
        SimpleNamespace(id="extract-1"),
        SimpleNamespace(id="extract-2"),
    ]
    job_post_repo.claim_unembedded_jobs_for_queue.return_value = [
        SimpleNamespace(id="embed-1"),
    ]
    repo = SimpleNamespace(
        job_post=job_post_repo,
        get_latest_ready_resume_fingerprint=Mock(return_value=None),
    )

    @contextmanager
    def fake_session_scope():
        yield Mock()

    pipeline_runs = _FakePipelineRuns()
    with patch("services.orchestrator.repair.db_session_scope", fake_session_scope), \
         patch("services.orchestrator.repair.JobRepository", return_value=repo), \
         patch("services.orchestrator.repair.enqueue_job") as enqueue_job:
        result = run_stuck_job_repair(
            task_id="repair-1",
            pipeline_runs=pipeline_runs,
            extraction_limit=10,
            embedding_limit=5,
        )

    assert result["extraction_queued"] == 2
    assert result["embedding_queued"] == 1
    job_post_repo.claim_unextracted_jobs_for_queue.assert_called_once_with(limit=10)
    job_post_repo.claim_unembedded_jobs_for_queue.assert_called_once_with(limit=5)
    assert enqueue_job.call_count == 2
    for call in enqueue_job.call_args_list:
        payload = call.args[1]
        assert payload["pipeline_run_id"] == "repair-run-1"
        assert payload["pipeline_stage_id"] == "repair-stage-1"
    assert ("start_stage", {"task_id": "repair-1", "stage": "repair", "run_type": "repair"}) in pipeline_runs.calls
    assert any(name == "complete_stage" for name, _ in pipeline_runs.calls)

def test_repair_uses_preference_aware_matching_backlog():
    job_post_repo = Mock()
    job_post_repo.claim_unextracted_jobs_for_queue.return_value = []
    job_post_repo.claim_unembedded_jobs_for_queue.return_value = []
    preferences = {"remote_mode": "remote"}
    repo = SimpleNamespace(
        job_post=job_post_repo,
        get_latest_ready_resume_fingerprint=Mock(return_value="resume-fp"),
        get_structured_resume_by_fingerprint=Mock(
            return_value=SimpleNamespace(owner_id="owner-1", tenant_id="tenant-1")
        ),
        count_pending_matching_jobs=Mock(return_value=0),
    )

    @contextmanager
    def fake_session_scope():
        yield Mock()

    pipeline_runs = _FakePipelineRuns()
    with patch("services.orchestrator.repair.db_session_scope", fake_session_scope), \
         patch("services.orchestrator.repair.JobRepository", return_value=repo), \
         patch(
             "services.orchestrator.repair._load_candidate_preferences",
             return_value=preferences,
         ) as load_preferences, \
         patch("services.orchestrator.repair.enqueue_job") as enqueue_job:
        result = run_stuck_job_repair(
            task_id="repair-1",
            pipeline_runs=pipeline_runs,
            extraction_limit=10,
            embedding_limit=5,
        )

    load_preferences.assert_called_once_with(repo, "owner-1")
    repo.count_pending_matching_jobs.assert_called_once_with(
        "resume-fp",
        tenant_id="tenant-1",
        candidate_preferences=preferences,
    )
    assert result["ready_unmatched_count"] == 0
    assert result["matching_queued"] == 0
    enqueue_job.assert_not_called()

def test_repair_matching_payload_includes_owner_when_backlog_exists():
    job_post_repo = Mock()
    job_post_repo.claim_unextracted_jobs_for_queue.return_value = []
    job_post_repo.claim_unembedded_jobs_for_queue.return_value = []
    repo = SimpleNamespace(
        job_post=job_post_repo,
        get_latest_ready_resume_fingerprint=Mock(return_value="resume-fp"),
        get_structured_resume_by_fingerprint=Mock(
            return_value=SimpleNamespace(owner_id="owner-1", tenant_id=None)
        ),
        count_pending_matching_jobs=Mock(return_value=4),
    )

    @contextmanager
    def fake_session_scope():
        yield Mock()

    pipeline_runs = _FakePipelineRuns()
    with patch("services.orchestrator.repair.db_session_scope", fake_session_scope), \
         patch("services.orchestrator.repair.JobRepository", return_value=repo), \
         patch("services.orchestrator.repair._load_candidate_preferences", return_value=None), \
         patch("services.orchestrator.repair.enqueue_job") as enqueue_job:
        result = run_stuck_job_repair(
            task_id="repair-1",
            pipeline_runs=pipeline_runs,
            extraction_limit=10,
            embedding_limit=5,
        )

    assert result["ready_unmatched_count"] == 4
    assert result["matching_queued"] == 4
    enqueue_job.assert_called_once()
    assert enqueue_job.call_args.args[0] == "matching:jobs"
    assert enqueue_job.call_args.args[1]["owner_id"] == "owner-1"
    assert enqueue_job.call_args.args[1]["resume_fingerprint"] == "resume-fp"
