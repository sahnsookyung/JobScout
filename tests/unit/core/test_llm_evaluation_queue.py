from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from core import llm_evaluation_queue
from database.models import LLM_EVALUATION_FAILED


class _ExistingJob:
    def __init__(self, status):
        self.status = status
        self.deleted = False

    def get_status(self, refresh=True):
        return self.status

    def delete(self):
        self.deleted = True


class _FakeQueue:
    def __init__(self, existing=None):
        self.existing = existing
        self.enqueued = []

    def fetch_job(self, job_id):
        self.fetched_job_id = job_id
        return self.existing

    def enqueue(self, *args, **kwargs):
        self.enqueued.append((args, kwargs))
        return Mock(id=kwargs["job_id"])

class _CountQueue(_FakeQueue):
    name = "llm_evaluations"

    def __len__(self):
        return 4

class _Registry:
    def __init__(self, count):
        self.count = count

    def __len__(self):
        return self.count


def test_enqueue_unique_reuses_active_job():
    queue = _FakeQueue(existing=_ExistingJob("queued"))

    job_id = llm_evaluation_queue._enqueue_unique(queue, "eval-1", None, {})

    assert job_id == "llm-evaluation:eval-1"
    assert queue.enqueued == []
    assert queue.existing.deleted is False


def test_enqueue_unique_replaces_terminal_job():
    existing = _ExistingJob("finished")
    queue = _FakeQueue(existing=existing)

    job_id = llm_evaluation_queue._enqueue_unique(queue, "eval-2", {"payload": True}, {})

    assert job_id == "llm-evaluation:eval-2"
    assert existing.deleted is True
    assert queue.enqueued[0][1]["job_id"] == "llm-evaluation:eval-2"


def test_process_task_skips_when_row_not_claimed():
    with patch(
        "core.llm_evaluation_queue._claim_evaluation_for_execution",
        return_value=False,
    ) as claim, patch("core.llm_evaluation_queue.SessionLocal") as session_local:
        result = llm_evaluation_queue.process_llm_evaluation_task("eval-3")

    assert result == "eval-3"
    claim.assert_called_once_with("eval-3")
    session_local.assert_not_called()


def test_process_task_marks_claimed_row_retryable_on_unexpected_failure():
    db = Mock()
    with patch(
        "core.llm_evaluation_queue._claim_evaluation_for_execution",
        return_value=True,
    ), patch("core.llm_evaluation_queue.SessionLocal", return_value=db), patch(
        "core.llm_evaluation_queue.MatchLlmEvaluationService",
    ) as service_cls, patch(
        "core.llm_evaluation_queue._mark_evaluation_worker_failure",
        return_value=True,
    ) as mark_failure:
        service_cls.return_value.resume_pending_evaluation.side_effect = RuntimeError(
            "provider payload rebuild failed"
        )

        with pytest.raises(RuntimeError, match="provider payload rebuild failed"):
            llm_evaluation_queue.process_llm_evaluation_task("eval-4")

    db.rollback.assert_called_once()
    db.close.assert_called_once()
    mark_failure.assert_called_once_with("eval-4", error_code="worker_error")


def test_process_task_raises_to_rq_for_retryable_domain_failure():
    db = Mock()
    retryable_evaluation = SimpleNamespace(
        id="eval-5",
        status=LLM_EVALUATION_FAILED,
        retryable=True,
        error_code="provider_timeout",
    )
    with patch(
        "core.llm_evaluation_queue._claim_evaluation_for_execution",
        return_value=True,
    ), patch("core.llm_evaluation_queue.SessionLocal", return_value=db), patch(
        "core.llm_evaluation_queue.MatchLlmEvaluationService",
    ) as service_cls, patch(
        "core.llm_evaluation_queue._mark_evaluation_worker_failure",
    ) as mark_failure:
        service_cls.return_value.resume_pending_evaluation.return_value = retryable_evaluation

        with pytest.raises(
            llm_evaluation_queue.RetryableLlmEvaluationError,
            match="provider_timeout",
        ):
            llm_evaluation_queue.process_llm_evaluation_task("eval-5")

    db.rollback.assert_not_called()
    db.close.assert_called_once()
    mark_failure.assert_not_called()


def test_queue_status_reports_bounded_registry_depths():
    queue = _CountQueue()
    with patch("core.llm_evaluation_queue.StartedJobRegistry", return_value=_Registry(1)), patch(
        "core.llm_evaluation_queue.DeferredJobRegistry",
        return_value=_Registry(2),
    ), patch("core.llm_evaluation_queue.ScheduledJobRegistry", return_value=_Registry(3)), patch(
        "core.llm_evaluation_queue.FailedJobRegistry",
        return_value=_Registry(5),
    ):
        status = llm_evaluation_queue.get_llm_evaluation_queue_status(queue)

    assert status == {
        "queue": "llm_evaluations",
        "queued": 4,
        "started": 1,
        "deferred": 2,
        "scheduled": 3,
        "failed": 5,
    }
