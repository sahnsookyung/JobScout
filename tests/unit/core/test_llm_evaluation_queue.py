import uuid
from datetime import datetime, timezone
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
        self.enqueued_in = []
        self.connection = Mock()

    def fetch_job(self, job_id):
        self.fetched_job_id = job_id
        return self.existing

    def enqueue(self, *args, **kwargs):
        self.enqueued.append((args, kwargs))
        return Mock(id=kwargs["job_id"])

    def enqueue_in(self, *args, **kwargs):
        self.enqueued_in.append((args, kwargs))
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

class _PauseRedis:
    def __init__(self):
        self.value = None
        self.ttl_value = -1
        self.deleted = False

    def get(self, key):
        return self.value

    def ttl(self, key):
        return self.ttl_value

    def set(self, key, value):
        self.value = value
        self.ttl_value = -1

    def setex(self, key, seconds, value):
        self.value = value
        self.ttl_value = seconds

    def delete(self, key):
        self.value = None
        self.deleted = True


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
    ), patch(
        "core.llm_evaluation_queue._db_backlog_status",
        return_value={
            "db_pending": 6,
            "db_running": 1,
            "db_failed": 2,
            "db_retryable_failed": 1,
            "oldest_pending_age_seconds": 30,
            "oldest_retryable_failed_age_seconds": 60,
            "drain_estimate_seconds": 11,
        },
    ), patch(
        "core.llm_evaluation_queue.get_llm_evaluation_queue_pause_status",
        return_value={"paused": False, "pause_reason": None, "pause_ttl_seconds": None},
    ):
        status = llm_evaluation_queue.get_llm_evaluation_queue_status(queue)

    assert status == {
        "queue": "llm_evaluations",
        "queued": 4,
        "started": 1,
        "deferred": 2,
        "scheduled": 3,
        "failed": 5,
        "db_pending": 6,
        "db_running": 1,
        "db_failed": 2,
        "db_retryable_failed": 1,
        "oldest_pending_age_seconds": 30,
        "oldest_retryable_failed_age_seconds": 60,
        "drain_estimate_seconds": 11,
        "paused": False,
        "pause_reason": None,
        "pause_ttl_seconds": None,
    }

def test_pause_status_and_controls_round_trip_through_redis():
    redis = _PauseRedis()

    with patch("core.llm_evaluation_queue._redis_conn", return_value=redis):
        paused = llm_evaluation_queue.set_llm_evaluation_queue_paused(
            reason="maintenance",
            ttl_seconds=60,
        )
        resumed = llm_evaluation_queue.resume_llm_evaluation_queue()

    assert paused == {
        "paused": True,
        "pause_reason": "maintenance",
        "pause_ttl_seconds": 60,
    }
    assert resumed == {
        "paused": False,
        "pause_reason": None,
        "pause_ttl_seconds": None,
    }
    assert redis.deleted is True

def test_process_task_defers_without_claiming_when_queue_paused():
    queue = _FakeQueue()
    with patch(
        "core.llm_evaluation_queue.get_llm_evaluation_queue_pause_status",
        return_value={"paused": True, "pause_reason": "maintenance"},
    ), patch("core.llm_evaluation_queue._queue", return_value=queue), patch(
        "core.llm_evaluation_queue._claim_evaluation_for_execution",
    ) as claim:
        result = llm_evaluation_queue.process_llm_evaluation_task(
            "eval-paused",
            provider_payload={"payload": True},
            truncation={"trimmed": False},
        )

    assert result == "eval-paused"
    claim.assert_not_called()
    args, kwargs = queue.enqueued_in[0]
    assert args[1:] == (
        llm_evaluation_queue.process_llm_evaluation_task,
        "eval-paused",
        {"payload": True},
        {"trimmed": False},
    )
    assert kwargs["job_id"].startswith("llm-evaluation-paused:eval-paused:")

def test_top_n_scheduler_defers_without_db_when_queue_paused():
    queue = _FakeQueue()
    with patch(
        "core.llm_evaluation_queue.get_llm_evaluation_queue_pause_status",
        return_value={"paused": True, "pause_reason": "maintenance"},
    ), patch("core.llm_evaluation_queue._queue", return_value=queue), patch(
        "core.llm_evaluation_queue.SessionLocal",
    ) as session_local:
        result = llm_evaluation_queue.process_llm_top_n_selection_task(
            "selection-paused",
            "owner-paused",
            None,
            5,
            9,
        )

    assert result == {
        "attempted": 0,
        "reused": 0,
        "created": 0,
        "enqueued": 0,
        "failed": 0,
    }
    session_local.assert_not_called()
    args, kwargs = queue.enqueued_in[0]
    assert args[1:] == (
        llm_evaluation_queue.process_llm_top_n_selection_task,
        "selection-paused",
        "owner-paused",
        None,
        5,
        9,
    )
    assert kwargs["job_id"].startswith("llm-top-n-paused:")


def test_enqueue_top_n_scheduler_uses_stable_job_id_and_reuses_active_job():
    queue = _FakeQueue(existing=_ExistingJob("queued"))

    result = llm_evaluation_queue._enqueue_top_n_scheduler_unique(
        queue,
        selection_run_id="selection-1",
        owner_id="owner-1",
        tenant_id="tenant-1",
        top_n=5,
        policy_revision=7,
    )

    assert result == {
        "state": "reused",
        "job_id": "llm-top-n:owner-1:tenant-1:selection-1:r7:n5",
    }
    assert queue.enqueued == []


def test_enqueue_top_n_scheduler_enqueues_terminal_job():
    existing = _ExistingJob("failed")
    queue = _FakeQueue(existing=existing)

    result = llm_evaluation_queue._enqueue_top_n_scheduler_unique(
        queue,
        selection_run_id="selection-2",
        owner_id="owner-2",
        tenant_id=None,
        top_n=3,
        policy_revision=4,
    )

    assert result == {
        "state": "scheduled",
        "job_id": "llm-top-n:owner-2:none:selection-2:r4:n3",
    }
    assert existing.deleted is True
    args, kwargs = queue.enqueued[0]
    assert args[:6] == (
        llm_evaluation_queue.process_llm_top_n_selection_task,
        "selection-2",
        "owner-2",
        None,
        3,
        4,
    )
    assert kwargs["job_id"] == "llm-top-n:owner-2:none:selection-2:r4:n3"


def test_process_top_n_selection_task_delegates_to_evaluation_service():
    db = Mock()
    stats = {"attempted": 3, "reused": 1, "created": 2, "enqueued": 2, "failed": 0}
    with patch("core.llm_evaluation_queue.SessionLocal", return_value=db), patch(
        "core.llm_evaluation_queue.MatchLlmEvaluationService",
    ) as service_cls:
        service_cls.return_value.evaluate_selection_run.return_value = stats

        result = llm_evaluation_queue.process_llm_top_n_selection_task(
            "selection-3",
            "owner-3",
            "tenant-3",
            3,
            8,
        )

    assert result == stats
    service_cls.return_value.evaluate_selection_run.assert_called_once_with(
        "selection-3",
        owner_id="owner-3",
        tenant_id="tenant-3",
        top_n=3,
    )
    db.close.assert_called_once()


def test_env_int_uses_default_invalid_and_clamps_negative(monkeypatch, caplog):
    monkeypatch.delenv("LLM_EVALUATION_RETRY_MAX", raising=False)
    assert llm_evaluation_queue._env_int("LLM_EVALUATION_RETRY_MAX", 3) == 3

    monkeypatch.setenv("LLM_EVALUATION_RETRY_MAX", "7")
    assert llm_evaluation_queue._env_int("LLM_EVALUATION_RETRY_MAX", 3) == 7

    monkeypatch.setenv("LLM_EVALUATION_RETRY_MAX", "-2")
    assert llm_evaluation_queue._env_int("LLM_EVALUATION_RETRY_MAX", 3) == 0

    monkeypatch.setenv("LLM_EVALUATION_RETRY_MAX", "oops")
    assert llm_evaluation_queue._env_int("LLM_EVALUATION_RETRY_MAX", 3) == 3
    assert "Invalid LLM_EVALUATION_RETRY_MAX" in caplog.text


def test_env_int_list_uses_default_invalid_and_clamps_negative(monkeypatch, caplog):
    monkeypatch.delenv("LLM_EVALUATION_RETRY_INTERVALS_SECONDS", raising=False)
    assert llm_evaluation_queue._env_int_list(
        "LLM_EVALUATION_RETRY_INTERVALS_SECONDS",
        [60, 300],
    ) == [60, 300]

    monkeypatch.setenv("LLM_EVALUATION_RETRY_INTERVALS_SECONDS", "5,-1, 20")
    assert llm_evaluation_queue._env_int_list(
        "LLM_EVALUATION_RETRY_INTERVALS_SECONDS",
        [60, 300],
    ) == [5, 0, 20]

    monkeypatch.setenv("LLM_EVALUATION_RETRY_INTERVALS_SECONDS", "")
    assert llm_evaluation_queue._env_int_list(
        "LLM_EVALUATION_RETRY_INTERVALS_SECONDS",
        [60, 300],
    ) == [60, 300]

    monkeypatch.setenv("LLM_EVALUATION_RETRY_INTERVALS_SECONDS", "5,bad,20")
    assert llm_evaluation_queue._env_int_list(
        "LLM_EVALUATION_RETRY_INTERVALS_SECONDS",
        [60, 300],
    ) == [60, 300]
    assert "Invalid LLM_EVALUATION_RETRY_INTERVALS_SECONDS" in caplog.text


def test_retry_policy_uses_retry_environment(monkeypatch):
    monkeypatch.setenv("LLM_EVALUATION_RETRY_MAX", "4")
    monkeypatch.setenv("LLM_EVALUATION_RETRY_INTERVALS_SECONDS", "1,2,3")

    retry = llm_evaluation_queue._retry_policy()

    assert retry.max == 4
    assert retry.intervals == [1, 2, 3]


def test_queue_uses_configured_redis_url():
    config = SimpleNamespace(
        orchestrator=SimpleNamespace(redis_url="redis://queue-host:6379/4"),
    )
    redis_conn = Mock()

    with patch("core.llm_evaluation_queue.load_config", return_value=config), patch(
        "core.llm_evaluation_queue.Redis.from_url",
        return_value=redis_conn,
    ) as from_url, patch("core.llm_evaluation_queue.Queue") as queue_cls:
        queue = llm_evaluation_queue._queue()

    from_url.assert_called_once_with("redis://queue-host:6379/4")
    queue_cls.assert_called_once_with(
        llm_evaluation_queue.LLM_EVALUATION_QUEUE,
        connection=redis_conn,
    )
    assert queue is queue_cls.return_value


def test_enqueue_llm_evaluation_normalizes_inputs():
    queue = Mock()

    with patch("core.llm_evaluation_queue._queue", return_value=queue), patch(
        "core.llm_evaluation_queue._enqueue_unique",
        return_value="llm-evaluation:42",
    ) as enqueue_unique:
        job_id = llm_evaluation_queue.enqueue_llm_evaluation(
            42,
            provider_payload={"raw": True},
        )

    assert job_id == "llm-evaluation:42"
    enqueue_unique.assert_called_once_with(queue, "42", {"raw": True}, {})


def test_check_readiness_pings_redis_and_reports_queue_status():
    redis_conn = Mock()
    queue = Mock()
    status = {"queue": "llm_evaluations", "queued": 1, "started": 0}

    with patch(
        "core.llm_evaluation_queue._redis_url",
        return_value="redis://ready:6379/0",
    ), patch(
        "core.llm_evaluation_queue.Redis.from_url",
        return_value=redis_conn,
    ) as from_url, patch(
        "core.llm_evaluation_queue.Queue",
        return_value=queue,
    ) as queue_cls, patch(
        "core.llm_evaluation_queue.get_llm_evaluation_queue_status",
        return_value=status,
    ) as queue_status:
        result = llm_evaluation_queue.check_llm_evaluation_queue_readiness()

    from_url.assert_called_once_with("redis://ready:6379/0")
    redis_conn.ping.assert_called_once_with()
    queue_cls.assert_called_once_with(
        llm_evaluation_queue.LLM_EVALUATION_QUEUE,
        connection=redis_conn,
    )
    queue_status.assert_called_once_with(queue)
    assert result == {"ready": True, **status}


def test_claim_evaluation_rejects_invalid_id_without_db(caplog):
    with patch("core.llm_evaluation_queue.SessionLocal") as session_local:
        assert llm_evaluation_queue._claim_evaluation_for_execution("not-a-uuid") is False

    session_local.assert_not_called()
    assert "Skipping invalid LLM evaluation id" in caplog.text


def test_claim_evaluation_commits_rowcount_result():
    db = Mock()
    db.execute.return_value.rowcount = 1

    with patch("core.llm_evaluation_queue.SessionLocal", return_value=db):
        claimed = llm_evaluation_queue._claim_evaluation_for_execution(
            "11111111-1111-1111-1111-111111111111",
        )

    assert claimed is True
    db.execute.assert_called_once()
    db.commit.assert_called_once_with()
    db.rollback.assert_not_called()
    db.close.assert_called_once_with()


def test_claim_evaluation_rolls_back_on_db_error():
    db = Mock()
    db.execute.side_effect = RuntimeError("database unavailable")

    with patch("core.llm_evaluation_queue.SessionLocal", return_value=db):
        with pytest.raises(RuntimeError, match="database unavailable"):
            llm_evaluation_queue._claim_evaluation_for_execution(
                "11111111-1111-1111-1111-111111111111",
            )

    db.rollback.assert_called_once_with()
    db.close.assert_called_once_with()


def test_mark_worker_failure_rejects_invalid_id_without_db(caplog):
    with patch("core.llm_evaluation_queue.SessionLocal") as session_local:
        assert llm_evaluation_queue._mark_evaluation_worker_failure(None) is False

    session_local.assert_not_called()
    assert "Skipping invalid failed LLM evaluation id" in caplog.text


def test_mark_worker_failure_commits_retryable_failure_state():
    db = Mock()
    db.execute.return_value.rowcount = 0

    with patch("core.llm_evaluation_queue.SessionLocal", return_value=db):
        marked = llm_evaluation_queue._mark_evaluation_worker_failure(
            "22222222-2222-2222-2222-222222222222",
            error_code="provider_error",
        )

    assert marked is False
    db.execute.assert_called_once()
    db.commit.assert_called_once_with()
    db.rollback.assert_not_called()
    db.close.assert_called_once_with()


def test_mark_worker_failure_rolls_back_on_db_error():
    db = Mock()
    db.execute.side_effect = RuntimeError("update failed")

    with patch("core.llm_evaluation_queue.SessionLocal", return_value=db):
        with pytest.raises(RuntimeError, match="update failed"):
            llm_evaluation_queue._mark_evaluation_worker_failure(
                "22222222-2222-2222-2222-222222222222",
            )

    db.rollback.assert_called_once_with()
    db.close.assert_called_once_with()


def test_process_task_runs_provider_payload_path():
    db = Mock()
    evaluation = SimpleNamespace(id="eval-provider", status="succeeded", retryable=False)

    with patch(
        "core.llm_evaluation_queue._claim_evaluation_for_execution",
        return_value=True,
    ), patch("core.llm_evaluation_queue.SessionLocal", return_value=db), patch(
        "core.llm_evaluation_queue.MatchLlmEvaluationService",
    ) as service_cls:
        service_cls.return_value.run_pending_evaluation.return_value = evaluation

        result = llm_evaluation_queue.process_llm_evaluation_task(
            "eval-provider",
            provider_payload={"prompt": "payload"},
            truncation={"was_truncated": False},
        )

    assert result == "eval-provider"
    service_cls.return_value.run_pending_evaluation.assert_called_once_with(
        "eval-provider",
        {"prompt": "payload"},
        truncation={"was_truncated": False},
    )
    service_cls.return_value.resume_pending_evaluation.assert_not_called()
    db.rollback.assert_not_called()
    db.close.assert_called_once_with()


def test_process_task_logs_when_marking_worker_failure_also_fails(caplog):
    db = Mock()

    with patch(
        "core.llm_evaluation_queue._claim_evaluation_for_execution",
        return_value=True,
    ), patch("core.llm_evaluation_queue.SessionLocal", return_value=db), patch(
        "core.llm_evaluation_queue.MatchLlmEvaluationService",
    ) as service_cls, patch(
        "core.llm_evaluation_queue._mark_evaluation_worker_failure",
        side_effect=RuntimeError("cannot mark"),
    ):
        service_cls.return_value.resume_pending_evaluation.side_effect = RuntimeError(
            "provider failed",
        )

        with pytest.raises(RuntimeError, match="provider failed"):
            llm_evaluation_queue.process_llm_evaluation_task("eval-fail")

    assert "Failed to mark LLM evaluation eval-fail as retryable" in caplog.text
    db.rollback.assert_called_once_with()
    db.close.assert_called_once_with()


def test_is_retryable_failed_evaluation_requires_failed_retryable_status():
    assert (
        llm_evaluation_queue._is_retryable_failed_evaluation(
            SimpleNamespace(status=LLM_EVALUATION_FAILED, retryable=True),
        )
        is True
    )
    assert (
        llm_evaluation_queue._is_retryable_failed_evaluation(
            SimpleNamespace(status=LLM_EVALUATION_FAILED, retryable=False),
        )
        is False
    )
    assert (
        llm_evaluation_queue._is_retryable_failed_evaluation(
            SimpleNamespace(status="succeeded", retryable=True),
        )
        is False
    )


def test_enqueue_stale_or_retryable_evaluations_enqueues_each_row(caplog):
    db = Mock()
    execute_result = Mock()
    execute_result.scalars.return_value.all.return_value = [
        SimpleNamespace(id="eval-1"),
        SimpleNamespace(id="eval-2"),
    ]
    db.execute.return_value = execute_result

    with patch("core.llm_evaluation_queue.SessionLocal", return_value=db), patch(
        "core.llm_evaluation_queue.enqueue_llm_evaluation",
    ) as enqueue:
        count = llm_evaluation_queue.enqueue_stale_or_retryable_evaluations(
            stale_after_minutes=5,
            limit=2,
        )

    assert count == 2
    assert [call.args[0] for call in enqueue.call_args_list] == ["eval-1", "eval-2"]
    db.close.assert_called_once_with()
    assert "Enqueued 2 stale or retryable LLM evaluations" in caplog.text


def test_enqueue_stale_or_retryable_evaluations_handles_empty_result():
    db = Mock()
    execute_result = Mock()
    execute_result.scalars.return_value.all.return_value = []
    db.execute.return_value = execute_result

    with patch("core.llm_evaluation_queue.SessionLocal", return_value=db), patch(
        "core.llm_evaluation_queue.enqueue_llm_evaluation",
    ) as enqueue:
        count = llm_evaluation_queue.enqueue_stale_or_retryable_evaluations()

    assert count == 0
    enqueue.assert_not_called()
    db.close.assert_called_once_with()

def test_enqueue_stale_or_retryable_evaluations_paginates_large_backlog():
    first_created = datetime(2026, 7, 6, 1, tzinfo=timezone.utc)
    second_created = datetime(2026, 7, 6, 2, tzinfo=timezone.utc)
    rows_page_1 = [
        SimpleNamespace(id=uuid.uuid4(), created_at=first_created),
        SimpleNamespace(id=uuid.uuid4(), created_at=second_created),
    ]
    rows_page_2 = [
        SimpleNamespace(id=uuid.uuid4(), created_at=datetime(2026, 7, 6, 3, tzinfo=timezone.utc)),
    ]
    db1 = Mock()
    db2 = Mock()
    result1 = Mock()
    result1.scalars.return_value.all.return_value = rows_page_1
    result2 = Mock()
    result2.scalars.return_value.all.return_value = rows_page_2
    db1.execute.return_value = result1
    db2.execute.return_value = result2

    with patch("core.llm_evaluation_queue.SessionLocal", side_effect=[db1, db2]), patch(
        "core.llm_evaluation_queue.enqueue_llm_evaluation",
    ) as enqueue:
        count = llm_evaluation_queue.enqueue_stale_or_retryable_evaluations(
            limit=2,
            max_pages=2,
            enqueue_reason="resume_sweep",
        )

    assert count == 3
    assert enqueue.call_count == 3
    assert all(call.kwargs["enqueue_reason"] == "resume_sweep" for call in enqueue.call_args_list)
    db1.close.assert_called_once_with()
    db2.close.assert_called_once_with()

def test_schedule_recovery_sweep_reuses_active_job():
    queue = _FakeQueue(existing=_ExistingJob("scheduled"))

    result = llm_evaluation_queue.schedule_llm_recovery_sweep(queue=queue, delay_seconds=0)

    assert result == {
        "state": "reused",
        "job_id": llm_evaluation_queue.LLM_RECOVERY_SWEEP_JOB_ID,
    }
    assert queue.enqueued_in == []

def test_process_recovery_sweep_reschedules_after_enqueue():
    with patch(
        "core.llm_evaluation_queue.get_llm_evaluation_queue_pause_status",
        return_value={"paused": False},
    ), patch(
        "core.llm_evaluation_queue.enqueue_stale_or_retryable_evaluations",
        return_value=12,
    ) as sweep, patch(
        "core.llm_evaluation_queue.schedule_llm_recovery_sweep",
    ) as schedule:
        result = llm_evaluation_queue.process_llm_recovery_sweep_task()

    assert result == {"paused": False, "enqueued": 12}
    sweep.assert_called_once()
    schedule.assert_called_once()

def test_process_recovery_sweep_skips_backlog_when_paused_but_reschedules():
    with patch(
        "core.llm_evaluation_queue.get_llm_evaluation_queue_pause_status",
        return_value={"paused": True},
    ), patch(
        "core.llm_evaluation_queue.enqueue_stale_or_retryable_evaluations",
    ) as sweep, patch(
        "core.llm_evaluation_queue.schedule_llm_recovery_sweep",
    ) as schedule:
        result = llm_evaluation_queue.process_llm_recovery_sweep_task()

    assert result == {"paused": True, "enqueued": 0}
    sweep.assert_not_called()
    schedule.assert_called_once()
