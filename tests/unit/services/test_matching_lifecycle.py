"""Matching pages must not claim parent success before publication finishes."""

from contextlib import ExitStack, nullcontext
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from services.scorer_matcher.main import MatcherConsumer, _maybe_enqueue_next_matching_page


@pytest.fixture(autouse=True)
def stub_execution_lock():
    with patch("services.scorer_matcher.main.matching_execution_lock", side_effect=lambda task: nullcontext()):
        yield


@pytest.mark.asyncio
@pytest.mark.parametrize("warning,expected", [
    ("matching_backlog_page_enqueued", "running"),
    ("matching_backlog_page_queued", "running"),
    ("matching_backlog_enqueue_failed", "failed"),
    ("matching_backlog_no_progress", "failed"),
])
async def test_parent_outcome_reflects_unfinished_backlog(warning, expected):
    result = SimpleNamespace(success=True, cancelled=False, error=None,
        matches_count=3, saved_count=3, notified_count=0, execution_time=1)
    with ExitStack() as stack:
        for name, value in {
            "_run_matching_pipeline_sync": result,
            "_job_preparation_stats": {"jobs_pending_matching": 5},
            "_maybe_enqueue_preparation_backfill": [],
            "_maybe_enqueue_next_matching_page": [{"code": warning}],
            "_compute_stale_result_metadata": {},
            "is_task_cancellation_requested": False,
        }.items():
            stack.enter_context(patch(f"services.scorer_matcher.main.{name}", return_value=value))
        durable = stack.enter_context(patch("services.scorer_matcher.main.record_matching_state", side_effect=lambda task, state: state))
        stack.enter_context(patch("services.scorer_matcher.main.read_matching_state", return_value=None))
        redis = stack.enter_context(patch("services.scorer_matcher.main.set_task_state"))
        clear = stack.enter_context(patch("services.scorer_matcher.main.clear_task_cancellation_requested"))
        ok, completion = await MatcherConsumer(Mock())._do_process("message", {
            "task_id": "parent", "resume_fingerprint": "resume", "tenant_id": "tenant",
        })
    assert durable.call_args.args[0] == "parent"
    assert durable.call_args.args[1]["status"] == expected
    assert redis.call_args.args[1]["status"] == expected
    assert redis.call_args.args[1]["tenant_id"] == "tenant"
    assert ok is (expected == "running")
    assert completion["status"] == ("completed" if expected == "running" else "failed")
    if expected == "running":
        clear.assert_not_called()


def test_page_carries_resume_and_existing_orchestrator_correlation():
    with patch("services.scorer_matcher.main.get_redis_client") as redis, \
         patch("services.scorer_matcher.main.enqueue_job") as enqueue:
        redis.return_value.set.return_value = True
        _maybe_enqueue_next_matching_page(
            parent_task_id="parent", current_page=1, resume_fingerprint="resume",
            owner_id="owner", tenant_id="tenant", upload_id="upload", pipeline_run_id="durable-parent",
            result=SimpleNamespace(success=True, cancelled=False, saved_count=1),
            stats={"jobs_pending_matching": 1},
        )
    payload = enqueue.call_args.args[1]
    assert payload["resume_upload_id"] == "upload"
    assert payload["pipeline_run_id"] == "durable-parent"
    assert payload["owner_id"] == "owner" and payload["tenant_id"] == "tenant"


@pytest.mark.parametrize("claimed", [None, b"parent-match-page-2"])
def test_replayed_page_reuses_only_existing_continuation_and_never_enqueues(claimed):
    with patch("services.scorer_matcher.main.get_redis_client") as redis, \
         patch("services.scorer_matcher.main.enqueue_job") as enqueue:
        redis.return_value.get.return_value = claimed
        warnings = _maybe_enqueue_next_matching_page(
            parent_task_id="parent", current_page=1, resume_fingerprint="resume",
            owner_id="owner", tenant_id="tenant",
            result=SimpleNamespace(success=True, cancelled=False, saved_count=0, replayed=True),
            stats={"jobs_pending_matching": 1},
        )
    assert warnings[0]["code"] == ("matching_backlog_page_queued" if claimed else "matching_backlog_no_progress")
    enqueue.assert_not_called()
    redis.return_value.set.assert_not_called()


def test_existing_orchestrator_keeps_ownership_of_its_durable_lifecycle():
    with patch("services.scorer_matcher.main.record_matching_state") as durable, \
         patch("services.scorer_matcher.main.set_task_state") as redis:
        MatcherConsumer._write_task_state("parent", {"status": "running"},
            warning_message="Projection failed %s", durable=False)
    durable.assert_not_called()
    redis.assert_called_once()


def test_durable_write_failure_prevents_success_projection():
    with patch("services.scorer_matcher.main.record_matching_state", side_effect=RuntimeError("DB down")), \
         patch("services.scorer_matcher.main.set_task_state") as redis:
        with pytest.raises(RuntimeError, match="DB down"):
            MatcherConsumer._write_task_state("parent", {"status": "completed"}, warning_message="Failed %s")
    redis.assert_not_called()


@pytest.mark.asyncio
async def test_completed_parent_is_reprojected_without_duplicate_matching():
    state = {"task_id": "parent", "status": "completed", "stats": {"matches_saved": 5}}
    with patch("services.scorer_matcher.main.read_matching_state", return_value=state), \
         patch("services.scorer_matcher.main._job_preparation_stats", return_value={}), \
         patch("services.scorer_matcher.main._maybe_enqueue_preparation_backfill", return_value=[]) as backfill, \
         patch("services.scorer_matcher.main._run_matching_pipeline_sync") as matching, \
         patch("services.scorer_matcher.main.set_task_state") as redis:
        ok, result = await MatcherConsumer(Mock())._do_process("message", {
            "task_id": "parent", "resume_fingerprint": "resume",
        })
    assert ok and result["status"] == "completed"
    assert redis.call_args.args[1] == state
    matching.assert_not_called()
    backfill.assert_not_called()
