"""Durable matching outcomes remain useful after the Redis projection expires."""

import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from core.matching_run_state import read_matching_state, record_matching_state
from database.models import PipelineRun
from web.backend.routers.pipeline import get_pipeline_status

OWNER = uuid.UUID("00000000-0000-0000-0000-000000000011")
TENANT = uuid.UUID("00000000-0000-0000-0000-000000000012")


def test_failed_run_keeps_safe_phase_and_identity_without_sql_body():
    db = MagicMock()
    db.execute.return_value.scalar_one_or_none.return_value = None
    with patch("core.matching_run_state.db_session_scope") as scope:
        scope.return_value.__enter__.return_value = db
        record_matching_state("task", {
            "owner_id": str(OWNER), "tenant_id": str(TENANT), "status": "failed",
            "step": "saving_results", "error": "SQL secret resume parameters",
            "stats": {"matches_saved": 0}, "private_body": "not status",
        })
    run = db.add.call_args.args[0]
    assert run.owner_id == OWNER and run.tenant_id == TENANT
    assert run.status == "failed" and run.completed_at is not None
    assert run.last_error == "saving_failed"
    assert run.metadata_json == {"step": "saving_results", "stats": {"matches_saved": 0}}
    assert run.failed_count == 1


@pytest.mark.parametrize("field", ["owner_id", "tenant_id", "run_type"])
def test_worker_cannot_reassign_existing_run_identity(field):
    run = PipelineRun(owner_id=OWNER, tenant_id=TENANT, run_type="matching", status="completed")
    setattr(run, field, "pipeline" if field == "run_type" else uuid.uuid4())
    db = MagicMock()
    db.execute.return_value.scalar_one_or_none.return_value = run
    with patch("core.matching_run_state.db_session_scope") as scope:
        scope.return_value.__enter__.return_value = db
        with pytest.raises(ValueError, match="identity mismatch"):
            record_matching_state("task", {
                "owner_id": str(OWNER), "tenant_id": str(TENANT), "status": "running",
            })
    assert run.status == "completed"


def test_durable_lookup_filters_owner_and_tenant_without_relying_on_rls():
    db = MagicMock()
    db.execute.return_value.scalar_one_or_none.return_value = None
    with patch("core.matching_run_state.db_session_scope") as scope:
        scope.return_value.__enter__.return_value = db
        assert read_matching_state("task", owner_id=OWNER, tenant_id=TENANT) is None
    statement = db.execute.call_args.args[0]
    assert set(statement.compile().params.values()) == {"task", "matching", OWNER, TENANT}


@pytest.mark.parametrize("status", ["running", "completed", "cancelled"])
def test_existing_run_keeps_identity_and_records_lifecycle(status):
    run = PipelineRun(owner_id=OWNER, tenant_id=TENANT, run_type="matching", status="running")
    db = MagicMock()
    db.execute.return_value.scalar_one_or_none.return_value = run
    with patch("core.matching_run_state.db_session_scope") as scope:
        scope.return_value.__enter__.return_value = db
        record_matching_state("task", {
            "owner_id": str(OWNER), "tenant_id": str(TENANT), "status": status,
            "step": "saving_results", "stats": {"matches_saved": 3},
        })
    assert run.status == status and run.last_error is None
    assert (run.completed_at is not None) is (status != "running")
    assert run.processed_count == 3
    assert run.succeeded_count == (3 if status == "completed" else 0)
    db.add.assert_not_called()


def test_read_status_returns_durable_outcome_and_not_mutable_redis_fields():
    now = datetime.now(timezone.utc)
    run = PipelineRun(owner_id=OWNER, tenant_id=TENANT, run_type="matching", status="failed",
        started_at=now, heartbeat_at=now, last_error="saving_failed", resume_fingerprint="resume",
        metadata_json={"step": "saving_results", "status": "completed", "owner_id": "other"})
    db = MagicMock()
    db.execute.return_value.scalar_one_or_none.return_value = run
    with patch("core.matching_run_state.db_session_scope") as scope:
        scope.return_value.__enter__.return_value = db
        state = read_matching_state("task", owner_id=OWNER, tenant_id=TENANT)
    assert state["status"] == "failed" and state["error"] == "saving_failed"
    assert state["owner_id"] == str(OWNER) and state["tenant_id"] == str(TENANT)
    assert state["started_at"] == now.isoformat()


@pytest.mark.parametrize("terminal", ["completed", "failed", "cancelled"])
@pytest.mark.parametrize("incoming", ["running", "completed", "failed"])
def test_late_updates_cannot_reopen_or_replace_terminal_outcomes(terminal, incoming):
    now = datetime.now(timezone.utc)
    run = PipelineRun(task_id="task", owner_id=OWNER, tenant_id=TENANT, run_type="matching",
        status=terminal, started_at=now, completed_at=now, heartbeat_at=now,
        metadata_json={"step": "saving_results", "stats": {"matches_saved": 5}})
    db = MagicMock()
    db.execute.return_value.scalar_one_or_none.return_value = run
    with patch("core.matching_run_state.db_session_scope") as scope:
        scope.return_value.__enter__.return_value = db
        projected = record_matching_state("task", {
            "owner_id": str(OWNER), "tenant_id": str(TENANT), "status": incoming,
            "step": "initializing", "stats": {},
        })
    assert run.status == terminal and run.completed_at == now
    assert projected["status"] == terminal and projected["stats"]["matches_saved"] == 5


def test_expired_redis_status_recovers_durable_failed_result():
    with patch("web.backend.routers.pipeline.get_task_state", return_value=None), \
         patch("web.backend.routers.pipeline.read_matching_state", return_value={
             "status": "failed", "step": "saving_results", "owner_id": str(OWNER),
             "tenant_id": str(TENANT), "stats": {"matches_saved": 0},
         }) as read:
        result = get_pipeline_status("task", user=SimpleNamespace(id=OWNER),
            request=SimpleNamespace(state=SimpleNamespace(tenant_id=TENANT)))
    read.assert_called_once_with("task", owner_id=OWNER, tenant_id=TENANT)
    assert result.status == "failed"
    assert result.failure.code == "saving_failed"


def test_live_status_cannot_cross_selected_tenant():
    with patch("web.backend.routers.pipeline.get_task_state", return_value={
        "status": "running", "owner_id": str(OWNER), "tenant_id": str(TENANT),
    }):
        result = get_pipeline_status("task", user=SimpleNamespace(id=OWNER),
            request=SimpleNamespace(state=SimpleNamespace(tenant_id=uuid.uuid4())))
    assert result.status_code == 404


def test_status_database_outage_is_retryable_not_a_missing_or_successful_run():
    with patch("web.backend.routers.pipeline.get_task_state", return_value=None), \
         patch("web.backend.routers.pipeline.read_matching_state", side_effect=RuntimeError("private")):
        result = get_pipeline_status("task", user=SimpleNamespace(id=OWNER))
    assert result.status_code == 503
    assert "private" not in json.dumps(json.loads(result.body))


@pytest.mark.asyncio
async def test_execution_lock_waits_and_releases_without_transaction():
    from core.matching_run_state import matching_execution_lock
    connection = MagicMock()
    connection.execute.return_value.scalar.side_effect = [False, True]
    with patch("core.matching_run_state.get_engine") as engine, \
         patch("core.matching_run_state.asyncio.sleep") as sleep:
        engine.return_value.connect.return_value.execution_options.return_value = connection
        async with matching_execution_lock("parent"):
            assert connection.execute.call_count == 2
        sleep.assert_awaited_once_with(0.25)
    assert "pg_advisory_unlock" in str(connection.execute.call_args.args[0])
    connection.close.assert_called_once()
    engine.return_value.connect.return_value.execution_options.assert_called_once_with(isolation_level="AUTOCOMMIT")


@pytest.mark.asyncio
async def test_execution_lock_discards_connection_if_unlock_fails():
    from core.matching_run_state import matching_execution_lock
    connection = MagicMock()
    connection.execute.side_effect = [MagicMock(scalar=lambda: True), RuntimeError("lost connection")]
    with patch("core.matching_run_state.get_engine") as engine:
        engine.return_value.connect.return_value.execution_options.return_value = connection
        with pytest.raises(RuntimeError, match="lost connection"):
            async with matching_execution_lock("parent"):
                pass
    connection.invalidate.assert_called_once()
    connection.close.assert_called_once()


@pytest.mark.asyncio
async def test_execution_lock_timeout_does_not_unlock_another_worker():
    from core.matching_run_state import matching_execution_lock
    connection = MagicMock()
    connection.execute.return_value.scalar.return_value = False
    with patch("core.matching_run_state.get_engine") as engine:
        engine.return_value.connect.return_value.execution_options.return_value = connection
        with pytest.raises(TimeoutError):
            async with matching_execution_lock("parent", timeout_seconds=0):
                pytest.fail("must not execute")
    connection.execute.assert_called_once()
    connection.close.assert_called_once()
