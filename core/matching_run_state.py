"""Durable, tenant-scoped matching status shared by the worker and web app."""

import asyncio
import hashlib
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Optional

from sqlalchemy import select, text

from database.database import db_session_scope, get_engine
from database.models import PipelineRun
from database.repositories.pipeline_run import PipelineRunRepository


@asynccontextmanager
async def matching_execution_lock(task_id: str, *, timeout_seconds: float = 900) -> AsyncIterator[None]:
    """Serialize a parent's pages without holding an idle database transaction.

    Session locks disappear if the worker dies. Polling remains cancellable, and
    a queued continuation waits instead of being mistaken for a duplicate.
    """
    key = int.from_bytes(hashlib.blake2b(
        f"jobscout:matching:{task_id}".encode(), digest_size=8,
    ).digest(), "big", signed=True)
    acquired = False
    connection = get_engine().connect().execution_options(isolation_level="AUTOCOMMIT")
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    try:
        while not acquired:
            acquired = bool(connection.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": key}).scalar())
            if acquired:
                break
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("Matching execution lock timed out")
            await asyncio.sleep(0.25)
        yield
    finally:
        try:
            if acquired:
                connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
        except Exception:
            # Never put a connection with a session lock back in the pool.
            connection.invalidate()
            raise
        finally:
            connection.close()


def matching_failure_code(step: str) -> str:
    """Keep SQL parameters and provider error bodies out of persisted status."""
    return {
        "saving_results": "saving_failed",
        "scoring": "scoring_failed",
        "notifying": "notification_failed",
    }.get(step, "matching_failed")


def _run_state(run: PipelineRun) -> dict[str, Any]:
    return {
        **(run.metadata_json or {}),
        "task_id": run.task_id, "task_type": "matching", "status": run.status,
        "owner_id": str(run.owner_id) if run.owner_id else None,
        "tenant_id": str(run.tenant_id) if run.tenant_id else None,
        "resume_fingerprint": run.resume_fingerprint,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "updated_at": run.heartbeat_at.isoformat() if run.heartbeat_at else None,
        "error": run.last_error,
    }


def record_matching_state(task_id: str, state: dict[str, Any]) -> dict[str, Any]:
    """Commit status before projecting it to Redis; fail closed if it cannot be saved."""
    owner_id = uuid.UUID(str(state["owner_id"])) if state.get("owner_id") else None
    tenant_id = uuid.UUID(str(state["tenant_id"])) if state.get("tenant_id") else None
    status = state["status"]
    durable_status = status if status in {"completed", "failed", "cancelled"} else "running"
    step = str(state.get("step") or "initializing")
    # Only this allowlist is recoverable through the public status endpoint.
    metadata = {key: state[key] for key in (
        "step", "task_type", "parent_task_id", "upload_id", "stats", "warnings", "result", "updated_at",
        "stale_due_to_newer_upload", "latest_upload_id", "latest_resume_fingerprint", "stale_message",
    ) if key in state}
    now = datetime.now(timezone.utc)
    with db_session_scope() as db:
        repo = PipelineRunRepository(db)
        run = db.execute(select(PipelineRun).where(PipelineRun.task_id == task_id).with_for_update()).scalar_one_or_none()
        if run is None:
            run = repo.create_run(
                task_id=task_id, run_type="matching", owner_id=owner_id, tenant_id=tenant_id,
                resume_fingerprint=state.get("resume_fingerprint"), current_stage="matching",
            )
        elif run.owner_id != owner_id or run.tenant_id != tenant_id or run.run_type != "matching":
            raise ValueError("Matching run identity mismatch")
        elif run.status in {"completed", "failed", "cancelled"}:
            # Redelivery or a late callback must not reopen a terminal run.
            return _run_state(run)
        run.status = durable_status
        run.current_stage = "matching"
        run.heartbeat_at = now
        run.completed_at = now if durable_status in {"completed", "failed", "cancelled"} else None
        run.last_error = matching_failure_code(step) if durable_status == "failed" else None
        run.retry_eligible = False
        run.metadata_json = metadata
        stats = state.get("stats") or {}
        run.processed_count = int(stats.get("matches_saved") or 0)
        run.succeeded_count = run.processed_count if durable_status == "completed" else 0
        run.failed_count = int(durable_status == "failed")
        return _run_state(run)


def read_matching_state(
    task_id: str, *, owner_id: Any, tenant_id: Optional[Any] = None,
) -> Optional[dict[str, Any]]:
    """Read only the caller's run, even in deployments without RLS."""
    owner_uuid = uuid.UUID(str(owner_id)) if owner_id else None
    tenant_uuid = uuid.UUID(str(tenant_id)) if tenant_id else None
    with db_session_scope() as db:
        run = db.execute(select(PipelineRun).where(
            PipelineRun.task_id == task_id,
            PipelineRun.run_type == "matching",
            PipelineRun.owner_id == owner_uuid,
            PipelineRun.tenant_id == tenant_uuid,
        )).scalar_one_or_none()
        if run is None:
            return None
        return _run_state(run)
