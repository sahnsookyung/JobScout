from __future__ import annotations

import json
import logging
import math
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from redis import Redis
from rq import Queue, Retry
from rq.registry import DeferredJobRegistry, FailedJobRegistry, ScheduledJobRegistry, StartedJobRegistry
from sqlalchemy import and_, func, or_, select, update

from core.config_loader import load_config
from core.llm_evaluation import MatchLlmEvaluationService
from database.database import SessionLocal
from database.models import (
    LLM_EVALUATION_FAILED,
    LLM_EVALUATION_PENDING,
    LLM_EVALUATION_RUNNING,
    LlmMatchEvaluation,
)

logger = logging.getLogger(__name__)

LLM_EVALUATION_QUEUE = "llm_evaluations"
LLM_EVALUATION_JOB_TIMEOUT = "15m"
LLM_TOP_N_SCHEDULER_JOB_TIMEOUT = "5m"
LLM_RECOVERY_SWEEP_JOB_TIMEOUT = "5m"
LLM_EVALUATION_QUEUE_PAUSE_KEY = "llm-evaluations:queue-paused"
LLM_RECOVERY_SWEEP_JOB_ID = "llm-evaluation-recovery-sweep"
LLM_EVALUATION_RESULT_TTL_SECONDS = 24 * 60 * 60
LLM_EVALUATION_FAILURE_TTL_SECONDS = 7 * 24 * 60 * 60
_ACTIVE_RQ_STATUSES = {"queued", "started", "deferred", "scheduled"}
_DEFAULT_RETRY_INTERVALS_SECONDS = [60, 300, 900]
_ZERO_LLM_ENQUEUE_STATS = {
    "attempted": 0,
    "reused": 0,
    "created": 0,
    "enqueued": 0,
    "failed": 0,
}


class RetryableLlmEvaluationError(RuntimeError):
    """Raised after a retryable evaluation failure so RQ applies backoff."""


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using %s", name, raw, default)
        return default
    return max(value, 0)

def _env_int_list(name: str, default: list[int]) -> list[int]:
    raw = os.getenv(name)
    if not raw:
        return list(default)
    values: list[int] = []
    for part in raw.split(","):
        try:
            values.append(max(int(part.strip()), 0))
        except ValueError:
            logger.warning("Invalid %s=%r; using %s", name, raw, default)
            return list(default)
    return values or list(default)

def _retry_policy() -> Retry:
    return Retry(
        max=_env_int("LLM_EVALUATION_RETRY_MAX", 3),
        interval=_env_int_list(
            "LLM_EVALUATION_RETRY_INTERVALS_SECONDS",
            _DEFAULT_RETRY_INTERVALS_SECONDS,
        ),
    )


def _redis_url() -> str:
    return load_config().orchestrator.redis_url


def _redis_conn() -> Redis:
    return Redis.from_url(_redis_url())


def _queue() -> Queue:
    return Queue(LLM_EVALUATION_QUEUE, connection=_redis_conn())


def _job_id(evaluation_id: Any) -> str:
    return f"llm-evaluation-{_job_component(evaluation_id)}"


def _paused_job_id(prefix: str, stable_id: str) -> str:
    return f"{_job_component(prefix)}-{_job_component(stable_id)}-{int(time.time())}"


def _job_component(value: Any) -> str:
    text = str(value or "none").strip().lower()
    safe = "".join(character if character.isalnum() else "-" for character in text)
    return safe.strip("-")[:80] or "none"


def _top_n_scheduler_job_id(
    *,
    selection_run_id: Any,
    owner_id: Any,
    tenant_id: Any | None,
    policy_revision: Any,
    top_n: int,
) -> str:
    return (
        "llm-top-n-"
        f"{_job_component(owner_id)}-"
        f"{_job_component(tenant_id)}-"
        f"{_job_component(selection_run_id)}-"
        f"r{_job_component(policy_revision)}-"
        f"n{max(int(top_n), 0)}"
    )


def _enqueue_unique(
    queue: Queue,
    evaluation_id: str,
    provider_payload: dict[str, Any] | None,
    truncation: dict[str, Any],
) -> str:
    job_id = _job_id(evaluation_id)
    existing = queue.fetch_job(job_id)
    if existing is not None:
        raw_status = existing.get_status(refresh=True)
        status = getattr(raw_status, "value", raw_status)
        if status in _ACTIVE_RQ_STATUSES:
            return job_id
        existing.delete()

    job = queue.enqueue(
        process_llm_evaluation_task,
        evaluation_id,
        provider_payload,
        truncation,
        retry=_retry_policy(),
        job_timeout=LLM_EVALUATION_JOB_TIMEOUT,
        result_ttl=LLM_EVALUATION_RESULT_TTL_SECONDS,
        failure_ttl=LLM_EVALUATION_FAILURE_TTL_SECONDS,
        job_id=job_id,
    )
    return str(job.id)

def _update_evaluation_queue_metadata(
    evaluation_id: Any,
    *,
    enqueue_reason: str | None = None,
    queue_job_id: str | None = None,
    queue_state: str | None = None,
    next_retry_at: datetime | None = None,
    retry_after_seconds: int | float | None = None,
    provider_status_message: str | None = None,
) -> None:
    try:
        lookup_id = uuid.UUID(str(evaluation_id))
    except (TypeError, ValueError):
        return

    updates = {
        "enqueue_reason": enqueue_reason,
        "queue_job_id": queue_job_id,
        "queue_state": queue_state,
        "next_retry_at": next_retry_at.isoformat() if next_retry_at else None,
        "retry_after_seconds": retry_after_seconds,
        "provider_status_message": provider_status_message,
        "queued_at": datetime.now(timezone.utc).isoformat() if queue_job_id else None,
    }
    updates = {key: value for key, value in updates.items() if value is not None}
    if not updates:
        return

    db = SessionLocal()
    try:
        evaluation = db.get(LlmMatchEvaluation, lookup_id)
        if evaluation is None or evaluation.deleted_at is not None:
            return
        analysis = evaluation.analysis if isinstance(evaluation.analysis, dict) else {}
        queue_metadata = analysis.get("queue")
        if not isinstance(queue_metadata, dict):
            queue_metadata = {}
        queue_metadata.update(updates)
        analysis["queue"] = queue_metadata
        if enqueue_reason is not None:
            analysis["enqueue_reason"] = enqueue_reason
        if queue_job_id is not None:
            analysis["queue_job_id"] = queue_job_id
        evaluation.analysis = analysis
        db.commit()
    except Exception:
        db.rollback()
        logger.warning(
            "Could not update LLM evaluation queue metadata for %s",
            evaluation_id,
            exc_info=True,
        )
    finally:
        db.close()


def _enqueue_top_n_scheduler_unique(
    queue: Queue,
    *,
    selection_run_id: Any,
    owner_id: Any,
    tenant_id: Any | None,
    top_n: int,
    policy_revision: int,
) -> dict[str, Any]:
    job_id = _top_n_scheduler_job_id(
        selection_run_id=selection_run_id,
        owner_id=owner_id,
        tenant_id=tenant_id,
        policy_revision=policy_revision,
        top_n=top_n,
    )
    existing = queue.fetch_job(job_id)
    if existing is not None:
        raw_status = existing.get_status(refresh=True)
        status = getattr(raw_status, "value", raw_status)
        if status in _ACTIVE_RQ_STATUSES:
            try:
                from core.metrics import record_llm_judge_scheduler_job

                record_llm_judge_scheduler_job("reused")
            except Exception:
                pass
            return {"state": "reused", "job_id": job_id}
        existing.delete()

    job = queue.enqueue(
        process_llm_top_n_selection_task,
        str(selection_run_id),
        str(owner_id),
        str(tenant_id) if tenant_id is not None else None,
        int(top_n),
        int(policy_revision),
        retry=_retry_policy(),
        job_timeout=LLM_TOP_N_SCHEDULER_JOB_TIMEOUT,
        result_ttl=LLM_EVALUATION_RESULT_TTL_SECONDS,
        failure_ttl=LLM_EVALUATION_FAILURE_TTL_SECONDS,
        job_id=job_id,
    )
    try:
        from core.metrics import record_llm_judge_scheduler_job

        record_llm_judge_scheduler_job("scheduled")
    except Exception:
        pass
    return {"state": "scheduled", "job_id": str(job.id)}


def _queue_pause_defer_seconds() -> int:
    return max(_env_int("LLM_EVALUATION_QUEUE_PAUSE_DEFER_SECONDS", 300), 30)


def get_llm_evaluation_queue_pause_status(
    redis_conn: Redis | None = None,
) -> dict[str, Any]:
    """Return application-owned pause metadata for the LLM queue."""
    client = redis_conn or _redis_conn()
    try:
        raw = client.get(LLM_EVALUATION_QUEUE_PAUSE_KEY)
        if raw is None:
            return {"paused": False, "pause_reason": None, "pause_ttl_seconds": None}
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            payload = json.loads(str(raw))
        except json.JSONDecodeError:
            payload = {"reason": str(raw)}
        ttl = client.ttl(LLM_EVALUATION_QUEUE_PAUSE_KEY)
        return {
            "paused": True,
            "pause_reason": str(payload.get("reason") or "manual"),
            "pause_ttl_seconds": None if ttl is None or int(ttl) < 0 else int(ttl),
        }
    except Exception:
        logger.warning("Could not inspect LLM evaluation queue pause state", exc_info=True)
        return {"paused": False, "pause_reason": None, "pause_ttl_seconds": None}


def set_llm_evaluation_queue_paused(
    *,
    reason: str | None = None,
    ttl_seconds: int | None = None,
) -> dict[str, Any]:
    """Pause LLM queue execution at the application worker boundary."""
    client = _redis_conn()
    payload = json.dumps(
        {
            "reason": (reason or "manual").strip()[:200] or "manual",
            "paused_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    if ttl_seconds is not None and int(ttl_seconds) > 0:
        client.setex(LLM_EVALUATION_QUEUE_PAUSE_KEY, int(ttl_seconds), payload)
    else:
        client.set(LLM_EVALUATION_QUEUE_PAUSE_KEY, payload)
    return get_llm_evaluation_queue_pause_status(client)


def resume_llm_evaluation_queue() -> dict[str, Any]:
    """Resume LLM queue execution by clearing the pause flag."""
    client = _redis_conn()
    client.delete(LLM_EVALUATION_QUEUE_PAUSE_KEY)
    return get_llm_evaluation_queue_pause_status(client)


def _schedule_paused_evaluation_retry(
    evaluation_id: str,
    provider_payload: dict[str, Any] | None,
    truncation: dict[str, Any] | None,
) -> str:
    defer_seconds = _queue_pause_defer_seconds()
    next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=defer_seconds)
    job = _queue().enqueue_in(
        timedelta(seconds=defer_seconds),
        process_llm_evaluation_task,
        evaluation_id,
        provider_payload,
        truncation or {},
        retry=_retry_policy(),
        job_timeout=LLM_EVALUATION_JOB_TIMEOUT,
        result_ttl=LLM_EVALUATION_RESULT_TTL_SECONDS,
        failure_ttl=LLM_EVALUATION_FAILURE_TTL_SECONDS,
        job_id=_paused_job_id("llm-evaluation-paused", evaluation_id),
    )
    logger.info(
        "Deferred LLM evaluation %s for %ss because queue is paused",
        evaluation_id,
        defer_seconds,
    )
    _update_evaluation_queue_metadata(
        evaluation_id,
        queue_job_id=str(job.id),
        queue_state="deferred",
        next_retry_at=next_retry_at,
        retry_after_seconds=defer_seconds,
        provider_status_message="LLM queue is paused; retry is deferred.",
    )
    return str(job.id)


def _schedule_paused_top_n_retry(
    *,
    selection_run_id: str,
    owner_id: str,
    tenant_id: str | None,
    top_n: int,
    policy_revision: int,
) -> str:
    defer_seconds = _queue_pause_defer_seconds()
    stable_id = _top_n_scheduler_job_id(
        selection_run_id=selection_run_id,
        owner_id=owner_id,
        tenant_id=tenant_id,
        policy_revision=policy_revision,
        top_n=top_n,
    )
    job = _queue().enqueue_in(
        timedelta(seconds=defer_seconds),
        process_llm_top_n_selection_task,
        selection_run_id,
        owner_id,
        tenant_id,
        int(top_n),
        int(policy_revision),
        retry=_retry_policy(),
        job_timeout=LLM_TOP_N_SCHEDULER_JOB_TIMEOUT,
        result_ttl=LLM_EVALUATION_RESULT_TTL_SECONDS,
        failure_ttl=LLM_EVALUATION_FAILURE_TTL_SECONDS,
        job_id=_paused_job_id("llm-top-n-paused", stable_id),
    )
    logger.info(
        "Deferred LLM top-N scheduler %s for %ss because queue is paused",
        selection_run_id,
        defer_seconds,
    )
    return str(job.id)


def _claim_evaluation_for_execution(
    evaluation_id: str,
    *,
    stale_after_minutes: int = 30,
) -> bool:
    try:
        lookup_id = uuid.UUID(str(evaluation_id))
    except (TypeError, ValueError):
        logger.warning("Skipping invalid LLM evaluation id %s", evaluation_id)
        return False

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=stale_after_minutes)
    now = datetime.now(timezone.utc)
    db = SessionLocal()
    try:
        stmt = (
            update(LlmMatchEvaluation)
            .where(
                LlmMatchEvaluation.id == lookup_id,
                LlmMatchEvaluation.deleted_at.is_(None),
                or_(
                    LlmMatchEvaluation.status == LLM_EVALUATION_PENDING,
                    and_(
                        LlmMatchEvaluation.status == LLM_EVALUATION_RUNNING,
                        or_(
                            LlmMatchEvaluation.started_at.is_(None),
                            LlmMatchEvaluation.started_at <= cutoff,
                        ),
                    ),
                    and_(
                        LlmMatchEvaluation.status == LLM_EVALUATION_FAILED,
                        LlmMatchEvaluation.retryable.is_(True),
                    ),
                ),
            )
            .values(
                status=LLM_EVALUATION_RUNNING,
                retryable=False,
                error_code=None,
                started_at=now,
                completed_at=None,
            )
        )
        result = db.execute(stmt)
        db.commit()
        return bool(result.rowcount)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _mark_evaluation_worker_failure(
    evaluation_id: str,
    *,
    error_code: str = "worker_error",
) -> bool:
    try:
        lookup_id = uuid.UUID(str(evaluation_id))
    except (TypeError, ValueError):
        logger.warning("Skipping invalid failed LLM evaluation id %s", evaluation_id)
        return False

    db = SessionLocal()
    try:
        result = db.execute(
            update(LlmMatchEvaluation)
            .where(
                LlmMatchEvaluation.id == lookup_id,
                LlmMatchEvaluation.deleted_at.is_(None),
                LlmMatchEvaluation.status == LLM_EVALUATION_RUNNING,
            )
            .values(
                status=LLM_EVALUATION_FAILED,
                retryable=True,
                error_code=error_code,
                completed_at=datetime.now(timezone.utc),
            )
        )
        db.commit()
        return bool(result.rowcount)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def enqueue_llm_evaluation(
    evaluation_id: Any,
    *,
    provider_payload: dict[str, Any] | None = None,
    truncation: dict[str, Any] | None = None,
    enqueue_reason: str | None = None,
) -> str:
    """Enqueue provider execution for a durable evaluation row."""
    job_id = _enqueue_unique(
        _queue(),
        str(evaluation_id),
        provider_payload,
        truncation or {},
    )
    _update_evaluation_queue_metadata(
        evaluation_id,
        enqueue_reason=enqueue_reason,
        queue_job_id=job_id,
        queue_state="queued",
    )
    return job_id


def enqueue_llm_top_n_for_selection(
    *,
    selection_run_id: Any,
    owner_id: Any,
    tenant_id: Any | None = None,
    top_n: int,
    policy_revision: int = 0,
) -> dict[str, Any]:
    """Schedule top-N evaluation row creation for a selection run."""
    if not selection_run_id or not owner_id or int(top_n) <= 0:
        return {"state": "skipped", "job_id": None}
    return _enqueue_top_n_scheduler_unique(
        _queue(),
        selection_run_id=selection_run_id,
        owner_id=owner_id,
        tenant_id=tenant_id,
        top_n=int(top_n),
        policy_revision=int(policy_revision),
    )


def _age_seconds(value: datetime | None) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return max(int((datetime.now(timezone.utc) - value).total_seconds()), 0)


def _first_configured_provider_rpm() -> int | None:
    try:
        from core.llm.provider_chain import configured_provider_entries

        config = load_config()
        runtime = config.matching.llm_judge.runtime
        for entry in configured_provider_entries(runtime):
            rpm = getattr(entry, "requests_per_minute", None)
            if rpm is not None and int(rpm) > 0:
                return int(rpm)
    except Exception:
        logger.debug("Could not inspect LLM provider RPM for drain estimate", exc_info=True)
    return None


def _db_backlog_status() -> dict[str, int | None]:
    db = SessionLocal()
    try:
        rows = db.execute(
            select(LlmMatchEvaluation.status, func.count(LlmMatchEvaluation.id))
            .where(LlmMatchEvaluation.deleted_at.is_(None))
            .group_by(LlmMatchEvaluation.status)
        ).all()
        counts = {str(status): int(count or 0) for status, count in rows}
        retryable_failed = int(
            db.scalar(
                select(func.count(LlmMatchEvaluation.id)).where(
                    LlmMatchEvaluation.deleted_at.is_(None),
                    LlmMatchEvaluation.status == LLM_EVALUATION_FAILED,
                    LlmMatchEvaluation.retryable.is_(True),
                )
            )
            or 0
        )
        oldest_pending = db.scalar(
            select(func.min(LlmMatchEvaluation.created_at)).where(
                LlmMatchEvaluation.deleted_at.is_(None),
                LlmMatchEvaluation.status == LLM_EVALUATION_PENDING,
            )
        )
        oldest_retryable_failed = db.scalar(
            select(func.min(LlmMatchEvaluation.created_at)).where(
                LlmMatchEvaluation.deleted_at.is_(None),
                LlmMatchEvaluation.status == LLM_EVALUATION_FAILED,
                LlmMatchEvaluation.retryable.is_(True),
            )
        )
    finally:
        db.close()

    pending = int(counts.get(LLM_EVALUATION_PENDING, 0))
    retryable_backlog = int(retryable_failed)
    rpm = _first_configured_provider_rpm()
    total_backlog = pending + retryable_backlog
    if total_backlog <= 0:
        drain_estimate = 0
    elif rpm:
        drain_estimate = int(math.ceil((total_backlog * 60) / rpm))
    else:
        drain_estimate = None
    stats = {
        "db_pending": pending,
        "db_running": int(counts.get(LLM_EVALUATION_RUNNING, 0)),
        "db_failed": int(counts.get(LLM_EVALUATION_FAILED, 0)),
        "db_retryable_failed": retryable_backlog,
        "oldest_pending_age_seconds": _age_seconds(oldest_pending),
        "oldest_retryable_failed_age_seconds": _age_seconds(oldest_retryable_failed),
        "drain_estimate_seconds": drain_estimate,
    }
    try:
        from core.metrics import set_llm_evaluation_backlog_metrics

        set_llm_evaluation_backlog_metrics(stats)
    except Exception:
        pass
    return stats


def get_llm_evaluation_queue_status(queue: Queue | None = None) -> dict[str, int | str | None]:
    """Return bounded RQ registry depths for health checks and metrics."""
    q = queue or _queue()
    status = {
        "queue": q.name,
        "queued": len(q),
        "started": len(StartedJobRegistry(queue=q)),
        "deferred": len(DeferredJobRegistry(queue=q)),
        "scheduled": len(ScheduledJobRegistry(queue=q)),
        "failed": len(FailedJobRegistry(queue=q)),
    }
    try:
        status.update(_db_backlog_status())
    except Exception:
        logger.warning("Could not inspect LLM evaluation database backlog", exc_info=True)
    try:
        status.update(get_llm_evaluation_queue_pause_status(q.connection))
    except Exception:
        logger.warning("Could not inspect LLM evaluation pause state", exc_info=True)
    return status

def check_llm_evaluation_queue_readiness() -> dict[str, int | str | bool]:
    """Ping Redis and return queue status for compose/Kubernetes health checks."""
    redis_conn = _redis_conn()
    redis_conn.ping()
    status = get_llm_evaluation_queue_status(
        Queue(LLM_EVALUATION_QUEUE, connection=redis_conn),
    )
    return {"ready": True, **status}


def _is_retryable_failed_evaluation(evaluation: Any) -> bool:
    return (
        getattr(evaluation, "status", None) == LLM_EVALUATION_FAILED
        and bool(getattr(evaluation, "retryable", False))
    )


def process_llm_evaluation_task(
    evaluation_id: str,
    provider_payload: dict[str, Any] | None = None,
    truncation: dict[str, Any] | None = None,
) -> str:
    """RQ task entrypoint for match-level LLM judge execution."""
    pause_status = get_llm_evaluation_queue_pause_status()
    if pause_status.get("paused"):
        _schedule_paused_evaluation_retry(evaluation_id, provider_payload, truncation)
        return str(evaluation_id)

    if not _claim_evaluation_for_execution(evaluation_id):
        logger.info("Skipping LLM evaluation %s because it is already claimed or terminal", evaluation_id)
        return str(evaluation_id)

    db = SessionLocal()
    try:
        service = MatchLlmEvaluationService(db)
        if provider_payload is not None:
            evaluation = service.run_pending_evaluation(
                evaluation_id,
                provider_payload,
                truncation=truncation or {},
            )
        else:
            evaluation = service.resume_pending_evaluation(evaluation_id)
        if _is_retryable_failed_evaluation(evaluation):
            error_code = getattr(evaluation, "error_code", None) or "retryable_failure"
            raise RetryableLlmEvaluationError(
                f"LLM evaluation {evaluation_id} failed retryably: {error_code}"
            )
        return str(getattr(evaluation, "id", evaluation_id))
    except RetryableLlmEvaluationError:
        logger.warning("LLM evaluation %s failed retryably; RQ will retry", evaluation_id)
        raise
    except Exception:
        db.rollback()
        logger.exception("LLM evaluation worker failed for %s", evaluation_id)
        try:
            _mark_evaluation_worker_failure(evaluation_id, error_code="worker_error")
        except Exception:
            logger.warning(
                "Failed to mark LLM evaluation %s as retryable after worker error",
                evaluation_id,
                exc_info=True,
            )
        raise
    finally:
        db.close()


def process_llm_top_n_selection_task(
    selection_run_id: str,
    owner_id: str,
    tenant_id: str | None,
    top_n: int,
    policy_revision: int = 0,
) -> dict[str, int]:
    """RQ task entrypoint that creates/enqueues per-match LLM evaluations."""
    pause_status = get_llm_evaluation_queue_pause_status()
    if pause_status.get("paused"):
        _schedule_paused_top_n_retry(
            selection_run_id=selection_run_id,
            owner_id=owner_id,
            tenant_id=tenant_id,
            top_n=int(top_n),
            policy_revision=int(policy_revision),
        )
        return dict(_ZERO_LLM_ENQUEUE_STATS)

    db = SessionLocal()
    try:
        stats = MatchLlmEvaluationService(db).evaluate_selection_run(
            selection_run_id,
            owner_id=owner_id,
            tenant_id=tenant_id,
            top_n=int(top_n),
        )
        try:
            from core.metrics import record_llm_judge_scheduler_job

            record_llm_judge_scheduler_job("succeeded")
        except Exception:
            pass
        logger.info(
            "LLM top-N scheduler completed for selection %s revision %s: %s",
            selection_run_id,
            policy_revision,
            stats,
        )
        return {
            **_ZERO_LLM_ENQUEUE_STATS,
            **{key: int(value or 0) for key, value in stats.items()},
        }
    except Exception:
        db.rollback()
        try:
            from core.metrics import record_llm_judge_scheduler_job

            record_llm_judge_scheduler_job("failed")
        except Exception:
            pass
        logger.exception("LLM top-N scheduler failed for selection %s", selection_run_id)
        raise
    finally:
        db.close()


def enqueue_stale_or_retryable_evaluations(
    *,
    stale_after_minutes: int = 30,
    limit: int = 100,
    max_pages: int = 1,
    enqueue_reason: str = "resume_sweep",
) -> int:
    """Paginated sweep for pending, stale-running, and retryable failed evaluations."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=stale_after_minutes)
    enqueued = 0
    page_size = max(int(limit), 1)
    page_count = max(int(max_pages), 1)
    last_created_at: datetime | None = None
    last_id: Any | None = None

    for _page in range(page_count):
        db = SessionLocal()
        try:
            stmt = (
                select(LlmMatchEvaluation)
                .where(
                    LlmMatchEvaluation.deleted_at.is_(None),
                    or_(
                        LlmMatchEvaluation.status == LLM_EVALUATION_PENDING,
                        and_(
                            LlmMatchEvaluation.status == LLM_EVALUATION_RUNNING,
                            or_(
                                LlmMatchEvaluation.started_at.is_(None),
                                LlmMatchEvaluation.started_at <= cutoff,
                            ),
                        ),
                        and_(
                            LlmMatchEvaluation.status == LLM_EVALUATION_FAILED,
                            LlmMatchEvaluation.retryable.is_(True),
                        ),
                    ),
                )
                .order_by(LlmMatchEvaluation.created_at.asc(), LlmMatchEvaluation.id.asc())
                .limit(page_size)
            )
            if last_created_at is not None:
                stmt = stmt.where(
                    or_(
                        LlmMatchEvaluation.created_at > last_created_at,
                        and_(
                            LlmMatchEvaluation.created_at == last_created_at,
                            LlmMatchEvaluation.id > last_id,
                        ),
                    )
                )
            evaluations = list(db.execute(stmt).scalars().all())
        finally:
            db.close()

        if not evaluations:
            break
        for evaluation in evaluations:
            enqueue_llm_evaluation(evaluation.id, enqueue_reason=enqueue_reason)
            enqueued += 1
        last = evaluations[-1]
        last_created_at = getattr(last, "created_at", None)
        last_id = getattr(last, "id", None)
        if len(evaluations) < page_size or last_created_at is None or last_id is None:
            break
    if enqueued:
        logger.info("Enqueued %d stale or retryable LLM evaluations", enqueued)
    return enqueued

def _recovery_sweep_interval_seconds() -> int:
    return max(_env_int("LLM_EVALUATION_SWEEP_INTERVAL_SECONDS", 300), 30)

def _recovery_sweep_page_size() -> int:
    return max(_env_int("LLM_EVALUATION_SWEEP_PAGE_SIZE", 100), 1)

def _recovery_sweep_max_pages() -> int:
    return max(_env_int("LLM_EVALUATION_SWEEP_MAX_PAGES", 10), 1)

def schedule_llm_recovery_sweep(
    *,
    delay_seconds: int | None = None,
    queue: Queue | None = None,
) -> dict[str, Any]:
    """Ensure a periodic recovery sweep exists for durable LLM evaluation rows."""
    q = queue or _queue()
    existing = q.fetch_job(LLM_RECOVERY_SWEEP_JOB_ID)
    if existing is not None:
        raw_status = existing.get_status(refresh=True)
        status = getattr(raw_status, "value", raw_status)
        if status in _ACTIVE_RQ_STATUSES:
            return {"state": "reused", "job_id": LLM_RECOVERY_SWEEP_JOB_ID}
        existing.delete()
    delay = _recovery_sweep_interval_seconds() if delay_seconds is None else max(int(delay_seconds), 0)
    job = q.enqueue_in(
        timedelta(seconds=delay),
        process_llm_recovery_sweep_task,
        retry=_retry_policy(),
        job_timeout=LLM_RECOVERY_SWEEP_JOB_TIMEOUT,
        result_ttl=LLM_EVALUATION_RESULT_TTL_SECONDS,
        failure_ttl=LLM_EVALUATION_FAILURE_TTL_SECONDS,
        job_id=LLM_RECOVERY_SWEEP_JOB_ID,
    )
    return {"state": "scheduled", "job_id": str(job.id)}

def process_llm_recovery_sweep_task() -> dict[str, int | bool]:
    """RQ task that periodically recovers pending/stale/retryable LLM evaluation rows."""
    paused = bool(get_llm_evaluation_queue_pause_status().get("paused"))
    enqueued = 0
    try:
        if not paused:
            enqueued = enqueue_stale_or_retryable_evaluations(
                limit=_recovery_sweep_page_size(),
                max_pages=_recovery_sweep_max_pages(),
                enqueue_reason="resume_sweep",
            )
        return {"paused": paused, "enqueued": enqueued}
    finally:
        try:
            schedule_llm_recovery_sweep()
        except Exception:
            logger.warning("Could not schedule next LLM recovery sweep", exc_info=True)
