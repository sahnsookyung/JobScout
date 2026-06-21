from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from redis import Redis
from rq import Queue, Retry
from rq.registry import DeferredJobRegistry, FailedJobRegistry, ScheduledJobRegistry, StartedJobRegistry
from sqlalchemy import and_, or_, select, update

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
LLM_EVALUATION_RESULT_TTL_SECONDS = 24 * 60 * 60
LLM_EVALUATION_FAILURE_TTL_SECONDS = 7 * 24 * 60 * 60
_ACTIVE_RQ_STATUSES = {"queued", "started", "deferred", "scheduled"}
_DEFAULT_RETRY_INTERVALS_SECONDS = [60, 300, 900]


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


def _queue() -> Queue:
    return Queue(LLM_EVALUATION_QUEUE, connection=Redis.from_url(_redis_url()))


def _job_id(evaluation_id: Any) -> str:
    return f"llm-evaluation:{evaluation_id}"


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
) -> str:
    """Enqueue provider execution for a durable evaluation row."""
    return _enqueue_unique(
        _queue(),
        str(evaluation_id),
        provider_payload,
        truncation or {},
    )

def get_llm_evaluation_queue_status(queue: Queue | None = None) -> dict[str, int | str]:
    """Return bounded RQ registry depths for health checks and metrics."""
    q = queue or _queue()
    return {
        "queue": q.name,
        "queued": len(q),
        "started": len(StartedJobRegistry(queue=q)),
        "deferred": len(DeferredJobRegistry(queue=q)),
        "scheduled": len(ScheduledJobRegistry(queue=q)),
        "failed": len(FailedJobRegistry(queue=q)),
    }

def check_llm_evaluation_queue_readiness() -> dict[str, int | str | bool]:
    """Ping Redis and return queue status for compose/Kubernetes health checks."""
    redis_conn = Redis.from_url(_redis_url())
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


def enqueue_stale_or_retryable_evaluations(
    *,
    stale_after_minutes: int = 30,
    limit: int = 100,
) -> int:
    """Startup sweep for pending, stale-running, and retryable failed evaluations."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=stale_after_minutes)
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
            .order_by(LlmMatchEvaluation.created_at.asc())
            .limit(limit)
        )
        evaluations = list(db.execute(stmt).scalars().all())
    finally:
        db.close()

    enqueued = 0
    for evaluation in evaluations:
        enqueue_llm_evaluation(evaluation.id)
        enqueued += 1
    if enqueued:
        logger.info("Enqueued %d stale or retryable LLM evaluations", enqueued)
    return enqueued
