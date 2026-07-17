"""Match-level LLM evaluation cache and judge service."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from core.config_loader import load_config
from core.ephemeral_quota import (
    EphemeralQuotaExceeded,
    EphemeralQuotaUnavailable,
    consume_ephemeral_quota,
    public_testing_quotas_enabled,
)
from core.llm.interfaces import LLMProvider
from core.llm.provider_chain import (
    build_match_judge_provider,
    classify_llm_provider_error,
    configured_provider_entries,
    llm_error_is_retryable,
    sanitized_provider_config,
)
from core.llm.provider_factory import build_llm_provider, runtime_llm_config_from_match_judge
from core.redis_streams import _sanitize_log
from core.resume_evidence_selection import select_relevant_resume_evidence_units
from database.models import (
    LLM_EVALUATION_DELETED,
    LLM_EVALUATION_FAILED,
    LLM_EVALUATION_PENDING,
    LLM_EVALUATION_RUNNING,
    LLM_EVALUATION_SUCCEEDED,
    JobMatch,
    JobMatchRequirement,
    JobPost,
    JobRequirementUnit,
    LlmMatchEvaluation,
    ResumeEvidenceUnitEmbedding,
    StructuredResume,
)

logger = logging.getLogger(__name__)

MATCH_LLM_JUDGE_SCHEMA_NAME = "match_llm_judge_v2"
MATCH_LLM_JUDGE_SYSTEM_PROMPT = """
You are a careful resume-to-job relevance judge.

Task
- Independently compare the packed job description and extracted requirements
  against the full structured resume payload and owner-scoped resume evidence
  units.
- Recognize transferable evidence when technologies are closely related
  (for example Java and Kotlin), but explain the transfer rather than inventing
  direct experience.
- Treat a direct mention in either the full structured resume payload or the
  resume evidence units as explicit resume evidence.
- Do not invent experience, credentials, salary, location, or authorization facts.
- Prefer concise, user-safe explanations.
- Score must use 0-100 percentage points. Return 92 for a strong 92% match;
  do not return normalized 0-1 or 0-10 values such as 0.92 or 9.2.
- Return requirement_verdicts in the same order as the input requirement IDs
  (req_1, req_2, req_3, ...).

Return structured JSON only.
""".strip()


class RequirementEvaluation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requirement_id: str
    verdict: str = Field(pattern="^(strong|partial|missing|not_applicable)$")
    reason: str


class MatchEvaluationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    score: float = Field(ge=0.0, le=100.0)
    confidence: float = Field(ge=0.0, le=1.0)
    verdict: str = Field(pattern="^(strong|good|borderline|weak|mismatch)$")
    summary: str
    reason_codes: list[str] = Field(default_factory=list)
    requirement_verdicts: list[RequirementEvaluation] = Field(default_factory=list)
    transferable_strengths: list[str] = Field(default_factory=list)
    gaps: list[str] = Field(default_factory=list)
    ranking_rationale: str = ""


MATCH_LLM_JUDGE_SCHEMA_SPEC = {
    "name": MATCH_LLM_JUDGE_SCHEMA_NAME,
    "strict": True,
    "schema": MatchEvaluationResponse.model_json_schema(),
}

INVALID_LLM_SCORE_REASON = "invalid_llm_score"
MISSING_LLM_SCORE_REASON = "missing_llm_score"
EVIDENCE_REFERENCE_PATTERN = re.compile(r"\bev[_-](\d+)\b", re.IGNORECASE)


def normalize_llm_score(score: Any, verdict: str | None = None) -> float | None:
    """Return a structurally valid 0-100 LLM score without verdict-based inference."""
    _ = verdict
    if score is None:
        return None
    try:
        value = float(score)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    if value < 0.0 or value > 100.0:
        return None
    return round(value, 2)


def score_quality_metadata(score: Any, verdict: str | None) -> dict[str, Any]:
    """Describe whether a score satisfies the LLM judge's numeric 0-100 contract."""
    normalized_score = normalize_llm_score(score, verdict)
    metadata: dict[str, Any] = {
        "status": "valid" if normalized_score is not None else "invalid",
        "reason": (
            None
            if normalized_score is not None
            else MISSING_LLM_SCORE_REASON if score is None else INVALID_LLM_SCORE_REASON
        ),
        "normalized_score": normalized_score,
        "verdict": verdict,
    }
    return metadata


def _requirement_id_sort_key(requirement_id: Any) -> tuple[int, int, str]:
    normalized = str(requirement_id or "")
    for separator in ("_", "-"):
        prefix = f"req{separator}"
        if normalized.lower().startswith(prefix):
            suffix = normalized[len(prefix) :]
            if suffix.isdigit():
                return (0, int(suffix), normalized)
    return (1, 0, normalized)


DEFAULT_JOB_DESCRIPTION_MAX_CHARS = 128_000
DEFAULT_REQUIREMENTS_MAX_COUNT = 200
DEFAULT_REQUIREMENT_TEXT_MAX_CHARS = 2_000
DEFAULT_EVIDENCE_UNITS_MAX_COUNT = 200
DEFAULT_EVIDENCE_UNITS_SCAN_MAX_COUNT = 200
DEFAULT_EVIDENCE_UNIT_MAX_CHARS = 4_000
DEFAULT_RESUME_SUMMARY_MAX_CHARS = 64_000
DEFAULT_LLM_JUDGE_MAX_INPUT_TOKENS = 24_000
JUDGE_INPUT_ESTIMATED_CHARS_PER_TOKEN = 3.5
JUDGE_INPUT_MIN_COMPACTED_STRING_CHARS = 400
JUDGE_INPUT_COMPACTION_MAX_ITERATIONS = 300
JUDGE_INPUT_TOKEN_BUDGET_FIELD = "llm_judge.prompt_token_budget"
RESUME_PROVIDER_EXCLUDED_KEYS = {
    "embedding",
    "embeddings",
    "fingerprint",
    "owner_id",
    "resume_fingerprint",
    "tenant_id",
    "vector",
    "vectors",
}


@dataclass(frozen=True)
class EvaluationResult:
    evaluation: LlmMatchEvaluation
    reused: bool = False


@dataclass(frozen=True)
class EvaluationStartResult(EvaluationResult):
    should_run: bool = False
    provider_payload: dict[str, Any] | None = None
    truncation: dict[str, Any] | None = None


@dataclass(frozen=True)
class JudgeInput:
    provider_payload: dict[str, Any]
    cache_payload: dict[str, Any]
    hashes: dict[str, str]
    truncation: dict[str, Any]


class LlmJudgeUnavailableError(RuntimeError):
    """Raised when match-level LLM judging is unavailable."""

    def __init__(self, message: str, *, reason: str = "unavailable"):
        super().__init__(message)
        self.reason = reason


class LlmJudgeQuotaExceededError(RuntimeError):
    """Raised when the per-user LLM judge budget is exhausted."""


class LlmJudgeConflictError(RuntimeError):
    """Raised when an in-flight evaluation prevents a conflicting mutation."""


class MatchLlmEvaluationService:
    """Owner-scoped LLM evaluation cache for job matches."""

    def __init__(
        self,
        db: Session,
        *,
        llm_provider: Optional[LLMProvider] = None,
        config: Any = None,
    ):
        self.db = db
        self.config = config or load_config()
        self._llm_provider = llm_provider

    @property
    def judge_config(self):
        return self.config.matching.llm_judge

    @property
    def llm_config(self):
        return self.judge_config.runtime

    def is_available(self) -> bool:
        return self.availability_status()[0]

    def availability_status(self) -> tuple[bool, str]:
        if self.judge_config is None:
            return False, "config_missing"
        if not getattr(self.judge_config, "enabled", False):
            return False, "disabled"
        if self.llm_config is None:
            return False, "runtime_missing"

        provider_entries = self._configured_provider_entries()
        if provider_entries:
            return True, "available"

        base_url = str(self.llm_config.base_url or "").strip()
        model = str(self.llm_config.model or "").strip()
        if not base_url:
            return False, "base_url_missing"
        if not model:
            return False, "model_missing"
        has_auth = bool(
            getattr(self.llm_config, "api_key", None)
            or getattr(self.llm_config, "api_secret", None)
            or getattr(self.llm_config, "headers", None)
            or self._is_local_llm_endpoint(base_url)
        )
        if not has_auth:
            return False, "credentials_missing"
        return True, "available"

    def list_for_match(
        self,
        match_id: Any,
        *,
        owner_id: Any,
        tenant_id: Any | None = None,
    ) -> list[LlmMatchEvaluation]:
        match = self._get_match_for_owner(match_id, owner_id=owner_id, tenant_id=tenant_id)
        effective_tenant_id = self._effective_tenant_id(match, tenant_id)
        stmt = (
            select(LlmMatchEvaluation)
            .where(
                LlmMatchEvaluation.owner_id == owner_id,
                LlmMatchEvaluation.job_match_id == match.id,
                LlmMatchEvaluation.deleted_at.is_(None),
            )
            .order_by(LlmMatchEvaluation.created_at.desc())
        )
        if effective_tenant_id is None:
            stmt = stmt.where(LlmMatchEvaluation.tenant_id.is_(None))
        else:
            stmt = stmt.where(LlmMatchEvaluation.tenant_id == effective_tenant_id)
        evaluations = list(self.db.execute(stmt).scalars().all())
        for evaluation in evaluations:
            setattr(
                evaluation,
                "llm_effectiveness",
                self.evaluation_effectiveness(
                    match,
                    evaluation,
                    owner_id=owner_id,
                    tenant_id=effective_tenant_id,
                ),
            )
        return evaluations

    def generate_for_match(
        self,
        match_id: Any,
        *,
        owner_id: Any,
        tenant_id: Any | None = None,
        force: bool = False,
    ) -> EvaluationResult:
        available, reason = self.availability_status()
        if not available:
            raise LlmJudgeUnavailableError(
                self._unavailable_message(reason),
                reason=reason,
            )

        match = self._get_match_for_owner(match_id, owner_id=owner_id, tenant_id=tenant_id)
        effective_tenant_id = self._effective_tenant_id(match, tenant_id)
        judge_input = self.build_judge_input(
            match,
            owner_id=owner_id,
            tenant_id=effective_tenant_id,
        )
        hashes = judge_input.hashes
        existing = self._find_active_cache(
            owner_id=owner_id,
            tenant_id=effective_tenant_id,
            resume_fingerprint=match.resume_fingerprint,
            job_post_id=match.job_post_id,
            judge_config_hash=hashes["judge_config_hash"],
            evidence_hash=hashes["evidence_hash"],
        )

        if existing is not None and existing.status in {LLM_EVALUATION_PENDING, LLM_EVALUATION_RUNNING}:
            if force:
                raise LlmJudgeConflictError("An LLM evaluation is already running for this match.")
            return EvaluationResult(existing, reused=True)

        if existing is not None and not force and self._is_reusable(existing):
            return EvaluationResult(existing, reused=True)

        self._check_daily_quota(owner_id)
        if existing is not None and force:
            self._tombstone(existing)
            self.db.flush()
        elif existing is not None:
            self._tombstone(existing)
            self.db.flush()

        evaluation = self._create_pending_evaluation(
            match=match,
            owner_id=owner_id,
            tenant_id=effective_tenant_id,
            hashes=hashes,
        )
        try:
            self.db.flush()
        except IntegrityError:
            self.db.rollback()
            existing_after_race = self._find_active_cache(
                owner_id=owner_id,
                tenant_id=effective_tenant_id,
                resume_fingerprint=match.resume_fingerprint,
                job_post_id=match.job_post_id,
                judge_config_hash=hashes["judge_config_hash"],
                evidence_hash=hashes["evidence_hash"],
            )
            if existing_after_race is None:
                raise
            return EvaluationResult(existing_after_race, reused=True)

        self._run_provider(
            evaluation,
            judge_input.provider_payload,
            truncation=judge_input.truncation,
        )
        self.db.commit()
        return EvaluationResult(evaluation, reused=False)

    def start_for_match(
        self,
        match_id: Any,
        *,
        owner_id: Any,
        tenant_id: Any | None = None,
        force: bool = False,
    ) -> EvaluationStartResult:
        """Create or reuse a cache row without blocking on the provider call."""
        available, reason = self.availability_status()
        if not available:
            raise LlmJudgeUnavailableError(
                self._unavailable_message(reason),
                reason=reason,
            )

        match = self._get_match_for_owner(match_id, owner_id=owner_id, tenant_id=tenant_id)
        effective_tenant_id = self._effective_tenant_id(match, tenant_id)
        judge_input = self.build_judge_input(
            match,
            owner_id=owner_id,
            tenant_id=effective_tenant_id,
        )
        hashes = judge_input.hashes
        existing = self._find_active_cache(
            owner_id=owner_id,
            tenant_id=effective_tenant_id,
            resume_fingerprint=match.resume_fingerprint,
            job_post_id=match.job_post_id,
            judge_config_hash=hashes["judge_config_hash"],
            evidence_hash=hashes["evidence_hash"],
        )

        if existing is not None and existing.status in {LLM_EVALUATION_PENDING, LLM_EVALUATION_RUNNING}:
            if force:
                raise LlmJudgeConflictError("An LLM evaluation is already running for this match.")
            return EvaluationStartResult(existing, reused=True, should_run=False)

        if existing is not None and not force and self._is_reusable(existing):
            return EvaluationStartResult(existing, reused=True, should_run=False)

        self._check_daily_quota(owner_id)
        if existing is not None:
            self._tombstone(existing)
            self.db.flush()

        evaluation = self._create_pending_evaluation(
            match=match,
            owner_id=owner_id,
            tenant_id=effective_tenant_id,
            hashes=hashes,
        )
        try:
            self.db.flush()
        except IntegrityError:
            self.db.rollback()
            existing_after_race = self._find_active_cache(
                owner_id=owner_id,
                tenant_id=effective_tenant_id,
                resume_fingerprint=match.resume_fingerprint,
                job_post_id=match.job_post_id,
                judge_config_hash=hashes["judge_config_hash"],
                evidence_hash=hashes["evidence_hash"],
            )
            if existing_after_race is None:
                raise
            return EvaluationStartResult(existing_after_race, reused=True, should_run=False)

        self.db.commit()
        return EvaluationStartResult(
            evaluation,
            reused=False,
            should_run=True,
            provider_payload=judge_input.provider_payload,
            truncation=judge_input.truncation,
        )

    def run_pending_evaluation(
        self,
        evaluation_id: Any,
        payload: dict[str, Any],
        *,
        truncation: dict[str, Any] | None = None,
    ) -> LlmMatchEvaluation | None:
        """Run provider work for a pending evaluation loaded in this DB session."""
        try:
            lookup_id = uuid.UUID(str(evaluation_id))
        except (TypeError, ValueError):
            logger.warning("Skipping invalid LLM evaluation id %s", _sanitize_log(evaluation_id))
            return None

        evaluation = self.db.get(LlmMatchEvaluation, lookup_id)
        if evaluation is None:
            logger.warning("Skipping missing LLM evaluation %s", _sanitize_log(evaluation_id))
            return None
        if evaluation.deleted_at is not None:
            logger.info("Skipping deleted LLM evaluation %s", _sanitize_log(evaluation_id))
            return evaluation
        if evaluation.status not in {LLM_EVALUATION_PENDING, LLM_EVALUATION_RUNNING}:
            logger.info(
                "Skipping LLM evaluation %s with status %s",
                _sanitize_log(evaluation_id),
                _sanitize_log(evaluation.status),
            )
            return evaluation

        self._run_provider(evaluation, payload, truncation=truncation)
        self.db.commit()
        return evaluation

    def resume_pending_evaluation(self, evaluation_id: Any) -> LlmMatchEvaluation | None:
        """Rebuild judge input and run a pending, stale-running, or retryable failed row."""
        try:
            lookup_id = uuid.UUID(str(evaluation_id))
        except (TypeError, ValueError):
            logger.warning("Skipping invalid LLM evaluation id %s", _sanitize_log(evaluation_id))
            return None

        evaluation = self.db.get(LlmMatchEvaluation, lookup_id)
        if evaluation is None:
            logger.warning("Skipping missing LLM evaluation %s", _sanitize_log(evaluation_id))
            return None
        if evaluation.deleted_at is not None:
            logger.info("Skipping deleted LLM evaluation %s", _sanitize_log(evaluation_id))
            return evaluation
        if evaluation.status == LLM_EVALUATION_FAILED and not evaluation.retryable:
            logger.info("Skipping terminal failed LLM evaluation %s", _sanitize_log(evaluation_id))
            return evaluation
        if evaluation.status not in {LLM_EVALUATION_PENDING, LLM_EVALUATION_RUNNING, LLM_EVALUATION_FAILED}:
            logger.info(
                "Skipping LLM evaluation %s with status %s",
                _sanitize_log(evaluation_id),
                _sanitize_log(evaluation.status),
            )
            return evaluation

        match = None
        if evaluation.job_match_id is not None:
            match = self.db.get(JobMatch, evaluation.job_match_id)
        if match is None:
            stmt = select(JobMatch).where(
                JobMatch.job_post_id == evaluation.job_post_id,
                JobMatch.resume_fingerprint == evaluation.resume_fingerprint,
            )
            match = self.db.execute(stmt).scalar_one_or_none()
        if match is None:
            evaluation.status = LLM_EVALUATION_FAILED
            evaluation.error_code = "match_not_found"
            evaluation.retryable = False
            evaluation.completed_at = datetime.now(timezone.utc)
            self.db.commit()
            return evaluation

        if evaluation.status == LLM_EVALUATION_FAILED:
            evaluation.status = LLM_EVALUATION_PENDING
            evaluation.retryable = False
            evaluation.error_code = None
            evaluation.completed_at = None
            self.db.flush()

        judge_input = self.build_judge_input(
            match,
            owner_id=evaluation.owner_id,
            tenant_id=evaluation.tenant_id,
        )
        return self.run_pending_evaluation(
            evaluation.id,
            judge_input.provider_payload,
            truncation=judge_input.truncation,
        )

    def retry_evaluation(
        self,
        match_id: Any,
        evaluation_id: Any,
        *,
        owner_id: Any,
        tenant_id: Any | None = None,
    ) -> EvaluationStartResult:
        """Reset one retryable failed evaluation row for queue-based execution."""
        available, reason = self.availability_status()
        if not available:
            raise LlmJudgeUnavailableError(
                self._unavailable_message(reason),
                reason=reason,
            )

        match = self._get_match_for_owner(match_id, owner_id=owner_id, tenant_id=tenant_id)
        effective_tenant_id = self._effective_tenant_id(match, tenant_id)
        evaluation = self._get_evaluation_for_owner(
            evaluation_id,
            owner_id=owner_id,
            tenant_id=effective_tenant_id,
        )
        if str(evaluation.job_match_id) != str(match.id):
            raise LookupError("Evaluation not found")
        if evaluation.status != LLM_EVALUATION_FAILED or not bool(evaluation.retryable):
            raise LlmJudgeConflictError("Only retryable failed LLM evaluations can be retried.")

        self._check_daily_quota(owner_id)

        analysis = evaluation.analysis if isinstance(evaluation.analysis, dict) else {}
        queue_metadata = analysis.get("queue")
        if not isinstance(queue_metadata, dict):
            queue_metadata = {}
        queue_metadata.update(
            {
                "enqueue_reason": "retry_now",
                "queue_state": "pending",
                "provider_status_message": "Retry requested by operator.",
            }
        )
        analysis["queue"] = queue_metadata
        analysis["enqueue_reason"] = "retry_now"
        evaluation.analysis = analysis
        evaluation.status = LLM_EVALUATION_PENDING
        evaluation.retryable = False
        evaluation.error_code = None
        evaluation.completed_at = None
        self.db.commit()
        return EvaluationStartResult(evaluation, reused=False, should_run=True)

    def delete_evaluation(
        self,
        match_id: Any,
        evaluation_id: Any,
        *,
        owner_id: Any,
        tenant_id: Any | None = None,
    ) -> None:
        match = self._get_match_for_owner(match_id, owner_id=owner_id, tenant_id=tenant_id)
        effective_tenant_id = self._effective_tenant_id(match, tenant_id)
        evaluation = self._get_evaluation_for_owner(
            evaluation_id,
            owner_id=owner_id,
            tenant_id=effective_tenant_id,
        )
        if str(evaluation.job_match_id) != str(match.id):
            raise LookupError("Evaluation not found")
        self._tombstone(evaluation)
        self.db.commit()

    def evaluate_selection_run(
        self,
        selection_run_id: Any,
        *,
        owner_id: Any,
        tenant_id: Any | None = None,
        top_n: int,
    ) -> dict[str, int]:
        if not self.is_available() or top_n <= 0:
            return {"attempted": 0, "reused": 0, "created": 0, "enqueued": 0, "failed": 0}

        from database.models import MatchSelectionItem
        from core.llm_evaluation_queue import enqueue_llm_evaluation

        stmt = (
            select(MatchSelectionItem)
            .where(
                MatchSelectionItem.selection_run_id == selection_run_id,
                MatchSelectionItem.selection_tier == "primary",
            )
            .options(joinedload(MatchSelectionItem.job_match))
            .order_by(MatchSelectionItem.rank_position.asc())
            .limit(min(top_n, int(self.judge_config.max_per_run)))
        )
        items = list(self.db.execute(stmt).scalars().all())
        stats = {"attempted": 0, "reused": 0, "created": 0, "enqueued": 0, "failed": 0}
        for item in items:
            match = item.job_match
            if match is None:
                continue
            stats["attempted"] += 1
            try:
                result = self.start_for_match(
                    match.id,
                    owner_id=owner_id,
                    tenant_id=tenant_id,
                    force=False,
                )
                if result.reused:
                    stats["reused"] += 1
                else:
                    stats["created"] += 1
                if getattr(result, "should_run", False):
                    enqueue_llm_evaluation(
                        result.evaluation.id,
                        provider_payload=getattr(result, "provider_payload", None) or {},
                        truncation=getattr(result, "truncation", None) or {},
                        enqueue_reason="auto_top_n",
                        owner_id=owner_id,
                        tenant_id=tenant_id,
                    )
                    stats["enqueued"] += 1
            except LlmJudgeQuotaExceededError:
                logger.info("LLM judge daily quota exhausted for owner %s", _sanitize_log(owner_id))
                break
            except Exception as exc:
                stats["failed"] += 1
                logger.warning("LLM judge skipped match %s: %s", _sanitize_log(match.id), exc)
        return stats

    def _get_match_for_owner(self, match_id: Any, *, owner_id: Any, tenant_id: Any | None) -> JobMatch:
        stmt = (
            select(JobMatch)
            .join(StructuredResume, StructuredResume.resume_fingerprint == JobMatch.resume_fingerprint)
            .join(JobPost, JobPost.id == JobMatch.job_post_id)
            .where(
                JobMatch.id == match_id,
                StructuredResume.owner_id == owner_id,
            )
            .options(joinedload(JobMatch.job_post))
        )
        if tenant_id is not None:
            stmt = stmt.where(JobPost.tenant_id == tenant_id)
        match = self.db.execute(stmt).scalar_one_or_none()
        if match is None:
            raise LookupError("Match not found")
        return match

    def _get_evaluation_for_owner(
        self,
        evaluation_id: Any,
        *,
        owner_id: Any,
        tenant_id: Any | None,
    ) -> LlmMatchEvaluation:
        stmt = select(LlmMatchEvaluation).where(
            LlmMatchEvaluation.id == evaluation_id,
            LlmMatchEvaluation.owner_id == owner_id,
            LlmMatchEvaluation.deleted_at.is_(None),
        )
        if tenant_id is None:
            stmt = stmt.where(LlmMatchEvaluation.tenant_id.is_(None))
        else:
            stmt = stmt.where(LlmMatchEvaluation.tenant_id == tenant_id)
        evaluation = self.db.execute(stmt).scalar_one_or_none()
        if evaluation is None:
            raise LookupError("Evaluation not found")
        return evaluation

    @staticmethod
    def _effective_tenant_id(match: JobMatch, explicit_tenant_id: Any | None) -> Any | None:
        if explicit_tenant_id is not None:
            return explicit_tenant_id
        job = getattr(match, "job_post", None)
        return getattr(job, "tenant_id", None)

    def _find_active_cache(
        self,
        *,
        owner_id: Any,
        tenant_id: Any | None,
        resume_fingerprint: str,
        job_post_id: Any,
        judge_config_hash: str,
        evidence_hash: str,
    ) -> LlmMatchEvaluation | None:
        stmt = select(LlmMatchEvaluation).where(
            LlmMatchEvaluation.owner_id == owner_id,
            LlmMatchEvaluation.resume_fingerprint == resume_fingerprint,
            LlmMatchEvaluation.job_post_id == job_post_id,
            LlmMatchEvaluation.judge_config_hash == judge_config_hash,
            LlmMatchEvaluation.evidence_hash == evidence_hash,
            LlmMatchEvaluation.deleted_at.is_(None),
        )
        if tenant_id is None:
            stmt = stmt.where(LlmMatchEvaluation.tenant_id.is_(None))
        else:
            stmt = stmt.where(LlmMatchEvaluation.tenant_id == tenant_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def build_judge_input(
        self,
        match: JobMatch,
        *,
        owner_id: Any | None,
        tenant_id: Any | None = None,
    ) -> JudgeInput:
        """Build the provider-safe judge payload and local cache hashes."""
        del tenant_id
        return self._build_judge_input(match, owner_id=owner_id)

    def _build_hash_payload(self, match: JobMatch) -> tuple[dict[str, Any], dict[str, str]]:
        judge_input = self._build_judge_input(match, owner_id=None)
        return judge_input.provider_payload, judge_input.hashes

    def _build_judge_input(self, match: JobMatch, *, owner_id: Any | None) -> JudgeInput:
        truncation: dict[str, Any] = {"truncated": False, "fields": {}}
        job = self._load_job_for_match(match)
        job_id = self._match_job_id(match)
        description = self._truncate_with_metadata(
            getattr(job, "description", None),
            self._judge_limit("job_description_max_chars", DEFAULT_JOB_DESCRIPTION_MAX_CHARS),
            truncation,
            "job.description",
        )
        match_requirements = self._load_match_requirements(match)
        job_requirements = self._load_job_requirements(job)
        if not job_requirements:
            job_requirements = [
                getattr(req, "requirement", None)
                for req in match_requirements
                if getattr(req, "requirement", None) is not None
            ]
        requirement_payload = self._serialize_requirements(
            job_requirements,
            truncation,
        )
        resume = self._load_resume(owner_id, getattr(match, "resume_fingerprint", None))
        evidence_units = self._load_resume_evidence_units(
            owner_id,
            getattr(match, "resume_fingerprint", None),
        )
        selected_evidence_units = select_relevant_resume_evidence_units(
            evidence_units,
            job_requirements,
            max_count=self._judge_limit(
                "evidence_units_max_count",
                DEFAULT_EVIDENCE_UNITS_MAX_COUNT,
            ),
            extra_count=1,
            job_texts=(getattr(job, "title", None), description),
        )

        provider_payload = {
            "task": "independent_resume_job_match_review",
            "job": {
                "title": self._truncate(getattr(job, "title", None), 200),
                "company": self._truncate(getattr(job, "company", None), 200),
                "location": self._truncate(getattr(job, "location_text", None), 200),
                "is_remote": bool(getattr(job, "is_remote", False)),
                "description": description,
                "description_metadata": {
                    "source": str(getattr(job, "description_source", None) or "unknown"),
                    "completeness": (
                        str(getattr(job, "description_completeness", None) or "unknown")
                        if description
                        else "missing"
                    ),
                    "warning_code": getattr(job, "description_warning_code", None),
                    "truncated_for_prompt": bool(
                        truncation["fields"].get("job.description", {}).get("truncated")
                    ),
                },
            },
            "requirements": requirement_payload,
            "resume": self._serialize_resume_summary(resume, truncation),
            "resume_evidence_units": self._serialize_resume_evidence_units(
                selected_evidence_units,
                truncation,
            ),
            "input_metadata": {
                "schema_version": str(self.judge_config.schema_version),
                "prompt_version": str(self.judge_config.prompt_version),
                "max_input_tokens": self._judge_token_budget(),
                "truncation": truncation,
            },
        }
        self._apply_provider_token_budget(provider_payload, truncation)
        judge_config_payload = self._judge_config_payload()
        cache_payload = {
            "job_post_id": str(job_id or ""),
            "job_content_hash": getattr(job, "content_hash", None),
            "job_description_hash": getattr(job, "description_hash", None),
            "match_job_content_hash": getattr(match, "job_content_hash", None),
            "provider_payload_hash": self._hash_json(provider_payload),
        }
        evidence_hash = self._hash_json(provider_payload)
        judge_config_hash = self._hash_json(judge_config_payload)
        input_hash = self._hash_json(
            {
                "provider_payload": provider_payload,
                "judge_config": judge_config_payload,
            }
        )
        return JudgeInput(
            provider_payload=provider_payload,
            cache_payload=cache_payload,
            hashes={
                "evidence_hash": evidence_hash,
                "judge_config_hash": judge_config_hash,
                "input_hash": input_hash,
            },
            truncation=truncation,
        )

    def evaluation_effectiveness(
        self,
        match: JobMatch,
        evaluation: LlmMatchEvaluation | None,
        *,
        owner_id: Any | None,
        tenant_id: Any | None = None,
    ) -> dict[str, Any]:
        """Return read-only rerank eligibility metadata for an evaluation."""
        if evaluation is None:
            return {
                "effective_for_rerank": False,
                "ignored_for_rerank_reason": "no_evaluation",
                "stale_status": "missing",
                "freshness": self._freshness_metadata(
                    match,
                    None,
                    status="missing",
                    reason="no_evaluation",
                ),
                "input_truncation": {},
            }
        if getattr(evaluation, "deleted_at", None) is not None:
            return {
                "effective_for_rerank": False,
                "ignored_for_rerank_reason": "deleted",
                "stale_status": "ignored",
                "freshness": self._freshness_metadata(
                    match,
                    evaluation,
                    status="ignored",
                    reason="deleted",
                ),
                "input_truncation": {},
            }
        status = getattr(evaluation, "status", None)
        if status != LLM_EVALUATION_SUCCEEDED:
            reason = f"status_{status or 'unknown'}"
            return {
                "effective_for_rerank": False,
                "ignored_for_rerank_reason": reason,
                "stale_status": "ignored",
                "freshness": self._freshness_metadata(
                    match,
                    evaluation,
                    status="ignored",
                    reason=reason,
                ),
                "input_truncation": {},
            }
        if getattr(evaluation, "llm_score", None) is None:
            return {
                "effective_for_rerank": False,
                "ignored_for_rerank_reason": "missing_llm_score",
                "stale_status": "ignored",
                "freshness": self._freshness_metadata(
                    match,
                    evaluation,
                    status="ignored",
                    reason="missing_llm_score",
                ),
                "input_truncation": {},
            }

        analysis = getattr(evaluation, "analysis", None)
        if not isinstance(analysis, dict):
            analysis = {}
        score_quality = score_quality_metadata(
            getattr(evaluation, "llm_score", None),
            getattr(evaluation, "verdict", None),
        )
        if score_quality.get("status") == "invalid":
            reason = str(score_quality.get("reason") or INVALID_LLM_SCORE_REASON)
            return {
                "effective_for_rerank": False,
                "ignored_for_rerank_reason": reason,
                "stale_status": "ignored",
                "freshness": self._freshness_metadata(
                    match,
                    evaluation,
                    status="ignored",
                    reason=reason,
                ),
                "input_truncation": {},
                "score_quality": score_quality,
            }

        job = self._load_job_for_match(match)
        current_job_hash = getattr(job, "content_hash", None)
        match_job_hash = getattr(match, "job_content_hash", None)
        if match_job_hash and current_job_hash and str(match_job_hash) != str(current_job_hash):
            return {
                "effective_for_rerank": False,
                "ignored_for_rerank_reason": "stale_job_content",
                "stale_status": "stale",
                "freshness": self._freshness_metadata(
                    match,
                    evaluation,
                    status="stale",
                    reason="stale_job_content",
                    job=job,
                ),
                "input_truncation": {},
            }

        try:
            judge_input = self.build_judge_input(match, owner_id=owner_id, tenant_id=tenant_id)
        except Exception as exc:
            logger.warning("Could not build current LLM judge input: %s", exc)
            return {
                "effective_for_rerank": False,
                "ignored_for_rerank_reason": "current_input_unavailable",
                "stale_status": "unknown",
                "freshness": self._freshness_metadata(
                    match,
                    evaluation,
                    status="unknown",
                    reason="current_input_unavailable",
                    job=job,
                ),
                "input_truncation": {},
            }

        for field_name, reason in (
            ("judge_config_hash", "stale_config_hash"),
            ("input_hash", "stale_input_hash"),
            ("evidence_hash", "stale_evidence_hash"),
        ):
            if getattr(evaluation, field_name, None) != judge_input.hashes[field_name]:
                return {
                    "effective_for_rerank": False,
                    "ignored_for_rerank_reason": reason,
                    "stale_status": "stale",
                    "freshness": self._freshness_metadata(
                        match,
                        evaluation,
                        status="stale",
                        reason=reason,
                        job=job,
                    ),
                    "input_truncation": judge_input.truncation,
                }

        return {
            "effective_for_rerank": True,
            "ignored_for_rerank_reason": None,
            "stale_status": "current",
            "freshness": self._freshness_metadata(
                match,
                evaluation,
                status="current",
                reason=None,
                job=job,
            ),
            "input_truncation": judge_input.truncation,
        }

    def _freshness_metadata(
        self,
        match: JobMatch,
        evaluation: LlmMatchEvaluation | None,
        *,
        status: str,
        reason: str | None,
        job: JobPost | None = None,
    ) -> dict[str, Any]:
        if job is None and status in {"current", "stale", "unknown"}:
            job = self._load_job_for_match(match)
        source = self._primary_source_for_job(job)
        actions: list[str] = []
        if status in {"stale", "unknown"}:
            actions.append("regenerate_llm_evaluation")
        if source is not None:
            actions.append("refresh_availability")
        return {
            "status": status,
            "reason": reason,
            "evaluated_at": self._iso(getattr(evaluation, "completed_at", None)),
            "match_calculated_at": self._iso(getattr(match, "calculated_at", None)),
            "job_last_seen_at": self._iso(getattr(job, "last_seen_at", None)),
            "source_last_seen_at": self._iso(getattr(source, "last_seen_at", None)),
            "source_is_active": getattr(source, "is_active", None),
            "available_actions": actions,
        }

    @staticmethod
    def _primary_source_for_job(job: JobPost | None):
        try:
            sources = list(getattr(job, "sources", None) or [])
        except TypeError:
            sources = []
        if not sources:
            return None
        active_sources = [source for source in sources if getattr(source, "is_active", False)]
        candidates = active_sources or sources
        return max(
            candidates,
            key=lambda source: (
                getattr(source, "last_seen_at", None) is not None,
                getattr(source, "last_seen_at", None),
            ),
        )

    @staticmethod
    def _iso(value: Any) -> str | None:
        if type(value).__module__.startswith("unittest.mock"):
            return None
        return value.isoformat() if hasattr(value, "isoformat") else None

    def _judge_limit(self, name: str, default: int) -> int:
        value = getattr(self.judge_config, name, default)
        try:
            return max(1, int(value))
        except Exception:
            return default

    def _configured_provider_entries(self):
        try:
            return configured_provider_entries(self.llm_config)
        except Exception:
            return []

    def _judge_token_budget(self) -> int:
        provider_budgets = []
        for entry in self._configured_provider_entries():
            try:
                provider_budgets.append(max(1, int(entry.max_input_tokens)))
            except Exception:
                continue
        if provider_budgets:
            return min(provider_budgets)
        value = getattr(self.llm_config, "max_input_tokens", DEFAULT_LLM_JUDGE_MAX_INPUT_TOKENS)
        try:
            return max(1, int(value))
        except Exception:
            return DEFAULT_LLM_JUDGE_MAX_INPUT_TOKENS

    def _apply_provider_token_budget(
        self,
        provider_payload: dict[str, Any],
        truncation: dict[str, Any],
    ) -> None:
        """Compact the public judge payload only when it exceeds runtime input budget."""
        budget = self._judge_token_budget()
        input_metadata = provider_payload.setdefault("input_metadata", {})
        token_budget_metadata = {
            "max_input_tokens": budget,
            "estimation": "chars_per_token",
            "estimated_chars_per_token": JUDGE_INPUT_ESTIMATED_CHARS_PER_TOKEN,
            "compacted": False,
            "within_budget": True,
        }
        input_metadata["token_budget"] = token_budget_metadata

        initial_tokens = self._estimate_judge_prompt_tokens(provider_payload)
        token_budget_metadata["initial_estimated_tokens"] = initial_tokens
        if initial_tokens <= budget:
            token_budget_metadata["final_estimated_tokens"] = self._estimate_judge_prompt_tokens(
                provider_payload
            )
            return

        truncation["truncated"] = True
        truncation["fields"][JUDGE_INPUT_TOKEN_BUDGET_FIELD] = {
            "truncated": True,
            "original_estimated_tokens": initial_tokens,
            "max_input_tokens": budget,
            "reason": "runtime_token_budget",
        }
        token_budget_metadata["compacted"] = True

        for _ in range(JUDGE_INPUT_COMPACTION_MAX_ITERATIONS):
            current_tokens = self._estimate_judge_prompt_tokens(provider_payload)
            if current_tokens <= budget:
                break
            candidates = self._compaction_candidates(provider_payload)
            if not candidates:
                break
            path, parent, key, text = max(candidates, key=lambda item: len(item[3]))
            excess_tokens = max(1, current_tokens - budget)
            excess_chars = math.ceil(
                excess_tokens * JUDGE_INPUT_ESTIMATED_CHARS_PER_TOKEN * 1.15
            ) + 256
            new_len = max(JUDGE_INPUT_MIN_COMPACTED_STRING_CHARS, len(text) - excess_chars)
            if new_len >= len(text):
                new_len = max(JUDGE_INPUT_MIN_COMPACTED_STRING_CHARS, int(len(text) * 0.8))
            if new_len >= len(text):
                break
            compacted = self._truncate(text, new_len)
            if isinstance(parent, list) and isinstance(key, int):
                parent[key] = compacted
            elif isinstance(parent, dict):
                parent[key] = compacted
            self._record_token_budget_truncation(
                truncation,
                path=path,
                original_chars=len(text),
                included_chars=len(compacted),
            )

        final_tokens = self._estimate_judge_prompt_tokens(provider_payload)
        token_budget_metadata["final_estimated_tokens"] = final_tokens
        token_budget_metadata["within_budget"] = final_tokens <= budget
        truncation["fields"][JUDGE_INPUT_TOKEN_BUDGET_FIELD][
            "final_estimated_tokens"
        ] = final_tokens
        truncation["fields"][JUDGE_INPUT_TOKEN_BUDGET_FIELD]["within_budget"] = (
            final_tokens <= budget
        )
        job = provider_payload.get("job")
        if isinstance(job, dict):
            description_metadata = job.get("description_metadata")
            if isinstance(description_metadata, dict):
                description_metadata["truncated_for_prompt"] = bool(
                    truncation["fields"].get("job.description", {}).get("truncated")
                )

    def _estimate_judge_prompt_tokens(self, provider_payload: dict[str, Any]) -> int:
        serialized_payload = json.dumps(provider_payload, sort_keys=True, default=str)
        schema_text = json.dumps(MATCH_LLM_JUDGE_SCHEMA_SPEC["schema"], sort_keys=True)
        prompt_text = (
            f"{MATCH_LLM_JUDGE_SYSTEM_PROMPT}\n"
            "Evaluate the resume evidence against the job and return the requested JSON.\n\n"
            "<JUDGE_INPUT_JSON>\n"
            f"{serialized_payload}\n"
            "</JUDGE_INPUT_JSON>\n\n"
            "Return a single valid JSON object matching this JSON Schema. "
            "Do not include markdown fences or prose outside the JSON object.\n"
            f"{schema_text}"
        )
        return max(1, math.ceil(len(prompt_text) / JUDGE_INPUT_ESTIMATED_CHARS_PER_TOKEN))

    def _compaction_candidates(
        self,
        provider_payload: dict[str, Any],
    ) -> list[tuple[str, Any, Any, str]]:
        candidates: list[tuple[str, Any, Any, str]] = []
        for key in ("job", "requirements", "resume", "resume_evidence_units"):
            value = provider_payload.get(key)
            self._collect_compaction_candidates(value, key, None, None, candidates)
        return candidates

    def _collect_compaction_candidates(
        self,
        value: Any,
        path: str,
        parent: Any,
        key: Any,
        candidates: list[tuple[str, Any, Any, str]],
    ) -> None:
        if isinstance(value, str):
            if len(value) > JUDGE_INPUT_MIN_COMPACTED_STRING_CHARS:
                candidates.append((path, parent, key, value))
            return
        if isinstance(value, dict):
            for child_key, child_value in value.items():
                child_path = f"{path}.{child_key}" if path else str(child_key)
                self._collect_compaction_candidates(
                    child_value,
                    child_path,
                    value,
                    child_key,
                    candidates,
                )
            return
        if isinstance(value, list):
            for index, child_value in enumerate(value):
                child_path = f"{path}.{index}" if path else str(index)
                self._collect_compaction_candidates(
                    child_value,
                    child_path,
                    value,
                    index,
                    candidates,
                )

    def _record_token_budget_truncation(
        self,
        truncation: dict[str, Any],
        *,
        path: str,
        original_chars: int,
        included_chars: int,
    ) -> None:
        field = truncation["fields"].setdefault(path, {})
        field["truncated"] = True
        field["original_chars"] = max(int(field.get("original_chars", 0)), original_chars)
        field["included_chars"] = included_chars
        field["reason"] = "runtime_token_budget"

    def _judge_config_payload(self) -> dict[str, Any]:
        provider_entries = self._configured_provider_entries()
        runtime_payload: dict[str, Any]
        if provider_entries:
            runtime_payload = {
                "providers": [
                    sanitized_provider_config(entry)
                    for entry in provider_entries
                ],
            }
        else:
            runtime_payload = {
                "provider": str(self.llm_config.provider),
                "model": str(self.llm_config.model),
                "structured_output_mode": str(self.llm_config.structured_output_mode),
            }
        return {
            **runtime_payload,
            "max_input_tokens": self._judge_token_budget(),
            "prompt_version": str(self.judge_config.prompt_version),
            "schema_version": str(self.judge_config.schema_version),
            "job_description_max_chars": self._judge_limit(
                "job_description_max_chars",
                DEFAULT_JOB_DESCRIPTION_MAX_CHARS,
            ),
            "requirements_max_count": self._judge_limit(
                "requirements_max_count",
                DEFAULT_REQUIREMENTS_MAX_COUNT,
            ),
            "requirement_text_max_chars": self._judge_limit(
                "requirement_text_max_chars",
                DEFAULT_REQUIREMENT_TEXT_MAX_CHARS,
            ),
            "evidence_units_max_count": self._judge_limit(
                "evidence_units_max_count",
                DEFAULT_EVIDENCE_UNITS_MAX_COUNT,
            ),
            "evidence_unit_max_chars": self._judge_limit(
                "evidence_unit_max_chars",
                DEFAULT_EVIDENCE_UNIT_MAX_CHARS,
            ),
            "resume_summary_max_chars": self._judge_limit(
                "resume_summary_max_chars",
                DEFAULT_RESUME_SUMMARY_MAX_CHARS,
            ),
        }

    @staticmethod
    def _match_job_id(match: Any) -> Any | None:
        return getattr(match, "job_post_id", None) or getattr(match, "job_id", None)

    def _load_job_for_match(self, match: Any) -> Any:
        job = getattr(match, "job_post", None)
        if job is not None:
            return job
        job_id = self._match_job_id(match)
        if job_id is None:
            return None
        try:
            get_method = getattr(self.db, "get", None)
            if callable(get_method) and not type(get_method).__module__.startswith("unittest.mock"):
                return get_method(JobPost, job_id)
            return self.db.query(JobPost).get(job_id)
        except Exception:
            return None

    def _load_job_requirements(self, job: Any) -> list[Any]:
        if job is None:
            return []
        requirements = getattr(job, "requirements", None)
        if requirements:
            return sorted(
                list(requirements),
                key=lambda item: (
                    getattr(item, "ordinal", None) is None,
                    getattr(item, "ordinal", 0) or 0,
                    str(getattr(item, "id", "")),
                ),
            )
        job_id = getattr(job, "id", None)
        if job_id is None:
            return []
        try:
            return list(
                self.db.query(JobRequirementUnit)
                .filter(JobRequirementUnit.job_post_id == job_id)
                .order_by(JobRequirementUnit.ordinal.asc(), JobRequirementUnit.created_at.asc())
                .all()
            )
        except Exception:
            return []

    def _load_match_requirements(self, match: Any) -> list[Any]:
        try:
            return list(
                self.db.query(JobMatchRequirement)
                .options(joinedload(JobMatchRequirement.requirement))
                .filter(JobMatchRequirement.job_match_id == match.id)
                .order_by(JobMatchRequirement.created_at.asc(), JobMatchRequirement.id.asc())
                .all()
            )
        except Exception:
            return []

    def _load_resume(self, owner_id: Any | None, resume_fingerprint: Any | None) -> Any:
        if owner_id is None or resume_fingerprint is None:
            return None
        try:
            return (
                self.db.query(StructuredResume)
                .filter(
                    StructuredResume.owner_id == owner_id,
                    StructuredResume.resume_fingerprint == resume_fingerprint,
                )
                .order_by(StructuredResume.updated_at.desc())
                .first()
            )
        except Exception:
            return None

    def _load_resume_evidence_units(
        self,
        owner_id: Any | None,
        resume_fingerprint: Any | None,
    ) -> list[Any]:
        if owner_id is None or resume_fingerprint is None:
            return []
        limit = self._judge_limit(
            "evidence_units_max_count",
            DEFAULT_EVIDENCE_UNITS_MAX_COUNT,
        )
        scan_limit = max(limit + 1, DEFAULT_EVIDENCE_UNITS_SCAN_MAX_COUNT)
        try:
            return list(
                self.db.query(ResumeEvidenceUnitEmbedding)
                .filter(
                    ResumeEvidenceUnitEmbedding.owner_id == owner_id,
                    ResumeEvidenceUnitEmbedding.resume_fingerprint == resume_fingerprint,
                )
                .order_by(
                    ResumeEvidenceUnitEmbedding.source_section.asc(),
                    ResumeEvidenceUnitEmbedding.evidence_unit_id.asc(),
                    ResumeEvidenceUnitEmbedding.created_at.asc(),
                )
                .limit(scan_limit + 1)
                .all()
            )
        except Exception:
            return []

    def _serialize_requirements(
        self,
        requirements: list[Any],
        truncation: dict[str, Any],
    ) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {
            "required": [],
            "preferred": [],
            "responsibility": [],
            "constraint": [],
            "benefit": [],
            "other": [],
        }
        max_count = self._judge_limit("requirements_max_count", DEFAULT_REQUIREMENTS_MAX_COUNT)
        text_max_chars = self._judge_limit(
            "requirement_text_max_chars",
            DEFAULT_REQUIREMENT_TEXT_MAX_CHARS,
        )
        if len(requirements) > max_count:
            truncation["truncated"] = True
            truncation["fields"]["requirements"] = {
                "truncated": True,
                "original_count": len(requirements),
                "included_count": max_count,
            }
        for index, requirement in enumerate(requirements[:max_count], start=1):
            public_id = f"req_{index}"
            req_type = str(getattr(requirement, "req_type", None) or "other").lower()
            if req_type not in grouped:
                req_type = "other"
            item = {
                "requirement_id": public_id,
                "type": req_type,
                "text": self._truncate_with_metadata(
                    getattr(requirement, "text", None),
                    text_max_chars,
                    truncation,
                    f"requirements.{public_id}.text",
                ),
                "tags": self._safe_json_object(getattr(requirement, "tags", None)),
                "min_years": self._float(getattr(requirement, "min_years", None)),
                "years_context": self._truncate(getattr(requirement, "years_context", None), 120),
            }
            grouped[req_type].append(item)
        return grouped

    def _serialize_resume_summary(self, resume: Any, truncation: dict[str, Any]) -> dict[str, Any]:
        if resume is None:
            return {}
        data = getattr(resume, "extracted_data", None)
        summary = data if isinstance(data, dict) else {}
        public_summary = self._public_resume_summary(summary)
        if getattr(resume, "total_experience_years", None) is not None:
            public_summary["total_experience_years"] = self._float(resume.total_experience_years)
        return self._cap_json_payload(
            public_summary,
            self._judge_limit("resume_summary_max_chars", DEFAULT_RESUME_SUMMARY_MAX_CHARS),
            truncation,
            "resume.summary",
        )

    def _public_resume_summary(self, summary: dict[str, Any]) -> dict[str, Any]:
        return self._public_resume_value(summary)

    def _public_resume_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                str(key)[:120]: self._public_resume_value(item)
                for key, item in value.items()
                if str(key).strip().lower() not in RESUME_PROVIDER_EXCLUDED_KEYS
            }
        if isinstance(value, list):
            return [self._public_resume_value(item) for item in value]
        if isinstance(value, str):
            return value.replace("\x00", "").strip()
        if isinstance(value, (int, float, bool)) or value is None:
            return value
        return str(value)

    def _serialize_resume_evidence_units(
        self,
        units: list[Any],
        truncation: dict[str, Any],
    ) -> list[dict[str, Any]]:
        max_count = self._judge_limit(
            "evidence_units_max_count",
            DEFAULT_EVIDENCE_UNITS_MAX_COUNT,
        )
        evidence_max_chars = self._judge_limit(
            "evidence_unit_max_chars",
            DEFAULT_EVIDENCE_UNIT_MAX_CHARS,
        )
        if len(units) > max_count:
            truncation["truncated"] = True
            truncation["fields"]["resume_evidence_units"] = {
                "truncated": True,
                "original_count": len(units),
                "included_count": max_count,
            }
        payload: list[dict[str, Any]] = []
        for index, unit in enumerate(units[:max_count], start=1):
            unit_id = f"ev_{index}"
            payload.append(
                {
                    "unit_id": unit_id,
                    "source_section": self._truncate(getattr(unit, "source_section", None), 80),
                    "source_text": self._truncate_with_metadata(
                        getattr(unit, "source_text", None),
                        evidence_max_chars,
                        truncation,
                        f"resume_evidence_units.{unit_id}.source_text",
                    ),
                    "tags": self._safe_json_object(getattr(unit, "tags", None)),
                    "years_value": self._float(getattr(unit, "years_value", None)),
                    "years_context": self._truncate(getattr(unit, "years_context", None), 120),
                    "is_total_years_claim": bool(getattr(unit, "is_total_years_claim", False)),
                }
            )
        return payload

    def _truncate_with_metadata(
        self,
        value: Any,
        max_chars: int,
        truncation: dict[str, Any],
        field: str,
    ) -> str:
        text = "" if value is None else str(value)
        if len(text) <= max_chars:
            return text
        truncation["truncated"] = True
        truncation["fields"][field] = {
            "truncated": True,
            "original_chars": len(text),
            "included_chars": max_chars,
        }
        return self._truncate(text, max_chars)

    def _cap_json_payload(
        self,
        payload: dict[str, Any],
        max_chars: int,
        truncation: dict[str, Any],
        field: str,
    ) -> dict[str, Any]:
        encoded = json.dumps(payload, sort_keys=True, default=str)
        if len(encoded) <= max_chars:
            return payload
        truncation["truncated"] = True
        truncation["fields"][field] = {
            "truncated": True,
            "original_chars": len(encoded),
            "included_chars": max_chars,
        }
        return {"summary_text": self._truncate(encoded, max_chars)}

    @staticmethod
    def _safe_json_object(value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        safe: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)[:80]
            if isinstance(item, (str, int, float, bool)) or item is None:
                safe[key_text] = item
            elif isinstance(item, list):
                safe[key_text] = [
                    child
                    for child in item[:20]
                    if isinstance(child, (str, int, float, bool)) or child is None
                ]
        return safe

    def _initial_provider_identity(self) -> tuple[str, str]:
        provider_entries = self._configured_provider_entries()
        if provider_entries:
            entry = provider_entries[0]
            return str(entry.name or entry.provider), str(entry.model or "")
        return str(self.llm_config.provider), str(self.llm_config.model)

    def _create_pending_evaluation(
        self,
        *,
        match: JobMatch,
        owner_id: Any,
        tenant_id: Any | None,
        hashes: dict[str, str],
    ) -> LlmMatchEvaluation:
        provider_name, model_name = self._initial_provider_identity()
        evaluation = LlmMatchEvaluation(
            owner_id=owner_id,
            tenant_id=tenant_id,
            job_post_id=match.job_post_id,
            job_match_id=match.id,
            resume_fingerprint=match.resume_fingerprint,
            provider=provider_name,
            model=model_name,
            prompt_version=str(self.judge_config.prompt_version),
            schema_version=str(self.judge_config.schema_version),
            judge_config_hash=hashes["judge_config_hash"],
            evidence_hash=hashes["evidence_hash"],
            input_hash=hashes["input_hash"],
            status=LLM_EVALUATION_PENDING,
            analysis={},
        )
        self.db.add(evaluation)
        return evaluation

    def _run_provider(
        self,
        evaluation: LlmMatchEvaluation,
        payload: dict[str, Any],
        *,
        truncation: dict[str, Any] | None = None,
    ) -> None:
        lifecycle_metadata = self._lifecycle_metadata(evaluation.analysis)
        evaluation.status = LLM_EVALUATION_RUNNING
        evaluation.started_at = self._utcnow()
        self.db.flush()
        try:
            provider = self._provider()
            serialized_payload = json.dumps(payload, sort_keys=True)
            raw = provider.extract_structured_data(
                text=serialized_payload,
                schema_spec=MATCH_LLM_JUDGE_SCHEMA_SPEC,
                system_prompt=MATCH_LLM_JUDGE_SYSTEM_PROMPT,
                user_message=(
                    "Evaluate the resume evidence against the job and return the requested JSON.\n\n"
                    "<JUDGE_INPUT_JSON>\n"
                    f"{serialized_payload}\n"
                    "</JUDGE_INPUT_JSON>"
                ),
            )
            parsed = MatchEvaluationResponse.model_validate(raw)
            evaluation.status = LLM_EVALUATION_SUCCEEDED
            success_metadata = self._provider_success_metadata(provider)
            if success_metadata:
                evaluation.provider = success_metadata["provider"]
                evaluation.model = success_metadata["model"]
            score_quality = score_quality_metadata(parsed.score, parsed.verdict)
            evaluation.llm_score = score_quality.get("normalized_score")
            evaluation.confidence = round(float(parsed.confidence), 4)
            evaluation.verdict = parsed.verdict
            evaluation.summary = self._truncate(parsed.summary, 1000)
            evaluation.reason_codes = self._safe_reason_codes(parsed.reason_codes)
            evaluation.requirement_verdicts = [
                item.model_dump()
                for item in self._ordered_requirement_verdicts(parsed.requirement_verdicts)[:50]
            ]
            evaluation.analysis = self._analysis_payload(
                parsed,
                truncation or {},
                provider_payload=payload,
                score_quality=score_quality,
                provider_attempts=self._provider_attempts(provider),
                lifecycle_metadata=lifecycle_metadata,
            )
            evaluation.error_code = None
            evaluation.retryable = False
        except Exception as exc:
            logger.warning(
                "LLM match evaluation failed for %s with %s",
                _sanitize_log(evaluation.id),
                self._provider_error_code(exc),
            )
            evaluation.status = LLM_EVALUATION_FAILED
            evaluation.llm_score = None
            evaluation.confidence = None
            evaluation.verdict = None
            evaluation.summary = None
            evaluation.reason_codes = []
            evaluation.requirement_verdicts = []
            evaluation.analysis = self._failure_analysis_payload(
                truncation or {},
                provider_attempts=self._provider_attempts(self._llm_provider),
                lifecycle_metadata=lifecycle_metadata,
            )
            evaluation.error_code = self._provider_error_code(exc)
            evaluation.retryable = self._provider_error_retryable(exc)
        finally:
            evaluation.completed_at = self._utcnow()
            self.db.flush()

    def _provider(self) -> LLMProvider:
        if self._llm_provider is not None:
            return self._llm_provider
        self._llm_provider = build_match_judge_provider(self.llm_config)
        if self._llm_provider is None:
            self._llm_provider = build_llm_provider(
                runtime_llm_config_from_match_judge(self.llm_config)
            )
        return self._llm_provider

    @staticmethod
    def _is_local_llm_endpoint(base_url: str) -> bool:
        lowered = base_url.lower()
        return any(
            host in lowered
            for host in ("localhost", "127.0.0.1", "host.docker.internal", "ollama")
        )

    @staticmethod
    def _unavailable_message(reason: str) -> str:
        messages = {
            "config_missing": "LLM judge configuration is missing.",
            "disabled": "LLM judge is disabled.",
            "runtime_missing": "LLM judge runtime configuration is missing.",
            "base_url_missing": "LLM judge provider base URL is missing.",
            "model_missing": "LLM judge model is missing.",
            "credentials_missing": (
                "LLM judge provider credentials are missing. "
                "Set NVIDIA_API_KEY, GROQ_API_KEY, CEREBRAS_API_KEY, "
                "or LLM_AS_A_JUDGE_API_KEY."
            ),
        }
        return messages.get(reason, "LLM judge is unavailable.")

    @staticmethod
    def _provider_error_code(exc: Exception) -> str:
        category = getattr(exc, "error_category", None) or classify_llm_provider_error(exc)
        if category == "input_too_large":
            return "llm_judge_input_too_large"
        if category == "rate_limit":
            return "llm_judge_token_quota_exceeded"
        if category == "timeout":
            return "llm_judge_provider_timeout"
        if category == "connection_error":
            return "llm_judge_provider_connection_error"
        if category == "server_error":
            return "llm_judge_provider_unavailable"
        if category == "invalid_auth":
            return "llm_judge_invalid_credentials"
        if category == "invalid_request":
            return "llm_judge_invalid_request"
        if category == "schema_error":
            return "llm_judge_invalid_schema_response"
        if category == "unsupported_model":
            return "llm_judge_unsupported_model"
        return "llm_judge_failed"

    @staticmethod
    def _provider_error_retryable(exc: Exception) -> bool:
        retryable = getattr(exc, "retryable", None)
        if retryable is not None:
            return bool(retryable)
        category = getattr(exc, "error_category", None) or classify_llm_provider_error(exc)
        return llm_error_is_retryable(category)

    @staticmethod
    def _provider_attempts(provider: LLMProvider | None) -> list[dict[str, Any]]:
        attempts = getattr(provider, "last_attempts", None)
        if not isinstance(attempts, list):
            return []
        sanitized: list[dict[str, Any]] = []
        for attempt in attempts[:8]:
            if not isinstance(attempt, dict):
                continue
            try:
                elapsed_ms = max(int(attempt.get("elapsed_ms") or 0), 0)
            except (TypeError, ValueError):
                elapsed_ms = 0
            sanitized.append(
                {
                    "provider": str(attempt.get("provider") or "")[:80],
                    "provider_type": str(attempt.get("provider_type") or "")[:80],
                    "model": str(attempt.get("model") or "")[:120],
                    "status": str(attempt.get("status") or "unknown")[:40],
                    "error_category": attempt.get("error_category"),
                    "retryable": bool(attempt.get("retryable", False)),
                    "elapsed_ms": elapsed_ms,
                }
            )
        return sanitized

    @staticmethod
    def _provider_success_metadata(provider: LLMProvider | None) -> dict[str, str] | None:
        success = getattr(provider, "last_success", None)
        if not isinstance(success, dict):
            return None
        provider_name = str(success.get("provider") or "").strip()
        model_name = str(success.get("model") or "").strip()
        if not provider_name or not model_name:
            return None
        return {"provider": provider_name, "model": model_name}

    @classmethod
    def _ordered_requirement_verdicts(
        cls,
        requirement_verdicts: list[RequirementEvaluation],
    ) -> list[RequirementEvaluation]:
        return sorted(requirement_verdicts, key=cls._requirement_verdict_sort_key)

    @staticmethod
    def _requirement_verdict_sort_key(item: RequirementEvaluation) -> tuple[int, int, str]:
        return _requirement_id_sort_key(getattr(item, "requirement_id", ""))

    def _is_reusable(self, evaluation: LlmMatchEvaluation) -> bool:
        if evaluation.status != LLM_EVALUATION_SUCCEEDED:
            return False
        if evaluation.completed_at is None:
            return True
        cutoff = self._utcnow() - timedelta(days=int(self.judge_config.reuse_ttl_days))
        return evaluation.completed_at >= cutoff

    def _check_daily_quota(self, owner_id: Any) -> None:
        if public_testing_quotas_enabled():
            try:
                consume_ephemeral_quota(owner_id, "llm_evaluations", default_limit=3)
            except EphemeralQuotaExceeded as exc:
                raise LlmJudgeQuotaExceededError(str(exc)) from exc
            except EphemeralQuotaUnavailable as exc:
                raise LlmJudgeUnavailableError(
                    str(exc),
                    reason="quota_backend_unavailable",
                ) from exc
            return
        since = self._utcnow() - timedelta(days=1)
        count = self.db.scalar(
            select(func.count(LlmMatchEvaluation.id)).where(
                LlmMatchEvaluation.owner_id == owner_id,
                LlmMatchEvaluation.created_at >= since,
            )
        ) or 0
        if int(count) >= int(self.judge_config.max_per_owner_per_day):
            raise LlmJudgeQuotaExceededError("Daily LLM judge quota exceeded.")

    def _tombstone(self, evaluation: LlmMatchEvaluation) -> None:
        evaluation.status = LLM_EVALUATION_DELETED
        evaluation.llm_score = None
        evaluation.confidence = None
        evaluation.verdict = None
        evaluation.summary = None
        evaluation.reason_codes = []
        evaluation.requirement_verdicts = []
        evaluation.analysis = {}
        evaluation.error_code = None
        evaluation.retryable = False
        evaluation.deleted_at = self._utcnow()

    def _analysis_payload(
        self,
        parsed: MatchEvaluationResponse,
        truncation: dict[str, Any],
        *,
        provider_payload: dict[str, Any] | None = None,
        score_quality: dict[str, Any] | None = None,
        provider_attempts: list[dict[str, Any]] | None = None,
        lifecycle_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "transferable_strengths": [
                self._truncate(item, 240)
                for item in parsed.transferable_strengths[:8]
                if str(item).strip()
            ],
            "gaps": [
                self._truncate(item, 240)
                for item in parsed.gaps[:8]
                if str(item).strip()
            ],
            "ranking_rationale": self._truncate(
                parsed.ranking_rationale,
                self._judge_limit("public_analysis_max_chars", 2_000),
            ),
            "input_truncation": truncation if isinstance(truncation, dict) else {},
        }
        if score_quality:
            payload["score_quality"] = score_quality
        evidence_references = self._evidence_references_for_public_analysis(
            parsed,
            provider_payload if isinstance(provider_payload, dict) else {},
        )
        if evidence_references:
            payload["evidence_references"] = evidence_references
        if provider_attempts:
            payload["provider_attempts"] = provider_attempts
        if lifecycle_metadata:
            payload.update(lifecycle_metadata)
        return payload

    def _evidence_references_for_public_analysis(
        self,
        parsed: MatchEvaluationResponse,
        provider_payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        referenced_ids = self._referenced_evidence_unit_ids(parsed)
        if not referenced_ids:
            return []
        evidence_by_id: dict[str, dict[str, Any]] = {}
        units = provider_payload.get("resume_evidence_units")
        if isinstance(units, list):
            for unit in units:
                if not isinstance(unit, dict):
                    continue
                unit_id = self._normalized_evidence_reference_id(unit.get("unit_id"))
                if not unit_id:
                    continue
                evidence_by_id[unit_id] = {
                    "id": unit_id,
                    "source_section": self._truncate(unit.get("source_section"), 80),
                    "source_text": self._truncate(unit.get("source_text"), 240),
                }
        references: list[dict[str, Any]] = []
        for unit_id in referenced_ids[:20]:
            reference = evidence_by_id.get(unit_id)
            if reference is None:
                references.append({"id": unit_id})
            else:
                references.append(reference)
        return references

    @classmethod
    def _referenced_evidence_unit_ids(cls, parsed: MatchEvaluationResponse) -> list[str]:
        texts: list[str] = [
            parsed.summary,
            parsed.ranking_rationale,
            *parsed.transferable_strengths,
            *parsed.gaps,
            *(item.reason for item in parsed.requirement_verdicts),
        ]
        referenced: list[str] = []
        seen: set[str] = set()
        for text in texts:
            for match in EVIDENCE_REFERENCE_PATTERN.finditer(str(text or "")):
                unit_id = f"ev_{int(match.group(1))}"
                if unit_id not in seen:
                    referenced.append(unit_id)
                    seen.add(unit_id)
        return referenced

    @staticmethod
    def _normalized_evidence_reference_id(value: Any) -> str | None:
        match = EVIDENCE_REFERENCE_PATTERN.search(str(value or ""))
        if not match:
            return None
        return f"ev_{int(match.group(1))}"

    @staticmethod
    def _failure_analysis_payload(
        truncation: dict[str, Any],
        *,
        provider_attempts: list[dict[str, Any]] | None = None,
        lifecycle_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "input_truncation": truncation if isinstance(truncation, dict) else {},
        }
        if provider_attempts:
            payload["provider_attempts"] = provider_attempts
        if lifecycle_metadata:
            payload.update(lifecycle_metadata)
        return payload

    @staticmethod
    def _lifecycle_metadata(analysis: Any) -> dict[str, Any]:
        if not isinstance(analysis, dict):
            return {}
        preserved: dict[str, Any] = {}
        queue_metadata = analysis.get("queue")
        if isinstance(queue_metadata, dict):
            preserved["queue"] = {
                key: value
                for key, value in queue_metadata.items()
                if key in {
                    "enqueue_reason",
                    "queue_job_id",
                    "queue_state",
                    "queued_at",
                    "next_retry_at",
                    "retry_after_seconds",
                    "provider_status_message",
                }
            }
        for key in ("enqueue_reason", "queue_job_id"):
            if key in analysis:
                preserved[key] = analysis[key]
        return preserved

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _hash_json(payload: dict[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _truncate(value: Any, max_chars: int) -> str:
        if value is None:
            return ""
        text = str(value)
        if max_chars <= 3:
            return text[:max_chars]
        return text if len(text) <= max_chars else text[: max_chars - 3] + "..."

    @staticmethod
    def _safe_reason_codes(codes: list[str]) -> list[str]:
        safe: list[str] = []
        for code in codes[:20]:
            normalized = "".join(ch for ch in str(code).lower().replace(" ", "_") if ch.isalnum() or ch == "_")
            if normalized:
                safe.append(normalized[:64])
        return safe

    @staticmethod
    def _float(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return float(value)
        try:
            return float(value)
        except Exception:
            return None


def evaluation_public_dict(evaluation: LlmMatchEvaluation) -> dict[str, Any]:
    def _iso(value):
        return value.isoformat() if value is not None else None

    def _float(value):
        if value is None:
            return None
        return float(value)

    def _cap_public(value, max_chars: int = 2000):
        if isinstance(value, str):
            return value if len(value) <= max_chars else value[: max_chars - 3] + "..."
        if isinstance(value, list):
            return [_cap_public(item, max_chars) for item in value[:50]]
        if isinstance(value, dict):
            return {
                str(key)[:80]: _cap_public(item, max_chars)
                for key, item in list(value.items())[:50]
                if key not in {"provider_payload", "raw_payload", "prompt_payload"}
            }
        return value

    def _ordered_public_requirement_verdicts(value):
        if not isinstance(value, list):
            return []
        return sorted(
            [item for item in value[:50] if isinstance(item, dict)],
            key=lambda item: _requirement_id_sort_key(item.get("requirement_id")),
        )

    analysis = getattr(evaluation, "analysis", None)
    if not isinstance(analysis, dict):
        analysis = {}
    public_analysis = dict(analysis)
    queue_metadata = analysis.get("queue")
    if not isinstance(queue_metadata, dict):
        queue_metadata = {}
    effectiveness = getattr(evaluation, "llm_effectiveness", None)
    if not isinstance(effectiveness, dict):
        effectiveness = {}
    retry_after_seconds = queue_metadata.get("retry_after_seconds")
    try:
        retry_after_seconds = (
            None
            if retry_after_seconds is None
            else max(int(float(retry_after_seconds)), 0)
        )
    except (TypeError, ValueError):
        retry_after_seconds = None

    provider_status_message = queue_metadata.get("provider_status_message")
    if not isinstance(provider_status_message, str):
        provider_status_message = None
    if provider_status_message is None and evaluation.status == LLM_EVALUATION_FAILED:
        if evaluation.retryable:
            provider_status_message = "Retryable provider failure; this review can be queued again."
        elif evaluation.error_code == "llm_judge_token_quota_exceeded":
            provider_status_message = "Provider rate limit or token quota stopped this review."
        elif evaluation.error_code:
            provider_status_message = str(evaluation.error_code).replace("_", " ")
    score_quality = score_quality_metadata(evaluation.llm_score, evaluation.verdict)
    if isinstance(effectiveness.get("score_quality"), dict):
        score_quality = effectiveness["score_quality"]
    public_analysis["score_quality"] = score_quality

    return {
        "id": str(evaluation.id),
        "match_id": str(evaluation.job_match_id) if evaluation.job_match_id else None,
        "job_id": str(evaluation.job_post_id),
        "status": evaluation.status,
        "llm_score": (
            _float(score_quality.get("normalized_score"))
            if isinstance(score_quality, dict)
            and score_quality.get("normalized_score") is not None
            else normalize_llm_score(evaluation.llm_score, evaluation.verdict)
        ),
        "confidence": _float(evaluation.confidence),
        "verdict": evaluation.verdict,
        "summary": evaluation.summary,
        "reason_codes": evaluation.reason_codes if isinstance(evaluation.reason_codes, list) else [],
        "requirement_verdicts": _cap_public(
            _ordered_public_requirement_verdicts(evaluation.requirement_verdicts)
        ),
        "analysis": _cap_public(public_analysis),
        "score_quality": _cap_public(score_quality),
        "effective_for_rerank": bool(effectiveness.get("effective_for_rerank", False)),
        "ignored_for_rerank_reason": effectiveness.get("ignored_for_rerank_reason"),
        "stale_status": effectiveness.get("stale_status"),
        "freshness": _cap_public(effectiveness.get("freshness", {})),
        "input_truncation": _cap_public(effectiveness.get("input_truncation", {})),
        "provider": evaluation.provider,
        "model": evaluation.model,
        "prompt_version": evaluation.prompt_version,
        "schema_version": evaluation.schema_version,
        "error_code": evaluation.error_code,
        "retryable": bool(evaluation.retryable),
        "queued_reason": analysis.get("enqueue_reason") or queue_metadata.get("enqueue_reason"),
        "queue_job_id": analysis.get("queue_job_id") or queue_metadata.get("queue_job_id"),
        "queue_state": queue_metadata.get("queue_state"),
        "next_retry_at": queue_metadata.get("next_retry_at"),
        "retry_after_seconds": retry_after_seconds,
        "provider_status_message": provider_status_message,
        "created_at": _iso(evaluation.created_at),
        "started_at": _iso(evaluation.started_at),
        "completed_at": _iso(evaluation.completed_at),
    }
