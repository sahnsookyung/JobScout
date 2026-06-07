"""Match-level LLM evaluation cache and judge service."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from core.config_loader import load_config
from core.llm.interfaces import LLMProvider
from core.llm.provider_factory import build_llm_provider, runtime_llm_config_from_fit
from core.redis_streams import _sanitize_log
from database.models import (
    LLM_EVALUATION_DELETED,
    LLM_EVALUATION_FAILED,
    LLM_EVALUATION_PENDING,
    LLM_EVALUATION_RUNNING,
    LLM_EVALUATION_SUCCEEDED,
    JobMatch,
    JobMatchRequirement,
    JobPost,
    LlmMatchEvaluation,
    StructuredResume,
)

logger = logging.getLogger(__name__)

MATCH_LLM_JUDGE_SCHEMA_NAME = "match_llm_judge_v1"
MATCH_LLM_JUDGE_SYSTEM_PROMPT = """
You are a careful resume-to-job relevance judge.

Task
- Review the provided deterministic match evidence.
- Judge whether the job is relevant to the candidate based only on the supplied data.
- Do not invent experience, credentials, salary, location, or authorization facts.
- Prefer concise, user-safe explanations.

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


MATCH_LLM_JUDGE_SCHEMA_SPEC = {
    "name": MATCH_LLM_JUDGE_SCHEMA_NAME,
    "strict": True,
    "schema": MatchEvaluationResponse.model_json_schema(),
}


@dataclass(frozen=True)
class EvaluationResult:
    evaluation: LlmMatchEvaluation
    reused: bool = False


class LlmJudgeUnavailableError(RuntimeError):
    """Raised when match-level LLM judging is unavailable."""


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
        return self.config.matching.scorer.semantic_fit.llm

    def is_available(self) -> bool:
        return bool(
            self.judge_config.enabled
            and self.llm_config.enabled
            and str(self.llm_config.base_url or "").strip()
            and str(self.llm_config.model or "").strip()
        )

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
        return list(self.db.execute(stmt).scalars().all())

    def generate_for_match(
        self,
        match_id: Any,
        *,
        owner_id: Any,
        tenant_id: Any | None = None,
        force: bool = False,
    ) -> EvaluationResult:
        if not self.is_available():
            raise LlmJudgeUnavailableError("LLM judge is not configured.")

        match = self._get_match_for_owner(match_id, owner_id=owner_id, tenant_id=tenant_id)
        effective_tenant_id = self._effective_tenant_id(match, tenant_id)
        payload, hashes = self._build_hash_payload(match)
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

        self._run_provider(evaluation, payload)
        self.db.commit()
        return EvaluationResult(evaluation, reused=False)

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
            return {"attempted": 0, "reused": 0, "created": 0, "failed": 0}

        from database.models import MatchSelectionItem

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
        stats = {"attempted": 0, "reused": 0, "created": 0, "failed": 0}
        for item in items:
            match = item.job_match
            if match is None:
                continue
            stats["attempted"] += 1
            try:
                result = self.generate_for_match(
                    match.id,
                    owner_id=owner_id,
                    tenant_id=tenant_id,
                    force=False,
                )
                if result.reused:
                    stats["reused"] += 1
                else:
                    stats["created"] += 1
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

    def _build_hash_payload(self, match: JobMatch) -> tuple[dict[str, Any], dict[str, str]]:
        requirements = (
            self.db.query(JobMatchRequirement)
            .options(joinedload(JobMatchRequirement.requirement))
            .filter(JobMatchRequirement.job_match_id == match.id)
            .order_by(JobMatchRequirement.created_at.asc(), JobMatchRequirement.id.asc())
            .all()
        )
        job = match.job_post
        requirement_payload = [
            {
                "requirement_id": str(req.job_requirement_unit_id),
                "requirement_text": self._truncate(getattr(req.requirement, "text", None), 600),
                "req_type": req.req_type,
                "evidence_text": self._truncate(req.evidence_text, 900),
                "evidence_section": self._truncate(req.evidence_section, 80),
                "similarity_score": self._float(req.similarity_score),
                "evidence_score": self._float(req.evidence_score),
                "is_covered": bool(req.is_covered),
            }
            for req in requirements
        ]
        payload = {
            "resume_fingerprint": match.resume_fingerprint,
            "job": {
                "job_id": str(match.job_post_id),
                "title": self._truncate(getattr(job, "title", None), 200),
                "company": self._truncate(getattr(job, "company", None), 200),
                "location": self._truncate(getattr(job, "location_text", None), 200),
                "is_remote": bool(getattr(job, "is_remote", False)),
                "content_hash": getattr(job, "content_hash", None),
                "summary": self._truncate(
                    getattr(job, "canonical_job_summary", None) or getattr(job, "description", None),
                    1800,
                ),
            },
            "match": {
                "fit_score": self._float(match.fit_score),
                "preference_score": self._float(match.preference_score),
                "required_coverage": self._float(match.required_coverage),
                "preferred_requirement_coverage": self._float(match.preferred_requirement_coverage),
                "penalties": self._float(match.penalties),
                "fit_components": self._safe_components(match.fit_components),
                "preference_components": self._safe_components(match.preference_components),
            },
            "requirements": requirement_payload,
        }
        judge_config_payload = {
            "provider": self.llm_config.provider,
            "model": self.llm_config.model,
            "prompt_version": self.judge_config.prompt_version,
            "schema_version": self.judge_config.schema_version,
        }
        evidence_hash = self._hash_json(payload)
        judge_config_hash = self._hash_json(judge_config_payload)
        input_hash = self._hash_json(
            {
                "payload": payload,
                "judge_config": judge_config_payload,
            }
        )
        return payload, {
            "evidence_hash": evidence_hash,
            "judge_config_hash": judge_config_hash,
            "input_hash": input_hash,
        }

    def _create_pending_evaluation(
        self,
        *,
        match: JobMatch,
        owner_id: Any,
        tenant_id: Any | None,
        hashes: dict[str, str],
    ) -> LlmMatchEvaluation:
        evaluation = LlmMatchEvaluation(
            owner_id=owner_id,
            tenant_id=tenant_id,
            job_post_id=match.job_post_id,
            job_match_id=match.id,
            resume_fingerprint=match.resume_fingerprint,
            provider=str(self.llm_config.provider),
            model=str(self.llm_config.model),
            prompt_version=str(self.judge_config.prompt_version),
            schema_version=str(self.judge_config.schema_version),
            judge_config_hash=hashes["judge_config_hash"],
            evidence_hash=hashes["evidence_hash"],
            input_hash=hashes["input_hash"],
            status=LLM_EVALUATION_PENDING,
        )
        self.db.add(evaluation)
        return evaluation

    def _run_provider(self, evaluation: LlmMatchEvaluation, payload: dict[str, Any]) -> None:
        evaluation.status = LLM_EVALUATION_RUNNING
        evaluation.started_at = self._utcnow()
        self.db.flush()
        try:
            provider = self._provider()
            raw = provider.extract_structured_data(
                text=json.dumps(payload, sort_keys=True),
                schema_spec=MATCH_LLM_JUDGE_SCHEMA_SPEC,
                system_prompt=MATCH_LLM_JUDGE_SYSTEM_PROMPT,
                user_message=(
                    "Evaluate this deterministic match record and return the requested JSON.\n\n"
                    f"{json.dumps(payload, sort_keys=True)}"
                ),
            )
            parsed = MatchEvaluationResponse.model_validate(raw)
            evaluation.status = LLM_EVALUATION_SUCCEEDED
            evaluation.llm_score = round(float(parsed.score), 2)
            evaluation.confidence = round(float(parsed.confidence), 4)
            evaluation.verdict = parsed.verdict
            evaluation.summary = self._truncate(parsed.summary, 1000)
            evaluation.reason_codes = self._safe_reason_codes(parsed.reason_codes)
            evaluation.requirement_verdicts = [
                item.model_dump() for item in parsed.requirement_verdicts[:50]
            ]
            evaluation.error_code = None
            evaluation.retryable = False
        except Exception:
            logger.exception("LLM match evaluation failed for %s", _sanitize_log(evaluation.id))
            evaluation.status = LLM_EVALUATION_FAILED
            evaluation.llm_score = None
            evaluation.confidence = None
            evaluation.verdict = None
            evaluation.summary = None
            evaluation.reason_codes = []
            evaluation.requirement_verdicts = []
            evaluation.error_code = "llm_judge_failed"
            evaluation.retryable = True
        finally:
            evaluation.completed_at = self._utcnow()
            self.db.flush()

    def _provider(self) -> LLMProvider:
        if self._llm_provider is not None:
            return self._llm_provider
        self._llm_provider = build_llm_provider(runtime_llm_config_from_fit(self.llm_config))
        return self._llm_provider

    def _is_reusable(self, evaluation: LlmMatchEvaluation) -> bool:
        if evaluation.status != LLM_EVALUATION_SUCCEEDED:
            return False
        if evaluation.completed_at is None:
            return True
        cutoff = self._utcnow() - timedelta(days=int(self.judge_config.reuse_ttl_days))
        return evaluation.completed_at >= cutoff

    def _check_daily_quota(self, owner_id: Any) -> None:
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
        evaluation.error_code = None
        evaluation.retryable = False
        evaluation.deleted_at = self._utcnow()

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
    def _safe_components(value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        allowed = {
            "fit_confidence",
            "semantic_fit_fallback_reason",
            "provider_route",
            "fit_scorer",
            "preference_mode_used",
            "preference_fallback_reason",
        }
        return {key: value[key] for key in allowed if key in value}

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

    return {
        "id": str(evaluation.id),
        "match_id": str(evaluation.job_match_id) if evaluation.job_match_id else None,
        "job_id": str(evaluation.job_post_id),
        "status": evaluation.status,
        "llm_score": _float(evaluation.llm_score),
        "confidence": _float(evaluation.confidence),
        "verdict": evaluation.verdict,
        "summary": evaluation.summary,
        "reason_codes": evaluation.reason_codes if isinstance(evaluation.reason_codes, list) else [],
        "requirement_verdicts": (
            evaluation.requirement_verdicts
            if isinstance(evaluation.requirement_verdicts, list)
            else []
        ),
        "provider": evaluation.provider,
        "model": evaluation.model,
        "prompt_version": evaluation.prompt_version,
        "schema_version": evaluation.schema_version,
        "error_code": evaluation.error_code,
        "retryable": bool(evaluation.retryable),
        "created_at": _iso(evaluation.created_at),
        "started_at": _iso(evaluation.started_at),
        "completed_at": _iso(evaluation.completed_at),
    }
