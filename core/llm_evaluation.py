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
  against the structured resume summary and owner-scoped resume evidence units.
- Recognize transferable evidence when technologies are closely related
  (for example Java and Kotlin), but explain the transfer rather than inventing
  direct experience.
- Treat a direct mention in either the structured resume summary or the resume
  evidence units as explicit resume evidence.
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
    transferable_strengths: list[str] = Field(default_factory=list)
    gaps: list[str] = Field(default_factory=list)
    ranking_rationale: str = ""


MATCH_LLM_JUDGE_SCHEMA_SPEC = {
    "name": MATCH_LLM_JUDGE_SCHEMA_NAME,
    "strict": True,
    "schema": MatchEvaluationResponse.model_json_schema(),
}

DEFAULT_JOB_DESCRIPTION_MAX_CHARS = 6_000
DEFAULT_REQUIREMENTS_MAX_COUNT = 40
DEFAULT_REQUIREMENT_TEXT_MAX_CHARS = 500
DEFAULT_EVIDENCE_UNITS_MAX_COUNT = 32
DEFAULT_EVIDENCE_UNITS_SCAN_MAX_COUNT = 200
DEFAULT_EVIDENCE_UNIT_MAX_CHARS = 450
DEFAULT_RESUME_SUMMARY_MAX_CHARS = 2_000


@dataclass(frozen=True)
class EvaluationResult:
    evaluation: LlmMatchEvaluation
    reused: bool = False

@dataclass(frozen=True)
class JudgeInput:
    provider_payload: dict[str, Any]
    cache_payload: dict[str, Any]
    hashes: dict[str, str]
    truncation: dict[str, Any]


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
        return self.judge_config.runtime

    def is_available(self) -> bool:
        base_url = str(self.llm_config.base_url or "").strip()
        model = str(self.llm_config.model or "").strip()
        has_auth = bool(
            getattr(self.llm_config, "api_key", None)
            or getattr(self.llm_config, "api_secret", None)
            or getattr(self.llm_config, "headers", None)
            or self._is_local_llm_endpoint(base_url)
        )
        return bool(
            self.judge_config.enabled
            and base_url
            and model
            and has_auth
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
        if not self.is_available():
            raise LlmJudgeUnavailableError("LLM judge is not configured.")

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
                "truncation": truncation,
            },
        }
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
                "input_truncation": {},
            }
        if getattr(evaluation, "deleted_at", None) is not None:
            return {
                "effective_for_rerank": False,
                "ignored_for_rerank_reason": "deleted",
                "stale_status": "ignored",
                "input_truncation": {},
            }
        status = getattr(evaluation, "status", None)
        if status != LLM_EVALUATION_SUCCEEDED:
            return {
                "effective_for_rerank": False,
                "ignored_for_rerank_reason": f"status_{status or 'unknown'}",
                "stale_status": "ignored",
                "input_truncation": {},
            }
        if getattr(evaluation, "llm_score", None) is None:
            return {
                "effective_for_rerank": False,
                "ignored_for_rerank_reason": "missing_llm_score",
                "stale_status": "ignored",
                "input_truncation": {},
            }

        job = self._load_job_for_match(match)
        current_job_hash = getattr(job, "content_hash", None)
        match_job_hash = getattr(match, "job_content_hash", None)
        if match_job_hash and current_job_hash and str(match_job_hash) != str(current_job_hash):
            return {
                "effective_for_rerank": False,
                "ignored_for_rerank_reason": "stale_job_content",
                "stale_status": "stale",
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
                    "input_truncation": judge_input.truncation,
                }

        return {
            "effective_for_rerank": True,
            "ignored_for_rerank_reason": None,
            "stale_status": "current",
            "input_truncation": judge_input.truncation,
        }

    def _judge_limit(self, name: str, default: int) -> int:
        value = getattr(self.judge_config, name, default)
        try:
            return max(1, int(value))
        except Exception:
            return default

    def _judge_config_payload(self) -> dict[str, Any]:
        return {
            "provider": str(self.llm_config.provider),
            "model": str(self.llm_config.model),
            "structured_output_mode": str(self.llm_config.structured_output_mode),
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
        public_summary = {
            key: self._compact_resume_value(summary[key])
            for key in (
                "headline",
                "summary",
                "skills",
                "experience",
                "projects",
                "education",
                "certifications",
                "languages",
            )
            if key in summary
        }
        profile = summary.get("profile")
        if isinstance(profile, dict):
            for key in (
                "headline",
                "summary",
                "skills",
                "experience",
                "projects",
                "education",
                "certifications",
                "languages",
            ):
                if key in profile and key not in public_summary:
                    public_summary[key] = self._compact_resume_value(profile[key])
        return public_summary

    def _compact_resume_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            if isinstance(value.get("all"), list):
                return {
                    "all": [
                        self._truncate(item.get("name"), 80)
                        for item in value["all"][:80]
                        if isinstance(item, dict) and str(item.get("name", "")).strip()
                    ]
                }
            return {
                str(key)[:80]: self._compact_resume_value(item)
                for key, item in list(value.items())[:20]
                if key not in {"raw_text", "source_text"}
            }
        if isinstance(value, list):
            return [self._compact_resume_value(item) for item in value[:12]]
        if isinstance(value, str):
            return self._truncate(value, 260)
        return value

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
            evaluation.llm_score = round(float(parsed.score), 2)
            evaluation.confidence = round(float(parsed.confidence), 4)
            evaluation.verdict = parsed.verdict
            evaluation.summary = self._truncate(parsed.summary, 1000)
            evaluation.reason_codes = self._safe_reason_codes(parsed.reason_codes)
            evaluation.requirement_verdicts = [
                item.model_dump() for item in parsed.requirement_verdicts[:50]
            ]
            evaluation.analysis = self._analysis_payload(parsed, truncation or {})
            evaluation.error_code = None
            evaluation.retryable = False
        except Exception as exc:
            logger.exception("LLM match evaluation failed for %s", _sanitize_log(evaluation.id))
            evaluation.status = LLM_EVALUATION_FAILED
            evaluation.llm_score = None
            evaluation.confidence = None
            evaluation.verdict = None
            evaluation.summary = None
            evaluation.reason_codes = []
            evaluation.requirement_verdicts = []
            evaluation.analysis = {}
            evaluation.error_code = self._provider_error_code(exc)
            evaluation.retryable = True
        finally:
            evaluation.completed_at = self._utcnow()
            self.db.flush()

    def _provider(self) -> LLMProvider:
        if self._llm_provider is not None:
            return self._llm_provider
        self._llm_provider = build_llm_provider(runtime_llm_config_from_match_judge(self.llm_config))
        return self._llm_provider

    @staticmethod
    def _is_local_llm_endpoint(base_url: str) -> bool:
        lowered = base_url.lower()
        return any(
            host in lowered
            for host in ("localhost", "127.0.0.1", "host.docker.internal", "ollama")
        )

    @staticmethod
    def _provider_error_code(exc: Exception) -> str:
        status_code = getattr(exc, "status_code", None)
        message = str(exc).lower()
        if status_code == 413 or "request too large" in message:
            return "llm_judge_input_too_large"
        return "llm_judge_failed"

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
        evaluation.analysis = {}
        evaluation.error_code = None
        evaluation.retryable = False
        evaluation.deleted_at = self._utcnow()

    def _analysis_payload(
        self,
        parsed: MatchEvaluationResponse,
        truncation: dict[str, Any],
    ) -> dict[str, Any]:
        return {
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

    analysis = getattr(evaluation, "analysis", None)
    if not isinstance(analysis, dict):
        analysis = {}
    effectiveness = getattr(evaluation, "llm_effectiveness", None)
    if not isinstance(effectiveness, dict):
        effectiveness = {}

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
        "analysis": _cap_public(analysis),
        "effective_for_rerank": bool(effectiveness.get("effective_for_rerank", False)),
        "ignored_for_rerank_reason": effectiveness.get("ignored_for_rerank_reason"),
        "stale_status": effectiveness.get("stale_status"),
        "input_truncation": _cap_public(effectiveness.get("input_truncation", {})),
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
