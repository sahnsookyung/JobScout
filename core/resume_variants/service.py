"""Application service for native resume variant generation."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, joinedload

from core.resume_variants.generator import (
    EVIDENCE_POLICY_VERSION,
    GENERATOR_VERSION,
    MAX_CONTENT_JSON_BYTES,
    RENDERER_VERSION,
    TEMPLATE_VERSION,
    build_evidence_map,
    generate_resume_variant_content,
    validate_resume_content_quality,
)
from core.resume_variants.hashing import canonical_json_bytes, canonical_json_hash
from core.resume_variants.llm_generator import EvidenceGroundedResumeGenerator
from core.resume_variants.quota import ResumeVariantQuota
from core.llm.provider_chain import LLMProviderChainError
from database.models import (
    JobMatchRequirement,
    JobPost,
    ResumeEvidenceUnitEmbedding,
    StructuredResume,
)
from database.repositories.match import MatchRepository
from database.repositories.resume_variant import ResumeVariantRepository

logger = logging.getLogger(__name__)


class ResumeVariantNotFound(Exception):
    """Raised when a variant or source match is not visible to the user."""


class ResumeVariantConflict(Exception):
    """Raised when generation is not allowed for the selected match."""


class ResumeVariantValidationError(Exception):
    """Raised when generation cannot produce a safe bounded variant."""


@dataclass(frozen=True)
class ResumeVariantRequest:
    template_key: str = "compact"
    tone: str = "concise"
    force: bool = False


@dataclass(frozen=True)
class ResumeVariantResult:
    variant: Any
    reused: bool
    quota_status: dict[str, int] | None = None


class ResumeVariantService:
    """Generate, persist, and read job-specific resume drafts."""

    def __init__(
        self,
        db: Session,
        *,
        quota: ResumeVariantQuota | None = None,
        llm_generator: EvidenceGroundedResumeGenerator | None = None,
    ) -> None:
        self.db = db
        self.quota = quota or ResumeVariantQuota()
        self.llm_generator = llm_generator
        self.repo = ResumeVariantRepository(db)

    def create_for_match(
        self,
        *,
        match_id: Any,
        owner_id: Any,
        tenant_id: Any | None,
        request: ResumeVariantRequest,
    ) -> ResumeVariantResult:
        match, job, resume = self._load_sources(
            match_id=match_id, owner_id=owner_id, tenant_id=tenant_id
        )
        if not _has_current_contact_schema(resume.extracted_data):
            raise ResumeVariantConflict(
                "This resume was processed with an older extraction schema. "
                "Re-upload it to include candidate contact details before generating a draft."
            )
        desired_generation_mode = (
            self.llm_generator.generation_mode
            if self.llm_generator is not None
            else "deterministic"
        )
        identity = self._identity(
            owner_id=owner_id,
            tenant_id=tenant_id,
            match=match,
            job=job,
            resume=resume,
            request=request,
            generation_mode=desired_generation_mode,
        )

        if not request.force:
            existing = self.repo.find_current(identity)
            if existing is not None:
                return ResumeVariantResult(variant=existing, reused=True)

        owner_key = str(owner_id)
        with self.quota.lease(owner_key) as lease:
            requirement_matches = self._requirement_matches(match.id)
            content, evidence_map, warnings = generate_resume_variant_content(
                resume_data=resume.extracted_data,
                job=job,
                match=match,
                requirement_matches=requirement_matches,
                resume_evidence_units=self._resume_evidence_units(
                    owner_id,
                    match.resume_fingerprint,
                ),
                template_key=request.template_key,
                tone=request.tone,
            )
            quality_errors = validate_resume_content_quality(content)
            if quality_errors:
                raise ResumeVariantConflict(
                    "The uploaded resume is too incomplete to generate a usable draft. "
                    "Re-upload a resume with experience, project, or education details."
                )

            actual_generation_mode = desired_generation_mode
            if self.llm_generator is not None:
                try:
                    tailored = self.llm_generator.generate(
                        content=content,
                        job=job,
                        requirement_matches=requirement_matches,
                    )
                except (LLMProviderChainError, ValidationError, TypeError, ValueError) as exc:
                    if not self.llm_generator.config.fallback_to_deterministic:
                        raise ResumeVariantConflict(
                            "AI resume tailoring is temporarily unavailable."
                        ) from exc
                    actual_generation_mode = "deterministic_fallback"
                    warnings.append(
                        "AI tailoring was unavailable; generated a complete evidence-grounded draft instead."
                    )
                    logger.warning(
                        "Resume tailoring failed; using deterministic fallback error_type=%s",
                        exc.__class__.__name__,
                    )
                else:
                    content = tailored.content
                    evidence_map = build_evidence_map(content)
                    warnings.extend(tailored.warnings)
                    logger.info(
                        "Resume tailoring completed provider=%s model=%s "
                        "prompt_version=%s applied_claims=%d rejected_claims=%d",
                        tailored.provider,
                        tailored.model,
                        self.llm_generator.config.prompt_version,
                        tailored.applied_claim_count,
                        tailored.rejected_claim_count,
                    )

            if actual_generation_mode != desired_generation_mode:
                identity = self._identity(
                    owner_id=owner_id,
                    tenant_id=tenant_id,
                    match=match,
                    job=job,
                    resume=resume,
                    request=request,
                    generation_mode=actual_generation_mode,
                )
                if not request.force:
                    existing = self.repo.find_current(identity)
                    if existing is not None:
                        return ResumeVariantResult(variant=existing, reused=True)
            lease.assert_owned()
            self._validate_size(content)
            values = {
                **identity,
                "job_post_id": match.job_post_id,
                "resume_fingerprint": match.resume_fingerprint,
                "content_json": content,
                "evidence_map": evidence_map,
                "warnings": warnings,
            }
            try:
                variant = (
                    self.repo.replace_current(identity, values)
                    if request.force
                    else None
                )
                if variant is None:
                    variant = self.repo.create(values)
                self.repo.prune_scope(owner_id=owner_id, tenant_id=tenant_id, keep_id=variant.id)
                self.db.commit()
            except IntegrityError:
                self.db.rollback()
                if request.force:
                    variant = self.repo.replace_current(identity, values)
                    if variant is None:
                        raise
                    self.repo.prune_scope(
                        owner_id=owner_id,
                        tenant_id=tenant_id,
                        keep_id=variant.id,
                    )
                    self.db.commit()
                else:
                    existing = self.repo.find_current(identity)
                    if existing is None:
                        raise
                    return ResumeVariantResult(variant=existing, reused=True)

        quota_status = None
        if lease.status is not None:
            quota_status = {
                "daily_remaining": lease.status.daily_remaining,
                "hourly_remaining": lease.status.hourly_remaining,
            }
        return ResumeVariantResult(variant=variant, reused=False, quota_status=quota_status)

    def get_variant(self, *, variant_id: Any, owner_id: Any, tenant_id: Any | None):
        variant = self.repo.get_for_owner(variant_id, owner_id=owner_id, tenant_id=tenant_id)
        if variant is None:
            raise ResumeVariantNotFound("Resume variant not found.")
        return variant

    def list_for_match(
        self,
        *,
        match_id: Any,
        owner_id: Any,
        tenant_id: Any | None,
        limit: int = 50,
    ) -> list[Any]:
        self._load_sources(
            match_id=match_id,
            owner_id=owner_id,
            tenant_id=tenant_id,
            require_active=False,
        )
        return self.repo.list_for_match(
            owner_id=owner_id,
            tenant_id=tenant_id,
            match_id=match_id,
            limit=limit,
        )

    def _load_sources(
        self,
        *,
        match_id: Any,
        owner_id: Any,
        tenant_id: Any | None,
        require_active: bool = True,
    ):
        match = MatchRepository(self.db).get_match_by_id_for_owner(match_id, owner_id)
        if match is None:
            raise ResumeVariantNotFound("Match not found.")
        if require_active and match.status != "active":
            raise ResumeVariantConflict("Resume variants can only be generated for active matches.")

        job = self.db.get(JobPost, match.job_post_id)
        if job is None:
            raise ResumeVariantNotFound("Job not found.")
        if tenant_id is not None and str(job.tenant_id) != str(tenant_id):
            raise ResumeVariantNotFound("Match not found.")
        if tenant_id is None and job.tenant_id is not None:
            raise ResumeVariantNotFound("Match not found.")

        resume = self.db.execute(
            select(StructuredResume).where(
                StructuredResume.owner_id == owner_id,
                StructuredResume.resume_fingerprint == match.resume_fingerprint,
            )
        ).scalar_one_or_none()
        if resume is None:
            raise ResumeVariantConflict("Structured resume is not available for this match.")
        return match, job, resume

    def _requirement_matches(self, match_id: Any) -> list[Any]:
        return list(
            self.db.execute(
                select(JobMatchRequirement)
                .options(joinedload(JobMatchRequirement.requirement))
                .where(JobMatchRequirement.job_match_id == match_id)
            ).scalars()
        )

    def _resume_evidence_units(
        self, owner_id: Any, resume_fingerprint: Any, *, limit: int = 200
    ) -> list[Any]:
        try:
            return list(
                self.db.execute(
                    select(ResumeEvidenceUnitEmbedding)
                    .where(
                        ResumeEvidenceUnitEmbedding.owner_id == owner_id,
                        ResumeEvidenceUnitEmbedding.resume_fingerprint == resume_fingerprint,
                    )
                    .order_by(
                        ResumeEvidenceUnitEmbedding.source_section.asc(),
                        ResumeEvidenceUnitEmbedding.evidence_unit_id.asc(),
                        ResumeEvidenceUnitEmbedding.created_at.asc(),
                    )
                    .limit(limit)
                ).scalars()
            )
        except SQLAlchemyError as exc:
            logger.warning(
                "Resume evidence lookup failed; generation will use structured resume data error_type=%s",
                exc.__class__.__name__,
            )
            return []

    def _identity(
        self,
        *,
        owner_id: Any,
        tenant_id: Any | None,
        match: Any,
        job: Any,
        resume: Any,
        request: ResumeVariantRequest,
        generation_mode: str = "deterministic",
    ) -> dict[str, Any]:
        source_match_updated_at = match.updated_at or match.calculated_at
        source_match_calculated_at = match.calculated_at or match.updated_at
        source_resume_updated_at = resume.updated_at or resume.created_at
        if (
            source_match_updated_at is None
            or source_match_calculated_at is None
            or source_resume_updated_at is None
        ):
            raise ResumeVariantConflict("Source records are missing freshness timestamps.")
        return {
            "owner_id": owner_id,
            "tenant_id": tenant_id,
            "match_id": match.id,
            "template_key": request.template_key,
            "template_version": TEMPLATE_VERSION,
            "generation_mode": generation_mode,
            "tone": request.tone,
            "generator_version": GENERATOR_VERSION,
            "renderer_version": RENDERER_VERSION,
            "evidence_policy_version": EVIDENCE_POLICY_VERSION,
            "source_match_updated_at": source_match_updated_at,
            "source_match_calculated_at": source_match_calculated_at,
            "source_job_content_hash": match.job_content_hash or job.content_hash or "",
            "source_resume_updated_at": source_resume_updated_at,
            "source_resume_content_hash": canonical_json_hash(resume.extracted_data),
        }

    def _validate_size(self, content: dict[str, Any]) -> None:
        if len(canonical_json_bytes(content)) > MAX_CONTENT_JSON_BYTES:
            raise ResumeVariantValidationError("Generated resume variant exceeds size limit.")


def variant_to_response(
    variant: Any, *, reused: bool | None = None, quota_status: dict[str, int] | None = None
) -> dict[str, Any]:
    payload = {
        "id": str(variant.id),
        "match_id": str(variant.match_id),
        "job_post_id": str(variant.job_post_id),
        "template_key": variant.template_key,
        "generation_mode": variant.generation_mode,
        "created_at": variant.created_at.isoformat() if variant.created_at else None,
        "content": variant.content_json,
        "evidence_map": variant.evidence_map,
        "warnings": variant.warnings,
        "download_formats": ["markdown", "html", "docx"],
    }
    if reused is not None:
        payload["reused"] = reused
    if quota_status is not None:
        payload["quota_status"] = quota_status
    return payload


def content_size(value: Any) -> int:
    return len(json.dumps(value, default=str).encode("utf-8"))


def _has_current_contact_schema(resume_data: Any) -> bool:
    if not isinstance(resume_data, dict):
        return False
    profile = resume_data.get("profile")
    if not isinstance(profile, dict):
        return False
    contact = profile.get("contact")
    return isinstance(contact, dict) and bool(str(contact.get("name") or "").strip())
