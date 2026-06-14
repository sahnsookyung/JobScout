"""Application service for native resume variant generation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from core.resume_variants.generator import (
    EVIDENCE_POLICY_VERSION,
    GENERATOR_VERSION,
    MAX_CONTENT_JSON_BYTES,
    RENDERER_VERSION,
    TEMPLATE_VERSION,
    generate_resume_variant_content,
)
from core.resume_variants.hashing import canonical_json_bytes, canonical_json_hash
from core.resume_variants.quota import ResumeVariantQuota
from database.models import JobMatchRequirement, JobPost, ResumeEvidenceUnitEmbedding, StructuredResume
from database.repositories.match import MatchRepository
from database.repositories.resume_variant import ResumeVariantRepository


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

    def __init__(self, db: Session, *, quota: ResumeVariantQuota | None = None) -> None:
        self.db = db
        self.quota = quota or ResumeVariantQuota()
        self.repo = ResumeVariantRepository(db)

    def create_for_match(
        self,
        *,
        match_id: Any,
        owner_id: Any,
        tenant_id: Any | None,
        request: ResumeVariantRequest,
    ) -> ResumeVariantResult:
        match, job, resume = self._load_sources(match_id=match_id, owner_id=owner_id, tenant_id=tenant_id)
        identity = self._identity(
            owner_id=owner_id,
            tenant_id=tenant_id,
            match=match,
            job=job,
            resume=resume,
            request=request,
        )

        if not request.force:
            existing = self.repo.find_current(identity)
            if existing is not None:
                return ResumeVariantResult(variant=existing, reused=True)

        owner_key = str(owner_id)
        with self.quota.lease(owner_key) as lease:
            content, evidence_map, warnings = generate_resume_variant_content(
                resume_data=resume.extracted_data,
                job=job,
                match=match,
                requirement_matches=self._requirement_matches(match.id),
                resume_evidence_units=self._resume_evidence_units(
                    owner_id,
                    match.resume_fingerprint,
                ),
                template_key=request.template_key,
                tone=request.tone,
            )
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
                variant = self.repo.create(values)
                self.repo.prune_scope(owner_id=owner_id, tenant_id=tenant_id, keep_id=variant.id)
                self.db.commit()
            except IntegrityError:
                self.db.rollback()
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

    def _resume_evidence_units(self, owner_id: Any, resume_fingerprint: Any, *, limit: int = 200) -> list[Any]:
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
        except Exception:
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
    ) -> dict[str, Any]:
        source_match_updated_at = match.updated_at or match.calculated_at
        source_match_calculated_at = match.calculated_at or match.updated_at
        source_resume_updated_at = resume.updated_at or resume.created_at
        if source_match_updated_at is None or source_match_calculated_at is None or source_resume_updated_at is None:
            raise ResumeVariantConflict("Source records are missing freshness timestamps.")
        return {
            "owner_id": owner_id,
            "tenant_id": tenant_id,
            "match_id": match.id,
            "template_key": request.template_key,
            "template_version": TEMPLATE_VERSION,
            "generation_mode": "deterministic",
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


def variant_to_response(variant: Any, *, reused: bool | None = None, quota_status: dict[str, int] | None = None) -> dict[str, Any]:
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
