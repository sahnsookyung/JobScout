from __future__ import annotations

from typing import Any

from sqlalchemy import and_, delete, select
from sqlalchemy.orm import Session

from database.models import ResumeVariant


class ResumeVariantRepository:
    """Persistence boundary for generated resume variants."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def get_for_owner(
        self,
        variant_id: Any,
        *,
        owner_id: Any,
        tenant_id: Any | None,
    ) -> ResumeVariant | None:
        stmt = select(ResumeVariant).where(
            ResumeVariant.id == variant_id,
            ResumeVariant.owner_id == owner_id,
            _tenant_clause(tenant_id),
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def list_for_match(
        self,
        *,
        owner_id: Any,
        tenant_id: Any | None,
        match_id: Any,
        limit: int = 50,
    ) -> list[ResumeVariant]:
        stmt = (
            select(ResumeVariant)
            .where(
                ResumeVariant.owner_id == owner_id,
                ResumeVariant.match_id == match_id,
                _tenant_clause(tenant_id),
            )
            .order_by(ResumeVariant.created_at.desc(), ResumeVariant.id.desc())
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars())

    def find_current(self, identity: dict[str, Any]) -> ResumeVariant | None:
        stmt = select(ResumeVariant).where(
            *[getattr(ResumeVariant, key) == value for key, value in identity.items() if key != "tenant_id"],
            _tenant_clause(identity.get("tenant_id")),
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def create(self, values: dict[str, Any]) -> ResumeVariant:
        variant = ResumeVariant(**values)
        self.db.add(variant)
        self.db.flush()
        return variant

    def replace_current(
        self,
        identity: dict[str, Any],
        values: dict[str, Any],
    ) -> ResumeVariant | None:
        """Replace generated payload fields for the current identity in place."""
        variant = self.find_current(identity)
        if variant is None:
            return None
        variant.job_post_id = values["job_post_id"]
        variant.resume_fingerprint = values["resume_fingerprint"]
        variant.content_json = values["content_json"]
        variant.evidence_map = values["evidence_map"]
        variant.warnings = values["warnings"]
        self.db.flush()
        return variant

    def prune_scope(
        self,
        *,
        owner_id: Any,
        tenant_id: Any | None,
        keep_id: Any,
        max_variants: int = 25,
    ) -> int:
        stmt = (
            select(ResumeVariant.id)
            .where(ResumeVariant.owner_id == owner_id, _tenant_clause(tenant_id))
            .order_by(ResumeVariant.created_at.asc(), ResumeVariant.id.asc())
        )
        ids = list(self.db.execute(stmt).scalars())
        overflow = len(ids) - max_variants
        if overflow <= 0:
            return 0

        delete_ids = [variant_id for variant_id in ids if str(variant_id) != str(keep_id)][:overflow]
        if not delete_ids:
            return 0
        self.db.execute(delete(ResumeVariant).where(ResumeVariant.id.in_(delete_ids)))
        self.db.flush()
        return len(delete_ids)


def _tenant_clause(tenant_id: Any | None):
    if tenant_id is None:
        return ResumeVariant.tenant_id.is_(None)
    return and_(ResumeVariant.tenant_id == tenant_id)
