"""SQL guards for embedding generation boundaries during incremental migrations."""

from sqlalchemy import and_, select
from sqlalchemy.sql.elements import ColumnElement

from core.embedding_contract import LEGACY_EMBEDDING_SPACE, current_embedding_space
from database.models import JobPost, JobRequirementUnit, JobRequirementUnitEmbedding


def job_embeddings_ready_clause() -> ColumnElement:
    space = current_embedding_space()
    same_space = JobPost.embedding_space == space
    if space == LEGACY_EMBEDDING_SPACE:
        return same_space
    missing_requirement = (
        select(JobRequirementUnit.id)
        .outerjoin(JobRequirementUnitEmbedding, and_(
            JobRequirementUnitEmbedding.job_requirement_unit_id == JobRequirementUnit.id,
            JobRequirementUnitEmbedding.embedding_space == space,
        ))
        .where(JobRequirementUnit.job_post_id == JobPost.id,
               JobRequirementUnitEmbedding.job_requirement_unit_id.is_(None))
        .correlate(JobPost)
        .exists()
    )
    return and_(same_space, ~missing_requirement)
