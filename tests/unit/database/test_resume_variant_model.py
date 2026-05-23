import pytest

from database.models import ResumeVariant


@pytest.mark.security
def test_resume_variant_uses_sensitive_data_cascades() -> None:
    foreign_keys = {
        column.name: next(iter(column.foreign_keys))
        for column in ResumeVariant.__table__.columns
        if column.foreign_keys
    }

    assert foreign_keys["owner_id"].ondelete == "CASCADE"
    assert foreign_keys["tenant_id"].ondelete == "CASCADE"
    assert foreign_keys["match_id"].ondelete == "CASCADE"
    assert foreign_keys["job_post_id"].ondelete == "CASCADE"


def test_resume_variant_has_nullable_tenant_safe_unique_indexes() -> None:
    indexes = {index.name: index for index in ResumeVariant.__table__.indexes}

    assert "uq_resume_variant_current_tenant" in indexes
    assert "uq_resume_variant_current_global" in indexes
    assert indexes["uq_resume_variant_current_tenant"].unique is True
    assert indexes["uq_resume_variant_current_global"].unique is True
    assert "tenant_id IS NOT NULL" in str(indexes["uq_resume_variant_current_tenant"].dialect_options["postgresql"]["where"])
    assert "tenant_id IS NULL" in str(indexes["uq_resume_variant_current_global"].dialect_options["postgresql"]["where"])


def test_resume_variant_has_listing_and_pruning_indexes() -> None:
    indexes = {index.name for index in ResumeVariant.__table__.indexes}

    assert "idx_resume_variant_owner_tenant_match" in indexes
    assert "idx_resume_variant_owner_created" in indexes
    assert "idx_resume_variant_match" in indexes
