#!/usr/bin/env python3
"""Baseline schema migration for the hard-reset migration cutover."""

from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from database.database import DATABASE_URL
from database.models import DEFAULT_LEGACY_OWNER_ID

DEFAULT_DEV_USER_EMAIL = "dev-user@jobscout.local"
DEFAULT_DEV_USER_NAME = "JobScout Dev User"


DDL_STATEMENTS = [
    """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'auth_provider') THEN
            CREATE TYPE auth_provider AS ENUM ('google', 'password');
        END IF;
    END
    $$;
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'upload_status') THEN
            CREATE TYPE upload_status AS ENUM ('pending', 'scanned', 'rejected', 'ready');
        END IF;
    END
    $$;
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'file_type') THEN
            CREATE TYPE file_type AS ENUM ('resume');
        END IF;
    END
    $$;
    """,
    """
    CREATE TABLE IF NOT EXISTS app_settings (
        id SERIAL PRIMARY KEY,
        key VARCHAR(255) NOT NULL,
        value TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_app_settings_key ON app_settings (key)",
    """
    CREATE TABLE IF NOT EXISTS tenant (
        id UUID PRIMARY KEY,
        name TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        display_name TEXT,
        email_verified_at TIMESTAMPTZ,
        is_active BOOLEAN NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)",
    """
    CREATE TABLE IF NOT EXISTS user_auth_identity (
        id UUID PRIMARY KEY,
        user_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
        provider auth_provider NOT NULL,
        provider_subject TEXT NOT NULL,
        email TEXT,
        email_normalized TEXT,
        email_verified BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        CONSTRAINT uq_user_auth_identity_provider_subject UNIQUE (provider, provider_subject)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_user_auth_identity_email ON user_auth_identity (email_normalized)",
    "CREATE INDEX IF NOT EXISTS idx_user_auth_identity_user_provider ON user_auth_identity (user_id, provider)",
    f"""
    INSERT INTO users (id, email, display_name, is_active)
    VALUES (
        '{DEFAULT_LEGACY_OWNER_ID}'::UUID,
        '{DEFAULT_DEV_USER_EMAIL}',
        '{DEFAULT_DEV_USER_NAME}',
        TRUE
    )
    ON CONFLICT (id) DO NOTHING
    """,
    f"""
    INSERT INTO user_auth_identity (
        id,
        user_id,
        provider,
        provider_subject,
        email,
        email_normalized,
        email_verified
    )
    VALUES (
        '11111111-1111-1111-1111-111111111111'::UUID,
        '{DEFAULT_LEGACY_OWNER_ID}'::UUID,
        'password',
        'dev-bypass:{DEFAULT_DEV_USER_EMAIL}',
        '{DEFAULT_DEV_USER_EMAIL}',
        '{DEFAULT_DEV_USER_EMAIL}',
        TRUE
    )
    ON CONFLICT (provider, provider_subject) DO NOTHING
    """,
    """
    CREATE TABLE IF NOT EXISTS resume_evidence_unit_embedding (
        id UUID PRIMARY KEY,
        owner_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
        fingerprint_version INTEGER NOT NULL,
        resume_fingerprint TEXT NOT NULL,
        evidence_unit_id TEXT NOT NULL,
        source_text TEXT NOT NULL,
        source_section TEXT,
        tags JSONB,
        embedding VECTOR(1024) NOT NULL,
        years_value FLOAT,
        years_context TEXT,
        is_total_years_claim BOOLEAN,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rfue_fingerprint ON resume_evidence_unit_embedding (resume_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_rfue_owner_resume ON resume_evidence_unit_embedding (owner_id, resume_fingerprint)",
    "CREATE INDEX IF NOT EXISTS ix_resume_evidence_unit_embedding_resume_fingerprint ON resume_evidence_unit_embedding (resume_fingerprint)",
    """
    CREATE INDEX IF NOT EXISTS idx_rfue_embedding_hnsw
    ON resume_evidence_unit_embedding
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    """,
    """
    CREATE TABLE IF NOT EXISTS resume_processing_state (
        resume_fingerprint TEXT PRIMARY KEY,
        owner_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
        fingerprint_version INTEGER NOT NULL,
        processing_status TEXT NOT NULL,
        last_error TEXT,
        failure_stage TEXT,
        failure_class TEXT,
        retryable BOOLEAN,
        user_safe_message TEXT,
        extraction_completed_at TIMESTAMPTZ,
        embedding_completed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_resume_processing_state_updated_at ON resume_processing_state (updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_resume_processing_state_status ON resume_processing_state (processing_status)",
    "CREATE INDEX IF NOT EXISTS idx_resume_processing_state_owner ON resume_processing_state (owner_id, updated_at)",
    """
    CREATE TABLE IF NOT EXISTS resume_upload (
        id UUID PRIMARY KEY,
        owner_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
        resume_hash TEXT NOT NULL,
        fingerprint_version INTEGER NOT NULL,
        resume_fingerprint TEXT NOT NULL,
        original_filename TEXT,
        status TEXT NOT NULL,
        last_error TEXT,
        failure_stage TEXT,
        failure_class TEXT,
        retryable BOOLEAN,
        user_safe_message TEXT,
        failure_debug_context JSONB,
        processing_task_id TEXT,
        retry_of_upload_id UUID,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_resume_upload_owner_created ON resume_upload (owner_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_resume_upload_owner_hash ON resume_upload (owner_id, resume_hash)",
    "CREATE INDEX IF NOT EXISTS idx_resume_upload_owner_fingerprint ON resume_upload (owner_id, resume_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_resume_upload_processing_task_id ON resume_upload (processing_task_id)",
    """
    CREATE TABLE IF NOT EXISTS resume_section_embedding (
        id UUID PRIMARY KEY,
        owner_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
        fingerprint_version INTEGER NOT NULL,
        resume_fingerprint TEXT NOT NULL,
        section_type TEXT NOT NULL,
        section_index INTEGER NOT NULL,
        source_text TEXT NOT NULL,
        source_data JSONB NOT NULL,
        embedding VECTOR(1024) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rse_resume ON resume_section_embedding (resume_fingerprint, section_type, section_index)",
    "CREATE INDEX IF NOT EXISTS idx_rse_owner_resume ON resume_section_embedding (owner_id, resume_fingerprint)",
    "CREATE INDEX IF NOT EXISTS ix_resume_section_embedding_resume_fingerprint ON resume_section_embedding (resume_fingerprint)",
    """
    CREATE INDEX IF NOT EXISTS idx_rse_embedding_hnsw
    ON resume_section_embedding
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    """,
    """
    CREATE TABLE IF NOT EXISTS structured_resume (
        id UUID PRIMARY KEY,
        owner_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
        fingerprint_version INTEGER NOT NULL,
        resume_fingerprint TEXT NOT NULL,
        extracted_data JSONB NOT NULL,
        total_experience_years NUMERIC(4, 1),
        extraction_confidence NUMERIC(3, 2),
        extraction_warnings JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_structured_resume_years ON structured_resume (total_experience_years)",
    "CREATE INDEX IF NOT EXISTS idx_structured_resume_fingerprint ON structured_resume (resume_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_structured_resume_owner_resume ON structured_resume (owner_id, resume_fingerprint)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_structured_resume_resume_fingerprint ON structured_resume (resume_fingerprint)",
    """
    CREATE TABLE IF NOT EXISTS user_wants (
        id UUID PRIMARY KEY,
        owner_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
        wants_text TEXT NOT NULL,
        embedding VECTOR(1024) NOT NULL,
        facet_key TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_user_wants_owner ON user_wants (owner_id)",
    """
    CREATE TABLE IF NOT EXISTS job_post (
        id UUID PRIMARY KEY,
        tenant_id UUID REFERENCES tenant (id) ON DELETE CASCADE,
        title TEXT NOT NULL,
        company TEXT NOT NULL,
        location_text TEXT,
        is_remote BOOLEAN,
        canonical_fingerprint TEXT NOT NULL,
        fingerprint_version INTEGER NOT NULL,
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        status TEXT NOT NULL,
        is_extracted BOOLEAN NOT NULL,
        is_embedded BOOLEAN NOT NULL,
        extraction_status TEXT NOT NULL,
        extraction_attempts INTEGER NOT NULL,
        extraction_last_error TEXT,
        extraction_last_attempt_at TIMESTAMPTZ,
        extraction_next_retry_at TIMESTAMPTZ,
        embedding_status TEXT NOT NULL,
        embedding_attempts INTEGER NOT NULL,
        embedding_last_error TEXT,
        embedding_last_attempt_at TIMESTAMPTZ,
        embedding_next_retry_at TIMESTAMPTZ,
        facet_status TEXT,
        facet_claimed_by TEXT,
        facet_claimed_at TIMESTAMPTZ,
        facet_extraction_hash TEXT,
        facet_retry_count INTEGER,
        facet_last_error TEXT,
        job_type TEXT,
        job_level TEXT,
        currency TEXT,
        salary_min NUMERIC,
        salary_max NUMERIC,
        salary_interval TEXT,
        min_years_experience INTEGER,
        requires_degree BOOLEAN,
        security_clearance BOOLEAN,
        description TEXT,
        skills_raw TEXT,
        raw_payload JSONB NOT NULL,
        content_hash TEXT,
        emails TEXT,
        company_industry TEXT,
        company_url TEXT,
        company_logo TEXT,
        company_url_direct TEXT,
        company_addresses TEXT,
        company_num_employees TEXT,
        company_revenue TEXT,
        company_description TEXT,
        experience_range TEXT,
        company_rating NUMERIC,
        company_reviews_count INTEGER,
        vacancy_count INTEGER,
        work_from_home_type TEXT,
        summary_embedding VECTOR(1024),
        CONSTRAINT uq_job_post_fingerprint UNIQUE (tenant_id, fingerprint_version, canonical_fingerprint)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_job_post_company ON job_post (company)",
    "CREATE INDEX IF NOT EXISTS idx_job_post_tenant ON job_post (tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_job_post_embedding_retry ON job_post (embedding_status, embedding_next_retry_at)",
    "CREATE INDEX IF NOT EXISTS idx_job_post_last_seen ON job_post (last_seen_at)",
    "CREATE INDEX IF NOT EXISTS idx_job_post_extraction_retry ON job_post (extraction_status, extraction_next_retry_at)",
    "CREATE INDEX IF NOT EXISTS idx_job_post_remote ON job_post (is_remote)",
    "CREATE INDEX IF NOT EXISTS idx_job_post_content_hash ON job_post (content_hash)",
    """
    CREATE INDEX IF NOT EXISTS idx_job_post_summary_embedding_hnsw
    ON job_post
    USING hnsw (summary_embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    """,
    """
    CREATE TABLE IF NOT EXISTS user_files (
        id UUID PRIMARY KEY,
        owner_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
        original_filename TEXT NOT NULL,
        mime_type TEXT NOT NULL,
        size_bytes BIGINT NOT NULL,
        storage_key TEXT NOT NULL UNIQUE,
        upload_status upload_status NOT NULL DEFAULT 'pending'::upload_status,
        file_type file_type NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_user_files_owner_type ON user_files (owner_id, file_type)",
    "CREATE INDEX IF NOT EXISTS idx_user_files_storage_key ON user_files (storage_key)",
    "CREATE INDEX IF NOT EXISTS idx_user_files_created_at ON user_files (created_at)",
    """
    CREATE TABLE IF NOT EXISTS job_benefit (
        id UUID PRIMARY KEY,
        job_post_id UUID NOT NULL REFERENCES job_post (id) ON DELETE CASCADE,
        category TEXT NOT NULL,
        text TEXT NOT NULL,
        ordinal INTEGER,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_jb_job ON job_benefit (job_post_id)",
    "CREATE INDEX IF NOT EXISTS idx_jb_category ON job_benefit (category)",
    """
    CREATE TABLE IF NOT EXISTS job_facet_embedding (
        id UUID PRIMARY KEY,
        job_post_id UUID NOT NULL REFERENCES job_post (id) ON DELETE CASCADE,
        facet_key TEXT NOT NULL,
        facet_text TEXT NOT NULL,
        embedding VECTOR(1024),
        content_hash TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        CONSTRAINT uq_job_facet_job_key UNIQUE (job_post_id, facet_key)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_job_facet_key ON job_facet_embedding (facet_key)",
    """
    CREATE INDEX IF NOT EXISTS jru_facet_embedding_hnsw
    ON job_facet_embedding
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    """,
    "CREATE INDEX IF NOT EXISTS idx_job_facet_job ON job_facet_embedding (job_post_id)",
    """
    CREATE TABLE IF NOT EXISTS job_match (
        id UUID PRIMARY KEY,
        job_post_id UUID NOT NULL REFERENCES job_post (id) ON DELETE CASCADE,
        resume_fingerprint TEXT NOT NULL,
        job_content_hash TEXT,
        job_similarity NUMERIC(3, 2),
        fit_score NUMERIC(5, 2),
        want_score NUMERIC(5, 2),
        overall_score NUMERIC(5, 2),
        fit_components JSONB,
        want_components JSONB,
        fit_weight NUMERIC(3, 2),
        want_weight NUMERIC(3, 2),
        base_score NUMERIC(5, 2),
        penalties NUMERIC(5, 2),
        penalty_details JSONB,
        required_coverage NUMERIC(3, 2),
        preferred_coverage NUMERIC(3, 2),
        total_requirements INTEGER,
        matched_requirements_count INTEGER,
        match_type TEXT,
        similarity_threshold NUMERIC(3, 2),
        status TEXT,
        invalidated_reason TEXT,
        notified BOOLEAN,
        is_hidden BOOLEAN,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        calculated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        CONSTRAINT uq_job_match_job_resume UNIQUE (job_post_id, resume_fingerprint)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_job_match_notified ON job_match (notified)",
    "CREATE INDEX IF NOT EXISTS idx_job_match_fit ON job_match (fit_score)",
    "CREATE INDEX IF NOT EXISTS idx_job_match_created ON job_match (created_at)",
    "CREATE INDEX IF NOT EXISTS idx_job_match_hidden ON job_match (is_hidden)",
    "CREATE INDEX IF NOT EXISTS idx_job_match_want ON job_match (want_score)",
    "CREATE INDEX IF NOT EXISTS idx_job_match_resume ON job_match (resume_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_job_match_calculated ON job_match (calculated_at)",
    "CREATE INDEX IF NOT EXISTS idx_job_match_status ON job_match (status)",
    "CREATE INDEX IF NOT EXISTS idx_job_match_score ON job_match (overall_score)",
    """
    CREATE TABLE IF NOT EXISTS job_post_source (
        id UUID PRIMARY KEY,
        job_post_id UUID NOT NULL REFERENCES job_post (id) ON DELETE CASCADE,
        site TEXT NOT NULL,
        job_url TEXT NOT NULL,
        job_url_direct TEXT,
        source_job_id TEXT,
        date_posted DATE,
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        is_active BOOLEAN NOT NULL,
        CONSTRAINT uq_job_post_source_site_url UNIQUE (site, job_url)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_job_post_source_seen ON job_post_source (last_seen_at)",
    "CREATE INDEX IF NOT EXISTS idx_job_post_source_job ON job_post_source (job_post_id)",
    """
    CREATE TABLE IF NOT EXISTS job_requirement_unit (
        id UUID PRIMARY KEY,
        job_post_id UUID NOT NULL REFERENCES job_post (id) ON DELETE CASCADE,
        req_type TEXT NOT NULL,
        text TEXT NOT NULL,
        tags JSONB NOT NULL,
        ordinal INTEGER,
        min_years INTEGER,
        years_context TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_jru_job ON job_requirement_unit (job_post_id)",
    """
    CREATE TABLE IF NOT EXISTS job_match_requirement (
        id UUID PRIMARY KEY,
        job_match_id UUID NOT NULL REFERENCES job_match (id) ON DELETE CASCADE,
        job_requirement_unit_id UUID NOT NULL REFERENCES job_requirement_unit (id) ON DELETE CASCADE,
        evidence_text TEXT NOT NULL,
        evidence_section TEXT,
        evidence_tags JSONB,
        similarity_score NUMERIC(3, 2) NOT NULL,
        is_covered BOOLEAN,
        req_type TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_jmr_covered ON job_match_requirement (is_covered)",
    "CREATE INDEX IF NOT EXISTS idx_jmr_requirement ON job_match_requirement (job_requirement_unit_id)",
    "CREATE INDEX IF NOT EXISTS idx_jmr_match ON job_match_requirement (job_match_id)",
    "CREATE INDEX IF NOT EXISTS idx_jmr_similarity ON job_match_requirement (similarity_score)",
    """
    CREATE TABLE IF NOT EXISTS job_requirement_unit_embedding (
        job_requirement_unit_id UUID PRIMARY KEY REFERENCES job_requirement_unit (id) ON DELETE CASCADE,
        embedding VECTOR(1024) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS jru_embedding_hnsw
    ON job_requirement_unit_embedding
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    """,
    """
    CREATE TABLE IF NOT EXISTS notification_tracker (
        id UUID PRIMARY KEY,
        owner_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
        job_match_id UUID REFERENCES job_match (id) ON DELETE CASCADE,
        channel_type TEXT NOT NULL,
        dedup_hash TEXT NOT NULL,
        content_hash TEXT,
        event_type TEXT NOT NULL,
        event_data JSONB DEFAULT '{}',
        recipient TEXT NOT NULL,
        subject TEXT,
        body TEXT,
        sent_successfully BOOLEAN,
        error_message TEXT,
        first_sent_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        last_sent_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        send_count INTEGER,
        allow_resend BOOLEAN,
        resend_interval_hours INTEGER,
        CONSTRAINT uq_notification_dedup UNIQUE (dedup_hash)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_notification_recent ON notification_tracker (dedup_hash, last_sent_at)",
    "CREATE INDEX IF NOT EXISTS ix_notification_tracker_owner_id ON notification_tracker (owner_id)",
    "CREATE INDEX IF NOT EXISTS ix_notification_tracker_dedup_hash ON notification_tracker (dedup_hash)",
    "CREATE INDEX IF NOT EXISTS ix_notification_tracker_job_match_id ON notification_tracker (job_match_id)",
    "CREATE INDEX IF NOT EXISTS idx_notification_owner ON notification_tracker (owner_id, first_sent_at)",
]


def migrate(conn: Connection) -> None:
    """Create the baseline schema from zero."""
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
    for statement in DDL_STATEMENTS:
        conn.execute(text(statement))


def rollback(conn: Connection) -> None:
    """Rollback the baseline schema for local recovery only."""
    conn.execute(text("DROP TABLE IF EXISTS notification_tracker CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS job_requirement_unit_embedding CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS job_match_requirement CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS job_requirement_unit CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS job_post_source CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS job_match CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS job_facet_embedding CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS job_benefit CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS user_files CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS user_auth_identity CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS job_post CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS users CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS user_wants CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS tenant CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS structured_resume CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS resume_section_embedding CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS resume_upload CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS resume_processing_state CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS resume_evidence_unit_embedding CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS app_settings CASCADE"))
    conn.execute(text("DROP TYPE IF EXISTS file_type"))
    conn.execute(text("DROP TYPE IF EXISTS upload_status"))
    conn.execute(text("DROP TYPE IF EXISTS auth_provider"))


def _run_standalone(rollback_mode: bool) -> None:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        if rollback_mode:
            rollback(conn)
        else:
            migrate(conn)
        conn.commit()


if __name__ == "__main__":  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser(description="Create the baseline JobScout schema")
    parser.add_argument("--rollback", action="store_true", help="Drop the baseline schema")
    args = parser.parse_args()
    _run_standalone(args.rollback)
