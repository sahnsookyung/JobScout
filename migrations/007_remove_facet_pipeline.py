"""Remove facet extraction pipeline — table and columns are no longer used."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


def migrate(conn: Connection) -> None:
    conn.execute(text("DROP TABLE IF EXISTS job_facet_embedding CASCADE"))
    conn.execute(text("ALTER TABLE job_post DROP COLUMN IF EXISTS facet_status"))
    conn.execute(text("ALTER TABLE job_post DROP COLUMN IF EXISTS facet_claimed_by"))
    conn.execute(text("ALTER TABLE job_post DROP COLUMN IF EXISTS facet_claimed_at"))
    conn.execute(text("ALTER TABLE job_post DROP COLUMN IF EXISTS facet_extraction_hash"))
    conn.execute(text("ALTER TABLE job_post DROP COLUMN IF EXISTS facet_retry_count"))
    conn.execute(text("ALTER TABLE job_post DROP COLUMN IF EXISTS facet_last_error"))


def rollback(conn: Connection) -> None:
    conn.execute(text("""
        ALTER TABLE job_post
        ADD COLUMN IF NOT EXISTS facet_status TEXT,
        ADD COLUMN IF NOT EXISTS facet_claimed_by TEXT,
        ADD COLUMN IF NOT EXISTS facet_claimed_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS facet_extraction_hash TEXT,
        ADD COLUMN IF NOT EXISTS facet_retry_count INTEGER,
        ADD COLUMN IF NOT EXISTS facet_last_error TEXT
    """))
    conn.execute(text("""
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
    """))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_job_facet_key ON job_facet_embedding (facet_key)"))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS jru_facet_embedding_hnsw
        ON job_facet_embedding
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_job_facet_job ON job_facet_embedding (job_post_id)"))
