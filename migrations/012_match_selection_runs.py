"""Add committed match-selection run artifacts."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


def migrate(conn: Connection) -> None:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS match_selection_run (
            id UUID PRIMARY KEY,
            owner_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            resume_fingerprint TEXT NOT NULL,
            task_id TEXT,
            lifecycle_status TEXT NOT NULL DEFAULT 'pending',
            is_current BOOLEAN NOT NULL DEFAULT FALSE,
            policy_snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            ranking_mode_used TEXT NOT NULL,
            ranking_config_version TEXT NOT NULL,
            stable_tie_break_key TEXT NOT NULL,
            fit_floor_used NUMERIC(5, 2) NOT NULL DEFAULT 0,
            notification_fit_floor_used NUMERIC(5, 2) NOT NULL DEFAULT 0,
            top_k_used INTEGER NOT NULL DEFAULT 0,
            candidate_pool_size INTEGER NOT NULL DEFAULT 0,
            selected_count INTEGER NOT NULL DEFAULT 0,
            alert_candidate_count INTEGER NOT NULL DEFAULT 0,
            resume_resolution_reason TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS match_selection_item (
            id UUID PRIMARY KEY,
            selection_run_id UUID NOT NULL REFERENCES match_selection_run(id) ON DELETE CASCADE,
            job_match_id UUID NOT NULL REFERENCES job_match(id) ON DELETE CASCADE,
            rank_position INTEGER NOT NULL,
            fit_score_at_selection NUMERIC(5, 2) NOT NULL DEFAULT 0,
            preference_score_at_selection NUMERIC(5, 4),
            job_similarity_at_selection NUMERIC(3, 2) NOT NULL DEFAULT 0,
            required_coverage_at_selection NUMERIC(3, 2) NOT NULL DEFAULT 0,
            alert_eligible BOOLEAN NOT NULL DEFAULT FALSE,
            dominant_reason_code TEXT,
            explanation_label TEXT,
            ranking_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
        )
    """))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_match_selection_run_owner "
        "ON match_selection_run(owner_id, created_at)"
    ))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_match_selection_run_resume "
        "ON match_selection_run(resume_fingerprint, created_at)"
    ))
    conn.execute(text(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_match_selection_run_current "
        "ON match_selection_run(owner_id, resume_fingerprint) "
        "WHERE is_current AND lifecycle_status = 'committed'"
    ))
    conn.execute(text(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_match_selection_item_run "
        "ON match_selection_item(selection_run_id, rank_position)"
    ))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_match_selection_item_match "
        "ON match_selection_item(job_match_id)"
    ))


def rollback(conn: Connection) -> None:
    conn.execute(text("DROP TABLE IF EXISTS match_selection_item"))
    conn.execute(text("DROP TABLE IF EXISTS match_selection_run"))
