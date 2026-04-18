"""Two-tier canonical selection + per-requirement evidence rerank score.

Adds:
- `match_selection_item.selection_tier` ∈ {primary, excluded} with default 'primary'.
- `match_selection_item.excluded_reason` (nullable: below_min_fit, beyond_top_k,
  below_coverage_floor, truncated).
- Partial index `idx_msi_run_tier` to keep the common primary-only query fast.
- Backfill: all existing rows tagged 'primary' (current behaviour).
- `job_match_requirement.evidence_score` (NUMERIC(5,4) nullable) for the
  cross-encoder rerank score used by the evidence-picker.

Paired ORM edits:
- database/models/match_selection.py (selection_tier, excluded_reason)
- database/models/match.py (evidence_score on JobMatchRequirement)
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


def migrate(conn: Connection) -> None:
    conn.execute(text("""
        ALTER TABLE match_selection_item
            ADD COLUMN IF NOT EXISTS selection_tier TEXT NOT NULL DEFAULT 'primary',
            ADD COLUMN IF NOT EXISTS excluded_reason TEXT
    """))
    # Defensive: backfill NULL tiers from older instances that may have inserted
    # before the default was applied. The default should make this a no-op.
    conn.execute(text(
        "UPDATE match_selection_item SET selection_tier = 'primary' "
        "WHERE selection_tier IS NULL"
    ))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_msi_run_tier "
        "ON match_selection_item(selection_run_id, selection_tier)"
    ))
    conn.execute(text(
        "ALTER TABLE match_selection_item "
        "ADD CONSTRAINT msi_selection_tier_chk "
        "CHECK (selection_tier IN ('primary', 'excluded'))"
    ))
    conn.execute(text(
        "ALTER TABLE match_selection_item "
        "ADD CONSTRAINT msi_excluded_reason_chk "
        "CHECK ("
        "  (selection_tier = 'primary' AND excluded_reason IS NULL) OR "
        "  (selection_tier = 'excluded' AND excluded_reason IS NOT NULL)"
        ")"
    ))
    conn.execute(text("""
        ALTER TABLE job_match_requirement
            ADD COLUMN IF NOT EXISTS evidence_score NUMERIC(5, 4)
    """))


def rollback(conn: Connection) -> None:
    conn.execute(text(
        "ALTER TABLE match_selection_item DROP CONSTRAINT IF EXISTS msi_excluded_reason_chk"
    ))
    conn.execute(text(
        "ALTER TABLE match_selection_item DROP CONSTRAINT IF EXISTS msi_selection_tier_chk"
    ))
    conn.execute(text("DROP INDEX IF EXISTS idx_msi_run_tier"))
    conn.execute(text(
        "ALTER TABLE match_selection_item "
        "DROP COLUMN IF EXISTS excluded_reason, "
        "DROP COLUMN IF EXISTS selection_tier"
    ))
    conn.execute(text(
        "ALTER TABLE job_match_requirement DROP COLUMN IF EXISTS evidence_score"
    ))
