"""Guard tests for the ORM bootstrap schema contract."""

from pathlib import Path

from database import schema_snapshot

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = REPO_ROOT / "database" / "schema"


def test_numbered_sql_migration_chain_exists() -> None:
    numbered_scripts = sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.sql"))
    assert numbered_scripts, "Expected at least one immutable SQL migration."
    assert len({path.name[:3] for path in numbered_scripts}) == len(numbered_scripts)


def test_checked_in_schema_snapshot_exists() -> None:
    """The checked-in schema snapshot is the single schema stamp source of truth."""
    assert schema_snapshot.SNAPSHOT_PATH.exists(), (
        "Missing database/schema_snapshot.json. Regenerate it with "
        "`uv run python -m database.schema_snapshot --write --url=<db_url>`."
    )


def test_current_schema_migration_supports_0_to_100_preference_scores() -> None:
    migration = (MIGRATIONS_DIR / "100_current_additive_schema.sql").read_text()

    assert "ALTER COLUMN preference_score TYPE NUMERIC(5, 2)" in migration
    assert "ALTER COLUMN preference_score_at_selection TYPE NUMERIC(5, 2)" in migration
