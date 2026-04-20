"""Guard tests for the ORM bootstrap schema contract."""

from pathlib import Path

from database import schema_snapshot

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = REPO_ROOT / "migrations"


def test_no_numbered_migration_scripts_remain() -> None:
    """The repo should no longer rely on replaying numbered migration scripts."""
    numbered_scripts = sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.py"))
    assert not numbered_scripts, (
        "Legacy numbered migration scripts are still present even though startup now "
        f"bootstraps directly from ORM metadata: {numbered_scripts}"
    )


def test_checked_in_schema_snapshot_exists() -> None:
    """The checked-in schema snapshot is the single schema stamp source of truth."""
    assert schema_snapshot.SNAPSHOT_PATH.exists(), (
        "Missing database/schema_snapshot.json. Regenerate it with "
        "`uv run python -m database.schema_snapshot --write --url=<db_url>`."
    )
