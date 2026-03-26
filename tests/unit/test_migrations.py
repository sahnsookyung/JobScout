"""
Guard tests for database migration hygiene.

SQLAlchemy's create_all() only runs CREATE TABLE IF NOT EXISTS — it never alters
existing tables. So any new column added to an ORM model requires an explicit
migration script to be applied to the live database.

These tests catch the two structural failure modes:
  1. A migration file exists locally but was never committed (never applied to CI/prod).
  2. A migration file has no callable entry point and cannot be run.

Note: testing that ORM model columns exist is NOT done here — that only checks
the Python class definition, not the live database schema, so it cannot detect
the actual bug (create_all skipping column additions to existing tables).
"""

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = REPO_ROOT / "migrations"


def test_no_untracked_migration_files() -> None:
    """Untracked migration files are never applied — commit them or delete them."""
    result = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard", "migrations/"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    untracked = [
        line for line in result.stdout.strip().splitlines() if line.endswith(".py")
    ]
    assert not untracked, (
        f"Untracked migration files found — they will never be applied to CI/prod. "
        f"Run `git add` and commit them: {untracked}"
    )


def test_migration_files_are_sequentially_numbered() -> None:
    """Migration numbers must be contiguous so application order is unambiguous."""
    migrations = sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.py"))
    for expected_num, path in enumerate(migrations, start=1):
        actual_num = int(path.name[:3])
        assert actual_num == expected_num, (
            f"Migration numbering gap: expected {expected_num:03d}_*.py, found {path.name}"
        )


def test_all_migration_files_define_an_entry_point() -> None:
    """Every migration must expose either migrate() (custom style) or upgrade() (Alembic style)."""
    for path in sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.py")):
        source = path.read_text()
        has_entry = "def migrate(" in source or "def upgrade(" in source
        assert has_entry, (
            f"{path.name} defines neither migrate() nor upgrade() — "
            "add a migrate() function so start.sh can apply it automatically"
        )
