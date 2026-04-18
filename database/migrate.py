#!/usr/bin/env python3
"""Database migration runner and schema state checks."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import inspect as py_inspect
import logging
from pathlib import Path
from types import ModuleType
from typing import Iterable

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine

from database.database import get_database_url
from database.models import Base

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
SCHEMA_MIGRATIONS_TABLE = "schema_migrations"
MIGRATION_LOCK_KEY = 485_199_421
BASELINE_VERSION = "001_initial_schema.py"
APP_TABLE_NAMES = set(Base.metadata.tables.keys())


class DatabaseSchemaError(RuntimeError):
    """Raised when the database schema is missing, stale, or corrupted."""


def _migration_paths(migrations_dir: Path = MIGRATIONS_DIR) -> list[Path]:
    return sorted(migrations_dir.glob("[0-9][0-9][0-9]_*.py"))


def _checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _schema_migrations_exists(conn: Connection) -> bool:
    return inspect(conn).has_table(SCHEMA_MIGRATIONS_TABLE)


def _app_tables_present(conn: Connection) -> set[str]:
    return set(inspect(conn).get_table_names()) & APP_TABLE_NAMES


def _ensure_extension(conn: Connection) -> None:
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))


def _ensure_schema_migrations_table(conn: Connection) -> None:
    conn.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_MIGRATIONS_TABLE} (
                version TEXT PRIMARY KEY,
                checksum TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
            )
            """
        )
    )


def _release_migration_lock(conn: Connection) -> None:
    """Release the advisory lock from a clean transaction state."""
    conn.rollback()
    conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": MIGRATION_LOCK_KEY})
    conn.commit()


def _applied_migrations(conn: Connection) -> dict[str, str]:
    if not _schema_migrations_exists(conn):
        return {}

    rows = conn.execute(
        text(
            f"SELECT version, checksum FROM {SCHEMA_MIGRATIONS_TABLE} ORDER BY version"
        )
    ).fetchall()
    return {row.version: row.checksum for row in rows}


def _load_migration_module(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise DatabaseSchemaError(f"Unable to load migration module: {path.name}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_migration_callable(module: ModuleType, conn: Connection) -> None:
    migrate_fn = getattr(module, "migrate", None)
    if migrate_fn is None:
        raise DatabaseSchemaError(f"Migration {module.__name__} has no migrate()")

    signature = py_inspect.signature(migrate_fn)
    if len(signature.parameters) == 0:
        migrate_fn()
    elif len(signature.parameters) == 1:
        migrate_fn(conn)
    else:
        raise DatabaseSchemaError(
            f"Migration {module.__name__}.migrate() must accept 0 or 1 args"
        )


def _unsupported_pre_reset_error() -> DatabaseSchemaError:
    return DatabaseSchemaError(
        "Unsupported pre-reset database detected; recreate the database and rerun "
        "`uv run python -m database.migrate`."
    )


def _missing_schema_error() -> DatabaseSchemaError:
    return DatabaseSchemaError(
        "Database schema is not initialized. Run "
        "`uv run python -m database.migrate` before starting the application."
    )


def _validate_known_versions(
    applied: dict[str, str], migration_paths: Iterable[Path]
) -> None:
    expected_versions = {path.name for path in migration_paths}
    unknown_versions = sorted(set(applied) - expected_versions)
    if unknown_versions:
        raise DatabaseSchemaError(
            "Database contains unknown migration versions: "
            + ", ".join(unknown_versions)
        )


def _validate_applied_checksums(
    applied: dict[str, str], migration_paths: Iterable[Path]
) -> None:
    path_map = {path.name: path for path in migration_paths}
    for version, stored_checksum in applied.items():
        actual_checksum = _checksum(path_map[version])
        if actual_checksum != stored_checksum:
            raise DatabaseSchemaError(
                f"Immutable migration checksum mismatch for {version}."
            )


def _validate_schema_state(conn: Connection, migration_paths: list[Path]) -> None:
    applied = _applied_migrations(conn)
    app_tables = _app_tables_present(conn)

    if not applied:
        if app_tables:
            raise _unsupported_pre_reset_error()
        raise _missing_schema_error()

    if BASELINE_VERSION not in applied:
        if app_tables:
            raise _unsupported_pre_reset_error()
        raise DatabaseSchemaError(
            f"Missing required baseline migration entry: {BASELINE_VERSION}"
        )

    _validate_known_versions(applied, migration_paths)
    _validate_applied_checksums(applied, migration_paths)

    unapplied = [path.name for path in migration_paths if path.name not in applied]
    if unapplied:
        raise DatabaseSchemaError(
            "Database schema is behind the repository head. Run "
            "`uv run python -m database.migrate`."
        )


def check_database_schema(
    *, engine: Engine | None = None, migrations_dir: Path = MIGRATIONS_DIR
) -> None:
    """Verify that the database schema is migrated to the current head."""
    migration_paths = _migration_paths(migrations_dir)
    if not migration_paths:
        raise DatabaseSchemaError("No migration files found.")

    db_engine = engine or create_engine(get_database_url())
    created_engine = engine is None
    try:
        with db_engine.connect() as conn:
            conn.execute(text("SELECT pg_advisory_lock(:key)"), {"key": MIGRATION_LOCK_KEY})
            try:
                if not _schema_migrations_exists(conn) and _app_tables_present(conn):
                    raise _unsupported_pre_reset_error()
                if not _schema_migrations_exists(conn):
                    raise _missing_schema_error()
                _validate_schema_state(conn, migration_paths)
            finally:
                _release_migration_lock(conn)
    finally:
        if created_engine:
            db_engine.dispose()


def migrate_database(
    *, engine: Engine | None = None, migrations_dir: Path = MIGRATIONS_DIR
) -> list[str]:
    """Apply all unapplied migrations."""
    migration_paths = _migration_paths(migrations_dir)
    if not migration_paths:
        raise DatabaseSchemaError("No migration files found.")

    db_engine = engine or create_engine(get_database_url())
    created_engine = engine is None
    applied_now: list[str] = []

    try:
        with db_engine.connect() as conn:
            conn.execute(text("SELECT pg_advisory_lock(:key)"), {"key": MIGRATION_LOCK_KEY})
            try:
                _ensure_extension(conn)

                app_tables = _app_tables_present(conn)
                if app_tables and not _schema_migrations_exists(conn):
                    raise _unsupported_pre_reset_error()

                if not _schema_migrations_exists(conn):
                    _ensure_schema_migrations_table(conn)
                    conn.commit()

                applied = _applied_migrations(conn)
                if app_tables and BASELINE_VERSION not in applied:
                    raise _unsupported_pre_reset_error()

                _validate_known_versions(applied, migration_paths)
                _validate_applied_checksums(applied, migration_paths)

                for path in migration_paths:
                    if path.name in applied:
                        continue

                    logger.info("Applying migration %s", path.name)
                    module = _load_migration_module(path)
                    _run_migration_callable(module, conn)
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO {SCHEMA_MIGRATIONS_TABLE} (version, checksum)
                            VALUES (:version, :checksum)
                            """
                        ),
                        {"version": path.name, "checksum": _checksum(path)},
                    )
                    conn.commit()
                    applied_now.append(path.name)

                return applied_now
            finally:
                _release_migration_lock(conn)
    finally:
        if created_engine:
            db_engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run JobScout database migrations")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify schema state without applying migrations",
    )
    args = parser.parse_args()

    try:
        if args.check:
            check_database_schema()
            logger.info("Database schema is up to date.")
        else:
            applied = migrate_database()
            if applied:
                logger.info("Applied migrations: %s", ", ".join(applied))
            else:
                logger.info("Database schema is already up to date.")
        return 0
    except DatabaseSchemaError as exc:
        logger.error(str(exc))
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
