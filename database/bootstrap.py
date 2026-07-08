"""Database bootstrap runner and schema state checks."""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import uuid

from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

from core.auth import DEFAULT_DEV_USER_EMAIL, DEFAULT_DEV_USER_NAME
from database.database import get_database_url
from database.models import Base, SYSTEM_OWNER_ID, User, UserAuthIdentity
from database.schema_snapshot import SNAPSHOT_PATH, capture, load

logger = logging.getLogger(__name__)

SCHEMA_MIGRATIONS_TABLE = "schema_migrations"
MIGRATION_LOCK_KEY = 485_199_421
CURRENT_SCHEMA_VERSION = "orm_bootstrap"
CURRENT_SCHEMA_CHECKSUM_SOURCE = SNAPSHOT_PATH
APP_TABLE_NAMES = set(Base.metadata.tables.keys())
DEV_BYPASS_IDENTITY_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
LEGACY_MIGRATION_SUFFIXES = (".py", ".sql")
CREATE_SCHEMA_MIGRATIONS_SQL = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY,
        checksum TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
"""
SELECT_SCHEMA_MIGRATIONS_SQL = "SELECT version, checksum FROM schema_migrations ORDER BY version"
DELETE_SCHEMA_MIGRATIONS_SQL = "DELETE FROM schema_migrations"
INSERT_SCHEMA_MIGRATIONS_SQL = """
    INSERT INTO schema_migrations (version, checksum)
    VALUES (:version, :checksum)
"""


class DatabaseSchemaError(RuntimeError):
    """Raised when the database schema is missing, stale, or unsupported."""


def _schema_checksum() -> str:
    return hashlib.sha256(CURRENT_SCHEMA_CHECKSUM_SOURCE.read_bytes()).hexdigest()


def _schema_migrations_exists(conn: Connection) -> bool:
    return inspect(conn).has_table(SCHEMA_MIGRATIONS_TABLE)


def _app_tables_present(conn: Connection) -> set[str]:
    return set(inspect(conn).get_table_names()) & APP_TABLE_NAMES


def _ensure_extension(conn: Connection) -> None:
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))


def _ensure_schema_migrations_table(conn: Connection) -> None:
    conn.execute(text(CREATE_SCHEMA_MIGRATIONS_SQL))


def _release_migration_lock(conn: Connection) -> None:
    """Release the advisory lock from a clean transaction state."""
    conn.rollback()
    conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": MIGRATION_LOCK_KEY})
    conn.commit()


def _applied_migrations(conn: Connection) -> dict[str, str]:
    if not _schema_migrations_exists(conn):
        return {}

    rows = conn.execute(text(SELECT_SCHEMA_MIGRATIONS_SQL)).fetchall()
    return {row.version: row.checksum for row in rows}


def _unsupported_existing_database_error() -> DatabaseSchemaError:
    return DatabaseSchemaError(
        "Unsupported existing database detected; recreate the database and rerun "
        "`uv run python -m database.bootstrap`."
    )


def _missing_schema_error() -> DatabaseSchemaError:
    return DatabaseSchemaError(
        "Database schema is not initialized. Run "
        "`uv run python -m database.bootstrap` before starting the application."
    )


def _validate_known_versions(applied: dict[str, str]) -> None:
    unknown_versions = sorted(set(applied) - {CURRENT_SCHEMA_VERSION})
    if unknown_versions:
        raise DatabaseSchemaError(
            "Database contains unknown schema versions: "
            + ", ".join(unknown_versions)
        )
    if CURRENT_SCHEMA_VERSION not in applied:
        raise DatabaseSchemaError(
            f"Missing required schema version entry: {CURRENT_SCHEMA_VERSION}"
        )


def _validate_applied_checksums(applied: dict[str, str]) -> None:
    expected_checksum = _schema_checksum()
    actual_checksum = applied[CURRENT_SCHEMA_VERSION]
    if actual_checksum != expected_checksum:
        raise DatabaseSchemaError(
            "Database schema stamp does not match the current checked-in schema. "
            "Recreate the database and rerun `uv run python -m database.bootstrap`."
        )


def _is_legacy_migration_version(version: str) -> bool:
    prefix, separator, _ = version.partition("_")
    return (
        separator == "_"
        and len(prefix) == 3
        and prefix.isdigit()
        and version.endswith(LEGACY_MIGRATION_SUFFIXES)
    )


def _can_adopt_legacy_migration_history(applied: dict[str, str]) -> bool:
    return (
        CURRENT_SCHEMA_VERSION not in applied
        and bool(applied)
        and all(_is_legacy_migration_version(version) for version in applied)
    )


def _upgrade_legacy_schema_to_current(conn: Connection) -> None:
    Base.metadata.create_all(bind=conn)
    conn.execute(
        text(
            """
            ALTER TABLE job_match_requirement
            ADD COLUMN IF NOT EXISTS evidence_score NUMERIC(5, 4);

            ALTER TABLE match_selection_item
            ADD COLUMN IF NOT EXISTS excluded_reason TEXT;

            ALTER TABLE match_selection_item
            ADD COLUMN IF NOT EXISTS selection_tier TEXT;

            UPDATE match_selection_item
            SET selection_tier = 'primary'
            WHERE selection_tier IS NULL;

            ALTER TABLE match_selection_item
            ALTER COLUMN selection_tier SET DEFAULT 'primary';

            ALTER TABLE match_selection_item
            ALTER COLUMN selection_tier SET NOT NULL;

            ALTER TABLE user_notification_channel
            ADD COLUMN IF NOT EXISTS override_address TEXT;

            ALTER TABLE user_notification_channel
            ADD COLUMN IF NOT EXISTS override_verified_at TIMESTAMPTZ;

            ALTER TABLE user_notification_channel
            ADD COLUMN IF NOT EXISTS verification_sent_at TIMESTAMPTZ;

            ALTER TABLE user_notification_channel
            ADD COLUMN IF NOT EXISTS verification_token_expires_at TIMESTAMPTZ;

            ALTER TABLE user_notification_channel
            ADD COLUMN IF NOT EXISTS verification_token_hash TEXT;

            CREATE INDEX IF NOT EXISTS idx_msi_run_tier
            ON match_selection_item (selection_run_id, selection_tier);

            CREATE INDEX IF NOT EXISTS idx_msi_run_tier_rank_id
            ON match_selection_item (selection_run_id, selection_tier, rank_position, id);

            CREATE INDEX IF NOT EXISTS idx_notification_owner_last_sent
            ON notification_tracker (owner_id, last_sent_at);

            CREATE INDEX IF NOT EXISTS idx_notification_owner_channel_last_sent
            ON notification_tracker (owner_id, channel_type, last_sent_at);

            CREATE INDEX IF NOT EXISTS idx_unc_verif_hash_pending
            ON user_notification_channel (verification_token_hash)
            WHERE verification_token_hash IS NOT NULL;
            """
        )
    )
    _apply_current_additive_schema_upgrades(conn)


def _apply_current_additive_schema_upgrades(conn: Connection) -> None:
    """Apply idempotent additive changes that create_all() cannot add to old tables."""
    conn.execute(
        text(
            """
            ALTER TABLE job_post
            ADD COLUMN IF NOT EXISTS description_source TEXT DEFAULT 'unknown' NOT NULL;

            ALTER TABLE job_post
            ADD COLUMN IF NOT EXISTS description_completeness TEXT DEFAULT 'unknown' NOT NULL;

            ALTER TABLE job_post
            ADD COLUMN IF NOT EXISTS description_warning_code TEXT;

            ALTER TABLE job_post
            ADD COLUMN IF NOT EXISTS description_hash TEXT;

            ALTER TABLE job_post
            ADD COLUMN IF NOT EXISTS description_recovery_status TEXT DEFAULT 'not_needed' NOT NULL;

            ALTER TABLE job_post
            ADD COLUMN IF NOT EXISTS description_recovery_reason TEXT;

            ALTER TABLE job_post
            ADD COLUMN IF NOT EXISTS description_recovery_attempts INTEGER DEFAULT 0 NOT NULL;

            ALTER TABLE job_post
            ADD COLUMN IF NOT EXISTS description_recovery_last_attempt_at TIMESTAMPTZ;

            ALTER TABLE job_post
            ADD COLUMN IF NOT EXISTS description_recovery_next_retry_at TIMESTAMPTZ;

            ALTER TABLE job_post
            ADD COLUMN IF NOT EXISTS description_recovery_last_error TEXT;

            ALTER TABLE job_post
            ADD COLUMN IF NOT EXISTS description_recovery_run_id TEXT;

            ALTER TABLE candidate_preferences
            ADD COLUMN IF NOT EXISTS preference_rerank_top_n INTEGER;

            CREATE TABLE IF NOT EXISTS job_offerings_profile (
                job_post_id UUID PRIMARY KEY
                    REFERENCES job_post(id) ON DELETE CASCADE,
                profile_json JSONB DEFAULT '{}'::jsonb NOT NULL,
                profile_schema_version INTEGER DEFAULT 1 NOT NULL,
                source_description_hash TEXT,
                extraction_provider TEXT,
                extraction_model TEXT,
                confidence NUMERIC(5, 4),
                created_at TIMESTAMPTZ DEFAULT timezone('UTC', now()) NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT timezone('UTC', now()) NOT NULL
            );

            ALTER TABLE llm_match_evaluation
            ADD COLUMN IF NOT EXISTS analysis JSONB DEFAULT '{}'::jsonb NOT NULL;

            CREATE INDEX IF NOT EXISTS idx_job_post_description_hash
            ON job_post (description_hash);

            CREATE INDEX IF NOT EXISTS idx_job_post_description_recovery_scan
            ON job_post (
                tenant_id,
                status,
                description_recovery_status,
                description_recovery_next_retry_at,
                first_seen_at
            );

            CREATE INDEX IF NOT EXISTS idx_job_post_missing_description
            ON job_post (tenant_id, status, extraction_status, first_seen_at);

            CREATE INDEX IF NOT EXISTS idx_jop_source_hash
            ON job_offerings_profile (source_description_hash);

            CREATE INDEX IF NOT EXISTS idx_jop_schema_version
            ON job_offerings_profile (profile_schema_version);

            CREATE INDEX IF NOT EXISTS idx_llm_eval_owner_match_created
            ON llm_match_evaluation (owner_id, job_match_id, created_at);

            CREATE INDEX IF NOT EXISTS idx_llm_eval_backlog_status_created
            ON llm_match_evaluation (status, created_at)
            WHERE deleted_at IS NULL;

            CREATE INDEX IF NOT EXISTS idx_llm_eval_retryable_failed_created
            ON llm_match_evaluation (created_at)
            WHERE deleted_at IS NULL
              AND status = 'failed'
              AND retryable = TRUE;

            CREATE INDEX IF NOT EXISTS idx_msi_run_tier_rank_id
            ON match_selection_item (selection_run_id, selection_tier, rank_position, id);
            """
        )
    )
    conn.execute(
        text(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'msi_selection_tier_chk'
                      AND conrelid = 'match_selection_item'::regclass
                ) THEN
                    ALTER TABLE match_selection_item
                    ADD CONSTRAINT msi_selection_tier_chk
                    CHECK (selection_tier = ANY (ARRAY['primary'::text, 'excluded'::text]));
                END IF;

                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'msi_excluded_reason_chk'
                      AND conrelid = 'match_selection_item'::regclass
                ) THEN
                    ALTER TABLE match_selection_item
                    ADD CONSTRAINT msi_excluded_reason_chk
                    CHECK (
                        (
                            selection_tier = 'primary'::text
                            AND excluded_reason IS NULL
                        )
                        OR (
                            selection_tier = 'excluded'::text
                            AND excluded_reason IS NOT NULL
                        )
                    );
                END IF;
            END $$;
            """
        )
    )


def _adopt_legacy_schema_if_current(conn: Connection, applied: dict[str, str]) -> bool:
    if not _can_adopt_legacy_migration_history(applied):
        return False

    _upgrade_legacy_schema_to_current(conn)
    _verify_bootstrapped_schema(conn)
    _stamp_current_schema(conn)
    return True


def _upgrade_stamped_schema_if_current(conn: Connection, applied: dict[str, str]) -> bool:
    _validate_known_versions(applied)
    if applied[CURRENT_SCHEMA_VERSION] == _schema_checksum():
        return False

    Base.metadata.create_all(bind=conn)
    _apply_current_additive_schema_upgrades(conn)
    _verify_bootstrapped_schema(conn)
    _stamp_current_schema(conn)
    return True


def _validate_schema_state(conn: Connection) -> None:
    applied = _applied_migrations(conn)
    app_tables = _app_tables_present(conn)

    if not applied:
        if app_tables:
            raise _unsupported_existing_database_error()
        raise _missing_schema_error()

    if not app_tables:
        raise _missing_schema_error()

    _validate_known_versions(applied)
    _validate_applied_checksums(applied)


def _seed_dev_bypass_user(conn: Connection) -> None:
    user_id = uuid.UUID(os.getenv("DEV_BYPASS_USER_ID", SYSTEM_OWNER_ID))
    email = os.getenv("DEV_BYPASS_EMAIL", DEFAULT_DEV_USER_EMAIL).strip().lower()
    display_name = os.getenv("DEV_BYPASS_NAME", DEFAULT_DEV_USER_NAME).strip()
    provider_subject = f"dev-bypass:{email}"

    session = Session(bind=conn)
    try:
        user = session.get(User, user_id)
        if user is None:
            user = session.execute(select(User).where(User.email == email)).scalar_one_or_none()
        if user is None:
            user = User(
                id=user_id,
                email=email,
                display_name=display_name,
                is_active=True,
            )
            session.add(user)
            session.flush()

        identity = session.execute(
            select(UserAuthIdentity).where(
                UserAuthIdentity.provider == "password",
                UserAuthIdentity.provider_subject == provider_subject,
            )
        ).scalar_one_or_none()
        if identity is None:
            session.add(
                UserAuthIdentity(
                    id=DEV_BYPASS_IDENTITY_ID,
                    user_id=user.id,
                    provider="password",
                    provider_subject=provider_subject,
                    email=email,
                    email_normalized=email,
                    email_verified=True,
                )
            )
            session.flush()
    finally:
        session.close()


def _stamp_current_schema(conn: Connection) -> None:
    conn.execute(text(DELETE_SCHEMA_MIGRATIONS_SQL))
    conn.execute(
        text(INSERT_SCHEMA_MIGRATIONS_SQL),
        {
            "version": CURRENT_SCHEMA_VERSION,
            "checksum": _schema_checksum(),
        },
    )


def _snapshot_limited_to_expected_tables(
    snapshot: dict[str, object],
    expected: dict[str, object],
) -> dict[str, object]:
    expected_extensions = set(expected.get("extensions") or [])
    expected_tables = set((expected.get("tables") or {}).keys())
    expected_enums = {
        enum["name"]
        for enum in expected.get("enums", [])
        if isinstance(enum, dict) and isinstance(enum.get("name"), str)
    }
    expected_indexes = {
        index["name"]
        for index in expected.get("indexes", [])
        if isinstance(index, dict) and isinstance(index.get("name"), str)
    }
    expected_constraints = {
        constraint["name"]
        for constraint in expected.get("constraints", [])
        if isinstance(constraint, dict) and isinstance(constraint.get("name"), str)
    }
    actual_tables = snapshot.get("tables") or {}
    return {
        "extensions": [
            extension
            for extension in snapshot.get("extensions", [])
            if extension in expected_extensions
        ],
        "enums": [
            enum
            for enum in snapshot.get("enums", [])
            if isinstance(enum, dict) and enum.get("name") in expected_enums
        ],
        "tables": {
            table_name: actual_tables[table_name]
            for table_name in expected_tables
            if table_name in actual_tables
        },
        "indexes": [
            index
            for index in snapshot.get("indexes", [])
            if isinstance(index, dict)
            and index.get("table") in expected_tables
            and index.get("name") in expected_indexes
        ],
        "constraints": [
            constraint
            for constraint in snapshot.get("constraints", [])
            if isinstance(constraint, dict)
            and constraint.get("table") in expected_tables
            and constraint.get("name") in expected_constraints
        ],
    }


def _verify_bootstrapped_schema(conn: Connection) -> None:
    expected = load()
    actual = _snapshot_limited_to_expected_tables(capture(conn), expected)
    if actual != expected:
        raise DatabaseSchemaError(
            "ORM bootstrap schema drifted from the checked-in schema snapshot. "
            "Update the ORM/bootstrap code or regenerate "
            "`database/schema_snapshot.json` intentionally."
        )


def _bootstrap_schema(conn: Connection) -> None:
    Base.metadata.create_all(bind=conn)
    _seed_dev_bypass_user(conn)
    _verify_bootstrapped_schema(conn)
    _ensure_schema_migrations_table(conn)
    _stamp_current_schema(conn)


def check_database_schema(*, engine: Engine | None = None) -> None:
    """Verify that the database schema matches the checked-in current state."""
    db_engine = engine or create_engine(get_database_url())
    created_engine = engine is None
    try:
        with db_engine.connect() as conn:
            conn.execute(text("SELECT pg_advisory_lock(:key)"), {"key": MIGRATION_LOCK_KEY})
            try:
                if not _schema_migrations_exists(conn) and _app_tables_present(conn):
                    raise _unsupported_existing_database_error()
                if not _schema_migrations_exists(conn):
                    raise _missing_schema_error()
                _validate_schema_state(conn)
            finally:
                _release_migration_lock(conn)
    finally:
        if created_engine:
            db_engine.dispose()


def bootstrap_database(*, engine: Engine | None = None) -> list[str]:
    """Bootstrap an empty database or verify an initialized one."""
    db_engine = engine or create_engine(get_database_url())
    created_engine = engine is None

    try:
        with db_engine.connect() as conn:
            conn.execute(text("SELECT pg_advisory_lock(:key)"), {"key": MIGRATION_LOCK_KEY})
            try:
                _ensure_extension(conn)

                app_tables = _app_tables_present(conn)
                has_schema_table = _schema_migrations_exists(conn)

                if app_tables and not has_schema_table:
                    raise _unsupported_existing_database_error()

                if not app_tables and not has_schema_table:
                    logger.info("Bootstrapping database schema from ORM metadata")
                    _bootstrap_schema(conn)
                    conn.commit()
                    return [CURRENT_SCHEMA_VERSION]

                applied = _applied_migrations(conn)
                if _adopt_legacy_schema_if_current(conn, applied):
                    logger.info("Adopted legacy migration history as %s", CURRENT_SCHEMA_VERSION)
                    conn.commit()
                    return [CURRENT_SCHEMA_VERSION]

                if _upgrade_stamped_schema_if_current(conn, applied):
                    logger.info("Upgraded stamped schema to %s", CURRENT_SCHEMA_VERSION)
                    conn.commit()
                    return [CURRENT_SCHEMA_VERSION]

                _validate_schema_state(conn)
                return []
            finally:
                _release_migration_lock(conn)
    finally:
        if created_engine:
            db_engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap the JobScout database schema")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify schema state without bootstrapping an empty database",
    )
    args = parser.parse_args()

    try:
        if args.check:
            check_database_schema()
            logger.info("Database schema is up to date.")
        else:
            applied = bootstrap_database()
            if applied:
                logger.info("Bootstrapped schema version: %s", ", ".join(applied))
            else:
                logger.info("Database schema is already up to date.")
        return 0
    except DatabaseSchemaError:
        logger.exception("Database schema bootstrap failed")
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
