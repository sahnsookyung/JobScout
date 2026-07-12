"""Stable migration entrypoint for empty and existing JobScout databases."""

from __future__ import annotations

import logging

from sqlalchemy.engine import Engine

from database.bootstrap import (
    DatabaseSchemaError,
    bootstrap_database,
    check_database_schema as _check_database_schema,
)

logger = logging.getLogger(__name__)

DatabaseMigrationError = DatabaseSchemaError


def check_database_schema(*, engine: Engine | None = None) -> None:
    _check_database_schema(engine=engine)


def migrate_database(*, engine: Engine | None = None) -> list[str]:
    return bootstrap_database(engine=engine)


def main() -> int:
    try:
        applied = migrate_database()
        if applied:
            logger.info("Applied migrations: %s", ", ".join(applied))
        else:
            logger.info("Database schema is already up to date.")
        return 0
    except DatabaseSchemaError:
        logger.exception("Database migration failed")
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
