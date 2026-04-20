"""Deprecated migration entrypoint.

JobScout no longer supports in-place database upgrades through numbered
migration scripts. The schema is bootstrapped for empty databases via
`python -m database.bootstrap`.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

UNSUPPORTED_MESSAGE = (
    "In-place database migrations are no longer supported in this repository. "
    "For a fresh database, run `uv run python -m database.bootstrap`. "
    "For an existing database, export/import data separately or recreate it."
)


class DatabaseMigrationError(RuntimeError):
    """Raised when callers attempt to use the removed migration flow."""


def check_database_schema(*args, **kwargs):
    raise DatabaseMigrationError(UNSUPPORTED_MESSAGE)


def migrate_database(*args, **kwargs):
    raise DatabaseMigrationError(UNSUPPORTED_MESSAGE)


def main() -> int:
    logger.error(UNSUPPORTED_MESSAGE)
    return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
