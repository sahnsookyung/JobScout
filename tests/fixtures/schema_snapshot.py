"""Compatibility wrapper around the checked-in database schema snapshot helpers."""

from database.schema_snapshot import *  # noqa: F401,F403


if __name__ == "__main__":
    raise SystemExit(main())
