"""Compatibility wrapper around the checked-in database schema snapshot helpers."""

from database.schema_snapshot import SNAPSHOT_PATH, capture, dump, load, main, write

__all__ = ["SNAPSHOT_PATH", "capture", "dump", "load", "main", "write"]


if __name__ == "__main__":
    raise SystemExit(main())
