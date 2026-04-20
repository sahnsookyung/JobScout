# Database Bootstrap

JobScout no longer replays numbered Python migration scripts during startup.
Instead, `database/migrate.py` bootstraps an empty database directly from the
current ORM models, required bootstrap data, and the checked-in schema snapshot
at `database/schema_snapshot.json`.

## Current contract

1. The ORM models under `database/models/` define the current schema shape.
2. `database.migrate.migrate_database()` creates an empty schema from those models,
   ensures required bootstrap data exists, and stamps `schema_migrations` with the
   checksum of `database/schema_snapshot.json`.
3. `tests/integration/database/test_orm_schema_snapshot.py` enforces parity between
   the ORM bootstrap path and the checked-in snapshot.

## Changing the schema

1. Update the ORM models under `database/models/`.
2. Bootstrap a fresh database with `uv run python -m database.migrate`.
3. Regenerate the checked-in snapshot with:
   `uv run python -m database.schema_snapshot --write --url=<db_url>`
4. Run the schema snapshot tests and include the snapshot diff in your PR.

## Existing databases

In-place upgrades from older stamped schemas are not supported by the current
bootstrap flow. Recreate the database and rerun `uv run python -m database.migrate`
when the checked-in schema snapshot changes.
