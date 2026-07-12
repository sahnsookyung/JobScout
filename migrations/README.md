# Database Bootstrap

JobScout bootstraps empty databases from ORM metadata and applies immutable,
forward-only SQL migrations from `database/schema/` to existing databases.
Both paths are exposed through `python -m database.migrate` and protected by a
Postgres advisory lock.

## Current contract

1. The ORM models under `database/models/` define the current schema shape.
2. `database.migrate.migrate_database()` creates an empty schema from those models
   or applies pending numbered SQL migrations to an initialized database.
3. Applied migration checksums are immutable and retained in `schema_migrations`.
4. `tests/integration/database/test_orm_schema_snapshot.py` enforces parity between
   fresh bootstrap, the upgrade path, and the checked-in snapshot.

## Changing the schema

1. Update the ORM models under `database/models/`.
2. Add the next uniquely numbered idempotent SQL migration under
   `database/schema/`; never edit a migration that has shipped.
3. Bootstrap a fresh database with `uv run python -m database.migrate`.
4. Regenerate the checked-in snapshot with:
   `uv run python -m database.schema_snapshot --write --url=<db_url>`
5. Run the schema snapshot and N-1 upgrade tests and include the schema and
   migration diffs in your PR.

## Existing databases

Existing stamped databases are upgraded in place through pending numbered
migrations. Schema changes must remain forward-compatible with the previous
application release when rollback safety requires it.
