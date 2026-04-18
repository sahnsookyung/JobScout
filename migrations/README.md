# Database Migrations

Migrations are append-only and checksum-validated by `database/migrate.py`. Never edit a migration file after it has been applied to any environment — the checksum guard in `_validate_applied_checksums` will reject the mismatch and refuse to start the app.

## Authoring a new migration

1. Copy the next sequential filename (e.g. `015_my_change.py`).
2. Expose two functions: `migrate(conn)` and `rollback(conn)`. `rollback` is for local-dev only — production never calls it.
3. Mirror the change in the ORM models under `database/models/` so that `Base.metadata.create_all()` on a fresh DB produces the same schema as the migration chain. The `tests/integration/database/test_orm_schema_snapshot.py` test enforces this parity.

## Known historical quirks

- **`004_candidate_preferences_fit_only.py` recreates `candidate_preferences`.** That table is already created by `001_initial_schema.py` at line 235. The `CREATE TABLE IF NOT EXISTS` guard in 004 makes the duplicate DDL a no-op in practice, but the table definition was copy-pasted rather than referenced. Do not repeat this pattern in new migrations; rely on prior migrations to have created their tables.

## Baseline

`001_initial_schema.py` is the immutable baseline. `BASELINE_VERSION` in `database/migrate.py` points at it. Any pre-cutover database must be rebuilt from this baseline; upgrade-in-place from pre-reset schemas is not supported.
