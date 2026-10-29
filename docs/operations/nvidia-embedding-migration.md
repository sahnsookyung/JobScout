# NVIDIA embedding migration

The October 2026 benchmark selected `nvidia/nemotron-3.5-embed-8b` at 1024 dimensions: 67/72 positive holdout cases versus 62/72 for the 1B model at the same size. The benchmark is synthetic and its labels still need human adjudication. This is an operational choice within the current schema, not a claim that a universal similarity threshold is calibrated. Keep the existing LLM qualification stage and inspect real retrieval after rebuilding.

## Contract

Configure the ETL embedding endpoint with these settings on every application and worker process:

```dotenv
ETL_EMBEDDING_BASE_URL=https://integrate.api.nvidia.com/v1
ETL_EMBEDDING_MODEL=nvidia/nemotron-3.5-embed-8b
ETL_EMBEDDING_INPUT_TYPE_ENABLED=true
ETL_EMBEDDING_TIMEOUT_SECONDS=60
ETL_EMBEDDING_REQUESTS_PER_MINUTE=10
ETL_EMBEDDING_MODEL_HEADER_ENV_VARS={}
```

Supply `ETL_EMBEDDING_API_KEY` from the existing NVIDIA credential through the deployment secret mechanism. Remove any legacy embedding API secret and custom Modal headers. Keep `embedding_dimensions: 1024`. Restart all consumers after configuration changes: the model identity is immutable within a process. Changing model, dimensions, endpoint, or role contract requires another complete rebuild. Do not substitute a smaller model on transient failures; its vectors inhabit a different space.

Job summaries and resume evidence/sections use `passage`. Requirements and the separate resume-summary retrieval vector use `query`. Requests use `truncate=NONE`, float encoding, batches of at most 32, and the existing shared NVIDIA extraction rate bucket. Responses must have complete unique indices, finite nonzero vectors, and the configured dimensions. The standard request/token budgets remain enforced; scheduled catalog work retains its existing separately metered budget lane.

## Deployment and recovery

1. Deploy the compatible code and additive schema migration 105 with the new flag still false. Verify the normal schema bootstrap succeeds and all services use the same release.
2. Pause extraction, embedding, matching, and notification workers, and prevent interactive matching/resume writes for the maintenance window. Stop all old application writers before writing new vectors.
3. Take and verify a PostgreSQL backup. Retain a restricted-access snapshot of job summary vectors and requirement vectors for targeted rollback; include each row ID, embedding space, source text/content hash, and old embedding state. Record counts and the previous configuration. Never put credentials or resume contents into deployment output.
4. Apply the new configuration to a maintenance process running the same release. Use an explicit maintenance database connection with BYPASSRLS in `EMBEDDING_MIGRATION_DATABASE_URL`. Do not grant that privilege to application roles. Run `uv run python -m scripts.migrate_embedding_space` first to inspect aggregate pending counts.
5. Run a bounded canary with `--apply --include-resumes --max-batches 1 --workers 1`. This reuses stored structured resumes without re-extracting them, rebuilds their evidence and separate query vectors, and processes one catalog batch per stage. Exit 2 means rows remain; exit 1 means a failed batch. Inspect error types and HTTP status without logging source text or response bodies.
6. Run `--apply --include-resumes --workers 2` to finish active extracted jobs. Only one maintenance process should run at once. Calls run outside database transactions; source text is checked again before atomic writes. Successful batches remain checkpointed on failure, and reruns skip completed rows. Legacy resume vectors are retained. Unchanged match rows are rescored in place, preserving notified/hidden flags.
7. Require zero pending active job/requirement/resume vectors. Verify both query and passage dimensions, job/requirement generation agreement, resume readiness, and retrieval with the new generation. Restart all consumers with the same configuration and observe matching completion, LLM qualification, and normal notification delivery. Existing approved notification tests must not be resent merely for migration.
8. Inactive jobs can be rebuilt separately with `--include-inactive`; until then the generation filters keep them out of active matching. A job becomes eligible only after its summary and every requirement are in the current space.

## Rollback

Stop writers before changing configuration. Restore job and requirement vectors from the verified snapshot, checking source hashes/text before restoring each row so that subsequent edits are not overwritten. Restore their previous embedding state and space. Restart consumers with the previous configuration; retained legacy resume vectors become visible again. Match records from the replacement space are excluded from reuse and will be rescored while preserving notification history. A whole-database restore is a separate recovery operation that can discard writes made since the backup.

If the previous provider remains unavailable, rolling back code/configuration restores consistency but does not restore embedding service. Prefer fixing a resumable backfill failure and completing the new generation.
