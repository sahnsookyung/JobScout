# Testing Guide

JobScout uses three practical layers of coverage:

- `tests/unit/` for fast isolated checks
- `tests/integration/` for DB/Redis and cross-service lifecycle coverage
- `tests/smoke/` for manual/live-service verification that is not part of the default CI gate

## Common Commands

```bash
# All Python tests
uv run python -m pytest tests/ -v

# Fast unit slice
uv run python -m pytest tests/ -v -m "not db and not redis and not integration and not slow"

# CI-safe integration slice
uv run python -m pytest \
  tests/integration/test_full_pipeline.py \
  tests/integration/test_resume_pipeline_lifecycle.py \
  tests/integration/services/test_services_lifespan.py \
  -v

# Split-stack E2E resume flow (real Docker services, fake deterministic AI)
uv run python -m pytest \
  tests/integration/test_split_stack_resume_flow.py \
  -v

# Frontend component/hook tests
cd web/frontend && npm run test
```

## Markers

- `db`: requires PostgreSQL with pgvector
- `redis`: requires Redis
- `integration`: cross-service or persistence-heavy coverage
- `slow`: longer-running container-backed tests

## Infrastructure-Backed Tests

`tests/conftest.py` provides shared `test_database` and `redis_container` fixtures using `testcontainers`. Most integration tests can run locally without prestarting services as long as Docker is available.

You can still point tests at existing infrastructure:

```bash
export TEST_DATABASE_URL="postgresql://user:pass@localhost:5433/jobscout_test"
export TEST_REDIS_URL="redis://localhost:6379/0"
uv run python -m pytest tests/integration/test_resume_pipeline_lifecycle.py -v
```

## Integration Layout

- `tests/integration/test_full_pipeline.py`: repository-backed end-to-end pipeline coverage
- `tests/integration/test_resume_pipeline_lifecycle.py`: latest-upload eligibility and stale-result lifecycle rules
- `tests/integration/services/test_services_lifespan.py`: microservice startup/shutdown behavior
- `tests/integration/test_split_stack_resume_flow.py`: real Dockerized upload -> extract/embed -> matching happy/failure flow with deterministic fake AI

## Smoke Tests

`tests/smoke/` is reserved for manual or exploratory live-service checks. These tests are ignored unless you opt in:

```bash
SMOKE_TESTS=1 uv run python -m pytest tests/smoke/ -v
```

## CI Policy

- Unit tests are required on every PR.
- The CI-safe integration slice is also required on every PR in a separate job.
- The split-stack resume E2E flow is required in CI to catch service image, shared volume, and async handoff regressions.
- `AUTH_MODE=dev-bypass` safety is enforced by tests and startup checks; production-like environments must not boot with dev bypass enabled.
