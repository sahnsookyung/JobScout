# 10 — Observability + CI/CD

## Goal
Make the system maintainable: logs, metrics, traces, automated tests, and repeatable deployments.

## Tasks
### A. Observability
- Structured logs with request IDs.
- Metrics: search runs, scrape failures, cache hit rate, match computation time, export failures.
- Tracing across services (core → jobspy → generator).

### B. Testing strategy
- Unit tests for:
- parsing
- scoring
- dedup
- filter pipeline
- Integration tests using containers:
- jobspy-api running
- generator container running
- Snapshot tests for exported documents.

### C. CI
- Lint + typecheck.
- Run tests.
- Build images.

### D. CD
- Self-host: docker-compose + upgrade notes.
- Cloud: helm chart or terraform (later).

## Acceptance criteria
- A new contributor can run `make dev` and get a working environment.
- A PR cannot merge if tests fail.

## Risks / gotchas
- Scraper instability can make tests flaky; mock external calls and keep a small set of controlled fixtures.
