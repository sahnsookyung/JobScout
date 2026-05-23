# Native Resume Variants Test Matrix

This matrix is the acceptance bar for the native resume-variant feature. It is
kept close to the implementation so future work does not relax the security,
concurrency, Always Free, or CI parallelism guarantees by accident.

## Required Shards

- Fast unit shard: `rtk pytest tests/unit -m "not slow" -v`
- Security/concurrency shard: `rtk pytest tests -m "security or concurrency" -v`
- DB/Redis shard: `rtk pytest tests -m "db or redis" -v`
- Full backend shard: `rtk pytest tests -v`
- Frontend shard: unit tests, type check, lint, and build under `web/frontend`

## Security Coverage

- Cross-owner and cross-tenant access must be denied for create, get, list, and download.
- Raw `X-Tenant-Id` must not be trusted by resume-variant endpoints unless SaaS
  middleware has resolved it into request state.
- Client attempts to submit protected ownership, tenant, fingerprint, source hash,
  or evidence metadata fields must fail request validation.
- Renderer tests must cover stored-XSS payloads in JSON, HTML, Markdown, and DOCX.
- Logs must not include resume contents, job descriptions, generated text, tokens,
  webhook URLs, or full tenant/user identifiers.

## Concurrency And Quota Coverage

- Redis quota increments must be atomic under parallel calls.
- Redis generation locks must use token-owned compare-and-delete release.
- Fresh variant reuse must not consume quota or acquire a generation lock.
- Redis outage in hosted production must fail closed before generation.
- DB idempotency and pruning must remain deterministic under concurrent requests.

## Scalability And Always Free Coverage

- Tests must include bounded large resume/job/requirement cases and size-limit failures.
- Query-count or bounded-list tests must prevent N+1 and unbounded loads.
- Contract tests must prevent adding a new always-on service, large renderer, local
  model dependency, persistent generated-file volume, or paid provider enabled by default.
- Generated binary outputs must be streamed from in-memory data or cleaned temp files.

## Maintainability Rules

- New tests must be order-independent and safe with isolated temp dirs, fixed clocks,
  seeded IDs, and unique Redis prefixes.
- Add `security`, `concurrency`, and `performance` marks for test selection.
- Keep feature tests layered: unit, repository/DB, API/security, renderer, frontend.
- Frontend coverage must include on-demand generation, nonblank error states, and
  streamed downloads for every exposed format.
