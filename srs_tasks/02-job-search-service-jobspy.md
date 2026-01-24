# 02 — Job search service (JobSpy)

## Goal
Provide a stable internal API for job discovery while outsourcing scraping complexity to JobSpy.

## Why a service boundary
Scraping is noisy (rate limits, proxies, retries) and benefits from isolation, caching, and separate scaling.

## Recommended approach
Start by adopting **jobspy-api** as-is (Dockerized FastAPI wrapper around JobSpy), then add a thin adapter in your core backend.

## Tasks
### A. Decide API strategy
Option 1 (fastest): deploy jobspy-api directly and call it from core backend.
Option 2 (more control): fork jobspy-api and add your custom caching/dedup hooks.
Option 3 (lowest footprint): call JobSpy library directly from your monolith (not recommended long-term).

### B. Deploy jobspy-api
- Run it as a container in docker-compose.
- Configure API key auth, rate limiting, proxies (if needed), CORS.
- Confirm `/docs` and `/api/v1/search_jobs` work locally.

### C. Define the internal “Search Contract”
Your core backend should *not* leak JobSpy’s raw fields into your DB schema. Define a normalized contract:
- input: search_term, location, remote flag, sites, results_wanted, posted_within_hours, salary range, tech stack keywords.
- output: canonical job fields + raw payload (for debugging) + metadata (source, fetched_at).

### D. Caching & TTL policy
- Implement a cache key based on normalized search parameters.
- Use Redis (or Postgres table) to store:
- cached results blob
- fetched_at
- ttl_seconds
- serve_from_cache flag
- Default TTL target: 7 days (configurable) even if upstream defaults differ.

### E. Scheduling
- A scheduler (cron, Celery, Sidekiq, or a simple worker) runs saved searches.
- Each run writes:
- SearchRun record (started_at, ended_at, success, error)
- Discovered jobs -> inbox ingestion
- Idempotency: a rerun with same SearchRun ID must not duplicate inbox rows.

### F. Hardening for scrapers
- Retry policy with jitter.
- Circuit-breaker per site.
- Proxy pool management.
- Backoff on 429 and scrape failures.

## Acceptance criteria
- Saved searches execute on schedule and populate Job Inbox.
- Results are cached with a configurable TTL.
- Failures are captured with actionable error messages.

## Notes
JobSpy supports concurrent scraping, multiple job boards, and proxy support, which you should expose as configuration at the service layer.
