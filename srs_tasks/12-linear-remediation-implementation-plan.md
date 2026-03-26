# Linear Remediation Implementation Plan

## Summary
- This plan implements the phase-1 decisions captured in `11-linear-outstanding-remediation-spec.md`.
- It is structured to reduce rework by establishing repo ownership, tenancy enforcement, and auth/security before feature work.
- Cloud-only capabilities remain owned by `jobscout-cloud`; OSS work focuses on shared/core behavior and interfaces.

## Assumed Defaults
- Resume dedup is per-user/per-tenant only.
- `SHA-256` is the authoritative raw-file hash everywhere.
- Any faster local hash is optional optimization only and must never be authoritative for correctness.
- SaaS raw resume/object retention defaults to 24h max and is configurable.
- `pdf` / `docx` parsing uses Docling first, fail-open to legacy parser with metrics.
- ATS phase 1 is Greenhouse only, admin-configured by board token.
- Web auth uses cookie-based first-party sessions.
- Shared DB + row isolation is enforced in both app/repository code and DB-level policies.
- OSS bootstrap uses one default local tenant and one default local user.
- Because the application is not yet in production, DB reset/reinstall is allowed during rollout instead of full live backfill choreography.

## Ownership Boundaries
- OSS repo:
  - tenancy-aware domain interfaces
  - auth/session contract and local default-user/default-tenant mode
  - users, tenants, membership/auth context runtime needed by OSS
  - resume parsing, dedup, upload/data pipeline
  - numeric penalty correctness
  - canonical job/import data model contracts
- `jobscout-cloud`:
  - cloud deployment and infra
  - hosted ATS sync runtime, scheduler, secrets, and operational sync jobs
  - billing/subscriptions
  - recruiter workflows
- Shared interface boundary:
  - provider config contracts
  - tenant-aware repository/service interfaces
  - import payload schema/versioning
  - Greenhouse connector contract

## Phase 1: Tenancy and Security Foundation
- Introduce an authenticated request context carrying `user_id`, `tenant_id`, and role.
- Replace optional tenant filtering with required tenant/user scoping in repository/service boundaries for tenant-owned data.
- Add DB-level isolation policy support for tenant-owned tables.
- Apply RLS in phase 1 to tenant-owned business tables and use narrowly scoped service-role bypass for internal workers/migrations only.
- Define cookie-session auth contract:
  - Google token verification on bootstrap
  - first-party session issuance
  - CSRF/replay protection
  - secure cookie attributes
  - session rotation/revocation/logout semantics
- Add audit logging for auth events and failed verification attempts.
- Because pre-prod reset is acceptable, rollout can use destructive schema reset rather than backward-compatible live migration if that reduces risk.

## Phase 2: Resume Ownership, Dedup, and Parsing
- Redesign resume persistence around owned resume records:
  - each resume belongs to one user
  - one active resume per user in phase 1
  - no cross-user hash reuse/existence leakage
- Update dedup checks/endpoints so they operate within current user/tenant scope only.
- Compute authoritative `SHA-256` on raw file bytes before parsing.
- Replace current `pdf` / `docx` parsing path with Docling.
- Keep legacy parser only as fail-open fallback and emit:
  - fallback count/rate metrics
  - parser backend/version metadata
  - structured error logs
- Route SaaS resume uploads through object reference flow compatible with R2-backed processing.

## Phase 3: Matching Correctness and Numeric Logic
- Preserve regex fallback for years extraction, but prefer explicit numeric fields when available.
- Keep numeric comparison in penalty/scoring logic for phase 1; do not turn it into retrieval-time exclusion.
- Add tests proving that semantically similar phrasing with different numbers does not bypass penalties.
- Make provider/embedding compatibility explicit:
  - configuration-driven provider selection
  - documented vector dimension compatibility policy
  - observable fallback behavior

## Phase 4: Job Ingestion and Import Model
- Add Greenhouse ingestion contract with admin-configured board tokens.
- Add import workflow tables:
  - `job_import_batch`
  - `job_import_row_error` or equivalent
- Extend `job_post_source` with clear provider/source semantics and `source_account`.
- Reuse `job_post` as the canonical merged posting.
- Implement premium JSON import flow with:
  - strict schema validation
  - versioned payload format
  - size/item caps
  - MIME sniffing and extension allowlist
  - malware/quarantine policy for hosted uploads
  - decompression/resource limits
  - sanitization/normalization
  - async rate-limited processing
  - no arbitrary remote URL fetches in phase 1
- Ensure exact-source dedup happens inside provider/account/import namespace, with canonical dedup flowing through `job_post`.

## Phase 5: Job Lifecycle and Platform Work
- Implement 30-day stale job lifecycle handling with status/soft-delete semantics first.
- Define how stale jobs affect:
  - active matching eligibility
  - match visibility/status
  - notification history/audit data
- Keep JobSpy unchanged in phase 1; if URL checks are needed, implement lightweight validation in JobScout before any JobSpy API expansion.
- Keep cloud deployment work and hosted operational concerns in `jobscout-cloud`.
- Treat `JOB-32` as a prerequisite for cloud-only implementation work, but not for OSS foundation phases.

## Test and Audit Gates
- Tenancy:
  - repository tests for cross-tenant isolation
  - DB-policy tests for cross-tenant isolation
- Auth:
  - Google verification claim tests
  - CSRF/session/cookie behavior tests
- Resume pipeline:
  - owned-resume constraints
  - per-user dedup behavior
  - Docling success/fallback behavior
  - no cross-user existence leakage
- Matching:
  - numeric penalty correctness tests
  - regex fallback coverage tests
- Imports:
  - batch lifecycle tests
  - row validation/rejection tests
  - exact-source dedup tests
  - malformed/oversized hostile input tests
- Migrations:
  - constraint/index creation
  - reset/reinstall path for pre-prod rollout
  - rollback assumptions where destructive reset is not used
  - no incompatible data-type drift for user/tenant ownership keys

## Sequencing Rule
- Do not implement cloud-owned features in OSS before the repo ownership boundary and shared contracts are explicit.
- Do not implement phase-2 explainability/RAG, billing, or recruiter flows before phases 1-4 are complete.

## Deferred / Non-Phase-1 Items
- `JOB-12`: provider routing improvements beyond current config-driven baseline
- `JOB-17`: server-persisted notification settings UI after tenancy/auth foundation exists
- `JOB-18`: mood-lifter/login polish
- `JOB-19`: RAG explanations as paid/on-demand phase 2
- `JOB-32`: cloud repo bootstrap required before cloud-only runtime work begins
