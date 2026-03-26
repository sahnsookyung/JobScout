# Linear Outstanding Remediation Spec (JOB Team)

## Snapshot
- Source board: `JobScout-saas (JOB)`
- Snapshot date: `2026-03-25`
- Outstanding issues audited: `26`

## Scope Decisions From Audit
- Mark as completed (status stale): `JOB-5`, `JOB-6`, `JOB-13`
- Mark as irrelevant/superseded: `JOB-1`, `JOB-2`, `JOB-3`, `JOB-4`, `JOB-7`, `JOB-24`
- Candidate implementation scope (this spec): `JOB-8`, `JOB-9`, `JOB-10`, `JOB-11`, `JOB-12`, `JOB-14`, `JOB-15`, `JOB-16`, `JOB-17`, `JOB-18`, `JOB-19`, `JOB-20`, `JOB-21`, `JOB-22`, `JOB-23`, `JOB-25`, `JOB-32`

## Decision Log (2026-03-25)
- `JOB-8`: Keep regex fallback. Missing years data should not fail the pipeline; continue with best effort.
- `JOB-9`: Numeric comparison is primarily for penalty correctness, not strict retrieval-time exclusion by default.
- `JOB-10`: Resume dedup is per-user/per-tenant only. No cross-user reuse, even if raw file bytes are identical.
- `JOB-10`: Use `SHA-256` as the authoritative hash everywhere. Do not depend on `XXH64` for correctness.
- `JOB-11`: OSS should mirror SaaS per-user constraints using a transparent default local user.
- `JOB-12`: Do not hard-wire Groq. Provider/model selection must remain configurable through config/env vars.
- `JOB-14`: Reduce stale-job purge window from 180 days to 30 days.
- `JOB-15`: Cloud deployment artifacts belong in `jobscout-cloud`, not this OSS repo.
- `JOB-15`: SaaS should not rely on cloud-hosted scraping of job boards. Prefer ATS/API ingestion plus validated user-provided imports.
- SaaS ATS ingestion should be admin-configured in phase 1, not user-configured through product UI.
- `JOB-17`: Notification settings should persist server-side.
- `JOB-18`: Keep as low priority / afterthought.
- `JOB-19`: Treat RAG explanations as phase 2. Prefer async generation with streaming progress and caching.
- `JOB-20`: Prefer cookie-based first-party sessions for the web app.
- `JOB-21`: Use shared database with row isolation.
- `JOB-21`: OSS owns runtime schema and logic only for users, tenants, membership/auth context, resume ownership, and tenant-owned core records needed by the OSS app.
- `JOB-21`: Recruiter profiles, subscriptions, plans, and billing entitlements are cloud-owned and should not be fully implemented in OSS runtime.
- `JOB-22`: Recruiter double-blind flow is phase 3.
- `JOB-23`: Billing belongs only in `jobscout-cloud`.
- `JOB-25`: Pricing tiers can be drafted here, then implemented in `jobscout-cloud`.
- Resume parsing: Use Docling as the primary parser for `pdf` and `docx` rather than the current simple text extractor.
- Resume parsing: If Docling fails, fail open to the legacy parser and emit metrics/logging for the fallback.
- Git/GitNexus: If `jobscout-cloud` uses this OSS repo as a submodule, keep GitNexus indexes separate and avoid indexing the submodule contents twice.
- ATS phase 1: Launch with Greenhouse only.
- OSS bootstrap: use one default local tenant and one default local user so auth/tenancy code paths mirror hosted behavior.
- RLS phase 1: enforce on tenant-owned business tables, with tightly scoped service-role bypass for internal workers/migrations only.
- Storage retention: after source-object deletion, retain immutable metadata only (hashes, size, MIME type, ownership, timestamps, parser metadata, audit status).
- `JOB-32`: `jobscout-cloud` repo initialization is a prerequisite for cloud-only implementation, but not for OSS foundational work.
- Migration strategy: because the app is not yet in production, destructive DB reset/reinstall is allowed during rollout instead of full backfill/cutover complexity.

## Recommended Defaults
- `JOB-10` / `JOB-16`: Hash raw uploaded file bytes with `SHA-256` before parsing. Do not use Docling output as the primary dedup key.
- `JOB-16`: In SaaS, upload directly to private Cloudflare R2 with short-lived presigned URLs; avoid proxying raw file bytes through the app server when possible.
- `JOB-20`: Verify Google ID token once on backend, then issue first-party session credentials instead of reusing Google token for every API call.
- `JOB-20`: Prefer cookie-based first-party sessions for the web app unless non-browser/mobile clients become a first-class requirement.
- `JOB-32`: Initialize `jobscout-cloud` first if we want clean ownership boundaries for SaaS-only work.
- `JOB-15` / `JOB-32`: If `jobscout-cloud` is a superrepo using this OSS repo as a submodule, keep separate GitNexus indexes for each repo rather than trying to build one graph across the submodule boundary.
- SaaS ingestion: Support ATS/API imports first and premium validated JSON uploads second; defer arbitrary remote source plugins.
- Premium JSON imports should reuse canonical job tables but add explicit import-tracking tables for auditability and validation feedback.

## Tenancy Enforcement Contract
- Shared DB isolation must be enforced in both application code and the database.
- Every authenticated request must carry:
  - `user_id`
  - `tenant_id`
  - role/context needed for authorization decisions
- Repository/service methods handling tenant-owned data must require tenant context rather than treating it as optional.
- Database design should be compatible with row-level security for tenant-owned tables.
- Acceptance criteria:
  - cross-tenant reads/writes are blocked in repository tests
  - cross-tenant reads/writes are blocked by DB policy tests
  - OSS default-user mode remains local-only and does not weaken hosted-mode enforcement
  - tenant context bootstrap in OSS uses one local tenant + one local user by default

## Resume Identity Model
- Raw file identity:
  - compute `SHA-256` from raw file bytes before Docling or other parsing
- Dedup scope:
  - dedup applies per user/tenant only
  - identical files uploaded by different users must not reuse ownership or existence state
- Ownership model:
  - each stored resume belongs to exactly one user
  - one active resume per user in phase 1
  - future multi-resume support must add more owned resume records rather than changing cross-user dedup semantics
- Privacy requirement:
  - APIs must not reveal whether another user already uploaded the same resume

## Implementation Items

### JOB-8: Resume Years of Experience Handling
- Goal: Use structured numeric years fields as primary source for experience matching and penalties.
- Gap: Regex fallback and free-text parsing still influence penalties.
- Draft acceptance criteria:
  - Penalty logic prefers explicit numeric fields (`years_value`, `total_experience_years`) and does not rely on free-text when numeric data exists.
  - If numeric years are absent, regex/text fallback remains allowed as best-effort behavior.
  - Add/adjust tests for years extraction and scoring behavior.

### JOB-9: Scalar Filtering for Numeric Fields
- Goal: Enforce hard numeric filters (years, salary) independently of cosine similarity.
- Gap: Current implementation still relies on semantic similarity for numeric comparisons that should affect penalties based on explicit values.
- Draft acceptance criteria:
  - Penalty logic compares extracted numeric requirement values against extracted numeric resume values directly.
  - Similar wording with different numbers does not bypass penalty logic via cosine similarity.
  - Tests verify under-qualified resumes are penalized correctly for years/salary-style requirements.
- Explicit non-goal for phase 1:
  - do not turn this into a hard retrieval-time filter unless separately specified later

### JOB-10: Upload-N-Process-Once Resume Deduplication
- Goal: Ensure identical resume uploads are skipped safely and deterministically.
- Gap: Dedup exists but current hash and scope strategy should diverge between OSS/local and SaaS/cloud storage concerns.
- Draft acceptance criteria:
  - Hashing strategy is documented for both OSS/local and SaaS/cloud paths.
  - Primary dedup identity is based on raw file bytes, not parser-specific extracted output.
  - Docling output may be cached as a derived artifact but does not replace raw-file identity.
  - Duplicate uploads do not trigger extraction/embedding recompute.
  - SaaS path supports storing an object reference for R2-backed processing.
  - dedup checks are scoped to the current user/tenant only
  - Tests cover repeat upload behavior and hash mismatch handling.

### JOB-11: One Resume Per User (Paid Multi-Resume Ready)
- Goal: Enforce one active resume per user now, with schema path for future paid multi-resume.
- Gap: Frontend local storage is single-entry, but backend data model and dedup are not per-user enforced.
- Draft acceptance criteria:
  - Backend persists resume ownership and enforces one active resume per user in OSS/SaaS mode.
  - OSS mode uses a transparent default user without requiring interactive registration.
  - Data model supports future expansion to multiple resumes by plan/entitlement.
  - Migration path documented.

### JOB-12: LLM Workflows to GroqCloud + Modal
- Goal: Define provider routing for extraction and embeddings across local and remote providers.
- Gap: Config is partially provider-aware, but implementation remains OpenAI/Ollama-centric in places.
- Draft acceptance criteria:
  - Config supports provider selection per stage (extraction, embeddings).
  - Provider/model overrides work through config/env vars without code changes.
  - Optional remote embedding/runtime integrations have clear fallback behavior.

### JOB-14: JobSpy Scraper Microservice + 180-Day Purger
- Goal: Complete operational lifecycle for scraped jobs, including stale data removal.
- Gap: JobSpy is wired; staleness/liveness purge is missing.
- Draft acceptance criteria:
  - Scheduled purge process removes jobs older than 30 days and/or jobs deemed stale by policy.
  - Purge behavior is idempotent and auditable.
  - Tests for staleness-based purge flows.
- Implementation note:
  - Phase 1 should not require JobSpy API changes.
  - If URL checks are added, prefer lightweight validation inside JobScout first.
  - Add JobSpy "refresh by URL" only if later source-aware re-fetch semantics are truly needed.
- Purge semantics:
  - use soft-delete or status transition first, not immediate hard-delete
  - define follow-on handling for matches, notifications, and auditability before hard purge

### JOB-15: Move Scraping/Matching to Cloud (Koyeb + Cloud Run)
- Goal: Provide deployable cloud topology for scraping and matching workloads.
- Gap: Current setup is Docker/local split topology only, and cloud-hosted scraping of public job boards is likely to be blocked or unreliable.
- Draft acceptance criteria:
  - Cloud deployment design is documented for `jobscout-cloud`.
  - OSS repo keeps only interfaces/config required for deployment portability.
  - Handoff notes define what moves to the SaaS repo.
  - SaaS ingestion strategy is ATS/API-first rather than scraper-first.
  - ATS ingestion is admin-configured through backend config/database state in phase 1 rather than end-user self-service UI.
  - Premium import path can accept validated user-supplied joblist JSON without arbitrary server-side fetching.
- Repo/tooling note:
  - If using a superrepo + submodule layout, Git is managed separately for parent and child repos, and GitNexus must also be managed separately.

### JOB-16: Local-First Resume Storage via IndexedDB
- Goal: Avoid server persistence of raw resume files where possible.
- Gap: IndexedDB is present, but backend still accepts and temporarily writes uploaded files for processing.
- Draft acceptance criteria:
  - SaaS path supports direct browser upload to private R2 via short-lived presigned URLs.
  - Worker-side document parsing may download the object from R2 for processing, but app-server proxying of raw uploads is avoided.
  - Docling becomes the primary parser for `pdf` / `docx` resumes before LLM extraction.
  - Backend processes via object reference/metadata instead of long-lived app-server temp files where feasible.
  - OSS/local path may continue to use simple local processing.
  - Policy and docs clarify retention guarantees.
- Retention:
  - SaaS raw resume objects should be deleted after successful extraction or after a configurable max retention window, defaulting to 24 hours

### Resume Parsing Strategy
- Goal: Improve parsing quality for document resumes before LLM extraction.
- Decision:
  - Keep current parser for `json`, `yaml`, and `txt`.
  - Replace current `pdf` / `docx` text extraction path with Docling as the canonical parser.
- Rationale:
  - The current parser extracts plain text but loses document structure and keeps layout noise.
  - A fallback-selection mechanism adds complexity without a clear evaluation framework.
- Draft acceptance criteria:
  - `pdf` / `docx` upload flow routes through Docling by default.
  - Parsing metadata records parser backend/version for reproducibility.
  - Existing structured extraction pipeline consumes normalized Docling output rather than raw `pypdf`/`python-docx` text.
  - If Docling fails, the system falls back to the legacy parser and records structured metrics/log events for fallback rate.
  - Tests cover representative resume layouts and parser failure behavior.
- Operational note:
  - pin Docling version explicitly and make fallback rate observable

### JOB-17: Notification Settings Modal
- Goal: Add frontend UI for notification channel configuration and wire it to backend.
- Gap: Notification backend exists, but frontend settings UI for channels does not.
- Draft acceptance criteria:
  - Settings modal supports email and webhook channel configuration.
  - Validation and save flow implemented end-to-end.
  - Settings persist server-side and are available to background workers.
  - Tests for form validation and save/update behavior.

### JOB-18: Mood Lifter on Login
- Goal: Display supportive/encouraging message at login.
- Gap: Login screen exists but no dynamic emotional support content.
- Draft acceptance criteria:
  - Message/fact appears on login screen and rotates by deterministic or random strategy.
  - UX works in both OSS and SaaS auth modes.
  - Frontend tests cover rendering and rotation logic.
- Deferred priority:
  - Treat as low-priority polish after core platform work.

### JOB-19: RAG for Explainable Match Justification
- Goal: Generate natural-language explanations grounded in retrieved evidence.
- Gap: Existing explanation endpoint returns similarity breakdown, not RAG/LLM-generated narrative.
- Draft acceptance criteria:
  - Retrieval pipeline selects relevant evidence chunks.
  - LLM-generated explanation job streams progress asynchronously and caches completed output.
  - Explanation payload includes evidence citations/snippets and cache invalidation inputs.
  - Tests verify grounding format, cache hit behavior, and fallback behavior.
- Protocol recommendation:
  - Reuse HTTP + SSE rather than WebSockets for explanation generation lifecycle.

### JOB-20: Google OAuth + JWT Middleware
- Goal: Implement full backend auth and token verification for protected APIs.
- Gap: Frontend Google login shell exists; backend `get_current_user` is currently no-op.
- Draft acceptance criteria:
  - Backend verifies Google ID token during sign-in/bootstrap.
  - Backend issues first-party session credentials for subsequent API access.
  - Protected routes enforce authenticated user context.
  - Auth integration tests added.
- Session model:
  - Default to cookie-based first-party sessions for the web app.
  - Revisit bearer access/refresh tokens only if non-browser/mobile clients become first-class.
- Security checklist:
  - verify Google claims including `aud`, `iss`, `exp`, and stable subject identifier
  - require `email_verified` where email is used for UX/account recovery
  - protect login/bootstrap flow against CSRF/replay
  - set secure cookie attributes (`HttpOnly`, `Secure`, `SameSite`)
  - define session rotation, revocation, logout, and expiry behavior
  - audit authentication events and failed verification attempts

### JOB-21: Multi-Tenancy Schema (Users, Recruiters, Subscriptions)
- Goal: Define tenant-aware identity and billing-related data model.
- Gap: Tenant table and tenant_id on jobs exist; recruiter/subscription schema not implemented.
- Draft acceptance criteria:
  - OSS schema/migrations cover users, tenants, membership/auth context, resume ownership, and tenant-owned core records needed by the OSS app.
  - cloud-only schema for recruiter profiles, subscriptions/plans, and billing entitlements is explicitly excluded from OSS runtime implementation
  - Service/repository updates for shared-db row isolation.
  - Tests for tenant data isolation.
  - DB-level RLS or equivalent policies enforced for tenant-owned data.

## RLS Scope
- Phase 1 RLS should apply to tenant-owned business tables, including:
  - canonical job records and sources
  - owned resume records and related tenant-owned metadata
  - import batches/errors
  - tenant-owned notification/settings records where applicable
- Worker and migration bypass:
  - use tightly scoped internal service roles only
  - internal services must still carry tenant context in application logic
  - avoid broad bypass for interactive app roles

### JOB-22: Recruiter Onboarding (Double-Blind Proxy)
- Goal: Enable recruiter invitation flow without exposing candidate contact details directly.
- Gap: No recruiter onboarding/proxy workflow implemented in current app.
- Draft acceptance criteria:
  - Recruiter invite/request workflow and status tracking data model.
  - Candidate consent/acceptance flow before contact reveal.
  - API endpoints and audit logging for invite actions.
- Priority:
  - Phase 3, likely `jobscout-cloud` owned.

### JOB-23: Payment Gateway (Dodo Payments)
- Goal: Implement checkout and webhook-driven subscription state synchronization.
- Gap: No billing/payment codepaths currently exist in this repo.
- Draft acceptance criteria:
  - Checkout session endpoint.
  - Webhook verification and idempotent subscription upsert.
  - Entitlement checks wired to feature gating.
- Ownership:
  - Implement only in `jobscout-cloud`.

### JOB-25: Pricing Tiers
- Goal: Define monetization tiers and feature entitlements.
- Gap: No pricing/plan model or entitlement gating found.
- Draft acceptance criteria:
  - Tier definitions and entitlements documented.
  - Data model and API expose active plan and permitted capabilities.
  - Feature gates connected to auth/subscription context.
- Next step:
  - Draft tier proposal in planning unless final business inputs arrive first.

### SaaS Job Ingestion Strategy
- Goal: Support reliable hosted job ingestion without relying on blocked cloud scraping.
- Decision:
  - SaaS should not scrape public job boards directly from cloud infrastructure by default.
  - First-party ingestion should target ATS/provider APIs where available.
  - Premium import path should allow validated user-uploaded joblist JSON.
- Configuration model:
  - ATS/provider sources are admin-configured in phase 1.
  - Provider configuration includes an explicit list of source accounts/boards to sync, such as Greenhouse board tokens.
- Security posture:
  - Treat uploaded joblist JSON as hostile user input.
  - Enforce strict schema validation, field allowlists, size limits, item count limits, string length caps, sanitization/normalization, and async rate-limited processing.
  - Do not automatically fetch arbitrary remote URLs from uploaded JSON in phase 1.
- Draft acceptance criteria:
  - Phase 1 ATS/provider import path is Greenhouse with admin-configured board tokens.
  - JSON import format is versioned and validated strictly.
  - Imports are quota-controlled and attributable per user/tenant.
  - Imported job text is isolated as untrusted input before any downstream processing.
- Greenhouse nuance:
  - provider config must support a list of board tokens/companies to sync

### Job Import Storage and Schema
- Goal: Support premium user-provided joblist imports safely and observably.
- Decision:
  - Reuse `job_post` as the canonical normalized job record.
  - Reuse `job_post_source` for provenance and exact-source deduplication.
  - Add dedicated import-tracking tables rather than overloading existing canonical job tables with workflow state.
- Canonical storage model:
  - `job_post` remains the merged canonical posting for scoring, embedding, and matching.
  - `job_post_source` records where a job came from, including ATS/API imports and JSON uploads.
- Required schema additions:
  - `job_import_batch`
    - `id`
    - `tenant_id`
    - `uploaded_by_user_id`
    - `source_type` such as `greenhouse`, `ashby`, `lever`, `json_upload`
    - `status` such as `pending`, `validating`, `processing`, `completed`, `failed`, `partial`
    - `storage_key` for temporary uploaded source object
    - `content_sha256`
    - `row_count`
    - `accepted_count`
    - `rejected_count`
    - `error_summary`
    - timestamps
  - `job_import_row_error` or equivalent row-level error table
    - import batch foreign key
    - source row identifier/index
    - normalized row fingerprint if available
    - validation error code/message
    - optional sanitized rejected payload fragment
- Constraint/index expectations:
  - explicit FKs to tenant/user/import batch records
  - idempotency key or content hash to prevent duplicate batch processing
  - indexes on status, tenant, uploaded_by, and created_at for operations/support
  - enum/status values versioned in migrations rather than inferred ad hoc
- Recommended `job_post_source` extensions:
  - clarify `site` semantics to mean provider/source type
  - add `source_account` for provider account identity, such as Greenhouse board token or import batch namespace
  - retain `source_job_id` and normalized `job_url`
- Retention model:
  - uploaded JSON source files may be stored temporarily in object storage
  - default retention should be short-lived, similar to SaaS resume source retention
  - normalized DB records and row-level validation results may persist longer for audit/support
- Draft acceptance criteria:
  - import batches are attributable, retryable, and observable independently of canonical job records
  - exact-source dedup works within a provider/account/import namespace
  - canonical dedup still flows through `job_post` and `canonical_fingerprint`
  - rejected rows can be surfaced with actionable validation errors
  - migration plan covers new constraints, indexes, and rollback assumptions

## Ingestion Security Controls
- Treat resume files and uploaded job JSON as hostile input.
- Add:
  - parser fallback/failure metrics
  - MIME sniffing plus extension allowlist
  - object quarantine / malware scanning policy for hosted uploads
  - decompression/resource limits for parsers
  - document parsing sandbox/resource limits where feasible
  - ATS credential storage/rotation strategy
  - signed webhook verification for future billing/provider callbacks
  - prompt/input sanitization boundaries for imported text
  - XSS-safe rendering requirements for any imported/generated text shown in UI

## Provider and Embedding Compatibility Constraints
- Provider/model selection must remain configurable.
- Any embedding provider change must preserve or explicitly migrate vector dimensionality/index compatibility.
- Fallback behavior for remote providers should be observable and documented.

## Repo Ownership Matrix
- OSS repo owns:
  - core matching/scoring correctness
  - local/OSS auth behavior
  - resume parsing pipeline
  - user/tenant/membership runtime needed by OSS
  - shared abstractions/interfaces used by cloud
- `jobscout-cloud` owns:
  - billing
  - cloud deployment manifests/runbooks
  - hosted ATS ingestion runtime, scheduler, secrets, and operational sync jobs
  - recruiter workflows
- Shared boundary:
  - data contracts, provider configs, and tenant-aware interfaces
  - Greenhouse connector contract in OSS, but hosted execution in cloud

### JOB-32: Initialize `jobscout-cloud` Private Repo
- Goal: Create SaaS companion repo and workspace/submodule integration.
- Gap: No submodule configured in this repo.
- Draft acceptance criteria:
  - `jobscout-cloud` repo initialized with expected structure.
  - Submodule linkage and uv workspace membership defined.
  - Bootstrap docs for local dev across both repos.
  - Repo tooling guidance documents Git and GitNexus workflows for the OSS repo and the superrepo separately.
- Repo model:
  - Parent repo Git tracks the SaaS/private code plus the submodule pointer.
  - Child repo Git tracks only the OSS code within the submodule.
  - GitNexus must be treated as separate indexes; do not assume deduplication across parent and child graphs.

## Proposed Planning Sequence (Draft)
1. Repo boundary and tenancy foundation: `Repo Ownership Matrix`, `JOB-21`, `Tenancy Enforcement Contract`
2. Auth/security baseline: `JOB-20`
3. Resume correctness/data integrity: `JOB-8`, `JOB-9`, `JOB-10`, `JOB-11`, `JOB-16`, `Resume Parsing Strategy`
4. Job ingestion foundation: `SaaS Job Ingestion Strategy`, `Job Import Storage and Schema`, `JOB-14`
5. Product UI and explainability: `JOB-17`, `JOB-18`, `JOB-19`
6. Platform/infrastructure: `JOB-12`, `JOB-15`, `JOB-32`
7. SaaS commercialization: `JOB-22`, `JOB-23`, `JOB-25`

## Blocking Decisions Needed Before Detailed Implementation Plan
- No major blocking product decisions remain for phase-1 planning.
- Planning should assume:
  - `SHA-256` authoritative hash everywhere
  - 24h default configurable SaaS object retention
  - Greenhouse-only ATS launch
  - per-user/per-tenant resume dedup
  - Docling fail-open with metrics
