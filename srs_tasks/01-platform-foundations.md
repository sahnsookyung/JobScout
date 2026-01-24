# 01 — Platform foundations

## Goal
Ship the core “spine” that every other feature plugs into: authentication, users/tenancy, persistence, authorization, and a consistent domain model.

## Scope
Backend API + database schema + minimal frontend scaffolding.

## Key decisions (recommended)
- Monolith-first backend (FastAPI or Go) with clear module boundaries; extract microservices only where a hard boundary exists (JobSpy search, generation, notifications).
- Postgres for production; SQLite acceptable for local/offline mode.
- Event log / audit table early (especially for approvals, exports, and notification sending).

## Tasks
### A. Repos and local dev
- Create mono-repo (or orchestrated multi-repo) with:
- docker-compose for Postgres, Redis, vector DB (optional), and service containers.
- `.env` management and secret handling (dev vs prod).
- Seed scripts for a demo user, demo resume, and a few fake jobs.

### B. Auth & user management
- Implement user signup/login (email magic link or OAuth) and sessions.
- Implement organizations/workspaces only if needed; otherwise keep it single-tenant-per-user.
- Implement RBAC: user, admin (for self-host instance) at minimum.

### C. Core data model (minimum)
- Users
- Resumes (versioned master)
- Jobs (canonical job + source job)
- Job matches (per resume version)
- Tailored variants (per job)
- Saved searches
- Notifications
- Audit log

### D. API conventions
- Standardize on:
- Request IDs and correlation IDs.
- Pagination format.
- Error envelope (code, message, details).
- Idempotency keys for scheduled jobs and notifications.

### E. Storage and encryption
- At-rest encryption: encrypt sensitive fields (LLM keys, tokens).
- File storage abstraction (local filesystem for self-host; S3-compatible optional).

## Acceptance criteria
- A user can sign up, create a master resume, and see it persist across sessions.
- A user can create a saved search definition (even if it doesn’t run yet).
- Audit log records key actions (resume edit, export, approval toggles).

## Risks / gotchas
- If you delay “versioning” of resume content, diff/approval and provenance will be harder later.
- If you delay audit logging, debugging “who exported what” becomes painful.
