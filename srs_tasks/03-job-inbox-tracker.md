# 03 — Job inbox + tracker

## Goal
Turn discovered jobs into an actionable workflow: review → shortlist → tailor → apply → follow up.

## Tasks
### A. Job ingestion pipeline
- Insert/Upsert by (source, source_job_id) and also map to a canonical job fingerprint.
- Store raw payload for debugging plus normalized fields for UI.
- Preserve job history (status changes, notes).

### B. Inbox UI
- Views: New, Shortlisted, Applied, Archived, All.
- Columns: title, company, location/remote, source, date_posted, match score, signal label, dedup cluster label.
- Actions: open job link, save, ignore, start tailoring.

### C. Tracker UI
- Kanban or list view with statuses.
- Status aging and “stale” highlighting.
- Notes, contacts, interview schedule, attachments.

### D. Data and API endpoints (examples)
- `POST /jobs/ingest` (internal)
- `GET /jobs?status=...&q=...`
- `PATCH /jobs/{id}/status`
- `POST /jobs/{id}/notes`

### E. Permissions
- Jobs belong to a user.
- Admin can inspect for debugging in self-host.

## Acceptance criteria
- User can process jobs end-to-end without leaving the app.
- All actions are auditable and reversible (archive vs delete).

## Risks / gotchas
- Without canonicalization + dedup early, inbox becomes noisy quickly.
