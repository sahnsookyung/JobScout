# 07 — Deduplication + clustering

## Goal
Detect reposts and near-duplicates across boards and group jobs so users don’t re-review the same posting.

## Tasks
### A. Canonical job fingerprint
- Create a fingerprint from:
- normalized company name
- normalized title
- location/remote flag
- key JD text shingles/embeddings

### B. Dedup rules
- Exact duplicate: same source job URL or same (source, source_job_id).
- Near-duplicate: high similarity on JD embeddings + close title/company match.

### C. Clustering
- Maintain a `job_cluster_id` representing a role family at a company.
- Cluster view shows:
- all postings in cluster
- earliest seen date
- variations (locations, seniority)

### D. UX
- Inbox collapses clusters by default.
- User can expand and mark “reviewed once” to suppress future reposts.

## Acceptance criteria
- Reposts are grouped together with high precision.
- Users can avoid re-reviewing the same job multiple times.

## Risks / gotchas
- Company normalization (Inc vs Ltd) needs a robust alias system.
