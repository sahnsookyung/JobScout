# 06 — Quality filters service

## Goal
Reduce noise and scams by labeling jobs with configurable “signal” and “risk” heuristics, without deleting anything.

## Tasks
### A. Filter pipeline
- Implement filters as independent modules that output:
- label(s)
- score delta
- explanation
Examples:
- missing company name
- suspicious domain
- overly vague description
- unrealistic salary ranges
- known scam keywords/patterns

### B. Configuration
- Store filter configs per user (or global default + user overrides).
- Support enabling/disabling filters and adjusting weights.

### C. API & data
- `POST /filters/evaluate` (internal)
- Store results on JobPost: `signal_score`, `signal_labels`, `signal_explanations`.

### D. UX integration
- Inbox shows signal label.
- User can override (mark as “good” even if low signal) and that feedback is stored.

## Acceptance criteria
- Jobs are labeled high/low signal with reasons.
- Users can change thresholds and immediately see different labeling.

## Risks / gotchas
- False positives can frustrate users; always allow override and show reasons.
