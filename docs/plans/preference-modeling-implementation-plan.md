# Preference Modeling Implementation Plan

- Date: 2026-04-01
- Related ADR: [ADR 0001](../adr/0001-preference-modeling-architecture.md)
- Status: Superseded by fit-semantics-implementation-plan.md and preference-semantics-implementation-plan.md

## Goal

Replace the current facet-based `want_score` architecture with:

- explicit structured hard filters
- fit-first hybrid retrieval
- free-text soft-preference reranking on a shortlist

This plan is designed to avoid the current half-shipped state while giving us an incremental path to the target architecture.
It assumes a destructive replacement of the old `want_score` model rather than a long compatibility period.

## Success Criteria

- Users can express hard constraints in the product UI.
- Matching applies hard constraints deterministically.
- Ranking remains fit-dominant.
- Free-text preferences can improve ordering among plausible jobs.
- The old facet-based `want_score` is removed from production ranking, APIs, and UI.
- We can explain why a job was shown using fit evidence, matched filters, and preference rationale.

## Non-Goals

- Train a learned-to-rank model in the first rollout.
- Replace the current fit pipeline from scratch.
- Make LLM judging the default online ranking mechanism for all jobs.

## Current-State Problems To Fix

- `want_score` is built around a brittle 7-facet ontology.
- wants are loaded from a file path rather than from a real per-user profile.
- preference logic is mixed with fields that should be hard filters.
- the old ranking path is easy to misinterpret as a trustworthy personalized score.

## High-Level Delivery Strategy

Implement this in five phases:

1. Neutralize the old preference path and make prerequisites trustworthy.
2. Add the new candidate preference model and UI.
3. Add deterministic filtering and concrete v1 hybrid candidate generation.
4. Add fit-first gating, recomputation rules, and shortlist preference reranking.
5. Remove old facet-based paths completely.

## Phase 0: Guardrails and Baseline

### Objectives

- Stop misleading preference behavior from affecting ranking.
- Make hard-filter data trustworthy before exposing those controls to users.
- Verify fit-score correctness before fit becomes the gating primitive for the new ranker.
- Establish an evaluation baseline before architectural changes.

### Tasks

- Set production ranking to fit-only while the new system is being built.
- Remove `want_score` from production ranking logic immediately.
- Remove or zero `want_score` in API responses and UI surfaces during migration. Do not keep it as a live compatibility feature.
- Audit job metadata coverage for:
  - remote mode
  - visa sponsorship
  - salary
  - employment type
  - geography
- Define null semantics and `unknown` handling for every hard-filter field.
- Backfill or improve extraction for hard-filter metadata before the UI depends on it.
- Review and tighten fit-score correctness before downstream ranking depends on it.
- Add logging and metrics for:
  - candidate retrieval count
  - post-filter count
  - fit score distribution
  - shortlist size
  - notification and click-through outcomes if available
- Freeze a benchmark dataset for offline comparisons:
  - representative resumes
  - representative job sets
  - expected top results

### Deliverables

- fit-only production ranking
- metadata coverage audit and backfill plan
- hard-filter field semantics document
- fit-score verification report
- baseline evaluation report
- destructive-removal checklist for old wants/facet code paths

## Phase 1: New Preference Domain Model

### Objectives

- Model hard constraints separately from soft preferences.
- Make preference data user-owned and product-facing.
- Define preference versioning and recomputation rules from the start.

### Data Model

Create a new candidate preference model with:

- hard constraints
  - remote mode
  - target locations
  - visa sponsorship required
  - salary floor
  - employment type
- soft preferences
  - free-text preference statement
  - optional preference summary generated for display only

### Backend Tasks

- Add new persistence for candidate preferences.
- Add read/write API endpoints for preference management.
- Add validation and normalization rules for structured filters.
- Keep free-text preferences raw; do not map them into a fixed taxonomy.
- Add preference versioning.
- Define stale-match invalidation and recomputation triggers when preferences change.
- Define notification behavior when preference changes reorder saved or newly eligible matches.

### Frontend Tasks

- Add a settings or onboarding flow with:
  - structured hard-filter controls
  - one free-text prompt for soft preferences
- Gate any hard-filter control behind metadata readiness from Phase 0.
- Show a clear product mental model:
  - "We filter by your must-haves, then rank by fit, then personalize among strong matches."

### Deliverables

- new preference schema
- API contract
- UI input flow
- preference versioning and invalidation spec

## Phase 2: Deterministic Filtering and Hybrid Retrieval

### Objectives

- Apply hard constraints before expensive scoring.
- Improve recall versus dense retrieval alone.

### Retrieval Design

V1 candidate generation will combine:

- top 200 dense ANN candidates over job summary embeddings
- top 100 PostgreSQL full-text candidates over title, skills, and requirements

Then:

- union candidate sets
- deduplicate
- enforce hard constraints
- pass the surviving set into fit scoring

V1 will not include a separate preference-oriented retrieval pass.
Soft preferences remain rerank-only until we have evidence that preference-oriented retrieval improves recall materially.

### Backend Tasks

- Add query-time structured filtering to the retrieval path.
- Add PostgreSQL full-text retrieval support.
- Use reciprocal rank fusion for v1 candidate merging.
- Add retrieval diagnostics for which retriever contributed each candidate.
- Set explicit latency budgets for:
  - dense retrieval
  - lexical retrieval
  - merge and filtering

### Data Tasks

- Ensure job records expose structured filterable fields cleanly.
- Enforce the `unknown` semantics defined in Phase 0 for missing salary, visa, remote, geography, and employment type data.

### Deliverables

- hybrid retrieval service
- filter-aware candidate generation
- retrieval diagnostics
- v1 retrieval budget and latency target document

## Phase 3: Fit-First Gating

### Objectives

- Preserve the current strength of JobScout: requirement-aware fit scoring.
- Ensure low-fit jobs do not reach preference reranking.
- Make stored match state correct when preferences change.

### Tasks

- Keep the current requirement-to-resume-evidence matching as the core fit engine.
- Define a fit floor for shortlist eligibility.
- Add offline tests covering:
  - exact-skill recall
  - requirement coverage
  - false positives
  - missing-required handling
- Implement stale marking and recomputation for preference changes.
- Decide whether notifications are fit-only, fit-plus-preference, or both, and make that behavior explicit in the pipeline.

### Deliverables

- fit eligibility rule for shortlist admission
- test coverage for fit gating
- preference-change invalidation and recomputation flow

## Phase 4: Soft-Preference Reranking

### Objectives

- Personalize ranking among plausible jobs without letting preference overpower fit.

### Ranking Design

Rerank only the top shortlist, for example top 50 to 100 jobs after fit gating.

Preferred default:

- cross-encoder or equivalent reranker using:
  - user free-text soft preferences
  - selected job text with a fixed input contract:
    - job title
    - normalized summary
    - required requirements
    - preferred requirements
    - benefits
    - company/team/work-style text
    - explicit truncation policy and token budget

Not first choice for online ranking:

- full LLM-as-judge over all candidates

### Combination Strategy

Do not use a flat additive blend such as:

- `0.8 * fit + 0.2 * preference`

Use a fit-dominant bounded combiner instead, such as:

- multiplicative adjustment
- gated preference bonus
- monotonic fit-preserving rerank within bands

### Explanation Design

Replace facet explanations with:

- matched hard filters
- fit evidence
- preference rationale
  - for example: "you said you want fast-moving teams and remote flexibility; this role emphasizes both"

### Deliverables

- shortlist reranker
- bounded final-score combiner
- explanation payload for UI
- reranker input contract
- latency and quality acceptance criteria

## Phase 5: Destructive Removal

### Objectives

- Remove the old architecture after the replacement path is stable.

### Tasks

- Remove `want_score` from persisted ranking flows, APIs, DTOs, UI, and sorting behavior.
- Remove `user_wants_file`-driven matching behavior.
- Remove facet-based preference weighting from scoring configuration.
- Remove old tables and code paths:
  - facet-based wants storage
  - facet-based want scoring
  - facet-based ranking blend
- Delete or migrate obsolete schema elements rather than leaving them in place for compatibility.

### Deliverables

- removal PR
- cleanup migration
- updated docs and config

## Suggested Work Breakdown

### Workstream A: Product and UX

- define the hard-filter set
- define onboarding and settings UX
- define explanation copy

### Workstream B: Preference Data Model and API

- schema
- validation
- persistence
- APIs

### Workstream C: Retrieval

- structured filtering
- lexical retrieval
- candidate merge logic

### Workstream D: Ranking

- fit gating
- preference reranking
- score combination

### Workstream E: Migration and Cleanup

- destructive replacement
- old-path neutralization
- schema cleanup

## Rollout Plan

### Step 1

- ship fit-only ranking in production
- remove old preference score from UI and API

### Step 2

- finish metadata backfill and hard-filter readiness
- ship candidate preference capture UI
- persist structured filters and free-text preferences

### Step 3

- ship deterministic filtering
- ship hybrid retrieval behind a feature flag

### Step 4

- ship preference reranking to internal users only
- compare against fit-only baseline

### Step 5

- roll out bounded preference reranking gradually
- remove old facet-based ranking path and schema

## Evaluation Plan

### Offline

- top-k precision on curated resume-job pairs
- recall for exact-skill and title matches
- fit-floor false-positive rate
- preference reorder quality on hand-labeled examples
- launch gate:
  - no regression to fit-only baseline on fit precision beyond agreed tolerance
  - reranking improves preference-ordering labels on the curated set

### Online

- click-through rate
- save / shortlist rate
- apply-start or apply-click rate
- hide / dismiss rate
- latency by retrieval and reranking stage
- launch gate:
  - no material regression in fit-driven engagement metrics
  - reranking latency stays within agreed SLA
  - hard-filter satisfaction defects stay below agreed threshold

## Risks

- Hard-filter extraction may be incomplete if job metadata is missing.
- Lexical retrieval may require new indexing infrastructure or query tuning.
- Cross-encoder reranking may add unacceptable latency if shortlist size is too large.
- Free-text preferences may be vague, causing weak reranking signal.
- Destructive removal may break consumers if API and UI cleanup are not coordinated tightly.

## Open Questions

- What exact fit floor should shortlist admission use in v1?
- What latency budget will we enforce for Phase 4 reranking?
- Should notification triggering depend only on fit-qualified jobs, or on final reranked jobs?

## Recommended First Milestone

The first milestone should be a low-risk architectural pivot:

- production ranking becomes fit-only
- old preference score is removed from production surfaces
- metadata readiness for hard filters is established
- fit-score correctness is verified
- new candidate preference model is introduced
- frontend captures hard filters plus free-text preferences

That milestone removes the current misleading state without requiring the full reranking system to be complete on day one.
