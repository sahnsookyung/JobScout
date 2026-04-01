# Fit Semantics and Explainability Implementation Plan

- Date: 2026-04-01
- Related ADR: [ADR 0002](../adr/0002-fit-semantics-and-explainability.md)
- Status: Superseded by [fit-semantics-rewire-plan.md](./fit-semantics-rewire-plan.md) for the next fit architecture slice

## Architecture Decisions

- Retrieval and fit scoring are separate concerns.
- Dense retrieval means ANN/pgvector search over `canonical_job_summary` embeddings.
- Lexical retrieval means exact-term / full-text candidate generation over job text surfaces.
- Hybrid retrieval means using both dense retrieval and lexical retrieval to generate the shortlist.
- Lexical fusion means merging dense and lexical candidate lists with reciprocal-rank fusion; it is not a fit scorer.
- `job_similarity` remains the dense semantic similarity signal used by fit scoring.
- Fused candidate ordering is tracked separately from `job_similarity` so lexical retrieval does not overwrite fit inputs.
- Semantic fit scoring happens after retrieval and candidate-evidence recall.

## Current State

- Milestone 1 foundation is merged.
- Milestone 2 fit semantic scoring is implemented on the active fit branch.
- Hybrid retrieval groundwork and retrieval diagnostics are implemented on the branch.
- Local cross-encoder routing now prefers FlagEmbedding-compatible runtimes in `auto`, with SentenceTransformers and heuristic fallback still supported behind the same provider interface.
- Offline fit evaluation is now intended to stay Python-only so it can directly exercise backend retrieval fusion and semantic scoring code.
- Entitlement administration remains internal-first; the branch provides DB-backed control plus an internal CLI rather than a public admin API/UI.
- The next canonical planning document for fit work is [fit-semantics-rewire-plan.md](./fit-semantics-rewire-plan.md).

## Milestone 1

- Persist `canonical_job_summary` and its contract version/hash on `job_post`
- Generate the summary during extraction
- Use the canonical summary as the primary dense retrieval surface for job embeddings
- Keep pgvector similarity as retrieval only

Status:

- implemented and merged

## Milestone 2

- Introduce a dedicated semantic fit scorer over shortlisted requirement/evidence pairs
- Move user-facing fit explanations to semantic fit outputs
- Keep old cosine diagnostics behind internal/debug surfaces only

Status:

- implemented on the PR 2 branch
- current default semantic scorer is LLM-backed
- threshold scoring is retained as a guarded fallback
- public fit explanation now comes from semantic verdicts rather than raw similarity percentages

## Next Architecture Slice

See [fit-semantics-rewire-plan.md](./fit-semantics-rewire-plan.md) for the replacement plan that formalizes:

- hybrid retrieval as the default retrieval mode
- cross-encoder scoring as the default semantic fit mode
- LLM scoring as an advanced gated mode
- DB-backed entitlements for fit mode access
- configurable recall depth and configurable truncation budgets with metrics
- commented config examples for user-tunable fit settings

Status update:

- the active fit branch now implements the rewire direction above
- hybrid retrieval defaults on
- semantic fit routing supports `cross_encoder`, `llm`, and threshold fallback
- match details surface effective fit mode, provider route, retrieval mode, and fallback messaging
