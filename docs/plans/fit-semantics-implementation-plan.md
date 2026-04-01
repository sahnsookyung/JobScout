# Fit Semantics and Explainability Implementation Plan

- Date: 2026-04-01
- Related ADR: [ADR 0002](../adr/0002-fit-semantics-and-explainability.md)
- Status: In Progress

## Milestone 1

- Persist `canonical_job_summary` and its contract version/hash on `job_post`
- Generate the summary during extraction
- Use the canonical summary as the primary dense retrieval surface for job embeddings
- Keep pgvector similarity as retrieval only

## Milestone 2

- Introduce a dedicated semantic fit scorer over shortlisted requirement/evidence pairs
- Move user-facing fit explanations to semantic fit outputs
- Keep old cosine diagnostics behind internal/debug surfaces only

## Milestone 3

- Add lexical retrieval and fusion
- Evaluate optional `LLMFitJudge` only after the default semantic fit scorer is stable
