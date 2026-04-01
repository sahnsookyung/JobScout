# Fit Semantics and Explainability Implementation Plan

- Date: 2026-04-01
- Related ADR: [ADR 0002](../adr/0002-fit-semantics-and-explainability.md)
- Status: In Progress

## Architecture Decisions

- Retrieval and fit scoring are separate concerns.
- Dense retrieval means ANN/pgvector search over `canonical_job_summary` embeddings.
- Lexical retrieval means exact-term / full-text candidate generation over job text surfaces.
- Hybrid retrieval means using both dense retrieval and lexical retrieval to generate the shortlist.
- Lexical fusion means merging dense and lexical candidate lists with reciprocal-rank fusion; it is not a fit scorer.
- `job_similarity` remains the dense semantic similarity signal used by fit scoring.
- Fused candidate ordering is tracked separately from `job_similarity` so lexical retrieval does not overwrite fit inputs.
- Semantic fit scoring happens after retrieval and candidate-evidence recall.

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

## Milestone 3

- Add lexical retrieval and fusion
- Evaluate optional `LLMFitJudge` only after the default semantic fit scorer is stable

Current direction:

- hybrid retrieval is being added behind matcher config
- lexical retrieval uses PostgreSQL full-text search over title, canonical summary, description, skills, and company/work-style text
- fusion uses reciprocal-rank fusion to combine dense and lexical ranks
- fused retrieval order is carried separately from dense `job_similarity`
- retrieval diagnostics are persisted inside `fit_components` / `fit_explanation` so match details can show whether a result came from dense-only or hybrid candidate generation
- semantic scorer diagnostics now include scorer identity, latency, judged-requirement counts, and explicit fallback metadata

Remaining work after the current slice:

- validate hybrid retrieval quality with offline fixtures and targeted integration tests
- decide whether hybrid retrieval should become enabled by default
- decide whether to add a dedicated cross-encoder scorer after the LLM scorer baseline
- keep preference semantic reranking as a separate follow-on milestone
