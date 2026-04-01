# ADR 0002: Fit Semantics and Explainability

- Status: Accepted
- Date: 2026-04-01

## Context

JobScout currently uses dense pgvector retrieval over summary embeddings plus requirement-to-evidence matching. That works as a first-pass recall mechanism, but cosine similarity is not a trustworthy final fit authority or a clean user-facing explanation source. Summary retrieval should stay cheap and broad; fit reasoning should become stricter and more explainable downstream.

## Decision

We split fit into explicit stages:

1. ANN retrieval over a first-class `canonical_job_summary`
2. requirement/evidence recall using existing JRU/REU embeddings
3. stronger semantic fit scoring on shortlisted evidence pairs
4. fit-first aggregation and explanation

`canonical_job_summary` is now a first-class persisted field generated during extraction with a stable contract and embedded for dense retrieval. pgvector cosine similarity remains a retrieval and recall signal, not the default user-facing explanation layer.

## Stable Summary Contract

The extraction pipeline must generate `canonical_job_summary` in this order:

1. role title and seniority
2. core responsibilities
3. key required skills or experience
4. key preferred skills or experience
5. work arrangement and location flexibility
6. compensation and visa or relocation if known
7. company, team, or work-style cues

The summary must be compact, deterministic in section order, and suitable for embedding. Contract version changes require regeneration and re-embedding.

## Consequences

- Dense retrieval becomes more stable because it no longer depends on human-written blurbs.
- Current cosine-threshold explainability is demoted to internal diagnostics.
- Future semantic fit scorers can operate on better retrieval input without changing the retrieval contract again.
