# ADR 0003: Preference Semantics and Personalization

- Status: Accepted
- Date: 2026-04-01

## Context

Candidate preferences are distinct from qualification fit. Fit answers whether a candidate can do the work; preference semantics answer whether the candidate would want the job. The backend also needs independent model configuration for preference parsing and future preference reranking so that personalization is not coupled to ETL extraction settings.

## Decision

We introduce a separate preference subsystem with its own configuration namespace and user preference mode.

- Hard constraints remain structured fields.
- Free-text soft preferences are parsed into a normalized `PreferenceProfile`.
- `semantic_rerank` is the default mode.
- `llm_judge` is optional and advanced, never the default.
- Preference logic operates only after fit-qualified jobs are identified.

The backend owns the effective allowed modes and returns them to the frontend. If a stored mode is not currently allowed, the backend falls back to the configured default mode.

## Initial Implementation Boundary

This milestone records the foundation:

- preference config is independent from `etl.llm`
- soft preference text is persisted alongside an optional parsed profile
- APIs expose `preference_mode`, `allowed_preference_modes`, and `effective_preference_mode`

The actual semantic shortlist reranker and optional judge are follow-on implementation phases, but their interfaces and config surface are now explicit.

## Consequences

- Preference parsing can use a different model or provider than ETL extraction.
- The frontend can explain the user mental model more clearly: filter first, fit second, personalize last.
- Personalization work can ship incrementally without changing fit semantics.
