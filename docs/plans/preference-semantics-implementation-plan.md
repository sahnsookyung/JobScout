# Preference Semantics and Personalization Implementation Plan

- Date: 2026-04-01
- Related ADR: [ADR 0003](../adr/0003-preference-semantics-and-personalization.md)
- Status: In Progress

## Milestone 1

- Add independent preference model configuration
- Persist `preference_mode`, `soft_preference_summary`, and optional parsed `PreferenceProfile`
- Return allowed and effective modes from the candidate preferences API

## Milestone 2

- Add default semantic preference reranking on fit-qualified shortlists
- Record fallback/degraded mode behavior explicitly
- Add latency and shortlist observability

## Milestone 3

- Evaluate optional `llm_judge`
- Expand capability gating beyond the current global-config allowlist when entitlement infrastructure exists
