# Preference Semantics and Personalization Implementation Plan

- Date: 2026-04-01
- Related ADR: [ADR 0003](../adr/0003-preference-semantics-and-personalization.md)
- Status: In Progress

## Milestone 1

- Add independent preference model configuration
- Persist `preference_mode`, `soft_preference_summary`, and optional parsed `PreferenceProfile`
- Return allowed and effective modes from the candidate preferences API

Status:

- implemented
- parser-backed profile persistence is now best-effort and degrades cleanly when the parser is unavailable

## Milestone 2

- Add default semantic preference reranking on fit-qualified shortlists
- Record fallback/degraded mode behavior explicitly
- Add latency and shortlist observability

Status:

- implemented on the preference-semantics branch
- reranking now runs after fit qualification and before final `top_k` truncation
- fit-band ordering is preserved so preference cannot override fit dominance across bands
- degraded runs now mark `preference_mode_used="fit_only_fallback"` with explicit fallback metadata in persisted match diagnostics

## Milestone 3

- Evaluate optional `llm_judge`
- Expand capability gating beyond the current global-config allowlist when entitlement infrastructure exists

Status:

- initial `llm_judge` support exists behind the same preference scorer contract
- broader rollout, evaluation, and gating beyond the current backend allowlist remain follow-up work
