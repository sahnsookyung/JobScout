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

## Implementation Update (2026-04-03)

The reranker and judge described as follow-on phases in the Initial Implementation Boundary are now shipped on the preference-semantics branch (PR 3).

What landed:

- `LLMPreferenceSemanticReranker` and `LLMPreferenceJudge` are implemented in `services/scorer_matcher/preference_semantics.py`, both backed by the same `_BaseLLMPreferenceScorer` contract.
- `apply_preference_semantic_reranking` is wired into the scorer-matcher pipeline after fit scoring and before final `top_k` truncation, preserving fit-band ordering through a bounded overall-score recomputation.
- Degraded runs (profile unavailable, reranker unavailable, exception) fall back to fit-only ordering and record `preference_mode_used: "fit_only_fallback"` plus a `preference_fallback_reason` in persisted `fit_components`.
- The split-stack E2E now validates `preference_score > 0`, `preference_mode_used: "semantic_rerank"`, and `tech_stack_match` in `preference_reason_codes` through real API calls and persisted match records.
- `llm_judge` remains `enabled: false` by default; `semantic_rerank` is the production default mode.

## Implementation Update (2026-04-04)

A cross-encoder preference reranking path was added as a cheap alternative to the LLM reranker.

**Motivation**: The LLM reranker requires a running model endpoint. A local cross-encoder can score `(preference_label, job_segment)` pairs offline with no LLM calls, making it suitable for free-tier or batch use cases.

What landed:

- `LocalCrossEncoderProvider.score_text_pairs(List[tuple[str, str]]) -> List[float]` added to `core/scorer/semantic_fit.py`. Mirrors `score_pairs` logic but accepts raw string pairs instead of `SerializedPair` domain objects; normalizes through the same `_normalize_semantic_score` path.
- `PreferenceCrossEncoderConfig` and `PreferencesConfig.reranker: Literal["llm", "cross_encoder"]` added to `core/config_loader.py`.
- `CrossEncoderPreferenceReranker` added to `services/scorer_matcher/preference_semantics.py`. Scores each `(preference_label, job_segment)` pair with the cross-encoder, surfaces top-K as `"{category}:{label}|{segment_label}"` detail codes plus `"{category}_match"` category codes.
- `build_preference_semantic_reranker` now accepts `PreferencesConfig` and routes to `CrossEncoderPreferenceReranker` when `config.reranker == "cross_encoder"`.
- `config.yaml`: `preferences.reranker: "llm"` is the default. Cross-encoder block is documented as a commented example.

**LLM vs cross-encoder trade-offs**:

| | LLM reranker | Cross-encoder reranker |
|---|---|---|
| Requires running model | yes | no (loads model locally on first call) |
| Explanation quality | free-text natural language | category + segment codes |
| Preference reason codes | LLM-generated labels | `category:label\|segment` format |
| Free-tier viable | no | yes |

The default remains `reranker: "llm"`. Set `reranker: "cross_encoder"` with `cross_encoder.enabled: true` to activate the local path.

## Consequences

- Preference parsing can use a different model or provider than ETL extraction.
- The frontend can explain the user mental model more clearly: filter first, fit second, personalize last.
- Personalization work can ship incrementally without changing fit semantics.
