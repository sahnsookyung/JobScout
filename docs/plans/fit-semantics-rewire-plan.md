# Fit Semantics Rewire Plan

- Date: 2026-04-02
- Related ADR: [ADR 0002](../adr/0002-fit-semantics-and-explainability.md)
- Status: Implemented direction on the active fit-semantics branch; remaining work is tuning, offline evaluation, and operationalization

## Summary

Rewire the fit subsystem so this branch becomes the home for all fit-related work: hybrid retrieval is enabled by default, semantic fit scoring is routed through one provider interface, cross-encoder scoring is the standard path, LLM scoring is an advanced gated path, and DB-backed entitlements control access to advanced modes.

The fit pipeline becomes:

1. hybrid retrieval by default
2. configurable requirement/evidence recall
3. semantic fit judgment through a routed provider
4. existing fit aggregation on normalized requirement verdicts
5. threshold fallback only when configured and needed
6. persisted diagnostics for retrieval mode, scorer mode, route, latency, truncation, and fallback

## Key Changes

### 1. Fit mode and provider routing

- Replace `semantic_fit_enabled` and `semantic_fit_fallback_to_threshold` with `matching.scorer.semantic_fit`.
- Supported fit modes:
  - `cross_encoder`
  - `llm`
  - `threshold` fallback only
- Cross-encoder route policies:
  - `local`
  - `remote`
  - `auto`
- Route selection happens after evidence recall and before semantic judging, based on the full pair count being submitted.
- `auto` routing:
  - choose `remote` when environment is production, `cross_encoder.remote.enabled=true`, and pair count exceeds `cross_encoder.remote_promote_pair_count`
  - otherwise choose `local`
  - if the chosen route fails or times out, try the other enabled cross-encoder route once
  - if both fail, use threshold fallback
- If `matching.scorer.semantic_fit.enabled=false`:
  - bypass entitlements and mode resolution
  - bypass semantic providers
  - run threshold scoring only
  - persist `effective_fit_mode="threshold"` and `semantic_fit_disabled=true`

### 2. Concrete providers and contracts

- Providers:
  - `LocalCrossEncoderProvider`
  - `RemoteCrossEncoderProvider`
  - `LLMFitProvider`
  - `ThresholdFallbackProvider`
- `LocalCrossEncoderProvider` is real in this slice, with runtime selection behind an interface.
- Add optional dependency group `fit-models`.
- Default local model: `BAAI/bge-reranker-v2-m3`.
- Local runtime selection:
  - `auto`: prefer FlagEmbedding, then SentenceTransformers, then heuristic fallback
  - `flag_embedding`
  - `sentence_transformers`
  - `heuristic`
- The code is written to the provider interface, not to hardcoded model names; multilingual BGE rerankers are examples, not protocol requirements.
- Shared provider output contract:
  - `PairAssessment { pair_id, requirement_id, semantic_score, confidence, reason }`
- Coverage is derived from `semantic_score`:
  - `>= 0.80` -> `covered`
  - `>= 0.55 and < 0.80` -> `partial`
  - `< 0.55` -> `missing`

### 3. Cross-encoder and LLM normalization

- Treat cross-encoder output as an unbounded logit.
- Normalize with `sigmoid(raw_logit)`.
- Confidence rule: `abs(semantic_score - 0.55) / 0.45`, clamped to `[0,1]`.
- Deterministic cross-encoder reasons:
  - `covered`: `Evidence strongly matches the requirement.`
  - `partial`: `Evidence is related but does not fully satisfy the requirement.`
  - `missing`: `Evidence does not support the requirement.`
- `LLMFitProvider` keeps the current structured judgment flow internally but emits `PairAssessment`.
- LLM mapping:
  - `covered` -> `semantic_score=max(model_score, 0.80)`
  - `partial` -> `semantic_score` in `[0.55, 0.79]`
  - `missing` -> `semantic_score<0.55`
- Any LLM provider failure falls back to threshold for the full request.
- For this slice, `llm.provider` is fixed to `openai_compatible`.

### 4. Recall budget and pair selection

- Replace hard-coded `k=3` with config:
  - `matching.scorer.semantic_fit.recall_top_k`
- Default `recall_top_k=5`.
- Treat recall depth as a tuning knob, not an architectural constant.
- Best-pair selection:
  - highest `semantic_score`
  - then highest `confidence`
  - then highest dense similarity
- If a requirement has zero recalled evidence pairs:
  - skip provider scoring
  - emit `missing`, `semantic_score=0.0`, `confidence=1.0`, reason `No supporting resume evidence was recalled for this requirement.`

### 5. Pair serialization, budgets, truncation, and IDs

- All providers use the same logical pair source fields and truncation rules.
- Field order:
  1. `requirement_text`
  2. `req_type`
  3. `evidence_text`
  4. `evidence_section`
  5. `job_title`
  6. `job_company`
  7. `job_summary`
- Operating budgets are configurable, not fixed constants in code:
  - `matching.scorer.semantic_fit.serialization.requirement_text_max_chars`
  - `matching.scorer.semantic_fit.serialization.evidence_text_max_chars`
  - `matching.scorer.semantic_fit.serialization.evidence_section_max_chars`
  - `matching.scorer.semantic_fit.serialization.job_title_max_chars`
  - `matching.scorer.semantic_fit.serialization.job_company_max_chars`
  - `matching.scorer.semantic_fit.serialization.job_summary_max_chars`
- Default operating budgets:
  - `requirement_text`: 500
  - `req_type`: 32
  - `evidence_text`: 2500
  - `evidence_section`: 64
  - `job_title`: 200
  - `job_company`: 200
  - `job_summary`: 1800
- Local serialization template:
  - `Requirement: {requirement_text}`
  - `Requirement Type: {req_type}`
  - `Evidence: {evidence_text}`
  - `Evidence Section: {evidence_section}`
  - `Job Title: {job_title}`
  - `Company: {job_company}`
  - `Job Summary: {job_summary}`
- Each line appears once, in that exact order, with missing values as empty strings.
- Remote provider receives structured fields directly.
- LLM provider receives the same fields in JSON in the same order.
- Pair IDs use post-truncation values:
  - `sha256(job_id + '|' + requirement_id + '|' + evidence_rank + '|' + evidence_section + '|' + evidence_text)[:32]`
- Truncation policy:
  - if a field exceeds the configured operating budget, truncate deterministically and continue
  - record diagnostics for every truncation
- Emergency safety ceiling:
  - exists behind the configurable budgets as a fail-safe
  - much higher than the operating budgets
  - not exposed as the normal tuning surface
  - only prevents runaway payloads or bad config from destabilizing scoring

### 6. Truncation diagnostics and dashboard metrics

- Persist per-request and per-pair truncation diagnostics in `fit_components` / `fit_explanation`:
  - original length by field
  - submitted length by field
  - `truncated: true|false`
  - truncated fields list
  - total truncated chars
  - whether the emergency ceiling was hit
- Emit aggregate metrics for later dashboard ingestion:
  - truncation rate by field
  - truncation rate by scorer mode/provider
  - average discarded chars
  - emergency-ceiling hit count

### 7. Hybrid retrieval defaults

- Set `matching.matcher.hybrid_retrieval_enabled=true` by default.
- Keep a global kill switch to disable it.
- Retrieval contract:
  - dense ANN over `canonical_job_summary`
  - PostgreSQL lexical retrieval over title, canonical summary, description, skills, and company/work-style text
  - reciprocal-rank fusion
- Preserve:
  - `job_similarity` as dense semantic signal
  - `retrieval_score` as fused candidate order
- Persist retrieval diagnostics:
  - `mode`
  - `sources`
  - `retrieval_score`
  - `job_similarity`
  - `lexical_score` when present

### 8. DB-backed entitlements

- Add `user_feature_entitlement`:
  - `id`
  - `owner_id`
  - `feature_key`
  - `enabled`
  - `value_json`
  - `source`
  - `created_at`
  - `updated_at`
  - unique on `(owner_id, feature_key)`
- Fit keys:
  - `fit.semantic.allowed_modes` with `{"modes": ["cross_encoder", "llm"]}`
  - `fit.semantic.preferred_mode` with `{"mode": "llm"}`
- Global config split:
  - `deploy_allowed_modes`
  - `baseline_allowed_modes`
  - `default_mode`
- Resolution order:
  1. internal per-run override
  2. entitled preferred mode if allowed
  3. global default
- Effective allowed modes:
  - entitlement row present: `intersection(deploy_allowed_modes, entitlement_modes)`
  - otherwise: `intersection(deploy_allowed_modes, baseline_allowed_modes)`
- `default_mode` must be both deploy-allowed and baseline-allowed.
- If an entitled user’s effective allowed modes exclude `default_mode`, resolve to first deterministic allowed mode in order: `cross_encoder`, then `llm`.
- Internal per-run override:
  - internal-only, never public API input
  - may bypass per-user entitlement
  - may not bypass `deploy_allowed_modes`

### 9. Explicit config schema and startup validation

- `matching.scorer.semantic_fit` keys:
  - `enabled: bool`
  - `deploy_allowed_modes: [cross_encoder|llm]`
  - `baseline_allowed_modes: [cross_encoder|llm]`
  - `default_mode: cross_encoder|llm`
  - `recall_top_k: int`
  - `cross_encoder.route_policy: local|remote|auto`
  - `cross_encoder.remote_promote_pair_count: int`
  - `cross_encoder.local.enabled: bool`
  - `cross_encoder.local.model_name: str`
  - `cross_encoder.local.model_cache_path: str`
  - `cross_encoder.local.device_policy: cpu`
  - `cross_encoder.local.max_batch_size: int`
  - `cross_encoder.local.max_concurrency: int`
  - `cross_encoder.local.timeout_ms: int`
  - `cross_encoder.remote.enabled: bool`
  - `cross_encoder.remote.base_url: str | null`
  - `cross_encoder.remote.api_key: str | null`
  - `cross_encoder.remote.model: str`
  - `cross_encoder.remote.timeout_ms: int`
  - `cross_encoder.remote.max_batch_size: int`
  - `llm.enabled: bool`
  - `llm.provider: openai_compatible`
  - `llm.base_url: str | null`
  - `llm.api_key: str | null`
  - `llm.api_secret: str | null`
  - `llm.headers: map | null`
  - `llm.model: str`
  - `llm.temperature: float`
  - `llm.timeout_seconds: int`
  - `llm.max_input_tokens: int`
  - `serialization.*` char-budget fields
- Local runtime policy:
  - CPU only
  - default `max_batch_size=32`
  - default `max_concurrency=1`
  - no auto-download of model weights
  - weights must already exist in configured cache/model path
- Threshold is the only fallback mode in this slice.
- Validation rules:
  - `default_mode` must be in `deploy_allowed_modes`
  - `baseline_allowed_modes` normalized to intersection with deploy-allowed; if empty, use `[default_mode]`
  - explicit route policy targeting a disabled route fails startup
  - if deploy allows `llm`, then `llm.enabled=true`, `llm.model` set, and `llm.api_key` present
  - if `cross_encoder.remote.enabled=true`, require `base_url`, `model`, `timeout_ms`

### 10. Diagnostics and compatibility

- Extend persisted `fit_components` / `fit_explanation` with:
  - `effective_fit_mode`
  - `provider_route`
  - `retrieval`
  - `semantic_fit_diagnostics`
  - `fallback_reason`
  - truncation diagnostics
- `semantic_fit_diagnostics` includes:
  - scorer mode
  - provider route
  - provider/model id
  - latency
  - judged requirement count
  - timeout/fallback flags
- Match list APIs unchanged.
- Match detail and explanation responses expose diagnostics.
- Legacy rows remain valid:
  - missing diagnostics return `null` / omission
  - no backfill required
- User-facing explanation stays limited to:
  - summary
  - requirement verdict reasons
  - retrieval mode
  - scorer mode / provider route label
  - fallback message

## Config Examples

```yaml
matching:
  matcher:
    hybrid_retrieval_enabled: true
    lexical_limit: 100
    fusion_rank_constant: 60
    # lexical_limit: 200          # Raise if dense-only retrieval misses exact skill/title matches
    # fusion_rank_constant: 40    # Lower values make top-ranked retrieval hits weigh more in fusion

  scorer:
    semantic_fit:
      enabled: true
      deploy_allowed_modes: ["cross_encoder"]
      baseline_allowed_modes: ["cross_encoder"]
      default_mode: "cross_encoder"
      recall_top_k: 5
      # recall_top_k: 8           # Raise for better recall, lower for lower latency/cost

      cross_encoder:
        route_policy: "local"
        remote_promote_pair_count: 40
        local:
          enabled: true
          model_name: "cross-encoder/ms-marco-MiniLM-L-6-v2"
          model_cache_path: "/models/cross-encoders"
          device_policy: "cpu"
          max_batch_size: 32
          max_concurrency: 1
          timeout_ms: 2000
          # max_batch_size: 64    # Raise if CPU/memory allow larger batches
          # timeout_ms: 4000      # Raise if long evidence/job contexts are common
        remote:
          enabled: false
          base_url: null
          api_key: null
          model: "fit-cross-encoder-v1"
          timeout_ms: 1500
          max_batch_size: 64
          # enabled: true         # Turn on remote scoring for production or larger pair volumes
          # base_url: "https://fit.example.com"
          # api_key: "..."

      llm:
        enabled: false
        provider: "openai_compatible"
        base_url: null
        api_key: null
        model: "gpt-4o-mini"
        temperature: 0.0
        timeout_seconds: 20
        max_input_tokens: 4000
        # enabled: true          # Advanced mode, intended for entitled users only

      serialization:
        requirement_text_max_chars: 500
        evidence_text_max_chars: 2500
        job_summary_max_chars: 1800
        # evidence_text_max_chars: 4000   # Raise if evidence is being truncated too aggressively
```

## Test Plan

- Unit tests:
  - config parsing for all semantic-fit fields
  - mode resolution from deploy config + baseline + entitlements
  - route resolution for `local`, `remote`, `auto`
  - local/remote fallback behavior
  - disabled semantic-fit path
  - denial when `llm` not entitled
  - top-`k` evidence recall
  - zero-evidence handling
  - deterministic pair ID generation
  - truncation diagnostics emission
  - legacy match detail compatibility
  - invalid entitlement payload handling
- Integration tests:
  - standard user uses `cross_encoder`
  - entitled user with preferred `llm` uses `llm`
  - entitled user falls back cleanly when `llm` is unavailable
  - partial remote pair failures fall back deterministically
  - persistence includes effective mode, route, retrieval diagnostics, fallback metadata, and truncation metadata
  - hybrid retrieval changes shortlist composition vs dense-only
- Offline evaluation fixtures:
  - Java vs Python mismatch
  - related-but-incomplete evidence
  - exact-skill lexical retrieval
  - title/acronym/product-name retrieval
- Evaluation harness implementation:
  - keep this harness in Python only
  - do not split it across Python and TypeScript, because the retrieval fusion, semantic fit scorer, and entitlement resolution all live in the backend Python code
  - frontend TypeScript remains for UI rendering and end-to-end verification only, not offline fit benchmarking

## Assumptions and Defaults

- Hybrid retrieval is enabled by default.
- `cross_encoder` is the default fit mode.
- `baseline_allowed_modes` defaults to `["cross_encoder"]`.
- `deploy_allowed_modes` may include `llm`, but standard users do not get it without entitlement.
- Threshold scoring is fallback-only.
- This slice includes a real local cross-encoder runtime.
- Operating budgets are configurable and instrumented.
- Emergency ceilings remain as internal safety rails only.
- Entitlement administration is intentionally internal-first:
  - current path: DB-backed rows plus an internal CLI
  - later options: internal admin API or admin UI on top of the same table/service
