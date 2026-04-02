# Semantic Architecture Handover

- Date: 2026-04-02
- Status: Foundation merged; fit semantic scoring and fit rewire merged; preference semantic reranking implementation active on PR 3 branch

## Recommended PR Split

### PR 1: Foundation

Scope already implemented on this branch:

- split ADRs and implementation plans
- independent `preferences.*` config namespace
- `preference_mode` / allowed-mode API surface
- optional parsed `PreferenceProfile` persistence surface
- first-class `canonical_job_summary` generation, persistence, and embedding input
- migration for new schema fields

Why separate:

- mostly schema, config, docs, and low-risk API groundwork
- reviewable without debating semantic scorer quality at the same time

### PR 2: Fit Semantic Scoring

Scope now implemented on this branch:

- introduce `SemanticFitScorer`
- keep pgvector similarity as retrieval/evidence recall only
- add stronger shortlist-level requirement/evidence semantic judgments
- move user-facing explainability to semantic fit outputs
- keep cosine-based explanation internal/admin-only if still needed

What landed:

- [core/scorer/semantic_fit.py](/Users/sookyungahn/repos/JobScout-fit-semantics/core/scorer/semantic_fit.py)
- [core/scorer/service.py](/Users/sookyungahn/repos/JobScout-fit-semantics/core/scorer/service.py)
- [services/scorer_matcher/pipeline.py](/Users/sookyungahn/repos/JobScout-fit-semantics/services/scorer_matcher/pipeline.py)
- [core/llm/fake_service.py](/Users/sookyungahn/repos/JobScout-fit-semantics/core/llm/fake_service.py)
- [web/backend/services/match_service.py](/Users/sookyungahn/repos/JobScout-fit-semantics/web/backend/services/match_service.py)
- [web/backend/routers/matches.py](/Users/sookyungahn/repos/JobScout-fit-semantics/web/backend/routers/matches.py)
- [web/frontend/src/features/matches/components/MatchDetailsModal.tsx](/Users/sookyungahn/repos/JobScout-fit-semantics/web/frontend/src/features/matches/components/MatchDetailsModal.tsx)

Behavior:

- `ScoringService` now routes fit evaluation through a dedicated `SemanticFitScorer` contract
- hybrid retrieval is now enabled by default, with reciprocal-rank fusion over dense and lexical candidates
- the default semantic implementation on this branch is cross-encoder mode with local/remote routing support
- local cross-encoder routing now prefers FlagEmbedding-compatible runtimes in `auto` for multilingual-friendly local scoring, with SentenceTransformers and heuristic fallback still supported through the same provider interface
- LLM semantic fit remains available as an advanced gated mode
- threshold scoring remains available as the explicit fallback path
- nested `matching.scorer.semantic_fit.*` config now controls fit-mode routing, recall depth, and serialization budgets
- per-user feature capabilities now gate advanced fit modes
- fit explanations are persisted during scoring and returned by the match explanation endpoint
- split-stack E2E now exercises `/api/matches` and `/api/matches/{id}/explanation` so the persisted fit diagnostics are verified through real API calls
- normal user-facing match details now use semantic verdicts and summaries instead of raw `% similarity` badges, and display fit mode/provider route/fallback state
- public explanation summaries are deterministic from verdicts; model free-text is kept internal/debug only
- fake AI service support was extended so tests can exercise semantic-fit behavior deterministically
- the next canonical fit plan is now [fit-semantics-rewire-plan.md](./fit-semantics-rewire-plan.md)

### PR 3: Preference Semantic Reranking

Scope now implemented on this branch:

- implement `PreferenceSemanticReranker` on fit-qualified shortlists
- add degraded/fallback observability
- enforce fit-band ordering guarantees
- optionally add internal-only `llm_judge` experiments after default reranker works

Why separate:

- product and ranking behavior change is distinct from fit correctness
- easier to evaluate personalization separately from fit improvements

What landed:

- [core/preference_semantics.py](/Users/sookyungahn/repos/JobScout-preference-semantics/core/preference_semantics.py)
- [services/scorer_matcher/preference_semantics.py](/Users/sookyungahn/repos/JobScout-preference-semantics/services/scorer_matcher/preference_semantics.py)
- [services/scorer_matcher/candidate_preferences.py](/Users/sookyungahn/repos/JobScout-preference-semantics/services/scorer_matcher/candidate_preferences.py)
- [services/scorer_matcher/pipeline.py](/Users/sookyungahn/repos/JobScout-preference-semantics/services/scorer_matcher/pipeline.py)
- [web/backend/services/candidate_preferences_service.py](/Users/sookyungahn/repos/JobScout-preference-semantics/web/backend/services/candidate_preferences_service.py)
- [core/llm/fake_service.py](/Users/sookyungahn/repos/JobScout-preference-semantics/core/llm/fake_service.py)

Behavior:

- candidate preference saves now parse and persist a normalized `PreferenceProfile` when a parser is available
- preference reranking now runs semantically on fit-qualified shortlists instead of using lexical token-overlap bonus logic
- final `top_k` truncation now happens after preference reranking so shortlisted jobs are not trimmed too early
- fit-band ordering is preserved through a bounded overall-score recomputation
- degraded reranking now records explicit fallback metadata in persisted `fit_components`
- split-stack candidate-preferences flow now validates semantic preference metadata through the real API and persisted matches

## What Landed In PR 1 Foundation

### Docs

- [0002-fit-semantics-and-explainability.md](../adr/0002-fit-semantics-and-explainability.md)
- [0003-preference-semantics-and-personalization.md](../adr/0003-preference-semantics-and-personalization.md)
- [fit-semantics-implementation-plan.md](./fit-semantics-implementation-plan.md)
- [preference-semantics-implementation-plan.md](./preference-semantics-implementation-plan.md)

### Canonical Summary Foundation

- [etl/canonical_summary.py](/Users/sookyungahn/repos/JobScout/etl/canonical_summary.py)
- [etl/orchestrator.py](/Users/sookyungahn/repos/JobScout/etl/orchestrator.py)
- [database/models/job.py](/Users/sookyungahn/repos/JobScout/database/models/job.py)
- [database/repositories/job_post.py](/Users/sookyungahn/repos/JobScout/database/repositories/job_post.py)
- [migrations/005_semantic_architecture_foundation.py](/Users/sookyungahn/repos/JobScout/migrations/005_semantic_architecture_foundation.py)

Behavior:

- extraction now generates `canonical_job_summary`
- job embeddings prefer `canonical_job_summary`
- content changes reset extraction/embedding readiness for jobs with descriptions

### Preference Foundation

- [services/scorer_matcher/preference_semantics.py](/Users/sookyungahn/repos/JobScout/services/scorer_matcher/preference_semantics.py)
- [core/config_loader.py](/Users/sookyungahn/repos/JobScout/core/config_loader.py)
- [config.yaml](/Users/sookyungahn/repos/JobScout/config.yaml)
- [web/backend/services/candidate_preferences_service.py](/Users/sookyungahn/repos/JobScout/web/backend/services/candidate_preferences_service.py)
- [web/backend/models/requests.py](/Users/sookyungahn/repos/JobScout/web/backend/models/requests.py)
- [web/backend/models/responses.py](/Users/sookyungahn/repos/JobScout/web/backend/models/responses.py)
- [web/backend/routers/candidate_preferences.py](/Users/sookyungahn/repos/JobScout/web/backend/routers/candidate_preferences.py)
- [web/frontend/src/features/preferences/components/CandidatePreferencesPanel.tsx](/Users/sookyungahn/repos/JobScout/web/frontend/src/features/preferences/components/CandidatePreferencesPanel.tsx)
- [web/frontend/src/types/api.ts](/Users/sookyungahn/repos/JobScout/web/frontend/src/types/api.ts)

Behavior:

- candidate preferences now expose:
  - `preference_mode`
  - `allowed_preference_modes`
  - `effective_preference_mode`
  - `soft_preference_summary`
- parser config is independent from `etl.llm`
- parsed preference profile is optional and safe to skip when parser model/config is unavailable

## Verification Completed

- `rtk uv run python -m py_compile etl/canonical_summary.py services/scorer_matcher/preference_semantics.py etl/orchestrator.py web/backend/services/candidate_preferences_service.py`
- `rtk uv run python -m pytest tests/unit/database/repositories/test_job_post.py tests/unit/etl/test_orchestrator.py tests/unit/core/test_config_loader.py tests/unit/services/scorer_matcher/test_candidate_preferences.py tests/unit/web/services/test_candidate_preferences_service.py -q`
- `rtk uv run python -m pytest tests/unit/web/test_backend_config.py tests/unit/services/scorer_matcher/test_pipeline_helpers.py -q`
- `rtk ./node_modules/.bin/vitest run src/features/preferences/components/__tests__/CandidatePreferencesPanel.test.tsx`

Observed results at handoff:

- focused Python unit suite: passing
- focused frontend preference-panel tests: passing
- split-stack integration rerun for candidate preferences: passing

## Post-Handoff Updates Landed On This Branch

- split-stack and integration coverage for candidate preferences were completed and verified
- frontend/unit/integration CI cleanup landed after the initial handoff snapshot
- Sonar / CodeQL follow-up cleanup landed:
  - accessible label fix for the visa sponsorship checkbox
  - duplicate SQL literal cleanup for candidate-preferences timestamps
  - dedicated unit coverage for `preference_semantics.py`
- web backend preference saving was decoupled from scorer-package imports so split-stack startup stays healthy
- rerun persistence semantics were hardened in [services/scorer_matcher/pipeline.py](/Users/sookyungahn/repos/JobScout/services/scorer_matcher/pipeline.py):
  - active match refresh now happens after a clean save batch, not before saving
  - rerun refresh is skipped when any per-match save fails, preserving the last good active set
  - `recalculate_existing=False` semantics are preserved for unchanged active matches
- fit semantic scoring was implemented on the follow-on worktree branch:
  - LLM-backed semantic requirement/evidence judgments feed fit aggregation
  - threshold scoring is retained as a guarded fallback, not the only fit path
  - persisted semantic explanations now back the explanation endpoint and match details modal
  - follow-up Sonar cleanup removed nested ternaries from the semantic explanation UI
 - hybrid retrieval is implemented on the fit-semantics branch:
  - dense retrieval uses `canonical_job_summary` embeddings
  - lexical retrieval uses PostgreSQL full-text candidate generation over existing job text fields
  - reciprocal-rank fusion merges dense and lexical candidate sets without overwriting dense `job_similarity`
  - retrieval diagnostics are persisted with fit outputs so match details can show whether a candidate was generated through dense-only or hybrid retrieval
  - semantic scorer diagnostics capture scorer identity, latency, judged-requirement counts, and fallback reasons in the saved fit payload
 - the broader fit rewire plan is now recorded in [fit-semantics-rewire-plan.md](./fit-semantics-rewire-plan.md):
   - hybrid retrieval becomes the default path
   - semantic fit adds dual provider support: cross-encoder default plus advanced gated LLM mode
   - fit mode access moves to a DB-backed capability model
   - recall depth becomes configurable
   - truncation budgets become configurable and instrumented rather than fixed hidden limits
   - `config.yaml` will carry commented tuning hints for fit controls
   - container-level observability rollout is tracked separately in [container-observability-plan.md](./container-observability-plan.md)

## Important Boundaries

- Preference reranking is now implemented, but it is still backed by LLM/fake-LLM structured judgments rather than a cheaper dedicated non-LLM reranker model.
- Preference mode gating is still backend allowlist-based; there is no separate per-user capability model for preference modes yet.
- No admin HTTP surface exists for capability management in this phase beyond config-driven allowed modes plus the internal CLI.
- A DB-backed capability control mechanism exists now, plus an internal CLI for dev/staging administration:
  - [manage_feature_capability.py](/Users/sookyungahn/repos/JobScout-fit-semantics/scripts/manage_feature_capability.py)
  - later options remain an internal admin API or admin UI on top of the same table/service if operations truly require it
- Truncation and fit-routing diagnostics are persisted, but they are not yet exported into a Grafana dashboard; use [container-observability-plan.md](./container-observability-plan.md) as the follow-on operational plan.
- Persisted reruns are authoritative only after a clean save batch completes; this safety behavior is now intentional and should be preserved when implementing later semantic stages.
- The current fit semantic scorer supports both a dedicated cross-encoder path and a gated LLM path.
- ANN/pgvector is still the retrieval and evidence-recall layer; the rewire plan keeps it as retrieval infrastructure rather than final semantic authority.
- hybrid retrieval is now default-on in config.
- A Python-only offline evaluation harness now exists for fit pair judgments and retrieval fusion:
  - [scripts/evaluate_fit_semantics.py](/Users/sookyungahn/repos/JobScout-fit-semantics/scripts/evaluate_fit_semantics.py)
  - [tests/fixtures/evaluations/fit_semantics_cases.json](/Users/sookyungahn/repos/JobScout-fit-semantics/tests/fixtures/evaluations/fit_semantics_cases.json)
- The offline harness intentionally lives in Python only; TypeScript remains for UI and end-to-end verification, not backend fit benchmarking.

## Next PR Starting Point

If continuing preference semantics next:

1. add explicit latency and shortlist-size observability for preference reranking runs
2. evaluate whether `llm_judge` should remain disabled by default
3. decide whether preference modes need their own capability layer or can remain backend allowlist-controlled
4. consider exposing preference diagnostics more directly in match detail UI if product wants that visibility

If continuing fit semantics after PR 2:

1. follow [fit-semantics-rewire-plan.md](./fit-semantics-rewire-plan.md) as the source of truth
2. tune and expand the offline fixture set and acceptance criteria for semantic fit quality
3. harden the local FlagEmbedding runtime path in deployment environments where model provisioning differs
4. add aggregate observability from the persisted diagnostics into reporting for latency, fallback frequency, truncation, and verdict distributions

## Suggested Merge Order

1. PR 1 foundation
2. PR 2 fit semantic scoring
3. PR 3 preference semantic reranking

Current state:

- PR 1 is merged
- PR 2 is merged
- PR 3 is the active preference-semantics branch
