# Semantic Architecture Handover

- Date: 2026-04-01
- Status: Ready for follow-on PRs

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

Target scope:

- introduce `SemanticFitScorer`
- keep pgvector similarity as retrieval/evidence recall only
- add stronger shortlist-level requirement/evidence semantic judgments
- move user-facing explainability to semantic fit outputs
- keep cosine-based explanation internal/admin-only if still needed

Why separate:

- highest correctness risk
- requires offline evaluation and likely prompt/model iteration
- changes fit semantics independently from preference personalization

### PR 3: Preference Semantic Reranking

Target scope:

- implement `PreferenceSemanticReranker` on fit-qualified shortlists
- add degraded/fallback observability
- enforce fit-band ordering guarantees
- optionally add internal-only `llm_judge` experiments after default reranker works

Why separate:

- product and ranking behavior change is distinct from fit correctness
- easier to evaluate personalization separately from fit improvements

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

## Important Boundaries

- No real `SemanticFitScorer` exists yet.
- No real `PreferenceSemanticReranker` exists yet.
- Current lexical soft-preference overlap still exists in the matcher pipeline and should be treated as an interim path, not the target architecture.
- No admin entitlement/capability store exists yet beyond config-driven allowed modes.
- Persisted reruns are authoritative only after a clean save batch completes; this safety behavior is now intentional and should be preserved when implementing later semantic stages.

## Next PR Starting Point

If picking up fit semantics next:

1. introduce a fit-scoring interface and default implementation
2. thread semantic fit outputs into persistence/explanations
3. gate UI explainability away from cosine internals
4. add offline fixtures and acceptance criteria

If picking up preference semantics next:

1. implement shortlist reranker using `PreferenceProfile`
2. add bounded fit-band ordering logic
3. log mode used, fallback reason, and latency
4. evaluate whether `llm_judge` should remain disabled by default

## Suggested Merge Order

1. PR 1 foundation
2. PR 2 fit semantic scoring
3. PR 3 preference semantic reranking

This keeps the highest-risk semantic behavior changes out of the schema/docs/config groundwork review.
