# ADR 0001: Replace Facet-Based Wants Scoring with Filters and Preference Reranking

- Status: Superseded by ADR 0002 and ADR 0003 for ongoing implementation detail
- Date: 2026-04-01

## Context

JobScout currently distinguishes between:

- `fit_score`: how well a candidate is qualified for a job
- legacy facet-based preference score: how well a job matches what the candidate wants

The legacy facet-based preference score uses a fixed 7-facet taxonomy and compares user want embeddings against job facet embeddings. That approach has several problems:

- It forces candidate preferences into a brittle hand-authored schema.
- Some "preferences" are actually hard constraints and should not be modeled as semantic similarity.
- The current implementation is only partially productized, which makes the signal easy to misuse.
- A flat weighted blend such as `0.8 * fit + 0.2 * want` allows preference logic to distort ranking in ways that are hard to reason about.

At the same time, JobScout already has a strong fit-oriented backbone:

- ANN retrieval over job embeddings
- requirement-to-resume-evidence matching
- rule-based fit scoring with explainable components

We want to preserve that strength while replacing the brittle preference architecture with a more flexible and product-sensible design.

## Decision

We will remove the 7-facet wants architecture as a ranking primitive and replace it with a split model:

1. Hard constraints

- Candidate constraints such as remote mode, visa sponsorship, geography, salary floor, and employment type will be modeled as explicit structured filters.
- These constraints will be captured in the frontend and enforced deterministically in retrieval and ranking.

2. Qualification fit

- Fit remains the primary ranking dimension.
- Candidate generation and fit scoring will continue to center on embeddings plus requirement/evidence matching.

3. Soft preferences

- Candidates may provide free-text soft preferences such as team style, pace, learning goals, mission, and stack preferences.
- These preferences will not be forced into a fixed ontology.
- They will be used as a reranking signal only after fit has already identified plausible jobs.

## Target Retrieval and Ranking Pipeline

The ranking architecture will be:

1. Apply hard filters as early as possible.
2. Generate candidates with hybrid retrieval:
   - dense ANN retrieval over job summary or section embeddings
   - lexical retrieval over titles, skills, and requirements
   - optional small preference-oriented retrieval over benefits, company, and work-style text
3. Union and deduplicate candidates.
4. Score candidate-job fit using requirement-to-evidence matching.
5. Discard jobs below a fit floor.
6. Rerank the remaining top shortlist using free-text candidate preferences and selected job text.
7. Compute a final score where fit remains dominant and preference only adjusts ordering within a bounded range.

## Algorithm Choices

### Retrieval

- Use HNSW ANN for dense retrieval.
- Use lexical retrieval alongside dense retrieval rather than relying on dense ANN alone.
- Prefer hybrid retrieval over pure vector retrieval to improve recall for exact skills, titles, and requirement phrasing.

### Preference Reranking

- Use a cross-encoder or equivalent reranker on the top shortlist of plausible jobs.
- Do not use the free-text preference signal as the primary retrieval mechanism.
- Do not use a large language model as the default online ranker unless traffic and latency requirements are low enough to justify it.

### LLM Usage

LLMs remain appropriate for:

- extracting structured hard constraints from onboarding text when needed
- generating natural-language explanations
- offline experimentation and evaluation

LLMs are not the primary online ranking primitive for broad candidate generation.

## Alternatives Considered

### Keep the existing 7-facet legacy preference score

Rejected.

Reasons:

- The fixed taxonomy is too brittle for real candidate intent.
- It mixes hard constraints with soft preferences.
- It does not compose cleanly with fit-first ranking.

### Replace everything with a pure LLM or RAG-based ranking system

Rejected.

Reasons:

- RAG is not the core ranking algorithm for this problem.
- Fully generative ranking is harder to evaluate, slower, and less deterministic.
- It would discard the strongest existing part of JobScout: requirement-aware fit scoring.

### Remove candidate preferences entirely

Rejected.

Reasons:

- Qualification fit alone is not enough for a candidate-centric product.
- We still need a "would this person actually want this job?" signal.
- The correct fix is to remove the facet ontology, not remove preferences themselves.

## Consequences

### Positive

- Candidate intent is modeled more naturally.
- Hard constraints become deterministic and explainable.
- Fit remains the dominant signal.
- Ranking becomes easier to reason about and evaluate.
- The new design builds on the current fit pipeline instead of discarding it.

### Negative

- The current facet tables and scoring path will need to be deprecated or migrated.
- We lose facet-based explanations and must replace them with filter matches and reranker explanations.
- Cross-encoder reranking adds latency and should be limited to a shortlist.

## Implementation Notes

- Do not persist the old facet-based preference score as part of the final ranking model.
- Frontend work should add first-class controls for hard constraints.
- Backend ranking should treat preference as a bounded reranking modifier rather than a peer of fit.
- If sufficient behavioral labels are collected later, learned-to-rank can be introduced on top of the same feature structure.

## Follow-Up

Expected follow-up work includes:

- define the new candidate preference data model
- add frontend hard-filter controls and free-text preference input
- update retrieval to hybrid dense plus lexical search
- add shortlist reranking for soft preferences
- deprecate facet-based preference scoring and related dead paths

## Implementation Update (2026-04-04)

All follow-up items are now complete:

- **Candidate preference data model**: `CandidatePreferences` table and `CandidatePreferencesRepository` shipped; `PreferenceProfile` Pydantic model in `services/scorer_matcher/preference_semantics.py`.
- **Frontend hard-filter controls**: structured filter fields (remote, visa, salary, employment type) captured in frontend and enforced deterministically in retrieval.
- **Hybrid retrieval**: dense ANN over job summary embeddings plus lexical retrieval over skills and titles.
- **Shortlist reranking**: `LLMPreferenceSemanticReranker` and `LLMPreferenceJudge` implemented in `services/scorer_matcher/preference_semantics.py`. Both use `extract_structured_data` against named schema specs (`preference_semantic_rerank_v1`, `preference_llm_judge_v1`). E2E covered by `test_candidate_preferences_round_trip_updates_matching_behavior`.
- **Facet pipeline removed** (this PR): `job_facet_embedding` table, all 7 facet columns from `job_post`, 12 repository methods, ETL orchestrator methods, LLM interface method, and all associated tests deleted. Migration `007_remove_facet_pipeline.py` drops the DB objects.

### Preference Reranker: LLM vs Cross-Encoder

ADR 0001 recommended a cross-encoder for preference reranking. After evaluation, the LLM reranker is the deliberate choice for the current stage:

- The preference shortlist is 5–20 jobs; LLM latency is acceptable for manual runs.
- A bare cross-encoder returns only scalar scores; it cannot produce `preference_reason_codes` (e.g., `tech_stack_match`, `work_style_match`) which are asserted by the E2E test and surfaced in the UI as explanations.
- A hybrid (cross-encoder scoring + LLM explanations) is architecturally clean but adds infrastructure complexity with no near-term benefit.

**Decision**: Keep the LLM reranker. If usage scales to automated high-frequency runs, introduce a cross-encoder for scoring and retain the LLM only for explanation generation on the top N results.
