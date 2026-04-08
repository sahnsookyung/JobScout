# Fit / Preference Benchmark Design

## Status

Planning only. Do not implement benchmark infrastructure in the current ranking-contract cleanup branch.

## Goal

Create a benchmark that tells us whether changes to retrieval, fit scoring, preference scoring, and final ranking improve or regress:

- ranking quality
- retrieval quality
- candidate usefulness
- latency
- compute cost
- token usage
- stability

## Benchmark Layers

### L1: Offline Replay Benchmark

Primary benchmark for architecture work.

- Deterministic replay over frozen fixtures
- No Docker dependency
- No live provider dependency
- Replays retrieval, fit scoring, preference reranking, and final ranking

### L2: Docker Packaged-System Benchmark

Secondary benchmark for system-level regressions.

- Runs the packaged stack end to end
- Validates service boundaries, orchestration, and runtime costs
- Uses local mocks instead of live external providers by default

### L3: Live-Provider Smoke Benchmark

Opt-in only.

- Verifies third-party connectivity and credential health
- Not the primary quality benchmark
- Not required for normal CI

## Evaluation Unit

Each benchmark case should be:

- one resume
- one candidate preference profile
- one fixed job corpus
- one adjudicated label set

## Truth Strategy

- Frontier LLMs may be used to bootstrap draft labels and explanations.
- Human adjudication is the source of truth for the gold benchmark.
- Do not use an online LLM judge as the final benchmark oracle.

## Dataset Shape

First useful slice:

- 15 to 25 resumes
- 40 to 80 jobs per resume
- golden and holdout splits

Each case should capture:

- retrieval relevance labels
- fit-quality labels
- missing-required error labels
- final ranking usefulness labels

## Metrics

### Retrieval

- recall@20
- recall@50
- precision@k
- nDCG@k
- hard-filter false-negative rate

### Fit

- fit-band accuracy
- requirement-level precision / recall / F1
- missing-required false-positive rate
- score calibration

### Final Ranking

- nDCG@10
- pairwise ranking accuracy
- top-3 usefulness rate
- proportion of runs where `preference_first` materially changes top-k membership

### Runtime / Cost

- stage latency p50 / p95
- total wall-clock time
- CPU time
- peak RSS / container memory
- token count
- external-call count
- retry count
- fallback rate

### Stability

- warm vs cold cache deltas
- run-to-run rank swap rate
- Kendall tau / Spearman agreement across repeated runs

## Comparison Method

- Always compare candidate changes against a frozen baseline.
- Use paired runs on the same dataset version.
- Record config digest, model versions, provider versions, and environment metadata.
- Prefer confidence intervals over raw point deltas.

## Suggested Implementation Order

1. Extend the existing offline evaluation harness in `scripts/evaluate_fit_semantics.py`.
2. Define a versioned fixture schema for replay cases and results.
3. Create a small golden corpus for fast repeatable CI runs.
4. Add a larger holdout corpus for deeper comparisons outside CI.
5. Add a baseline-vs-candidate comparison report.

## Fixture Schema

Each replay fixture should be versioned and stored as a single case bundle.

Required top-level fields:

- `fixture_version`
- `case_id`
- `resume`
- `candidate_preferences`
- `job_corpus`
- `labels`
- `metadata`

### `resume`

- `resume_id`
- `owner_id` or synthetic benchmark owner
- `resume_text`
- `resume_fingerprint`
- optional `structured_resume`
- optional `evidence_units`

### `candidate_preferences`

- `remote_mode`
- `target_locations`
- `visa_sponsorship_required`
- `salary_min`
- `employment_types`
- `soft_preferences`
- optional `preference_profile`
- `preference_mode`

### `job_corpus`

An array of jobs with:

- `job_id`
- `title`
- `company`
- `location_text`
- `is_remote`
- `description`
- optional structured requirement payloads
- optional embeddings or retrieval-side cached artifacts when needed for deterministic replay

### `labels`

The gold labels should be split by evaluation layer:

- `retrieval_labels`
  - relevance grade per job
  - whether the job should survive hard filters
- `fit_labels`
  - overall fit band
  - required-requirement coverage judgments
  - missing-required error flags
- `ranking_labels`
  - final usefulness grade
  - pairwise ordering judgments for disputed top jobs
  - shortlist membership labels such as top-3 and top-10

### `metadata`

- domain or persona tag
- collection date
- adjudication status
- adjudicator ids or anonymized references
- notes on ambiguity, drift risk, or known benchmark caveats

## Run Metadata Schema

Every benchmark run should emit a machine-readable run manifest.

Required fields:

- `run_id`
- `timestamp`
- `benchmark_version`
- `fixture_set_version`
- `baseline_ref`
- `candidate_ref`
- `repo_commit`
- `config_digest`
- `ranking_mode`
- `provider_versions`
- `environment`
- `execution_flags`

### `provider_versions`

- embedding model or provider
- semantic fit provider
- preference reranker or judge provider
- any fallback providers enabled

### `environment`

- Python version
- OS / architecture
- CPU descriptor
- memory limit
- Docker yes/no
- cache warm or cold

### `execution_flags`

- mocks vs live providers
- reranker enabled flags
- fit mode flags
- timeout settings
- retry settings

## Result Schema

Results should be emitted per case and then aggregated.

Required per-case fields:

- `case_id`
- `retrieval_result`
- `fit_result`
- `ranking_result`
- `runtime_result`
- `stability_result`
- `errors`

### `retrieval_result`

- retrieved job ids in order
- recall@20
- recall@50
- precision@k
- nDCG@k
- hard-filter false negatives

### `fit_result`

- fit scores by job
- fit-band accuracy
- requirement-level precision / recall / F1
- missing-required false-positive count
- calibration buckets

### `ranking_result`

- final ranked job ids
- nDCG@10
- pairwise ranking accuracy
- top-3 usefulness hit
- whether top-k membership changed relative to baseline

### `runtime_result`

- stage latency breakdown
- total wall-clock time
- CPU time
- peak RSS
- container memory if applicable
- token counts
- external call counts
- retry counts
- fallback counts

### `stability_result`

- repeated-run agreement metrics
- warm-vs-cold deltas
- rank swap counts in top-k

## Baseline Comparison Rules

Every candidate run should be compared against a frozen baseline on the same fixture set.

Rules:

- Never compare runs built from different fixture versions.
- Never compare runs with different config digests unless the config change is the experiment.
- Treat latency, quality, and cost as separate scorecards first; do not collapse them into one opaque number.
- Use paired comparisons at the case level.
- Report confidence intervals for primary metrics.
- Flag regressions when the candidate loses on the declared primary metric or exceeds guardrail budgets.

Suggested default guardrails:

- no statistically meaningful drop in retrieval recall@20
- no increase in missing-required false-positive rate
- no regression in top-3 usefulness rate
- no unacceptable increase in token cost or p95 latency

## Initial Corpus Construction Plan

Build the first benchmark in three passes:

1. Use frontier LLMs to draft candidate labels and disagreement notes.
2. Have humans adjudicate the gold labels and resolve disagreements.
3. Freeze the first `golden-mini` corpus for repeatable branch comparisons.

`golden-mini` should be small enough for local iteration but diverse enough to include:

- high-fit / low-preference cases
- low-fit / high-preference cases
- hard-filter edge cases
- duplicate-looking jobs with materially different requirements
- retrieval-near-miss cases
- ambiguous human-judgment cases that need explicit adjudication notes
