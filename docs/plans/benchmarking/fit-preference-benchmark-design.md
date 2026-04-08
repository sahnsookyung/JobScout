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
