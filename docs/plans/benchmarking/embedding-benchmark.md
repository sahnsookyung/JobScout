# Embedding model benchmark

This benchmark compares the embedding component needed for the proposed NVIDIA
migration. It does not change production, rebuild vectors, or measure final
matching, LLM review, or notification delivery.

## Questions and comparison matrix

1. Which model retrieves a fully relevant job or fully supporting evidence?
2. Which errors remain on negation, numeric requirements, adjacent technologies,
   ownership, optional requirements, and English/Japanese cross-language queries?
3. How much quality changes when storing fewer vector dimensions?
4. What latency and request volume does this hosted account actually observe?

| Model | Parameters | Dimensions tested |
| --- | ---: | --- |
| `nvidia/nemotron-3.5-embed-1b` | 1.55B total | 256, 512, 1024, 2048 |
| `nvidia/nemotron-3.5-embed-8b` | 8.38B total | 256, 512, 1024, 2048, 4096 |

The model name's size and the vector's dimension are different variables. Reducing
the latter reduces vector storage and similarity-search work; it does not reduce
the underlying model's parameter count. This experiment does not establish that
smaller vectors reduce inference time.

Availability probes on 2026-10-30 JST returned HTTP 404 for
`snowflake/arctic-embed-l` (335M) and `nvidia/nv-embedqa-mistral-7b-v2` (7B).
Record them as unavailable, not as quality failures. Qwen3-Embedding-4B cannot be
used as the current hosted baseline while the Modal workspace is disabled by its
usage allowance. This is not a measured NVIDIA-versus-Qwen comparison.

## Fixture and labels

`tests/fixtures/evaluations/embedding_cases.json` contains 28 fixed synthetic
families. Each family has a query, four candidate passages, English/Japanese
translations, explicit grades, and a short factual rationale. Four directions
expand each family into 112 cases: en-en, ja-ja, en-ja, ja-en.

- 8 job retrieval families distinguish the requested occupation from adjacent
  roles, recruiting, selling, or writing about the same technologies.
- 16 evidence families distinguish full support from partial experience, keyword
  matches that explicitly deny the required experience, and unrelated work.
- 4 no-match families contain no fully supporting candidate.

Grades mean **2 = fully supports the explicit query**, **1 = partially relevant**,
and **0 = does not support it**. The grading standard differs from general topic
similarity: a passage about recruiting Python developers is not a Python
development job. Requirement satisfaction is a stress test of evidence selection;
embedding similarity must not become the final eligibility decision.

Labels were frozen before provider calls. They are authored synthetic drafts,
not human-adjudicated gold. Candidate text and labels are never changed to reward
a model after observing its outputs. Any correction changes the fixture hash and
requires all comparisons to be rescored together. It can reuse existing vectors
when the text itself is unchanged.

Eight complete families (32 cases) form calibration; twenty (80 cases) form
holdout. Translations of a family never cross that boundary. The holdout includes
72 positive cases and eight no-match cases. These are fixed development splits,
not permanently blind tests: future tuning on these results requires new unseen
families for an independent evaluation.

## Metrics and decision rule

Report quality separately from latency and storage; do not create an opaque
weighted score.

- Full-match top-1, recall@3, and reciprocal rank use grade 2 only.
- nDCG@3 uses gains `2**grade - 1`, rewarding partial relevance less than full.
- Non-full outrank rate and full-minus-other cosine margin expose hard negatives.
- Per-task, per-language-direction, and per-error-slice metrics prevent an easy
  slice from concealing a serious failure.
- A diagnostic cosine threshold is fitted above every non-full calibration
  evidence score. Holdout reports wrong accepts, total accepts, and no-match
  false accepts. Zero observed calibration errors does not establish a population
  false-positive guarantee; low acceptance coverage is itself a limitation.
- Paired 95% bootstrap intervals resample whole scenario families (2,000 draws,
  fixed seed), preserving correlation between translations. They describe this
  synthetic scenario set, not the production job population. Comparisons against
  1B/1024 are exploratory; nine configurations are not nine confirmatory tests.

Prefer the smallest/faster configuration that preserves quality on each important
slice. Treat a point-score improvement with an interval crossing zero as
inconclusive. Do not approve a migration from this fixture alone. First adjudicate
its labels and then test a small consented, de-identified production sample against
the existing Qwen baseline when that baseline becomes available. Recheck matching
thresholds and LLM evidence selection after any embedding change.

## Efficient execution

The script uses the existing OpenAI-compatible NVIDIA endpoint, explicit `query`
and `passage` roles, float encoding, and `truncate=NONE`. Each unique text is
embedded once per model and role at native dimension, in batches up to 32.
Successful vectors are cached atomically using endpoint/model/native dimension/
role/truncation/encoding/text keys. Invalid dimensions, missing or duplicate
response indices, non-finite numbers, and zero vectors fail closed.

Shorter configurations reuse normalized native prefixes. For every shortened
dimension and both roles, two direct API outputs are checked against those
prefixes (cosine must be at least 0.995). This check allows small hosted numerical
variation; it does not establish bitwise identity. Native-prefix comparisons
must be reconsidered if the provider changes its model or dimension behavior.

Model order is deterministically interleaved between corpus batches. Three
repeated identical 32-passage requests measure native-output latency. Report
median and observed range, with all request timings and token counts retained.
There is no meaningful p95/SLA estimate from three repeats. Native output sizes
differ; observed latency includes both model serving and payload transfer.

The runner defaults to ten requests/minute, a 60-second timeout, no hidden SDK
retries, and serial requests. A failed call persists error type/status without
provider bodies or credentials and stops the run. Rerunning resumes cached
vectors. Provider accounting still applies; the local benchmark does not reserve
the application's Redis request budget. Run it outside busy periods or lower
`--rpm` if the account shares capacity with extraction.

For this fixture the initial run requires 560 native vectors across 18 corpus
requests, 14 prefix-check requests, and six latency requests: 38 total. Repeated
offline scoring needs zero API calls. Model aliases can change server weights;
use a fresh output/cache directory for a new dated provider snapshot.

## Commands

Use `uv` with the repository environment. `NVIDIA_API_KEY` must already be supplied
through the execution environment; never paste or commit its value.

```bash
# Validate fixture structure without a provider or key.
rtk proxy uv run python scripts/evaluate_embeddings.py --validate-only

# Explicit live run; writes only under the ignored output directory.
rtk proxy uv run python scripts/evaluate_embeddings.py --collect \
  --output output/embedding-benchmark-20261030 --rpm 10 --repeats 3

# Rescore or regenerate the report for the exact same cached provider snapshot.
rtk proxy uv run python scripts/evaluate_embeddings.py \
  --output output/embedding-benchmark-20261030

rtk proxy uv run python -m pytest tests/unit/scripts/test_evaluate_embeddings.py -q
```

Outputs are `report.html`, `report.json`, a per-request run manifest, and cached
vectors. The manifest records script and expanded-case hashes, API configuration,
timestamp, Python/platform, cache hits, prefix checks, token use, and timing.
`report.json` records the fixture hash, grades, scores, ranks, all slices, and paired
comparisons. Preserve the entire dated output directory to reproduce a run.

## Human review without reading every case

Open `report.html`. Its initial queue prioritizes up to twelve families with
failures, model disagreements, low margins, or false acceptance, plus up to four
seeded passing controls. Review a family's query, four labels/rationales, and both
translations once; all four language-direction results are nested underneath it.
This reuses one label audit across nine configurations and four directions.

Use **Accept labels**, **Needs correction**, or **Ambiguous**, with optional notes.
The page saves progress locally when browser storage is available; **Export label
review** downloads a JSON audit tied to the fixture hash. It never sends review
data anywhere or silently rewrites the fixture. Apply corrections to the fixture
explicitly, rerun offline scoring, and record adjudicator/date when promoting
labels to gold. The remaining families are available by clearing **Priority queue
only**. Sampling passing cases detects obvious systemic issues but cannot certify
every unreviewed label.

## Limits and migration implications

Four-candidate contrast tests expose specific errors efficiently but do not measure
recall against thousands of jobs, long-document truncation, real resume variation,
embedding-worker throughput, or final Discord usefulness. The distractor-context
case is short and is not a maximum-context stress test. Future stages should add
a larger adjudicated retrieval corpus, near-duplicate jobs, longer descriptions,
and real batch/concurrency behavior without mixing those effects into this first
component comparison.

Production currently uses 1024-dimensional Qwen vectors. Even an NVIDIA output of
1024 dimensions is a different space. A migration requires separately versioned
vectors, correct query/passage roles, re-embedding all affected job/resume tables,
invalidating embedding-dependent match reuse, and validation before switching.
Changing 1B to 8B also requires a separate vector space. No such changes are made
by this benchmark.

## Observed run: 2026-10-30 JST

Fixture SHA-256:
`7ec5053caa0b6907170379e99a342d096ab6c9013a176bb9804839a2ea0d0faf`.
All 38 requests succeeded (14,748 reported input/total tokens). All 14 dimension
checks passed; the lowest direct-versus-prefix cosine was 0.99999935. The native
vector cache contains 560 entries. Local rescoring made no provider calls.

| Configuration | Full-match top-1, heldout | MRR | Float32 bytes/vector |
| --- | ---: | ---: | ---: |
| 1B / 256 | 62/72 (86.1%) | 0.926 | 1,024 |
| 1B / 512 | 62/72 (86.1%) | 0.926 | 2,048 |
| 1B / 1024 | 62/72 (86.1%) | 0.928 | 4,096 |
| 1B / 2048 | 62/72 (86.1%) | 0.928 | 8,192 |
| 8B / 256 | 65/72 (90.3%) | 0.949 | 1,024 |
| 8B / 512 | 64/72 (88.9%) | 0.944 | 2,048 |
| 8B / 1024 | 67/72 (93.1%) | 0.965 | 4,096 |
| 8B / 2048 | 68/72 (94.4%) | 0.972 | 8,192 |
| 8B / 4096 | 69/72 (95.8%) | 0.979 | 16,384 |

The repeated native-output batch latency medians were 1.504s for 1B (range
1.500–1.520s) and 4.802s for 8B (2.810–10.609s), three samples each. Corpus and
validation requests also showed an isolated 13.898s 1B outlier. These measurements
do not establish a service-level latency guarantee.

**8B/4096 had the highest observed quality.** Its paired top-1 improvement over
1B/1024 was 9.7 percentage points (exploratory family-bootstrap 95% interval
2.8–19.4). At the current 1024-dimensional schema size, 8B scored 6.9 points above
1B, but that interval includes no difference (-1.4–16.7). Thus 8B/1024 is a
reasonable next pilot candidate and 1B/1024 is the faster candidate; this fixture
does not conclusively settle their production tradeoff. Changing parameter size
requires rebuilding the vector space regardless of output dimension.

Both 1024 configurations retrieved the right job in all 24 heldout job-role
cases. The difference came from evidence selection: 1B got 38/48 and 8B got 43/48.
Even 8B/4096 failed three evidence cases: two JLPT N2-versus-N3 cases involving
Japanese passages, and one Japanese required-Python/optional-Rust case.

All configurations found a full match within the top three of four candidates;
that deliberately small contrast set makes recall@3 a weak discriminator. A
broader corpus is required to evaluate production candidate-retrieval recall.
The zero-observed-error calibration thresholds accepted only 1–4 of 56 heldout
evidence cases, despite 48 having full support. Their zero false accepts are
therefore not evidence of a useful eligibility policy. Preserve downstream
qualification checks and evaluate richer evidence selection.

The generated review queue has 16 families (12 priorities plus four passing
controls). These draft labels and translations still require human adjudication.
The run is a basis for that review and a subsequent realistic pilot, not approval
to migrate production.

## Primary sources

- [NVIDIA 1B API: roles, dimensions, normalization, limits](https://docs.api.nvidia.com/nim/reference/nvidia-nemotron-3_5-embed-1b-infer)
- [NVIDIA 8B API: roles, dimensions, normalization, limits](https://docs.api.nvidia.com/nim/reference/nvidia-nemotron-3_5-embed-8b-infer)
- [Nemotron 3.5 Embed 1B model card](https://build.nvidia.com/nvidia/nemotron-3_5-embed-1b/modelcard)
- [Nemotron 3.5 Embed 8B model card](https://build.nvidia.com/nvidia/nemotron-3_5-embed-8b/modelcard)
- [Snowflake Arctic Embed L model card](https://huggingface.co/Snowflake/snowflake-arctic-embed-l)
- [NVIDIA embedding support matrix](https://docs.nvidia.com/nim/nemo-retriever/text-embedding/1.12.0/support-matrix.html)
