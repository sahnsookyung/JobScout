# Job extraction provider routing

Set `JOB_EXTRACTION_NVIDIA_FIRST=true` to use NVIDIA NIM for job-description
extraction, with the existing `etl.llm` endpoint (Cerebras on OCI) as fallback.
The default for standalone JobScout is disabled. Resume parsing and generic
structured requests keep their existing provider routing, with an explicit 4096
output-token cap. Job-specific resume tailoring is a separate NVIDIA route with a
32768 output-token cap. The embedding model and batch API are unchanged.

The personal evaluation deployment uses `nvidia/nemotron-3-super-120b-a12b`,
JSON Schema output, and disabled reasoning. The job-specific prompt matches the
actual JobExtraction schema. All results must pass that schema before import.

## Credentials and controls

Configure `NVIDIA_EXTRACTION_API_KEY` for this specific model. When absent, the
loader can use `NVIDIA_API_KEY`; verify that key's model entitlement before enabling
the route. Never assume a working key for another NVIDIA model grants access.
`NVIDIA_EXTRACTION_MODEL` selects the model without changing judging or resume models.

Each provider makes one attempt, with a 60-second timeout and no local rate-limit
sleep. NVIDIA extraction allows up to 32768 output tokens; the ETL fallback
allows 4096. `NVIDIA_EXTRACTION_REQUESTS_PER_MINUTE` defaults to 10; lower it if
the account's limit requires it. Timeout and output bounds are configurable through
`JOB_EXTRACTION_PROVIDER_TIMEOUT_SECONDS`, `JOB_EXTRACTION_MAX_OUTPUT_TOKENS`, and
`JOB_EXTRACTION_FALLBACK_MAX_OUTPUT_TOKENS`. Generic ETL and resume parsing use
the separate `ETL_LLM_EXTRACTION_MAX_OUTPUT_TOKENS` cap (4096 by default).
The 32768 NVIDIA ceiling matches the hosted [Nemotron Super API limit](https://docs.api.nvidia.com/nim/reference/nvidia-nemotron-3-super-120b-a12b-infer).
The smaller fallback default also limits its token-per-minute reservation on
Cerebras; its [rate limits vary by account tier](https://inference-docs.cerebras.ai/support/rate-limits).

The NVIDIA match judge defaults to 32768 output tokens and 229376 estimated
input tokens. `NVIDIA_MAX_CONTEXT` is its total context allowance (262144 by
default); the loader leaves room for the selected output cap and rejects a
context allowance that leaves no input room. `NVIDIA_MAX_INPUT_TOKENS` can lower
the judge input allowance independently. These two settings do not alter resume
tailoring, which defaults to 64000 estimated input tokens and 32768 output tokens.
Use `RESUME_GENERATION_MAX_INPUT_TOKENS` and
`RESUME_GENERATION_MAX_OUTPUT_TOKENS` to set that route explicitly.

Timeouts, 429 throttles, 5xx responses, invalid extraction output, and provider
quota failures can select the next provider. Authentication and configuration
errors remain visible. A 402 or explicitly daily/insufficient-quota 429 defers
that provider/model for one hour in Redis, surviving worker restarts. When the
chain fails, the durable job retry policy owns subsequent retries; the worker
does not repeat the whole chain three times immediately.
When a response ends with `finish_reason=length`, it is classified as
`output_truncated`; incomplete JSON is never persisted as an extraction.

All actual provider attempts still consume the shared request budget. Each
attempt reserves its configured output cap plus an estimate for the prompt and
schema before the request. Reported response usage reconciles the reservation,
including when JSON parsing fails. Failures with unknown usage retain their
reservation. The 200-request daily ceiling, 20-request interactive reserve,
2,000,000-token ceiling and UTC reset remain unchanged. Global budget errors
propagate with their reset time; switching providers never bypasses those limits.
Prompt-token estimates are approximate and are not tokenizer-enforced context
guarantees. When global budgeting is enabled, calls must use the factory's
`BudgetedLLMProvider`; direct service calls fail closed. Missing or zero reported
usage retains the reservation. Token accounting is not a monetary bill.

## Verification and operations

Logs record provider, model, bounded failure category and elapsed time, without
job content or credentials. Confirm real schema-valid English and Japanese job
extractions, failed-provider fallback, quota cooldown persistence and reset-time
deferral before enabling a model. A health response alone is insufficient.

The NVIDIA hosted free API is a trial/evaluation service with variable limits.
Its availability and model entitlement must be checked for this personal evaluation;
it is not a guaranteed production capacity tier. See the model's API trial terms.

Rollback: set `JOB_EXTRACTION_NVIDIA_FIRST=false` and recreate application workers
through the normal gated deployment. This restores the previous ETL provider and
does not alter jobs, retry timestamps, credentials, or budget counters.


### Optional personal-deployment budget exemptions

Daily cloud request/token caps remain enabled for ordinary users. Set
`JOBSCOUT_CLOUD_ADMIN_LLM_BUDGET_EXEMPT=true` to exempt the current verified,
active platform administrator whose protected database record matches
`JOBSCOUT_CLOUD_PLATFORM_ADMIN_EMAIL`. The operation must have the server-installed
owner context; shared tenant membership and system ownership do not confer this
exemption. Queued work rechecks the account when executed.

`JOBSCOUT_CLOUD_CATALOG_LLM_BUDGET_EXEMPT=true` separately exempts scheduled job
extraction and job embedding in the existing background catalog lane. Both options
default to false. Neither removes provider quotas, rate limits, bounded retries,
process/concurrency safeguards or per-request model context/output limits.

Exempt attempts, including retries and fallback, are recorded under separate
`jobscout-cloud:llm-usage:{admin|catalog}:{UTC-date}:{requests|tokens}` Redis keys.
Known provider token usage replaces the estimate; failed attempts without usage
retain their reservation. The ordinary 200/20/2,000,000 allowance is never reset or
consumed by exempt work. Accounting and identity lookup failures fail closed.
