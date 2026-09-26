# Job extraction provider routing

Set `JOB_EXTRACTION_NVIDIA_FIRST=true` to use NVIDIA NIM for job-description
extraction, with the existing `etl.llm` endpoint (Cerebras on OCI) as fallback.
The default for standalone JobScout is disabled. Resume extraction, generic
structured requests, and the existing embedding model and batch API are unchanged.

The personal evaluation deployment uses `nvidia/nemotron-3-super-120b-a12b`,
JSON Schema output, and disabled reasoning. The job-specific prompt matches the
actual JobExtraction schema. All results must pass that schema before import.

## Credentials and controls

Configure `NVIDIA_EXTRACTION_API_KEY` for this specific model. When absent, the
loader can use `NVIDIA_API_KEY`; verify that key's model entitlement before enabling
the route. Never assume a working key for another NVIDIA model grants access.
`NVIDIA_EXTRACTION_MODEL` selects the model without changing judging or resume models.

Each provider makes one attempt, with a 60-second timeout, no local rate-limit
sleep, and at most 4096 output tokens. `NVIDIA_EXTRACTION_REQUESTS_PER_MINUTE`
defaults to 10; lower it if the account's limit requires it. Timeout and output
bounds are configurable through `JOB_EXTRACTION_PROVIDER_TIMEOUT_SECONDS` and
`JOB_EXTRACTION_MAX_OUTPUT_TOKENS`.

Timeouts, 429 throttles, 5xx responses, invalid extraction output, and provider
quota failures can select the next provider. Authentication and configuration
errors remain visible. A 402 or explicitly daily/insufficient-quota 429 defers
that provider/model for one hour in Redis, surviving worker restarts. When the
chain fails, the durable job retry policy owns subsequent retries; the worker
does not repeat the whole chain three times immediately.

All actual provider attempts still consume the shared request budget. The
200-request daily ceiling, 20-request interactive reserve, token ceiling and UTC
reset remain unchanged. Global budget errors propagate with their reset time;
switching providers never bypasses those limits. Token accounting can include
conservative reservations for failed calls and is not a monetary bill.

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
