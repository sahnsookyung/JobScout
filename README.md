# JobScout

JobScout is an AI-assisted job search workshop. It ingests jobs, preserves the best available full job descriptions, builds resume evidence, ranks matches, and gives you an inspectable second-pass review before you spend time applying.

![JobScout dashboard](docs/assets/readme-dashboard.png)

## What It Does

- Imports and tracks job sources across configured seed feeds, job boards, and API-backed providers.
- Stores the fullest available job description with source/completeness metadata so later snippets do not overwrite richer postings.
- Extracts job requirements and compares them against resume evidence units, skills, projects, and experience.
- Ranks matches with deterministic scoring, preference filters, hidden/excluded states, and configurable result policies.
- Runs an optional independent LLM second-pass review against the job description and resume evidence.
- Shows why each job matched, which requirements are covered, what is missing, and whether an LLM result is current enough to affect ordering.
- Generates tailored resume drafts from the selected job, the resume profile, and match evidence.
- Supports notifications, source health checks, local development, and cloud deployment paths.

## Current UI

The current frontend is a workbench, not a static report. The workspace is split into two tabs:

- **Jobs** is the primary review surface for result policy controls, ranking mode controls, preference visibility, notifications, and the live match list.
- **Job Management** is the operations surface for provider/source configuration, durable pipeline runs, processing blockers, and imported-job state.

The match list is bounded and projection-based: the frontend renders the current page of compact rows, opens full match details lazily, and applies LLM reranking only to the configured Top N window after canonical filters and deterministic ordering.

### Job Management

Provider management appears first so source configuration and health are visible before inventory operations. The lower inventory area shows imported-job counts, latest durable runs, LLM queue state, and the oldest processing blockers from DB-backed pipeline state.

![Job management](docs/assets/readme-job-management.png)

### Match Review

Each result opens into a details modal with deterministic score cards, semantic-fit metadata, requirement coverage, LLM review status, resume draft actions, and the original posting.

![Match details](docs/assets/readme-match-details.png)

### Full Job Descriptions

JobScout now displays the best available job description in the modal, including completeness/source labels and warning codes when the system only has a partial or missing description. Long descriptions are wrapped and contained in a scrollable region.

![Full job description](docs/assets/readme-full-jd.png)

## Matching Flow

1. **Ingest jobs** from trusted source paths and seed feeds.
2. **Preserve job content** using no-downgrade merging so a later short snippet does not replace a fuller posting.
3. **Extract requirements** from the effective job description.
4. **Build resume evidence** from the owner's resume profile, normalized skills, projects, and experience.
5. **Generate candidates** with hybrid lexical and semantic retrieval.
6. **Score deterministically** with requirement coverage, preference penalties, confidence, and fit thresholds.
7. **Optionally judge with an LLM** using the full packed job description and owner-scoped resume evidence. The LLM review is cached by prompt, schema, config, resume, and job-content hashes so stale reviews can be displayed but ignored for ordering.
8. **Rerank the configured Top N window** after canonical filters and deterministic ranking. The backend fetches only the bounded rerank window plus the requested page, not the full job list.
9. **Draft a tailored resume** from the selected job, resume profile, and match context.

## LLM Runtime

The second-pass judge is configured separately from extraction. It is intended to be an independent review of the job and resume evidence, not a rephrasing of deterministic scores.

Typical local environment variables:

```bash
CEREBRAS_API_KEY=...
LLM_AS_A_JUDGE_PROVIDER=cerebras
LLM_AS_A_JUDGE_MODEL=gpt-oss-120b
LLM_AS_A_JUDGE_BASE_URL=https://api.cerebras.ai/v1
```

The runtime configuration lives under `matching.llm_judge` in `config.yaml`. Hosted or OCI deployments must receive the same secret and runtime variables through their environment configuration.

## Architecture

| Area | Responsibility |
| --- | --- |
| `etl/` | Source fetching, external seed imports, content normalization, requirement extraction |
| `database/` | SQLAlchemy models, repositories, migrations, pgvector-backed storage |
| `core/` | Matching, ranking, embeddings, LLM provider contracts, resume profiling |
| `notification/` | Notification tracking and channel delivery |
| `web/backend/` | FastAPI API used by the UI |
| `web/frontend/` | React/TypeScript workbench UI |
| `scripts/` | Local setup, deployment, smoke checks, and utility entrypoints |

### System Architecture Diagram

For the reader to wrap their mind around how things generally work, the high-level architecture diagram is tracked at `docs/assets/JobScout architecture.png`. Note that the jobscout-cloud is a private repo that deploys this to an actual instance, but is included in the architecture for showing how this repo could be deployed and integrated with various systems conceptually.

![JobScout architecture](docs/assets/JobScout%20architecture.png)

Runtime services:

- PostgreSQL with pgvector for jobs, resumes, requirements, embeddings, matches, and cached evaluations.
- Durable `PipelineRun` / `PipelineRunStage` rows as the source of truth for scrape, extraction, embedding, matching, repair, resume ETL, and worker progress.
- Redis for queue-backed workers, streams, task projections, and SSE fan-out.
- Optional Ollama/local embedding services depending on configuration.
- Cerebras or another configured OpenAI-compatible provider for the second-pass judge.

## Quick Start

### Prerequisites

- Python 3.13 or newer and [`uv`](https://docs.astral.sh/uv/).
- Node.js with npm.
- Docker with the Compose plugin (`docker compose`).
- Ollama for the default local extraction and embedding models, or OpenAI-compatible endpoints configured in `.env`.

The full stack is relatively large. Plan for at least 20 GB of available Docker storage for the JobSpy image, service build layers, and the roughly 2 GB local reranker download. Docker Desktop's own disk allocation can fill up even when the host still reports free space.

Install dependencies:

```bash
uv sync --all-groups
cd web/frontend
npm ci
cd ../..
```

Create local configuration and replace the development-only JobSpy token placeholder with a long random value:

```bash
cp .env.example .env
python -c 'import secrets; print(secrets.token_urlsafe(32))'
```

Paste that generated value into `JOBSPY_API_TOKEN` in `.env`. The PostgreSQL and Redis values in `.env.example` intentionally match the local Compose defaults; do not reuse those credentials in a shared or production environment.

The default local AI configuration uses Ollama. Download the configured extraction and embedding models, then make Ollama reachable from Docker containers:

```bash
ollama pull qwen3:14b
ollama pull qwen3-embedding:4b
OLLAMA_HOST=0.0.0.0:11434 ollama serve
```

Binding Ollama to `0.0.0.0` is appropriate only on a trusted development machine or behind a firewall. Native processes use `localhost`; Compose services use `host.docker.internal` automatically. To use external OpenAI-compatible services instead, set the `ETL_LLM_EXTRACTION_*` and `ETL_EMBEDDING_*` values described below.

Build and start the full local stack. The first run is slow because it builds the service images and downloads model weights:

```bash
./scripts/setup_local_env/start.sh --build
```

Later starts can omit `--build`. Open the UI at <http://localhost:5173>, the backend at <http://localhost:8080>, and the API documentation at <http://localhost:8080/docs>.

For lightweight frontend/backend development with hot reload, PostgreSQL, and Redis—but without the pipeline microservices:

```bash
WEB_DEV=true ./scripts/setup_local_env/start.sh --database --redis --web-app --web-ui
```

For hot reload with the complete pipeline running in Docker:

```bash
WEB_DEV=true ./scripts/setup_local_env/start.sh --microservices --web-app --web-ui --build
```

If the default Vite port is already in use, start the frontend manually on another port:

```bash
cd web/frontend
npm run dev -- --host 127.0.0.1 --port 5174
```

Stop all JobScout services with:

```bash
./scripts/setup_local_env/stop.sh
```

## Common Workflow

1. Open the web UI and review the Jobs tab.
2. Tune result policy, including Top N for LLM reranking, and run matching.
3. Upload or update the resume profile.
4. Open a job's match details to inspect deterministic requirement evidence, LLM status, and the original posting.
5. Generate or refresh the LLM second-pass review.
6. Generate a tailored resume draft for jobs worth applying to.
7. Use Job Management to add or refresh providers and job sources.
8. Watch imported-job state, pipeline runs, and processing blockers when operations need attention.
9. Hide, exclude, or notify on jobs as the shortlist changes.

## Configuration

Core settings live in `config.yaml` and can be overridden with environment variables.

Important groups:

- `database`: PostgreSQL connection and pgvector-backed persistence.
- `etl`: source extraction, provider settings, and LLM-backed requirement extraction.
- `matching`: thresholds, retrieval behavior, ranking policy, preferences, and LLM judge runtime.
- `notifications`: notification channels and delivery behavior.
- `web`: API and frontend runtime settings.

Secrets belong in `.env` locally and in the deployment environment for hosted runs. Do not commit API keys.

Important local environment values:

| Value | Required | Local behavior |
| --- | --- | --- |
| `JOBSPY_API_TOKEN` | Full stack | Shared secret used by the orchestrator and JobSpy API. Replace the `.env.example` placeholder. |
| `DATABASE_URL` | Native backend/tests | Defaults to `postgresql://user:password@localhost:5432/jobscout`. |
| `REDIS_URL` | Native backend/workers | Must include the local password: `redis://:jobscoutredis@localhost:6379/0`. |
| `ETL_LLM_EXTRACTION_BASE_URL`, `ETL_LLM_EXTRACTION_MODEL`, `ETL_LLM_EXTRACTION_API_KEY` | Optional override | Configure an external OpenAI-compatible extraction model instead of local Ollama. |
| `ETL_EMBEDDING_BASE_URL`, `ETL_EMBEDDING_MODEL`, `ETL_EMBEDDING_API_KEY` | Optional override | Configure a separate OpenAI-compatible embedding endpoint. Set the URL, model, and key together. |
| `NVIDIA_API_KEY` | Optional | Enables NVIDIA NIM resume tailoring when `RESUME_GENERATION_ENABLED=true`; otherwise resume generation uses the complete deterministic composer. |
| `RESUME_GENERATION_MODEL` | Optional | Defaults to `mistralai/mistral-medium-3.5-128b`. |
| `CEREBRAS_API_KEY` or another configured judge key | Optional | Enables the independent match-level LLM judge when that feature is enabled. |
| `HF_TOKEN` | Optional | Needed only if the configured Hugging Face reranker repository requires authentication. |

### Local troubleshooting

- **Compose says `JOBSPY_API_TOKEN` is not set:** copy `.env.example` to `.env` and replace the token placeholder.
- **Redis reports `NOAUTH`:** use the password-bearing `REDIS_URL` from `.env.example` for native processes.
- **A container cannot reach Ollama:** start Ollama with `OLLAMA_HOST=0.0.0.0:11434`; Compose maps `host.docker.internal` on Docker Desktop and Linux.
- **A build reports `no space left on device`:** run `docker system df`, increase Docker Desktop's disk allocation, or deliberately remove Docker data you no longer need. Do not run broad prune commands without reviewing what they will delete.
- **The scorer appears stuck on first start:** allow time for the `BAAI/bge-reranker-v2-m3` bootstrap download. The cached model is stored in the `scorer_models` Docker volume.

Useful local health checks:

```bash
docker compose --env-file .env -f docker-compose.yml -f docker-compose.microservices.yml -f docker-compose.web.yml --profile web config --quiet
curl --fail http://localhost:8080/health
curl --fail http://localhost:5173/
./scripts/setup_local_env/logs.sh -f
```

### Hosted public-testing boundary

The OSS repository contains the owner-scoped models, repository predicates,
worker context propagation, upload parser limits, Redis quotas, and browser
cache isolation used by the private `jobscout-cloud` deployment. They remain
off by default for normal local/self-hosted use. Do not enable
`JOBSCOUT_PUBLIC_TESTING_QUOTAS_ENABLED`, global LLM budget enforcement, or
forced RLS piecemeal: the hosted layer must first provision its web/worker/
retention database roles, sole platform admin, public tenant, retention worker,
Turnstile, and activation migration.

Private records carry an `owner_id`; hosted SQLAlchemy transactions reinstall
the user and tenant context after every transaction boundary. When forced RLS
is active, each context is HMAC-signed with `JOBSCOUT_DB_CONTEXT_SECRET`; raw
custom PostgreSQL settings are therefore not sufficient to impersonate another
owner. Missing or invalid context returns no protected rows. Shared job rows and
job-requirement vectors are not user-owned and survive temporary-account
deletion, while resumes, user vectors, matches, evaluations, variants,
preferences, notifications, and user-owned pipeline runs cascade with the user.
See the private deployment README/runbook for the staged public-mode and
two-account verification process.

## Testing

Run backend tests:

```bash
uv run python -m pytest tests/ -v
```

Run only tests that do not require a database:

```bash
uv run python -m pytest tests/ -v -m "not db"
```

Run frontend checks:

```bash
cd web/frontend
npm run type-check
npm run test -- --run
npm run build
```

Start the test database when database-marked tests are needed:

```bash
docker compose -f docker-compose.test.yml up -d
TEST_DATABASE_URL=postgresql://testuser:testpass@localhost:5433/jobscout_test \
  SKIP_DB_TESTS=false \
  uv run python -m pytest tests/ -v -m db
```

The three ORM schema-snapshot tests create their own ephemeral PostgreSQL containers even when `TEST_DATABASE_URL` is set, so they also require Docker and additional storage headroom.

## Project Structure

```text
core/           Matching, scoring, embeddings, LLM provider interfaces
database/       SQLAlchemy models, repositories, database setup
etl/            Job ingestion, extraction, normalization, resume profiling
notification/   Notification services and workers
web/            FastAPI backend and React frontend
tests/          Unit, router, service, ETL, and integration tests
scripts/        Local setup, smoke tests, deployment helpers
migrations/     Database migrations
docs/assets/    README and product screenshots
```

## License

This project is licensed under the AGPL License.
