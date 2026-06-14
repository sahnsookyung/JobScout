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

The current frontend is a workbench, not a static report. The main page combines source status, result policy controls, ranking mode controls, preference visibility, notifications, and the live match list in one place.

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
8. **Rerank the configured top-N window** only when a current successful LLM evaluation is eligible.
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

Runtime services:

- PostgreSQL with pgvector for jobs, resumes, requirements, embeddings, matches, and cached evaluations.
- Redis for queue-backed workers and background processing.
- Optional Ollama/local embedding services depending on configuration.
- Cerebras or another configured OpenAI-compatible provider for the second-pass judge.

## Quick Start

Install dependencies:

```bash
uv sync --all-groups
cd web/frontend
npm install
cd ../..
```

Create local configuration:

```bash
cp .env.example .env
```

Start the full local stack:

```bash
./scripts/setup_local_env/start.sh
```

For web development with hot reload:

```bash
WEB_DEV=true ./scripts/setup_local_env/start.sh --database --redis --web-app --web-ui
```

If the default Vite port is already in use, start the frontend manually on another port:

```bash
cd web/frontend
npm run dev -- --host 127.0.0.1 --port 5174
```

## Common Workflow

1. Open the web UI and review source status.
2. Add or refresh job sources.
3. Upload or update the resume profile.
4. Run matching and tune the result policy.
5. Open a job's match details to inspect deterministic requirement evidence.
6. Generate or refresh the LLM second-pass review.
7. Generate a tailored resume draft for jobs worth applying to.
8. Hide, exclude, or notify on jobs as the shortlist changes.

## Configuration

Core settings live in `config.yaml` and can be overridden with environment variables.

Important groups:

- `database`: PostgreSQL connection and pgvector-backed persistence.
- `etl`: source extraction, provider settings, and LLM-backed requirement extraction.
- `matching`: thresholds, retrieval behavior, ranking policy, preferences, and LLM judge runtime.
- `notifications`: notification channels and delivery behavior.
- `web`: API and frontend runtime settings.

Secrets belong in `.env` locally and in the deployment environment for hosted runs. Do not commit API keys.

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
docker-compose -f docker-compose.test.yml up -d
```

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
