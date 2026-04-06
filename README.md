# JobScout
![JobScout Dashboard](image.png)
AI-powered job matching pipeline that scrapes, analyzes, and ranks jobs against your resume and preferences.

## Features

- **ETL Pipeline**: Scrapes jobs from multiple sources (LinkedIn, Indeed, Glassdoor, TokyoDev, JapanDev)
- **AI Extraction**: Uses any OpenAI-compatible LLM (Ollama, OpenAI, etc.) to extract structured requirements from job descriptions and resume
- **Vector Search**: Hybrid retrieval with pgvector — dense embedding similarity + BM25 lexical fusion
- **Semantic Fit Scoring**: Cross-encoder reranker (`BAAI/bge-reranker-v2-m3`) scores resume evidence against each requirement
- **Preference Semantics**: LLM-powered reranker reads your `wants.txt` and adjusts ranking based on soft preferences
- **Ranking Pipeline**: Three ranking modes — `balanced`, `preference_first`, `fit_first` — configurable per request or globally
- **Dashboard**: Web interface to browse ranked matches, view per-requirement evidence, hide jobs, and trigger the pipeline

## Architecture

JobScout runs as a split microservice stack:

| Service | Role |
|---------|------|
| `orchestrator` | Schedules scraping, ETL, and matching; streams task progress over Redis |
| `extraction` | LLM-based structured extraction from job descriptions and resume |
| `embeddings` | Generates vector embeddings for resume and job requirements |
| `scorer-matcher` | Hybrid retrieval, cross-encoder fit scoring, preference reranking |
| `web-backend` | FastAPI dashboard API; serves match results and pipeline controls |
| `web-ui` | React/Vite frontend |

## Quick Start

### Prerequisites

- **Python 3.13+** and **uv** (package manager)
- **Docker** (for PostgreSQL with pgvector and Redis)
- **Ollama** (optional — for local LLM inference; any OpenAI-compatible endpoint also works)
- **Node.js 18+** (for frontend)

### First-Time Setup

```bash
# 1. Install dependencies
uv sync --all-groups
cd web/frontend && npm install && cd ../..

# 2. Copy and edit environment config
cp .env.example .env
# Edit .env — set notification secrets (e.g. DISCORD_WEBHOOK_URL), DB/Redis URLs if non-default

# 3. (Optional) Add your job preferences
# Create wants.txt — one preference per line in plain language
# Example: "I prefer remote-first companies", "I want a role focused on backend systems"
echo "I want remote work flexibility" >> wants.txt
```

> **Resume**: Upload your resume (PDF or JSON) via the web dashboard. The old `resume.json` file config is deprecated and no longer used.

### Start Everything

**Split stack (recommended):**

```bash
./scripts/setup_local_env/start.sh --split
```

**With local hot reload for backend and frontend:**

```bash
WEB_DEV=true ./scripts/setup_local_env/start.sh --split --web-app --web-ui
```

**Access the app:**
- Frontend: http://localhost:5173
- Backend API: http://localhost:8080
- API Docs: http://localhost:8080/docs

### View Logs

```bash
./scripts/setup_local_env/logs.sh           # Last 50 lines of all services
./scripts/setup_local_env/logs.sh -f        # Follow in real-time
./scripts/setup_local_env/logs.sh web-backend  # Single service
```

## Manual Startup

### 1. Start Docker Services

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.microservices.yml \
  -f docker-compose.web.yml \
  --profile split up -d

# Run Ollama natively if your config points at localhost:11434
ollama serve
```

### 2. Start Backend (FastAPI)

**Docker (recommended):**

```bash
docker compose -f docker-compose.yml -f docker-compose.web.yml --profile web up -d web-backend
```

**Local development (with hot reload):**

```bash
uv sync --group web
WEB_DEV=true uv run python -m uvicorn web.backend.app:app --host 127.0.0.1 --reload --port 8080
```

> **Security:** Use `--host 127.0.0.1` when running locally without Docker. Use `--host 0.0.0.0` only inside Docker containers where network isolation applies.

### 3. Start Frontend (Vite)

```bash
cd web/frontend
npm install  # first time only
npm run dev
```

Frontend: http://localhost:5173 (proxies API requests to localhost:8080)

## Running the Pipeline

Use the dashboard or API to upload your resume, trigger matching, and view results.

```bash
# Check whether the latest uploaded resume is eligible for matching
curl http://localhost:8080/api/pipeline/resume-eligibility

# Trigger a matching run
curl -X POST http://localhost:8080/api/pipeline/run-matching

# Check task status
curl http://localhost:8080/api/pipeline/status/<task_id>
```

## Configuration

All configuration lives in `config.yaml`. Key sections:

### LLM Provider

JobScout uses any OpenAI-compatible endpoint for extraction, embedding, and preference reranking.

```yaml
etl:
  llm:
    provider: "openai_compatible"
    base_url: "http://localhost:11434/v1"  # Ollama default; swap for OpenAI, etc.
    api_key: "ollama"
    extraction_model: "qwen3:14b"
    embedding_model: "qwen3-embedding:4b"
    embedding_dimensions: 1024
```

Separate `embedding_base_url` / `embedding_api_key` overrides are available if you use a different provider for embeddings.

### Matching & Fit Scoring

```yaml
matching:
  matcher:
    hybrid_retrieval_enabled: true    # dense + lexical fusion
    similarity_threshold: 0.5
  scorer:
    semantic_fit:
      cross_encoder:
        local:
          model_name: "BAAI/bge-reranker-v2-m3"
    weight_required: 0.7              # weight for required requirements in fit score
    weight_preferred: 0.3             # weight for preferred requirements
    penalty_missing_required: 15.0
    penalty_seniority_mismatch: 10.0
```

### Preference Semantics

JobScout parses your `wants.txt` and uses an LLM (or cross-encoder) to rerank matches by soft preferences.

```yaml
preferences:
  default_mode: "semantic_rerank"
  reranker: "llm"                     # "llm" or "cross_encoder"
  parser:
    model: "qwen3:14b"
  semantic_reranker:
    model: "qwen3:14b"
  llm_judge:
    enabled: false                    # optional second-pass LLM judge
```

### Ranking Pipeline

Three ranking modes control how fit score and preference score are blended:

```yaml
ranking:
  active_default_mode: "balanced"     # "balanced" | "preference_first" | "fit_first"
  balanced_w_pref: 0.6
  balanced_w_fit: 0.4
  default_top_k: 25
  max_ranking_candidates: 500
```

Override per-request via `?ranking_mode=preference_first` on the matches API.

### Notifications

```yaml
notifications:
  enabled: true
  min_score_threshold: 70.0
  channels:
    discord:
      enabled: true
      # Set DISCORD_WEBHOOK_URL env var
```

Supported channels: Discord, email (SMTP), Telegram, webhook. Mailpit is included in the local stack for email testing (SMTP at `localhost:1025`, UI at http://localhost:8025).

### Scrapers

```yaml
scrapers:
  - site_type: ["linkedin"]
    search_term: "software engineer"
    location: "Tokyo"
    results_wanted: 5
    hours_old: 168
  # Also: indeed, glassdoor, tokyodev, japandev
```

## Project Structure

```
jobscout/
├── config.yaml                  # All configuration
├── wants.txt                    # Your soft preferences (one per line)
├── core/                        # Shared AI/matching/scoring logic
│   ├── matcher/                 # Hybrid retrieval (pgvector + lexical)
│   ├── scorer/                  # Fit scoring, cross-encoder, persistence
│   ├── ranking/                 # Ranking pipeline (balanced/preference_first/fit_first)
│   └── preferences/             # Preference parser and semantic reranker
├── database/                    # SQLAlchemy models and repositories
├── etl/                         # ETL orchestrator, resume profiler, schemas
├── services/                    # Microservice entrypoints
│   ├── orchestrator/            # Task scheduling and Redis stream coordination
│   ├── extraction/              # LLM extraction service
│   ├── embeddings/              # Embedding generation service
│   └── scorer_matcher/          # Matching and scoring service
├── notification/                # Notification channels, worker, tracker
├── pipeline/                    # Matching pipeline
├── web/
│   ├── backend/                 # FastAPI backend (routers, services, models)
│   └── frontend/                # React + Vite frontend
├── tests/                       # Unit and integration tests
├── scripts/
│   └── setup_local_env/         # start.sh, logs.sh, local stack helpers
└── docker-compose*.yml          # Docker service definitions
```

## Testing

```bash
# All tests (unit + integration if DB available)
uv run python -m pytest tests/ -v

# Unit tests only (no database required)
uv run python -m pytest tests/ -v -m "not db"

# Start test database for integration tests
docker-compose -f docker-compose.test.yml up -d
```

## Dependencies

- **Python 3.13+** with **uv**
- **Docker**: PostgreSQL (pgvector), Redis
- **Ollama** or any OpenAI-compatible LLM endpoint
- **Node.js 18+** (frontend)

## License

GNU Affero General Public License v3.0
