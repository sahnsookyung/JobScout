# JobScout
![JobScout Dashboard](image.png)
AI-powered job matching pipeline that scrapes, analyzes, and matches jobs to your resume and preferences.

## Features

- **ETL Pipeline**: Scrapes jobs from multiple sources (LinkedIn, Indeed, Glassdoor, TokyoDev, JapanDev)
- **AI Analysis**: Uses local LLMs (Ollama) for job extraction and semantic matching
- **Vector Search**: pgvector for similarity matching between your skills and job requirements
- **Dashboard**: Web interface to browse matches, view stats, and configure notifications

## Quick Start

### Prerequisites

- **Python 3.13+** and **uv** (package manager)
- **Docker** (for PostgreSQL and Redis)
- **Ollama** optional, run natively if you use local models
- **Node.js 18+** (for frontend)

### First-Time Setup

```bash
# 1. Install dependencies
uv sync --all-groups
cd web/frontend && npm install && cd ../..

# 2. Set up your configuration files
cp resume.example.json resume.json
cp .env.example .env
# Edit resume.json with your actual resume data
# Edit .env with your notification settings (e.g., Discord webhook)

# 3. (Optional) Add your job preferences
cp wants.example.txt wants.txt
# Edit wants.txt - one preference per line in natural language
# Then uncomment user_wants_file in config.yaml
```

### Start Everything

**Option A: Split Topology (microservices)**

```bash
# Split mode: orchestrator + extraction + embeddings + scorer-matcher
./scripts/setup_local_env/start.sh --split
```

**Option B: Local Development (with hot reload)**

```bash
# Split mode with backend/frontend local development
WEB_DEV=true ./scripts/setup_local_env/start.sh --split --web-app --web-ui
```

**Access the app:**
- Frontend: http://localhost:5173
- Backend API: http://localhost:8080
- API Docs: http://localhost:8080/docs

### Topology

Run `--split` to start extraction, embeddings, matching, and orchestration as separate microservices.

### View Logs

```bash
# Show last 50 lines of all logs
./scripts/setup_local_env/logs.sh

# Follow logs in real-time
./scripts/setup_local_env/logs.sh -f

# Specific service logs
./scripts/setup_local_env/logs.sh web-backend  # Backend only
./scripts/setup_local_env/logs.sh web-ui       # Frontend only
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

**Option A: Using Docker (Recommended for production parity)**

```bash
# Start backend via Docker Compose
docker compose -f docker-compose.yml -f docker-compose.web.yml --profile web up -d web-backend

# Backend available at:
# - API: http://localhost:8080
# - API Docs: http://localhost:8080/docs
```

**Option B: Local Development (with hot reload)**

```bash
# Install dependencies
uv sync --group web

# Start server locally (127.0.0.1 only - secure for shared networks)
WEB_DEV=true uv run python -m uvicorn web.backend.app:app --host 127.0.0.1 --reload --port 8080
```

**⚠️ Security Note:** When running locally without Docker, use `--host 127.0.0.1` to bind only to localhost. Use `--host 0.0.0.0` only when running inside Docker containers where network isolation applies.

### 3. Start Frontend (Vite)

```bash
cd web/frontend
npm install  # First time only
npm run dev
```

**Frontend:** http://localhost:5173 (proxies API to localhost:8080)

## Running the Pipeline

The monolithic `python main.py` flow is no longer supported. Use the split
microservice stack and trigger work through the web backend or orchestrator APIs.

### Manual Pipeline Trigger

Use the dashboard or API to trigger resume processing and matching:

```bash
# Check whether the latest uploaded resume is eligible for matching
curl http://localhost:8080/api/pipeline/resume-eligibility

# Start matching against the latest ready upload
curl -X POST http://localhost:8080/api/pipeline/run-matching

# Check task status
curl http://localhost:8080/api/pipeline/status/<task_id>
```

## Configuration

Edit `config.yaml` to customize:

- **Scrapers**: Add/modify job sources in `scrapers:`
- **Schedule**: Change pipeline interval in `schedule.interval_seconds:`
- **Matching**: Adjust weights in `matching.scorer:`
- **Notifications**: Configure in `notifications:`

## Project Structure

```
jobscout/
├── scripts/
│   └── setup_local_env/     # Startup scripts and logs
│       ├── start.sh         # Main startup script
│       ├── logs.sh          # Log viewing utility
│       └── logs/            # Log files (auto-created)
│           ├── backend.log
│           └── frontend.log
├── config.yaml              # Application configuration
├── docker-compose.yml       # Docker services
├── core/                    # Core services (AI, matching, scoring)
├── database/                # SQLAlchemy models and DB logic
├── etl/                     # Extract-Transform-Load pipeline
├── pipeline/                # Matching pipeline
├── notification/            # Notification workers
└── web/                     # Web dashboard
    ├── backend/             # FastAPI backend
    └── frontend/            # React frontend
```

## Screenshots

_Coming soon: Dashboard screenshots and demo video_

## Dependencies

- **Python 3.13+**
- **uv**: Package manager
- **Docker**: PostgreSQL, Redis
- **Ollama**: optional local model runtime
- **Node.js 18+**: For frontend development

## License

GNU Affero General Public License v3.0
