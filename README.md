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
- **Docker** (for PostgreSQL, Redis, Ollama)
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

**Option A: Full Stack via Docker Compose (Recommended)**

```bash
# Start entire stack: infra + pipeline + web backend + web frontend
docker compose \
  -f docker-compose.yml \
  -f docker-compose.pipeline.yml \
  -f docker-compose.web.yml \
  --profile web up -d

# Or use the startup script (same command)
./scripts/setup_local_env/start.sh
```

**Option B: Local Development (with hot reload)**

```bash
# Start infra + pipeline in Docker, run backend/frontend locally
./scripts/setup_local_env/start.sh --infra --microservices --web-app --web-ui

# For backend hot reload during active development:
WEB_DEV=true ./scripts/setup_local_env/start.sh --infra --microservices --web-app --web-ui
```

**Access the app:**
- Frontend: http://localhost:5173
- Backend API: http://localhost:8080
- API Docs: http://localhost:8080/docs

### View Logs

```bash
# Show last 50 lines of all logs
./scripts/setup_local_env/logs.sh

# Follow logs in real-time
./scripts/setup_local_env/logs.sh -f

# Specific service logs
./scripts/setup_local_env/logs.sh backend    # Backend only
./scripts/setup_local_env/logs.sh frontend   # Frontend only
```

## Manual Startup

### 1. Start Docker Services

```bash
# Start PostgreSQL, Redis, and optionally Ollama
docker-compose up -d

# With Ollama
docker-compose --profile docker-ollama up -d
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

### Option A: Batch Pipeline (Main Driver)

Runs ETL → Matching cycles on a schedule:

```bash
# Full pipeline (ETL + Matching)
uv run python main.py

# ETL only
uv run python main.py --mode etl

# Matching only
uv run python main.py --mode matching
```

### Option B: Manual Pipeline Trigger

Use the dashboard or API to trigger pipelines manually:

```bash
# Start pipeline
curl -X POST http://localhost:8080/api/pipeline/start

# Check status
curl http://localhost:8080/api/pipeline/status
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
├── main.py                  # Batch pipeline driver
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
- **Docker**: PostgreSQL, Redis, Ollama
- **Node.js 18+**: For frontend development

## License

GNU Affero General Public License v3.0
