# AGENTS.md - JobScout Repository Guidelines

## Build, Test & Development Commands

### Setup
```bash
# Install dependencies (uses uv)
uv sync

# Install with all dependency groups
uv sync --all-groups
```

### Testing
```bash
# Run all tests (unit + integration if DB available)
uv run python -m pytest tests/ -v

# Run only unit tests (no database required)
uv run python -m pytest tests/ -v -m "not db"

# Run only database tests
uv run python -m pytest tests/ -v -m "db"

# Run single test file
uv run python -m pytest tests/test_models.py -v

# Run single test class
uv run python -m pytest tests/test_models.py::TestModels -v

# Run single test method
uv run python -m pytest tests/test_models.py::TestModels::test_job_post_instantiation -v

# Using unittest directly
uv run python -m unittest tests.test_models -v
uv run python -m unittest tests.test_models.TestModels.test_job_post_instantiation -v
```

### Database Setup for Tests
```bash
# Start test database (PostgreSQL with pgvector)
docker-compose -f docker-compose.test.yml up -d

# Or set custom test database URL
export TEST_DATABASE_URL="postgresql://user:pass@localhost:5433/jobscout_test"

# Skip DB tests entirely
export SKIP_DB_TESTS=true
```

### Running the Application
```bash
# Run main driver locally (requires services via docker-compose)
uv run python main.py

# Run with full stack
docker-compose up -d

# Run with local Ollama for embeddings
docker-compose --profile docker-ollama up -d
```

### Web Service
```bash
# Run FastAPI web interface
uv sync --group web
uv run python -m uvicorn web.app:app --reload
```

## Code Style Guidelines

### Python Version & Tools
- **Python 3.13+** required
- Use `uv` for dependency management and running commands
- No explicit linter configured - follow style conventions below

### Imports (Grouped & Ordered)
1. Standard library imports
2. Third-party package imports
3. Local application imports (absolute, no relative imports)

```python
# Standard library
import os
import json
from datetime import datetime
from typing import Optional, Dict, Any

# Third-party
from sqlalchemy import Column, Integer, String
from pydantic import BaseModel
from redis import Redis

# Local application (alphabetical within groups)
from core.ai_service import OpenAIService
from core.config_loader import load_config
from database.models import JobPost
from etl.orchestrator import JobETLOrchestrator
```

### Type Hints
- Use type hints for all function parameters and return types
- Use `Optional[Type]` for nullable values
- Use `Dict[str, Any]` for flexible dictionaries
- Import types from `typing` module

### Naming Conventions
- **Classes**: PascalCase (`JobPost`, `OpenAIService`)
- **Functions/Methods**: snake_case (`extract_structured_data`, `load_config`)
- **Variables**: snake_case (`test_db_url`, `canonical_fingerprint`)
- **Constants**: UPPER_SNAKE_CASE (module-level)
- **Private**: Prefix with underscore (`_db_available`)
- **Abstract Base Classes**: End with "Provider" (`LLMProvider`)

### Error Handling
- Use specific exceptions, avoid bare `except:`
- Log errors with context using `logging` module
- Use `try/except` blocks for external service calls (database, API, Redis)
- Graceful degradation for optional services (check availability flags)

### Configuration & Environment
- Use Pydantic models for all configuration (`AppConfig`, `LlmConfig`)
- Environment variable overrides supported via `load_config()`
- Store secrets in `.env` file (never commit secrets)
- Default configs defined in `config.yaml`

### Testing Conventions
- Mix of `unittest.TestCase` and `pytest` fixtures OK
- Use `pytest.mark.db` marker for database-dependent tests
- Mock external services in unit tests
- Integration tests prefixed with `integration_test_*.py`
- Test utilities in `tests/__init__.py`, fixtures in `conftest.py`

### Database Patterns
- SQLAlchemy declarative base models
- UUID primary keys with `default=uuid.uuid4`
- Timestamps with timezone: `server_default=sql_text("timezone('UTC', now())")`
- Use `db_session_scope()` context manager for transactions
- Check `is_database_available()` before DB tests

### Architecture Principles (SOLID)
- Use abstract base classes for pluggable components (`LLMProvider`)
- Single Responsibility: separate ETL, matching, scoring, notification services
- Dependency Injection: pass service instances, don't construct inside functions
- Interface Segregation: small, focused interfaces

### Logging
- Use module-level loggers: `logger = logging.getLogger(__name__)`
- Format: `'%(asctime)s - %(name)s - %(levelname)s - %(message)s'`
- Log at appropriate levels (INFO for operations, DEBUG for details)

### Docker & Services
- Services: PostgreSQL (pgvector), Redis, JobSpy API, Ollama (optional)
- Use docker-compose for local development
- Environment variables for service URLs

## Project Structure
```
core/           - AI service interfaces, matcher, scorer, config
database/       - SQLAlchemy models, repository, database connection
etl/            - Extract-Transform-Load orchestrator, schemas, resume profiler
notification/   - Notification channels, tracker, service, worker
web/            - FastAPI web interface
tests/          - All tests (unit + integration)
migrations/     - Database migration scripts
```

## Key Dependencies
- **Web**: FastAPI, uvicorn
- **Database**: SQLAlchemy, psycopg2-binary, pgvector
- **AI**: openai, pydantic
- **Queue**: redis, rq
- **Testing**: pytest
- **Utils**: pyyaml, requests, tenacity
