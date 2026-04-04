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
# Start the supported split stack locally
./scripts/setup_local_env/start.sh --split

# Run with full stack
docker-compose up -d

# Run Ollama natively for embeddings
ollama serve
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
scripts/        - Utility scripts
scripts/tests/  - Manual test scripts (require running services)
migrations/     - Database migration scripts
```

## Key Dependencies
- **Web**: FastAPI, uvicorn
- **Database**: SQLAlchemy, psycopg2-binary, pgvector
- **AI**: openai, pydantic
- **Queue**: redis, rq
- **Testing**: pytest
- **Utils**: pyyaml, requests, tenacity

## RTK — Token-Optimized Commands
Always use rtk equivalents instead of raw commands. rtk passes through
anything it doesn't recognize, so when in doubt, prefix with rtk anyway.

### Files
- `cat <file>` → `rtk read <file>`
- `ls <dir>` → `rtk ls <dir>`
- `find "*.rs" <dir>` → `rtk find "*.rs" <dir>`
- `rg / grep <pat>` → `rtk grep <pat> <dir>`

### Git
- `git status/diff/log/add/commit/push/pull/fetch/stash` → `rtk git <subcommand>`

### GitHub CLI
- `gh pr/issue/run <cmd>` → `rtk gh <cmd>`

### Tests & Linting
- `cargo test/build/clippy` → `rtk cargo <subcommand>`
- `pytest` → `rtk pytest`
- `go test` → `rtk go test`
- `eslint/biome/ruff check/tsc` → `rtk lint`

### Containers & Infra
- `docker ps/images/logs` → `rtk docker <subcommand>`
- `kubectl get pods/logs/services` → `rtk kubectl <subcommand>`

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **JobScout** (6615 symbols, 17702 relationships, 290 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## When Debugging

1. `gitnexus_query({query: "<error or symptom>"})` — find execution flows related to the issue
2. `gitnexus_context({name: "<suspect function>"})` — see all callers, callees, and process participation
3. `READ gitnexus://repo/JobScout/process/{processName}` — trace the full execution flow step by step
4. For regressions: `gitnexus_detect_changes({scope: "compare", base_ref: "main"})` — see what your branch changed

## When Refactoring

- **Renaming**: MUST use `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` first. Review the preview — graph edits are safe, text_search edits need manual review. Then run with `dry_run: false`.
- **Extracting/Splitting**: MUST run `gitnexus_context({name: "target"})` to see all incoming/outgoing refs, then `gitnexus_impact({target: "target", direction: "upstream"})` to find all external callers before moving code.
- After any refactor: run `gitnexus_detect_changes({scope: "all"})` to verify only expected files changed.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Tools Quick Reference

| Tool | When to use | Command |
|------|-------------|---------|
| `query` | Find code by concept | `gitnexus_query({query: "auth validation"})` |
| `context` | 360-degree view of one symbol | `gitnexus_context({name: "validateUser"})` |
| `impact` | Blast radius before editing | `gitnexus_impact({target: "X", direction: "upstream"})` |
| `detect_changes` | Pre-commit scope check | `gitnexus_detect_changes({scope: "staged"})` |
| `rename` | Safe multi-file rename | `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` |
| `cypher` | Custom graph queries | `gitnexus_cypher({query: "MATCH ..."})` |

## Impact Risk Levels

| Depth | Meaning | Action |
|-------|---------|--------|
| d=1 | WILL BREAK — direct callers/importers | MUST update these |
| d=2 | LIKELY AFFECTED — indirect deps | Should test |
| d=3 | MAY NEED TESTING — transitive | Test if critical path |

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/JobScout/context` | Codebase overview, check index freshness |
| `gitnexus://repo/JobScout/clusters` | All functional areas |
| `gitnexus://repo/JobScout/processes` | All execution flows |
| `gitnexus://repo/JobScout/process/{name}` | Step-by-step execution trace |

## Self-Check Before Finishing

Before completing any code modification task, verify:
1. `gitnexus_impact` was run for all modified symbols
2. No HIGH/CRITICAL risk warnings were ignored
3. `gitnexus_detect_changes()` confirms changes match expected scope
4. All d=1 (WILL BREAK) dependents were updated

## Keeping the Index Fresh

After committing code changes, the GitNexus index becomes stale. Re-run analyze to update it:

```bash
npx gitnexus analyze
```

If the index previously included embeddings, preserve them by adding `--embeddings`:

```bash
npx gitnexus analyze --embeddings
```

To check whether embeddings exist, inspect `.gitnexus/meta.json` — the `stats.embeddings` field shows the count (0 means no embeddings). **Running analyze without `--embeddings` will delete any previously generated embeddings.**

> Claude Code users: A PostToolUse hook handles this automatically after `git commit` and `git merge`.

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
