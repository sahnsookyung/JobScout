# Testing Guide

Run tests with standard Python tools. No custom scripts needed.

## Quick Start

```bash
# Run all tests (automatically detects DB availability)
uv run python -m pytest tests/ -v

# Run only unit tests (no database required)
uv run python -m pytest tests/ -v -k "not db"

# Run specific test file
uv run python -m pytest tests/test_matching_infrastructure.py::TestMatchingUnit -v

# Using unittest instead of pytest
uv run python -m unittest discover tests -v
```

## Test Categories

### 1. Unit Tests (`TestMatchingUnit`)
Fast tests using mocks. No database needed.

```bash
# Unit tests only
uv run python -m pytest tests/test_matching_infrastructure.py::TestMatchingUnit -v

# Or with unittest
uv run python -m unittest tests.test_matching_infrastructure.TestMatchingUnit -v
```

**Coverage:**
- Config loading
- Fingerprint generation
- Evidence extraction
- Coverage calculation
- Base score calculation
- Penalty calculation
- Complete scoring pipeline

### 2. Database Tests (`TestMatchingDatabase`)
Integration tests requiring PostgreSQL with pgvector.

```bash
# Requires test database to be running
uv run python -m pytest tests/test_matching_infrastructure.py::TestMatchingDatabase -v
```

**Coverage:**
- JobMatch model creation
- JobMatchRequirement model creation
- Repository: Get embedded jobs
- Repository: Match invalidation
- Repository: Get matches for resume
- End-to-end pipeline with DB

### 3. Preferences Tests
Tests for preferences-based matching.

```bash
uv run python -m pytest tests/test_preferences_matching.py -v

# Or quick verification script
uv run python tests/verify_preferences.py
```

### 4. Docker-Based Integration Tests
Integration tests that automatically spin up PostgreSQL containers using Docker. These tests verify the matcher service works correctly with real numpy array embeddings from pgvector.

**Requirements:**
- Docker must be installed and running
- `psycopg2-binary` must be installed: `uv add --dev psycopg2-binary`

**Usage:**

```bash
# Run integration test with automatic Docker containers
uv run python -m pytest tests/integration_test_matcher_real_embeddings.py -v

# Or with existing database (no Docker required)
TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/jobscout \
    uv run python -m pytest tests/integration_test_matcher_real_embeddings.py -v

# Disable Docker and use external DB only
USE_DOCKER_CONTAINERS=0 \
    TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/jobscout \
    uv run python -m pytest tests/integration_test_matcher_real_embeddings.py -v
```

**What it tests:**
- Job summary embedding boolean checks with numpy arrays
- Requirement embedding boolean checks
- Evidence unit embedding handling
- Full matcher service with real embeddings
- Similarity calculation with numpy arrays

**Coverage:**
Catches bugs like: "The truth value of an array with more than one element is ambiguous"

**How it works:**
The test uses `conftest_docker.py` which provides:
- `PostgresContainer` class - Manages PostgreSQL with pgvector container
- `RedisContainer` class - Manages Redis container (for future tests)
- Context managers `postgres_container()` and `redis_container()` for easy setup/teardown
- Pytest fixtures `docker_postgres` and `docker_redis` for pytest-based tests

## Database Setup (Optional)

Unit tests don't need a database. Only run this if you want to execute DB tests.

### Option 1: Docker Compose (Recommended)

```bash
# Start test database
docker-compose -f docker-compose.test.yml up -d

# Run DB tests
uv run python -m pytest tests/test_matching_infrastructure.py::TestMatchingDatabase -v

# Stop when done
docker-compose -f docker-compose.test.yml down
```

**Connection details:**
- Host: `localhost`
- Port: `5433` (avoids conflict with local PostgreSQL)
- User: `testuser`
- Password: `testpass`
- Database: `jobscout_test`
- URL: `postgresql://testuser:testpass@localhost:5433/jobscout_test`

### Option 2: Existing PostgreSQL

```bash
# Set environment variable to your DB
export TEST_DATABASE_URL="postgresql://user:pass@localhost:5432/jobscout_test"

# Run tests
uv run python -m pytest tests/ -v
```

### Option 3: Skip DB Tests

```bash
# Force skip all DB tests
export SKIP_DB_TESTS=true

# Or run with filter
uv run python -m pytest tests/ -v -k "not db"
```

## Test Files

```
tests/
├── __init__.py                              # Test utilities and DB detection
├── conftest.py                              # Pytest configuration
├── conftest_docker.py                       # Docker container management fixtures
├── test_matching_infrastructure.py          # Main test suite
│   ├── TestMatchingUnit                     # 7 unit tests (no DB)
│   └── TestMatchingDatabase                 # 6 DB integration tests
├── test_preferences_matching.py             # 19 preferences tests
├── verify_preferences.py                    # Quick verification script
├── integration_test_matcher_real_embeddings.py  # Integration test with Docker
└── README.md                                # This file
```

## Advanced Usage

### Run Specific Tests

```bash
# Single test method
uv run python -m pytest tests/test_matching_infrastructure.py::TestMatchingUnit::test_01_config_loading -v

# Multiple test files
uv run python -m pytest tests/test_matching_infrastructure.py tests/test_preferences_matching.py -v

# By keyword
uv run python -m pytest tests/ -v -k "config or fingerprint"
```

### With Coverage

```bash
# Install pytest-cov
uv add --dev pytest-cov

# Run with coverage report
uv run python -m pytest tests/ --cov=core --cov=database --cov-report=html

# View report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

### Parallel Execution

```bash
# Install pytest-xdist
uv add --dev pytest-xdist

# Run tests in parallel
uv run python -m pytest tests/ -n auto
```

## Troubleshooting

### Tests Are Skipped

If DB tests show `skipped 'SQLAlchemy not available...'` or connection errors:

```bash
# Check if DB is accessible
uv run python -c "from tests import check_db_available; print(check_db_available())"

# Start test DB if needed
docker-compose -f docker-compose.test.yml up -d
```

### Port 5433 Already in Use

```bash
# Edit docker-compose.test.yml, change port
# ports:
#   - "5434:5432"  # Use 5434 instead

# Then update connection
export TEST_DATABASE_URL="postgresql://testuser:testpass@localhost:5434/jobscout_test"
```

### pgvector Not Found

If tests fail with "pgvector extension not installed":

```bash
# For Docker setup - should be included automatically
# For local PostgreSQL:
psql -d jobscout_test -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

## CI/CD Examples

### GitHub Actions

```yaml
name: Tests

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: astral-sh/setup-uv@v3
      
      - name: Run unit tests
        run: uv run python -m pytest tests/ -v -k "not db"

  db-tests:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: ankane/pgvector:latest
        env:
          POSTGRES_USER: testuser
          POSTGRES_PASSWORD: testpass
          POSTGRES_DB: jobscout_test
        ports:
          - 5433:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    
    steps:
      - uses: actions/checkout@v3
      - uses: astral-sh/setup-uv@v3
      
      - name: Run DB tests
        env:
          TEST_DATABASE_URL: postgresql://testuser:testpass@localhost:5433/jobscout_test
        run: uv run python -m pytest tests/ -v -k "db"

  docker-integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: astral-sh/setup-uv@v3
      
      - name: Run Docker-based integration tests
        run: uv run python -m pytest tests/integration_test_matcher_real_embeddings.py -v
```

### GitLab CI

```yaml
test:unit:
  script:
    - uv run python -m pytest tests/ -v -k "not db"

test:db:
  services:
    - name: ankane/pgvector:latest
      alias: postgres
  variables:
    POSTGRES_USER: testuser
    POSTGRES_PASSWORD: testpass
    POSTGRES_DB: jobscout_test
    TEST_DATABASE_URL: "postgresql://testuser:testpass@postgres:5432/jobscout_test"
  script:
    - uv run python -m pytest tests/ -v -k "db"
```

## Summary

- **Unit tests** are fast (~0.5s), require no setup, use mocks
- **DB tests** require PostgreSQL with pgvector (via Docker or local)
- **All tests** can be run with standard `pytest` or `unittest` commands
- **No custom scripts** - just standard Python testing tools

For development, run unit tests frequently. Run full suite with DB before committing.
