# Test Runner Guide

## Overview

The test runner script (`scripts/run_tests.py`) automatically manages Redis for integration tests, avoiding conflicts with production Redis.

## Key Features

✅ **Smart Redis Detection**
- Checks for production Redis on port 6379 first
- Uses test Redis on port 6380 if available
- Starts test Redis container if needed
- Never stops production Redis

✅ **Port Separation**
- Production Redis: port 6379 (docker-compose)
- Test Redis: port 6380 (test runner)
- No port conflicts!

✅ **Automatic Cleanup**
- Only stops test Redis if script started it
- Keeps existing Redis running
- Reminds you how to stop test Redis

## Usage

### Run All Tests

```bash
# Automatically starts Redis if needed
python scripts/run_tests.py

# With coverage report
python scripts/run_tests.py --coverage

# Keep Redis running for next test run
python scripts/run_tests.py --no-cleanup
```

### Run Specific Tests

```bash
# Unit tests only (no Redis needed)
uv run pytest tests/unit/services/test_orchestrator.py -v

# Integration tests only (needs Redis)
python scripts/run_tests.py --path tests/integration/test_orchestrator_pipeline.py

# All orchestrator tests
python scripts/run_tests.py --path "tests/unit/services/test_orchestrator.py tests/integration/test_orchestrator_pipeline.py"
```

### Watch Mode

```bash
# Re-run tests on file changes
python scripts/run_tests.py --watch
```

## How It Works

### 1. Check for Existing Redis

```
┌─────────────────────────────────┐
│ Check port 6379 (production)    │
├─────────────────────────────────┤
│ ✅ Available → Use it           │
│ ❌ Not available → Check 6380   │
└─────────────────────────────────┘
```

### 2. Check for Test Redis

```
┌─────────────────────────────────┐
│ Check port 6380 (test)          │
├─────────────────────────────────┤
│ ✅ Available → Use it           │
│ ❌ Not available → Start it     │
└─────────────────────────────────┘
```

### 3. Start Test Redis (if needed)

```bash
docker run -d -p 6380:6379 --name redis-test redis:7-alpine
```

### 4. Run Tests

```bash
REDIS_URL=redis://localhost:{port}/1 pytest tests/
```

### 5. Cleanup (optional)

```
┌─────────────────────────────────┐
│ Did we start Redis?             │
├─────────────────────────────────┤
│ ✅ Yes → Stop it                │
│ ❌ No → Leave it running        │
└─────────────────────────────────┘
```

## Command Line Options

```
python scripts/run_tests.py [OPTIONS]

Options:
  --coverage, -c    Generate coverage report (HTML + XML)
  --watch, -w       Watch for changes and re-run tests
  --no-cleanup      Don't stop Redis after tests
  --path, -p PATH   Test path to run (default: tests/)
  --help, -h        Show help message
```

## Examples

### Quick Test Run

```bash
# Fastest - unit tests only
uv run pytest tests/unit/services/test_orchestrator.py -v

# Full test suite
python scripts/run_tests.py
```

### Development Workflow

```bash
# Start test Redis once
docker run -d -p 6380:6379 --name redis-test redis:7-alpine

# Run tests multiple times (Redis stays running)
python scripts/run_tests.py --no-cleanup
python scripts/run_tests.py --no-cleanup --coverage

# Stop Redis when done
docker stop redis-test
```

### CI/CD Simulation

```bash
# Simulate CI/CD environment (fresh Redis, full coverage)
python scripts/run_tests.py --coverage
```

## Output Examples

### With Production Redis Running

```
============================================================
🚀 JobScout Test Runner
============================================================
✅ Production Redis found on port 6379 - using it for tests

============================================================
🧪 Running Tests
============================================================
📍 Using Redis on port 6379

... (test results) ...

======================== 25 passed in 0.76s ========================
```

### Starting Test Redis

```
============================================================
🚀 JobScout Test Runner
============================================================
🚀 Starting Redis test container on port 6380...
📦 Creating new Redis test container on port 6380...
✅ Redis test container is ready on port 6380!

============================================================
🧪 Running Tests
============================================================
📍 Using Redis on port 6380

... (test results) ...

======================== 25 passed in 0.76s ========================

🛑 Stopping Redis test container...
✅ Redis container stopped
```

### Using Existing Test Redis

```
============================================================
🚀 JobScout Test Runner
============================================================
✅ Test Redis found on port 6380 - using it for tests

============================================================
🧪 Running Tests
============================================================
📍 Using Redis on port 6380

... (test results) ...

======================== 25 passed in 0.76s ========================

💡 Test Redis container still running on port 6380
   To stop it: docker stop redis-test
   Or run with --no-cleanup to keep it running for next test run
```

## Troubleshooting

### Port 6380 Already in Use

```bash
# Check what's using port 6380
lsof -i :6380

# Stop existing test Redis
docker stop redis-test
docker rm redis-test

# Run tests again
python scripts/run_tests.py
```

### Test Redis Won't Start

```bash
# Check Docker is running
docker ps

# Manually start test Redis
docker run -d -p 6380:6379 --name redis-test redis:7-alpine

# Verify it's working
docker exec redis-test redis-cli ping
# Should return: PONG

# Run tests
python scripts/run_tests.py
```

### Integration Tests Still Skip

```bash
# Check Redis is accessible
redis-cli -p 6380 ping
# Should return: PONG

# If not, check firewall/network settings
# Or try restarting test Redis
docker restart redis-test
```

### Coverage Report Not Generated

```bash
# Make sure to use --coverage flag
python scripts/run_tests.py --coverage

# Check output files
ls -la coverage.xml htmlcov/

# View HTML report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

## Best Practices

### 1. Daily Development

```bash
# Start test Redis once
docker run -d -p 6380:6379 --name redis-test redis:7-alpine

# Run tests multiple times
python scripts/run_tests.py --no-cleanup

# Stop at end of day
docker stop redis-test
```

### 2. Before Committing

```bash
# Run full test suite with coverage
python scripts/run_tests.py --coverage

# Check coverage meets threshold
# (Should be 70%+ overall)
```

### 3. CI/CD Parity

```bash
# Simulate CI/CD environment
python scripts/run_tests.py --coverage

# This runs the same tests as GitHub Actions
```

### 4. Quick Feedback

```bash
# Just unit tests (fast, no Redis)
uv run pytest tests/unit/services/test_orchestrator.py -v

# Takes ~1 second vs ~20 seconds for full suite
```

## Test Results Summary

| Test Type | Count | Status |
|-----------|-------|--------|
| Unit Tests | 14 | ✅ Always run |
| Integration Tests | 11 | ✅ Run with Redis |
| **Total** | **25** | **✅ 100% pass rate** |

## Resources

- [Test Status Summary](TEST_STATUS_SUMMARY.md)
- [Running Tests Locally](RUNNING_TESTS_LOCALLY.md)
- [SonarQube Setup](SONARQUBE_SETUP.md)
