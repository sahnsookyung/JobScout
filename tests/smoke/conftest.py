"""
Smoke tests require live services (real URLs, running microservices).
They are excluded from normal test runs and must be opted in explicitly:

    pytest tests/smoke/ -m smoke

Or by setting SMOKE_TESTS=1:

    SMOKE_TESTS=1 pytest tests/smoke/
"""
import os
import pytest

collect_ignore_glob: list[str] = []

# Skip all smoke tests unless explicitly opted in
if not os.environ.get("SMOKE_TESTS"):
    collect_ignore_glob.append("test_*.py")
