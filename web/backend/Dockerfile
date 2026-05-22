# syntax=docker/dockerfile:1
FROM python@sha256:7a500125bc50693f2214e842a621440a1b1b9cbb2188f74ab045d29ed2ea5856 AS builder

# Copy uv directly from the official image
COPY --from=ghcr.io/astral-sh/uv@sha256:c4f5de312ee66d46810635ffc5df34a1973ba753e7241ce3a08ef979ddd7bea5 /uv /uvx /bin/

WORKDIR /app

# Compile bytecode for faster startup
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Install dependencies first for layer caching
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-build --no-dev --no-install-project --group web

# Copy source code and install the project
# Core utilities: config loading, matcher explainability, app context
COPY core/ ./core/
# Database: models, repositories, session management
COPY database/ ./database/
COPY migrations/ ./migrations/
# ETL: ResumeParser for resume upload endpoint
COPY etl/ ./etl/
# Notification service for async notifications
COPY notification/ ./notification/
# Shared constants and utilities
COPY web/shared/ ./web/shared/
# Web backend application
COPY web/backend/ ./web/backend/
# Services: base utilities for compact mode (extraction and embeddings runners)
COPY services/base/ ./services/base/
COPY services/scorer_matcher/ ./services/scorer_matcher/
COPY config.yaml ./

RUN uv sync --frozen --no-build --no-dev --no-install-project --group web

# ---------------------------------------------------------
# Runtime Stage
FROM python@sha256:7a500125bc50693f2214e842a621440a1b1b9cbb2188f74ab045d29ed2ea5856 AS runtime

# Create non-root user
RUN groupadd -g 1000 appgroup && \
    useradd -u 1001 -g appgroup -m -d /app -s /bin/false appuser

WORKDIR /app

# Copy the environment and code from builder with proper permissions
COPY --from=builder --chown=appuser:appgroup --chmod=550 /app /app

# Put the virtual environment on the PATH
ENV PATH="/app/.venv/bin:$PATH"

USER appuser
EXPOSE 8080

# Run uvicorn directly from the virtual environment
CMD ["sh", "-c", "umask 0002 && exec uvicorn web.backend.app:app --host 0.0.0.0 --port 8080"]
