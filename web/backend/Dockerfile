# syntax=docker/dockerfile:1
FROM python:3.14-slim AS builder

# Copy uv directly from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Compile bytecode for faster startup
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Install dependencies first for layer caching
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project --group web

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
COPY config.yaml ./

RUN uv sync --frozen --no-dev --group web

# ---------------------------------------------------------
# Runtime Stage
FROM python:3.14-slim AS runtime

# Create non-root user
RUN groupadd -g 1001 appgroup && \
    useradd -u 1001 -g appgroup -m -d /app -s /bin/false appuser

WORKDIR /app

# Copy the environment and code from builder with proper permissions
COPY --from=builder --chown=appuser:appgroup --chmod=550 /app /app

# Put the virtual environment on the PATH
ENV PATH="/app/.venv/bin:$PATH"

USER appuser
EXPOSE 8080

# Run uvicorn directly from the virtual environment
CMD ["uvicorn", "web.backend.app:app", "--host", "0.0.0.0", "--port", "8080"]
