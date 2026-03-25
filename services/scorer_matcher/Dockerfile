# syntax=docker/dockerfile:1

FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Install dependencies first for layer caching
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project --group web

# Copy application source
COPY core/ ./core/
COPY database/ ./database/
COPY etl/ ./etl/
COPY notification/ ./notification/
COPY web/backend/models/ ./web/backend/models/
COPY pipeline/ ./pipeline/
COPY services/scorer_matcher/ ./services/scorer_matcher/
COPY config.yaml ./

RUN uv sync --frozen --no-dev --group web

# ---------------------------------------------------------
# Runtime Stage
FROM python:3.14-slim AS runtime

RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

COPY --from=builder --chown=appuser:appuser /app /app

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

USER appuser
EXPOSE 8083

CMD ["uvicorn", "services.scorer_matcher.main:app", "--host", "0.0.0.0", "--port", "8083"]
