# syntax=docker/dockerfile:1

FROM python:3.14-slim@sha256:7a500125bc50693f2214e842a621440a1b1b9cbb2188f74ab045d29ed2ea5856 AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11.2@sha256:c4f5de312ee66d46810635ffc5df34a1973ba753e7241ce3a08ef979ddd7bea5 /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Install dependencies first for layer caching
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project --group web

# Copy application source
COPY core/ ./core/
COPY database/ ./database/
COPY migrations/ ./migrations/
COPY etl/ ./etl/
COPY notification/ ./notification/
COPY services/base/ ./services/base/
COPY services/extraction/ ./services/extraction/
COPY services/embeddings/ ./services/embeddings/
COPY services/scorer_matcher/ ./services/scorer_matcher/
COPY services/orchestrator/ ./services/orchestrator/
COPY web/backend/ ./web/backend/
COPY config.yaml ./

RUN uv sync --frozen --no-dev --group web

# ---------------------------------------------------------
# Runtime Stage
FROM python:3.14-slim@sha256:7a500125bc50693f2214e842a621440a1b1b9cbb2188f74ab045d29ed2ea5856 AS runtime

RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

COPY --from=builder --chown=appuser:appuser /app /app

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

USER appuser

CMD ["python", "-c", "raise SystemExit('Use a service-specific Dockerfile or ./scripts/setup_local_env/start.sh; the root monolithic image is no longer runnable.')"]
