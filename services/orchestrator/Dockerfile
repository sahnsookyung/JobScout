# syntax=docker/dockerfile:1

FROM python@sha256:7a500125bc50693f2214e842a621440a1b1b9cbb2188f74ab045d29ed2ea5856 AS builder

COPY --from=ghcr.io/astral-sh/uv@sha256:c4f5de312ee66d46810635ffc5df34a1973ba753e7241ce3a08ef979ddd7bea5 /uv /uvx /bin/

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
COPY web/backend/models/ ./web/backend/models/
COPY services/base/ ./services/base/
COPY services/orchestrator/ ./services/orchestrator/
COPY config.yaml ./

RUN uv sync --frozen --no-dev --group web

# ---------------------------------------------------------
# Runtime Stage
FROM python@sha256:7a500125bc50693f2214e842a621440a1b1b9cbb2188f74ab045d29ed2ea5856 AS runtime

RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

COPY --from=builder --chown=root:root /app /app

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

USER appuser
EXPOSE 8084

CMD ["sh", "-c", "umask 0002 && exec uvicorn services.orchestrator.main:app --host 0.0.0.0 --port 8084"]
