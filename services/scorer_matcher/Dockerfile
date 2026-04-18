# syntax=docker/dockerfile:1

FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies first for layer caching
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project --group web --group fit-models

# Copy application source
COPY core/ ./core/
COPY database/ ./database/
COPY migrations/ ./migrations/
COPY etl/ ./etl/
COPY notification/ ./notification/
COPY web/backend/models/ ./web/backend/models/
COPY services/scorer_matcher/ ./services/scorer_matcher/
COPY config.yaml ./

RUN uv sync --frozen --no-dev --group web --group fit-models

# ---------------------------------------------------------
# Runtime Stage
FROM python:3.14-slim AS runtime

RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

COPY --from=builder --chown=root:root /app /app

# Hugging Face cache lives on a named volume (scorer_models) so the ~2 GB
# reranker weights persist across restarts and are pre-populated by the
# scorer-model-bootstrap init service. HF_TOKEN is only needed by the
# bootstrap step, not at runtime.
ENV HF_HOME=/home/appuser/.cache/huggingface
ENV SENTENCE_TRANSFORMERS_HOME=/home/appuser/.cache/huggingface
RUN mkdir -p /home/appuser/.cache/huggingface && \
    chown -R appuser:appuser /home/appuser/.cache

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

USER appuser
EXPOSE 8083

CMD ["uvicorn", "services.scorer_matcher.main:app", "--host", "0.0.0.0", "--port", "8083"]
