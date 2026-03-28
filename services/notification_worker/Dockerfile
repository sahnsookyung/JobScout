# syntax=docker/dockerfile:1

FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY core/ ./core/
COPY database/ ./database/
COPY migrations/ ./migrations/
COPY notification/ ./notification/
COPY config.yaml ./

RUN uv sync --frozen --no-dev

FROM python:3.14-slim AS runtime

RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

COPY --from=builder --chown=root:root /app /app

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

USER appuser

CMD ["python", "-m", "notification.worker"]
