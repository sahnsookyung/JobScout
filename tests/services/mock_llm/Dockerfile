# syntax=docker/dockerfile:1

FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project --group web

COPY tests/__init__.py ./tests/__init__.py
COPY tests/mocks/ ./tests/mocks/
COPY tests/services/ ./tests/services/

RUN uv sync --frozen --no-dev --group web

FROM python:3.14-slim AS runtime

RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

COPY --from=builder --chown=root:root /app /app

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

USER appuser
EXPOSE 8090

CMD ["uvicorn", "tests.services.mock_llm.main:app", "--host", "0.0.0.0", "--port", "8090"]
