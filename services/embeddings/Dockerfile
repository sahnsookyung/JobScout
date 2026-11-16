# syntax=docker/dockerfile:1

FROM python@sha256:7a500125bc50693f2214e842a621440a1b1b9cbb2188f74ab045d29ed2ea5856 AS builder

COPY --from=ghcr.io/astral-sh/uv@sha256:af22a52f5eafef3d78e551599d405c02df61919d18028f0cea8eda93bc9986d0 /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Install dependencies first for layer caching
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-build --no-dev --no-install-project --group web

# Copy application source
COPY core/ ./core/
COPY database/ ./database/
COPY migrations/ ./migrations/
COPY etl/ ./etl/
COPY notification/ ./notification/
COPY web/backend/models/ ./web/backend/models/
COPY services/base/ ./services/base/
COPY services/embeddings/ ./services/embeddings/
COPY config.yaml ./

RUN uv sync --frozen --no-build --no-dev --no-install-project --group web

# ---------------------------------------------------------
# Runtime Stage
FROM python@sha256:7a500125bc50693f2214e842a621440a1b1b9cbb2188f74ab045d29ed2ea5856 AS runtime

RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

COPY --from=builder --chown=root:root /app /app

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

USER appuser
EXPOSE 8082

CMD ["uvicorn", "services.embeddings.main:app", "--host", "0.0.0.0", "--port", "8082"]
