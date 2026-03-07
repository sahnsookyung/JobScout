FROM python:3.13-slim

WORKDIR /app

# Copy dependency files first for better layer caching
COPY pyproject.toml uv.lock ./

RUN pip install --no-cache-dir uv==0.10.8 && \
    uv sync --frozen --no-dev --group web

# Copy application code after dependencies are installed
COPY . /app

RUN adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser /app

USER appuser

EXPOSE 8084

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8084/health')" || exit 1

CMD ["uv", "run", "uvicorn", "services.orchestrator.main:app", "--host", "0.0.0.0", "--port", "8084"]
