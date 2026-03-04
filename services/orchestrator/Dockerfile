FROM python:3.13-slim

WORKDIR /app

# Copy dependency files first for better layer caching
COPY pyproject.toml uv.lock* ./

RUN pip install uv && \
    uv sync --frozen --no-dev

# Copy application code after dependencies are installed
COPY . /app

EXPOSE 8084

CMD ["uv", "run", "uvicorn", "services.orchestrator.main:app", "--host", "0.0.0.0", "--port", "8084"]
