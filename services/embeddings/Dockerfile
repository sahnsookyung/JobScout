FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY core/ ./core/
COPY database/ ./database/
COPY web/backend/models/ ./web/backend/models/
COPY config.yaml ./

RUN pip install uv && \
    uv sync --no-dev --group web

EXPOSE 8082

CMD ["python", "-m", "uvicorn", "services.embeddings.main:app", "--host", "0.0.0.0", "--port", "8082"]
