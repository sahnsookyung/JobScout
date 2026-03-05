FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY config.yaml ./

RUN pip install uv && \
    uv sync --no-dev --group web

COPY core/ ./core/
COPY database/ ./database/
COPY web/backend/models/ ./web/backend/models/
COPY services/embeddings/ ./services/embeddings/

EXPOSE 8082

CMD ["uv", "run", "uvicorn", "services.embeddings.main:app", "--host", "0.0.0.0", "--port", "8082"]
