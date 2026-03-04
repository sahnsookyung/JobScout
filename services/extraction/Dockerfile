FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY core/ ./core/
COPY database/ ./database/
COPY etl/ ./etl/
COPY web/backend/models/ ./web/backend/models/
COPY pipeline/ ./pipeline/
COPY services/extraction/ ./services/extraction/
COPY config.yaml ./

RUN pip install uv && \
    uv sync --no-dev --group web

EXPOSE 8081

CMD ["uv", "run", "uvicorn", "services.extraction.main:app", "--host", "0.0.0.0", "--port", "8081"]
