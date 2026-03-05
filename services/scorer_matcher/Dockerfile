FROM python:3.13-slim

WORKDIR /app

RUN useradd --create-home --shell /bin/bash appuser

COPY pyproject.toml uv.lock ./

RUN pip install --no-cache-dir uv==0.10.8 && \
    uv sync --no-dev --group web

COPY core/ ./core/
COPY database/ ./database/
COPY etl/ ./etl/
COPY notification/ ./notification/
COPY web/backend/models/ ./web/backend/models/
COPY pipeline/ ./pipeline/
COPY services/scorer_matcher/ ./services/scorer_matcher/
COPY config.yaml ./

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 8083

CMD ["uv", "run", "uvicorn", "services.scorer_matcher.main:app", "--host", "0.0.0.0", "--port", "8083"]
