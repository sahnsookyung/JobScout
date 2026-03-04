FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY core/ ./core/
COPY database/ ./database/
COPY etl/ ./etl/
COPY notification/ ./notification/
COPY web/backend/models/ ./web/backend/models/
COPY pipeline/ ./pipeline/
COPY services/scorer_matcher/ ./services/scorer_matcher/
COPY config.yaml ./

RUN pip install uv && \
    uv sync --no-dev --group web

EXPOSE 8083

CMD ["uv", "run", "uvicorn", "services.scorer_matcher.main:app", "--host", "0.0.0.0", "--port", "8083"]
