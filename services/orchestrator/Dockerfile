FROM python:3.13-slim

WORKDIR /app

COPY . /app

RUN pip install uv && \
    uv sync --frozen --no-dev

EXPOSE 8084

CMD ["python", "-m", "uvicorn", "services.orchestrator.main:app", "--host", "0.0.0.0", "--port", "8084"]
