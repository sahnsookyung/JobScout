FROM python:3.12-slim-bookworm

# Install uv
RUN pip install uv

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies using locked versions with increased timeout
ENV UV_HTTP_TIMEOUT=300
ENV UV_SYSTEM_PYTHON=1
RUN uv pip install --system -r pyproject.toml

# Copy application code
COPY main.py ./

# Expose port
EXPOSE 8001

# Run the application
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
