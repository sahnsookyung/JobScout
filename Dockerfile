FROM python:3.14-slim-bookworm
RUN pip install uv

WORKDIR /app

# Copy the project files first for better caching
COPY pyproject.toml uv.lock README.md /app/

# Create venv and install dependencies with explicit reinstall
RUN uv sync --reinstall

# Copy the rest of the application
COPY . /app/

# Ensure project is installed
RUN uv sync --reinstall

# Create non-root user for security (S6471)
RUN useradd --create-home --shell /bin/bash appuser && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app

# Default command
CMD ["python", "main.py"]
