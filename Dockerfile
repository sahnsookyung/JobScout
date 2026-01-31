FROM python:3.12-slim-bookworm
RUN pip install uv

WORKDIR /app

# Copy the project description first for better caching
COPY pyproject.toml uv.lock* /app/

# Install the project's dependencies
# --frozen: install from lockfile (if available)
# --no-install-project: only install dependencies, not the project itself yet (caching)
RUN uv sync --frozen --no-install-project

# Copy the rest of the application
COPY . /app/

# Install the project itself
RUN uv sync --frozen

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Default command
CMD ["python", "main.py"]
