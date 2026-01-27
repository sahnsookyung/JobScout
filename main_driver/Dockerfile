FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (needed for some python packages or tools)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create package structure so 'import main_driver.xxx' works
WORKDIR /app
RUN mkdir -p main_driver
COPY . main_driver/

# Add /app to PYTHONPATH so main_driver package is found
ENV PYTHONPATH=/app

# Default command
CMD ["python", "-m", "main_driver.main"]
