FROM python:3.11-slim

WORKDIR /app

# Install essential system dependencies for building extensions if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    netcat-openbsd \
    procps \
    && rm -rf /lib/apt/lists/*

# Copy and install shared Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service-specific source code files and protobuf definitions
COPY . .

# Environment flag to force unbuffered logging output inside Docker logs
ENV PYTHONUNBUFFERED=1

# Default execution
CMD ["python", "main.py"]

# Basic healthcheck: ensure process is running
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ps aux | grep -v grep | grep main.py || exit 1