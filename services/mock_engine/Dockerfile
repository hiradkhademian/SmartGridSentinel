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

# Use simulator.py as the default entrypoint for the mock engine
CMD ["python", "simulator.py"]

# Basic healthcheck: ensure gRPC control port is listening
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD nc -z localhost 50052 || exit 1