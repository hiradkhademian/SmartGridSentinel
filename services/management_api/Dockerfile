FROM python:3.11-slim

WORKDIR /app

# Install essential system dependencies for building extensions if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    netcat-openbsd \
    procps \
    curl \
    && rm -rf /lib/apt/lists/*

# Copy and install shared Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service-specific source code files and protobuf definitions
COPY . .

# Environment flag to force unbuffered logging output inside Docker logs
ENV PYTHONUNBUFFERED=1

# Run FastAPI via Uvicorn in the container
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

# Healthcheck: call the management API health endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD curl -f http://localhost:8000/healthz || exit 1