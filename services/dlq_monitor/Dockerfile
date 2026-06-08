FROM python:3.11-slim

WORKDIR /app

# Install essential system dependencies for building extensions if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    netcat-openbsd \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Assuming requirements.txt contains: aiokafka, sqlalchemy, asyncpg
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]

# Basic healthcheck: ensure process is running
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ps aux | grep -v grep | grep main.py || exit 1