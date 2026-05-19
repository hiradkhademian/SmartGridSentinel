FROM python:3.11-slim

WORKDIR /app

# Install essential system dependencies for building extensions if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /lib/apt/lists/*

# Copy and install shared Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service-specific source code files and protobuf definitions
COPY . .

# Environment flag to force unbuffered logging output inside Docker logs
ENV PYTHONUNBUFFERED=1

# The execution command is overridden individually inside the docker-compose file
CMD ["python", "main.py"]