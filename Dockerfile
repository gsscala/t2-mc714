# Dockerfile for distributed ledger nodes
FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Copy application source code
COPY distrib_ledger/ /app/distrib_ledger/
COPY scripts/ /app/scripts/

# Create persistent storage directory for WAL logs
RUN mkdir -p /app/wal_data

# Entrypoint runs the CLI node module
ENTRYPOINT ["python3", "-m", "distrib_ledger.cli"]
