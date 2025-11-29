FROM python:3.11-slim

WORKDIR /app

# System deps: pandoc is required for content conversion (aech-cli-msgraph).
RUN apt-get update && \
    apt-get install -y --no-install-recommends pandoc sqlite3 libsqlite3-dev build-essential && \
    rm -rf /var/lib/apt/lists/*

# Base Python deps for this project
COPY aech-rt-inbox-assistant/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install aech-cli-msgraph (required for all Graph interactions)
COPY aech-cli-msgraph/dist/aech_cli_msgraph-0.1.22-py3-none-any.whl /tmp/aech_cli_msgraph-0.1.22-py3-none-any.whl
RUN pip install uv && uv pip install --system /tmp/aech_cli_msgraph-0.1.22-py3-none-any.whl

# App source
COPY aech-rt-inbox-assistant/src/ src/
COPY aech-rt-inbox-assistant/cli/ cli/
COPY aech-rt-inbox-assistant/manifest.json manifest.json

# Create non-root user
RUN useradd -m -s /bin/bash agentaech && \
    mkdir -p /home/agentaech/inbox-assistant && \
    chown -R agentaech:agentaech /home/agentaech /app && \
    mkdir -p /data/users

USER agentaech

# Set python path and start the service
ENV PYTHONPATH=/app
CMD ["python", "-m", "src.main"]
