FROM python:3.11-slim

WORKDIR /app

# System deps: match aech-main worker for full document processing support
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        pandoc \
        sqlite3 \
        libsqlite3-dev \
        build-essential \
        poppler-utils \
        libreoffice \
        libreoffice-java-common \
        imagemagick \
        ghostscript \
    && rm -rf /var/lib/apt/lists/*

# Copy local wheels to /tmp/wheels (for --find-links)
COPY aech-cli-msgraph/dist/*.whl /tmp/wheels/
COPY aech-cli-documents/dist/*.whl /tmp/wheels/
COPY aech-main/packages/aech-llm-observability/dist/*.whl /tmp/wheels/

# Install Python deps (--find-links looks in /tmp/wheels/ first, then PyPI)
COPY aech-rt-inbox-assistant/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --find-links /tmp/wheels/

# App source
COPY aech-rt-inbox-assistant/src/ src/
COPY aech-rt-inbox-assistant/manifest.json manifest.json
COPY aech-rt-inbox-assistant/scripts/ scripts/

# Install the CLI package (provides aech-cli-inbox-assistant command)
COPY aech-rt-inbox-assistant/packages/aech-cli-inbox-assistant/ packages/aech-cli-inbox-assistant/
RUN pip install --no-cache-dir packages/aech-cli-inbox-assistant/

# Install the main package
COPY aech-rt-inbox-assistant/pyproject.toml .
RUN pip install --no-cache-dir -e .

# Create non-root user
RUN useradd -m -s /bin/bash agentaech && \
    mkdir -p /home/agentaech/.inbox-assistant && \
    chown -R agentaech:agentaech /home/agentaech /app && \
    mkdir -p /data/users

USER agentaech

# Set python path and start the service
ENV PYTHONPATH=/app
CMD ["python", "-m", "src.main"]
