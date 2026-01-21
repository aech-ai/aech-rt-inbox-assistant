# MacOS
# FROM python:3.11-slim
# DGX Spark
FROM nvcr.io/nvidia/pytorch:25.11-py3

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

# Install Python deps as root (--find-links looks in /tmp/wheels/ first, then PyPI)
COPY aech-rt-inbox-assistant/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --find-links /tmp/wheels/

# Create non-root user (align with aech-main UID/GID 1000)
RUN groupadd -r agentaech -g 1000 && \
    useradd -r -g agentaech -u 1000 -m -d /home/agentaech -s /bin/bash agentaech && \
    mkdir -p /home/agentaech/.inbox-assistant /data/users && \
    chown -R agentaech:agentaech /home/agentaech /app

# Switch to agentaech user for application code
USER agentaech

# Add user's local bin to PATH for CLI tools
ENV PATH="/home/agentaech/.local/bin:${PATH}"

# App source
COPY --chown=agentaech:agentaech aech-rt-inbox-assistant/src/ src/
COPY --chown=agentaech:agentaech aech-rt-inbox-assistant/scripts/ scripts/

# Install CLI packages
COPY --chown=agentaech:agentaech aech-rt-inbox-assistant/packages/aech-cli-inbox-assistant/ packages/aech-cli-inbox-assistant/
COPY --chown=agentaech:agentaech aech-rt-inbox-assistant/packages/aech-cli-inbox-assistant-mgmt/ packages/aech-cli-inbox-assistant-mgmt/
RUN pip install --no-cache-dir --user packages/aech-cli-inbox-assistant/ packages/aech-cli-inbox-assistant-mgmt/

# Install the main package
COPY --chown=agentaech:agentaech aech-rt-inbox-assistant/pyproject.toml .
RUN pip install --no-cache-dir --user -e .

# Set python path and start the service
ENV PYTHONPATH=/app
CMD ["python", "-m", "src.main"]
