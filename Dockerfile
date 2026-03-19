FROM python:3.13-slim

WORKDIR /app

# System deps: match aech-main worker for full document processing support
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash \
        pandoc \
        sqlite3 \
        libsqlite3-dev \
        build-essential \
        gcc \
        poppler-utils \
        libreoffice \
        libreoffice-java-common \
        default-jre \
        imagemagick \
        ghostscript \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency wheels from sibling repos (provided via additional_contexts: repos in docker-compose)
COPY --from=aech-main packages/aech-cli-msgraph/dist/*.whl /tmp/wheels/
COPY --from=aech-cli-documents dist/*.whl /tmp/wheels/
COPY --from=aech-main packages/aech-llm-observability/dist/*.whl /tmp/wheels/

# Install Python deps as root (--find-links looks in /tmp/wheels/ first, then PyPI)
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --find-links /tmp/wheels/

# App source
COPY ./src/ src/
COPY ./scripts/ scripts/
COPY ./README.md .

# Install the main package before local CLI wrappers that depend on it
COPY ./pyproject.toml .
RUN pip install --no-cache-dir -e .

# Install CLI packages
COPY ./packages/aech-cli-inbox-assistant/ packages/aech-cli-inbox-assistant/
RUN pip install --no-cache-dir packages/aech-cli-inbox-assistant/

# Create non-root user (align with aech-main UID/GID 1001)
RUN groupadd -r agentaech -g 1001 && \
    useradd -r -g agentaech -u 1001 -m -d /home/agentaech -s /bin/bash agentaech && \
    mkdir -p /home/agentaech/.inbox-assistant /data/users && \
    chown -R agentaech:agentaech /home/agentaech /app

# Set umask for group-writable files
RUN echo "umask 002" >> /home/agentaech/.bashrc
# Set python path and start the service
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/scripts/container_entrypoint.py"]
CMD ["python", "-m", "src.main"]
