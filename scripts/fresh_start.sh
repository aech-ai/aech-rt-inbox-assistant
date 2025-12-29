#!/bin/bash
# Fresh start - nuke everything and rebuild from scratch
# Usage: docker compose run --rm pipeline
#    or: DELEGATED_USER=user@example.com ./scripts/fresh_start.sh

set -e

echo "=============================================="
echo "  FRESH START - Full Pipeline from Scratch"
echo "=============================================="
echo ""
echo "User: ${DELEGATED_USER:-unknown}"
echo ""

# Determine DB path
if [ -f "/home/agentaech/.inbox-assistant/inbox.sqlite" ]; then
    DB_PATH="/home/agentaech/.inbox-assistant/inbox.sqlite"
elif [ -n "$INBOX_DB_PATH" ]; then
    DB_PATH="$INBOX_DB_PATH"
elif [ -n "$AECH_USER_DIR" ]; then
    DB_PATH="$AECH_USER_DIR/.inbox-assistant/inbox.sqlite"
else
    DB_PATH="./data/users/${DELEGATED_USER}/.inbox-assistant/inbox.sqlite"
fi

echo "Database: $DB_PATH"
echo ""

# Step 0: Delete existing database
echo "=============================================="
echo "Step 0: Deleting existing database"
echo "=============================================="
if [ -f "$DB_PATH" ]; then
    rm -f "$DB_PATH"
    rm -f "${DB_PATH}-wal" 2>/dev/null || true
    rm -f "${DB_PATH}-shm" 2>/dev/null || true
    echo "Deleted: $DB_PATH"
else
    echo "No existing database found"
fi
echo ""

# Step 1: Initialize fresh database
echo "=============================================="
echo "Step 1: Initialize Database"
echo "=============================================="
aech-cli-inbox-assistant list --limit 1 >/dev/null 2>&1 || true
echo "Database initialized"
echo ""

# Step 2: Full sync with bodies
echo "=============================================="
echo "Step 2: Sync All Emails (with bodies)"
echo "=============================================="
aech-cli-inbox-assistant sync --human
echo ""

# Step 3: Convert bodies to markdown (if any missed)
echo "=============================================="
echo "Step 3: Convert Bodies to Markdown"
echo "=============================================="
aech-cli-inbox-assistant convert-bodies
echo ""

# Step 4: Extract attachments
echo "=============================================="
echo "Step 4: Extract Attachments"
echo "=============================================="
aech-cli-inbox-assistant extract-attachments --limit 5000 --human
echo ""

# Step 5: LLM content extraction (thread summaries)
echo "=============================================="
echo "Step 5: LLM Extraction (thread summaries)"
echo "=============================================="
aech-cli-inbox-assistant extract-content --limit 2000 --human
echo ""

# Step 6: Index for search
echo "=============================================="
echo "Step 6: Create Search Index"
echo "=============================================="
aech-cli-inbox-assistant index --limit 10000 --human
echo ""

# Step 7: Generate embeddings
echo "=============================================="
echo "Step 7: Generate Embeddings"
echo "=============================================="
aech-cli-inbox-assistant embed --limit 10000 --human
echo ""

# Final stats
echo "=============================================="
echo "COMPLETE - Final Stats"
echo "=============================================="
aech-cli-inbox-assistant stats --human
echo ""
echo "Fresh start complete!"
