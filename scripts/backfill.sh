#!/bin/bash
# Turnkey backfill script - run inside Docker container
# Usage: docker compose exec inbox-assistant /app/scripts/backfill.sh
#    or: docker compose run --rm inbox-assistant /app/scripts/backfill.sh

set -e

# Defaults
BODY_LIMIT=${BODY_LIMIT:-2000}
ATTACHMENT_LIMIT=${ATTACHMENT_LIMIT:-1000}
EXTRACT_LIMIT=${EXTRACT_LIMIT:-500}
INDEX_LIMIT=${INDEX_LIMIT:-2000}
SYNC_SINCE=${SYNC_SINCE:-}

echo "=============================================="
echo "  Inbox Assistant Backfill"
echo "=============================================="
echo ""
echo "User: ${DELEGATED_USER:-unknown}"
if [ -n "$SYNC_SINCE" ]; then
    echo "Since: $SYNC_SINCE"
fi
echo "Body limit: $BODY_LIMIT"
echo "Attachment limit: $ATTACHMENT_LIMIT"
echo "Extract limit: $EXTRACT_LIMIT"
echo "Index limit: $INDEX_LIMIT"
echo ""

# Check we're in the right environment
if [ ! -f "/home/agentaech/.inbox-assistant/assistant.sqlite" ]; then
    echo "ERROR: Database not found at /home/agentaech/.inbox-assistant/assistant.sqlite"
    echo "Make sure you're running inside Docker with volumes mounted."
    exit 1
fi

# Step 1: Sync with Microsoft Graph (handles deletions)
echo "=============================================="
echo "Step 1/6: Syncing with Microsoft Graph"
echo "=============================================="
# Skip fetching bodies during sync; backfill step will fetch only missing ones
if [ -n "$SYNC_SINCE" ]; then
    aech-cli-inbox-assistant sync --human --no-bodies --since "$SYNC_SINCE"
else
    aech-cli-inbox-assistant sync --human --no-bodies
fi
echo ""

# Step 2: Backfill email bodies
echo "=============================================="
echo "Step 2/6: Backfilling email bodies"
echo "=============================================="
aech-cli-inbox-assistant backfill-bodies --limit "$BODY_LIMIT" --human
echo ""

# Step 3: Extract attachment text
echo "=============================================="
echo "Step 3/6: Extracting attachment text"
echo "=============================================="
aech-cli-inbox-assistant extract-attachments --limit "$ATTACHMENT_LIMIT" --human
echo ""

# Step 4: Extract email content (LLM-based quote removal)
echo "=============================================="
echo "Step 4/6: Extracting email content"
echo "=============================================="
aech-cli-inbox-assistant extract-content --limit "$EXTRACT_LIMIT" --human
echo ""

# Step 5: Index for search
echo "=============================================="
echo "Step 5/6: Indexing content for search"
echo "=============================================="
aech-cli-inbox-assistant index --limit "$INDEX_LIMIT" --human
echo ""

# Step 6: Generate embeddings for vector search
echo "=============================================="
echo "Step 6/6: Generating embeddings"
echo "=============================================="
aech-cli-inbox-assistant embed --limit "$INDEX_LIMIT" --human
echo ""

# Show final stats
echo "=============================================="
echo "Backfill Complete - Final Stats"
echo "=============================================="
aech-cli-inbox-assistant stats --human

echo ""
echo "Done! Search should now work for email bodies and attachments."
