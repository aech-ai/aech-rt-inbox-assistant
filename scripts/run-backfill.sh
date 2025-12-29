#!/bin/bash
set -e

export AZURE_CLIENT_ID=62bdac02-7d0f-4d84-ab5e-9f8b4aeb62c2
export AZURE_TENANT_ID=a5d9ac71-774b-4adf-bd65-7650fb6ee2b4
export DELEGATED_USER=steven@aech.ai
export AECH_USER_DIR=../aech-main/data/users/steven@aech.ai
export RT_OUTBOX_DIR=../aech-main/data/users/steven@aech.ai/.inbox-assistant/triggers/outbox
export RT_DEDUPE_DIR=../aech-main/data/users/steven@aech.ai/.inbox-assistant/triggers/dedupe

DB_PATH="$AECH_USER_DIR/.inbox-assistant/inbox.sqlite"

echo "=== Inbox Assistant Backfill ==="

# Step 1: Reset processed_at to reprocess all emails
echo "Resetting processed_at on all emails..."
sqlite3 "$DB_PATH" "UPDATE emails SET processed_at = NULL, outlook_categories = NULL, urgency = NULL" 2>/dev/null || true

# Step 2: Sync all emails from Inbox (with pagination)
echo "Syncing all emails from Outlook Inbox..."
python3 -c "
import os
os.environ['DELEGATED_USER'] = 'steven@aech.ai'
os.environ['AECH_USER_DIR'] = '../aech-main/data/users/steven@aech.ai'

from src.database import init_db
from src.poller import GraphPoller

init_db()
poller = GraphPoller()

folders = poller.get_all_folders()
inbox = next((f for f in folders if f.get('displayName', '').lower() == 'inbox'), None)
if inbox:
    print(f'Syncing Inbox: {inbox[\"id\"]}')
    count = poller.full_sync_folder(inbox['id'], 'Inbox', fetch_body=False)
    print(f'Synced {count} emails')
else:
    print('ERROR: Inbox not found')
    exit(1)
"

# Step 3: Show count
total=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM emails")
unprocessed=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM emails WHERE processed_at IS NULL")
echo "Total: $total, Unprocessed: $unprocessed"

# Step 4: Process in batches
echo "Processing emails..."
while true; do
    python -m src.main --once --concurrency 10 --backfill

    remaining=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM emails WHERE processed_at IS NULL")
    echo "Remaining: $remaining"

    if [ "$remaining" = "0" ]; then
        break
    fi
done

echo "=== Done ==="
