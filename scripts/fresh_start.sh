#!/bin/bash
# Fresh start - nuke everything and rebuild from scratch
# Usage: docker compose run --rm pipeline
#    or: DELEGATED_USER=user@example.com ./scripts/fresh_start.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Progress spinner
spinner() {
    local pid=$1
    local delay=0.1
    local spinstr='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
    while [ "$(ps a | awk '{print $1}' | grep $pid)" ]; do
        local temp=${spinstr#?}
        printf " ${CYAN}%c${NC}  " "$spinstr"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
        printf "\b\b\b\b"
    done
    printf "    \b\b\b\b"
}

# Print step header
step_header() {
    local step_num=$1
    local step_name=$2
    echo ""
    echo -e "${BOLD}${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}${BLUE}  Step ${step_num}: ${step_name}${NC}"
    echo -e "${BOLD}${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Print success
step_done() {
    echo -e "${GREEN}✓ Done${NC}"
}

# Print activity
activity() {
    echo -e "${CYAN}→${NC} $1"
}

echo ""
echo -e "${BOLD}${GREEN}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${GREEN}║     FRESH START - Full Pipeline from Scratch         ║${NC}"
echo -e "${BOLD}${GREEN}╚══════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BOLD}User:${NC}     ${DELEGATED_USER:-unknown}"

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

echo -e "${BOLD}Database:${NC} $DB_PATH"
echo ""

# Step 0: Delete existing database
step_header "0" "Deleting existing database"
if [ -f "$DB_PATH" ]; then
    activity "Removing database and WAL files..."
    rm -f "$DB_PATH"
    rm -f "${DB_PATH}-wal" 2>/dev/null || true
    rm -f "${DB_PATH}-shm" 2>/dev/null || true
    echo -e "  ${YELLOW}Deleted:${NC} $DB_PATH"
else
    echo -e "  ${YELLOW}No existing database found${NC}"
fi
step_done

# Step 1: Initialize fresh database
step_header "1" "Initialize Database"
activity "Creating schema..."
aech-cli-inbox-assistant list --limit 1 >/dev/null 2>&1 || true
step_done

# Step 2: Full sync with bodies
step_header "2" "Sync All Emails (with bodies)"
activity "Fetching emails from Microsoft Graph API..."
echo ""
aech-cli-inbox-assistant sync --human
echo ""
step_done

# Step 3: Backfill bodies to markdown (if any missed during sync)
step_header "3" "Backfill Bodies to Markdown"
activity "Converting HTML bodies to markdown..."
aech-cli-inbox-assistant backfill-bodies --limit 5000 --human
step_done

# Step 4: Extract attachments
step_header "4" "Extract Attachments"
activity "Downloading and extracting attachment content..."
echo ""
aech-cli-inbox-assistant extract-attachments --limit 5000 --human
echo ""
step_done

# Step 5: LLM content extraction (thread summaries)
step_header "5" "LLM Extraction"
activity "Extracting thread summaries, signatures, cleanup actions..."
echo ""
aech-cli-inbox-assistant extract-content --limit 2000 --human
echo ""
step_done

# Step 6: Index for search
step_header "6" "Create Search Index"
activity "Building FTS index and chunks..."
echo ""
aech-cli-inbox-assistant index --limit 10000 --human
echo ""
step_done

# Step 7: Generate embeddings
step_header "7" "Generate Embeddings"
activity "Computing vector embeddings (bge-m3)..."
echo ""
aech-cli-inbox-assistant embed --limit 10000 --human
echo ""
step_done

# Final stats
echo ""
echo -e "${BOLD}${GREEN}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${GREEN}║                    COMPLETE                          ║${NC}"
echo -e "${BOLD}${GREEN}╚══════════════════════════════════════════════════════╝${NC}"
echo ""
aech-cli-inbox-assistant stats --human
echo ""
echo -e "${GREEN}${BOLD}✓ Fresh start complete!${NC}"
echo ""
