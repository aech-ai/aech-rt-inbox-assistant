# Aech RT Inbox Assistant

A real-time email management capability for Agent Aech. This service runs as a background process that polls, organizes, and extracts intelligence from delegated email inboxes.

## Architecture

The system has two components:

1. **RT Service** (`src/main.py`) - Background service that polls M365, runs AI categorization, and emits triggers
2. **CLI** (`aech-cli-inbox`) - Public interface for querying state, installed in Agent Aech's worker environment

```
┌─────────────────────────────────────────────────────────────────┐
│                     SERVICE MAIN LOOP                           │
│                   (every POLL_INTERVAL sec)                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  1. poller.poll_inbox()                                         │
│     - Fetch unread emails from M365 Graph API                   │
│     - Upsert into `emails` table                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. organizer.organize_emails()                                 │
│     ┌──────────────────────────────────────────────────────┐    │
│     │  For each email WHERE processed_at IS NULL:          │    │
│     │    - Run AI categorization                           │    │
│     │    - Execute action (move/delete)                    │    │
│     │    - Persist labels, reply_tracking                  │    │
│     │    - Write triggers: urgent_email, reply_needed,     │    │
│     │      availability_requested                          │    │
│     │    - Mark processed_at = NOW                         │    │
│     └──────────────────────────────────────────────────────┘    │
│                              │                                  │
│                              ▼                                  │
│     ┌──────────────────────────────────────────────────────┐    │
│     │  _emit_followup_triggers()                           │    │
│     │    - Query reply_tracking for items > N days old     │    │
│     │    - Write no_reply_after_n_days triggers            │    │
│     └──────────────────────────────────────────────────────┘    │
│                              │                                  │
│                              ▼                                  │
│     ┌──────────────────────────────────────────────────────┐    │
│     │  _emit_weekly_digest_trigger()                       │    │
│     │    - Check if current time is in digest window       │    │
│     │    - If yes & not already sent this week → trigger   │    │
│     └──────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. sleep(POLL_INTERVAL)  →  loop back to step 1                │
└─────────────────────────────────────────────────────────────────┘
```

## Proactive Triggers

Triggers are written to `/triggers/outbox/*.json` for the Agent Aech worker to consume:

| Trigger | When | Purpose |
|---------|------|---------|
| `urgent_email` | Immediate | Email categorized as Urgent or marked important |
| `reply_needed` | Immediate | AI detected you need to respond |
| `availability_requested` | Immediate | Someone asking to schedule a meeting |
| `no_reply_after_n_days` | Deferred | N days passed with no reply (default: 2 days) |
| `weekly_digest_ready` | Scheduled | Weekly summary (configurable day/time) |

## Prerequisites

1. **aech-cli-msgraph**: Must be installed and available in PATH (version 0.1.22+)
2. **Delegate Access**: The Agent's M365 account must have delegate access to the target mailbox

## Running the Service

```bash
# Via Docker Compose
docker compose up -d

# Or directly
DELEGATED_USER=user@example.com python -m src.main
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DELEGATED_USER` | (required) | Email of the mailbox to manage |
| `POLL_INTERVAL` | `5` | Seconds between poll cycles |
| `MODEL_NAME` | `openai-responses:gpt-5-mini` | AI model for categorization |
| `FOLDER_PREFIX` | `aa_` | Prefix for managed folders |
| `CLEANUP_STRATEGY` | `medium` | `low`, `medium`, or `aggressive` |
| `FOLLOWUP_N_DAYS` | `2` | Days before follow-up reminder |

## CLI Usage (aech-cli-inbox)

The CLI is the public interface for querying inbox state:

```bash
# List recent emails
aech-cli-inbox list --limit 20

# Search emails (FTS, vector, or hybrid)
aech-cli-inbox search "contract renewal" --mode hybrid

# Check sync status
aech-cli-inbox sync-status

# View corpus statistics
aech-cli-inbox stats

# View emails needing reply
aech-cli-inbox reply-needed

# View/set preferences
aech-cli-inbox prefs show
aech-cli-inbox prefs set vip_senders '["ceo@company.com"]'
aech-cli-inbox prefs set followup_n_days 3
```

## Data Storage

All state is stored in `~/.inbox-assistant/` (per capability convention):

```
~/.inbox-assistant/
├── inbox.sqlite      # Main database (emails, labels, triage_log, etc.)
├── queries/          # SQL query templates
└── preferences.json  # User preferences (optional)
```

## Standard Folders

The service manages emails into these prefixed folders (e.g., `aa_Work`):

- Work, Personal, Travel, Finance
- Newsletters, Social, Cold Outreach
- Urgent, Action Required, FYI, Should Delete
