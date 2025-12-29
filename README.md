# Aech RT Inbox Assistant

A real-time email management capability for Agent Aech. This service runs as a background process that polls, organizes, and extracts intelligence from delegated email inboxes.

## Architecture

The system has two components:

1. **RT Service** (`src/main.py`) - Background service that polls M365, runs AI categorization, and emits triggers
2. **CLI** (`aech-cli-inbox-assistant`) - Public interface for querying state, installed in Agent Aech's worker environment

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
| `availability_requested_enhanced` | Immediate | Availability request with real calendar data |
| `daily_briefing` | Scheduled | Morning briefing with day's schedule and prep |
| `meeting_prep_ready` | Before meeting | Pre-meeting prep notification |
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

### Docker Tools

One-shot tools are available via docker compose profiles:

```bash
# Test the full pipeline on 10 emails (non-destructive, uses temp DB)
docker compose run --rm test-pipeline

# Full backfill (sync, extract content, chunk, embed)
docker compose run --rm backfill

# Fresh start (delete DB and rebuild from scratch)
docker compose run --rm pipeline

# Interactive CLI shell
docker compose run --rm cli aech-cli-inbox-assistant --help

# GPU-accelerated backfill (for Nvidia hosts)
docker compose run --rm backfill-gpu
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

## CLI Usage (aech-cli-inbox-assistant)

The CLI is the public interface for querying inbox state:

```bash
# List recent emails
aech-cli-inbox-assistant list --limit 20

# Search emails (FTS, vector, or hybrid)
aech-cli-inbox-assistant search "contract renewal" --mode hybrid

# Check sync status
aech-cli-inbox-assistant sync-status

# View corpus statistics
aech-cli-inbox-assistant stats

# View emails needing reply
aech-cli-inbox-assistant reply-needed

# Inbox cleanup (LLM-classified)
aech-cli-inbox-assistant cleanup              # Show summary of delete/archive suggestions
aech-cli-inbox-assistant cleanup delete       # Delete calendar accepts, delivery receipts, etc.
aech-cli-inbox-assistant cleanup archive      # Archive read newsletters, FYI notifications
aech-cli-inbox-assistant cleanup delete --dry-run  # Preview without executing

# View/set preferences
aech-cli-inbox-assistant prefs show
aech-cli-inbox-assistant prefs set vip_senders '["ceo@company.com"]'
aech-cli-inbox-assistant prefs set followup_n_days 3
```

### Inbox Cleanup

The LLM automatically classifies emails for cleanup during processing:

| Action | Email Types |
|--------|-------------|
| `delete` | Calendar accepts/declines/tentative, delivery receipts, read receipts, out-of-office auto-replies, unsubscribe confirmations, expired auth codes |
| `archive` | Read newsletters, FYI-only notifications, automated reports |
| `keep` | Real conversations, actionable items, unexpired auth codes |

## Calendar Integration

Direct Microsoft Graph API integration for calendar operations (no local sync - always real-time):

```bash
# View schedule
aech-cli-inbox-assistant calendar today --human
aech-cli-inbox-assistant calendar upcoming --hours 48
aech-cli-inbox-assistant calendar view --start 2025-01-20 --end 2025-01-27

# Check availability
aech-cli-inbox-assistant calendar free-busy --start 2025-01-20 --end 2025-01-24
aech-cli-inbox-assistant calendar find-times --attendees "jane@example.com" --duration 60

# Create events (no invites sent by default)
aech-cli-inbox-assistant calendar create-event --subject "Team Sync" --start 2025-01-20T14:00:00 --online

# Working hours
aech-cli-inbox-assistant calendar working-hours
```

## Meeting Prep

Executive assistant features for meeting preparation:

```bash
# Daily briefing with schedule overview and alerts
aech-cli-inbox-assistant calendar briefing --human

# Prep for next meeting needing attention
aech-cli-inbox-assistant calendar prep --next --human

# Prep for specific event
aech-cli-inbox-assistant calendar prep --event-id AAMkAG...

# View/configure prep rules
aech-cli-inbox-assistant calendar prep-config --human
```

### Meeting Prep Features

- **Daily Briefing**: Schedule overview, busy/free hours, back-to-back alerts, early meeting warnings
- **Attendee Context**: Cross-references attendees with email corpus (recent emails, last subject)
- **Configurable Rules**: Which meetings get prep based on:
  - External attendees
  - Meeting size (5+ attendees)
  - Keywords in subject (interview, review, board, exec, client, partner)
  - VIP attendee list
  - Sender domains

Configure via `prefs set meeting_prep '{"rules": [...]}'`

## Working Memory (EA Cognitive State)

The EA maintains continuous awareness through a "working memory" system that tracks:

- **Active Threads** - Ongoing conversations and their status
- **Pending Decisions** - Questions awaiting user response
- **Commitments** - Promises the user made to others
- **Contacts** - People and relationship context
- **Projects** - Inferred initiatives from email patterns
- **Observations** - Passive learnings from CC'd emails

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     TWO PARALLEL LOOPS                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  EMAIL LOOP (existing)          MEMORY ENGINE LOOP (new)        │
│  ─────────────────────          ─────────────────────────       │
│  Poll inbox                     Run every N minutes             │
│       ↓                              ↓                          │
│  Categorize email               Re-evaluate urgency             │
│       ↓                              ↓                          │
│  Update working memory          Synthesize insights             │
│  (email-triggered)                   ↓                          │
│       ↓                         Generate nudges                 │
│  Emit triggers                       ↓                          │
│       ↓                         Prune/consolidate               │
│  Sleep → repeat                 Sleep → repeat                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### CC Mode (Passive Learning)

When the user is CC'd on an email (not in TO), the EA:
- Does **NOT** trigger actions or notifications
- **Updates** working memory with learned context
- **Builds** understanding of projects, people, and terminology

### CLI Commands

```bash
# Complete state snapshot
aech-cli-inbox-assistant wm snapshot --human

# Query active threads
aech-cli-inbox-assistant wm threads --needs-reply
aech-cli-inbox-assistant wm threads --urgency today

# Query contacts
aech-cli-inbox-assistant wm contacts --external
aech-cli-inbox-assistant wm contacts --search "acme"

# Pending decisions
aech-cli-inbox-assistant wm decisions

# Open commitments
aech-cli-inbox-assistant wm commitments --overdue

# Passive observations
aech-cli-inbox-assistant wm observations --days 7

# Inferred projects
aech-cli-inbox-assistant wm projects
```

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `WM_ENGINE_INTERVAL` | `300` | Seconds between engine cycles (5 min) |
| `WM_STALE_THRESHOLD_DAYS` | `3` | Days before thread marked stale |
| `WM_URGENCY_ESCALATION_DAYS` | `2` | Days before urgency escalates |
| `WM_OBSERVATION_RETENTION_DAYS` | `30` | Days to retain observations |

### Proactive Nudges

The memory engine generates nudges for:
- Overdue replies (threads awaiting response > N days)
- Overdue commitments (past due date)
- Stale urgent threads (no activity for 24h)
- Pending decisions (waiting > 3 days)

## Content Processing Pipeline

Emails are processed through a multi-stage pipeline:

1. **HTML → Markdown** (`body_parser.py`): Converts HTML email bodies to semantic markdown, stripping CSS, comments, and tracking pixels
2. **LLM Extraction** (`updater.py`): Extracts structured content:
   - `thread_summary`: 1-3 sentence conversation summary
   - `signature_block`: Sender's contact info from signature
   - `extracted_new_content`: Just the new content (no quoted replies)
   - `suggested_action`: Cleanup classification (keep/archive/delete)
3. **Chunking** (`chunker.py`): Splits content into searchable chunks
4. **Embedding** (`embeddings.py`): Generates vector embeddings (bge-m3) for semantic search

### Search Modes

```bash
# Full-text search (FTS5 with BM25 ranking)
aech-cli-inbox-assistant search "contract" --mode fts

# Semantic similarity search (vector embeddings)
aech-cli-inbox-assistant search "legal agreement terms" --mode vector

# Hybrid search (RRF fusion of FTS + vector)
aech-cli-inbox-assistant search "contract renewal" --mode hybrid
```

## Data Storage

All state is stored in `~/.inbox-assistant/` (per capability convention):

```
~/.inbox-assistant/
├── assistant.sqlite  # Main database (emails, labels, triage_log, calendar, working memory, etc.)
├── queries/          # SQL query templates
└── preferences.json  # User preferences (optional)
```

### Data Integrity Controls

The SQLite schema enforces enterprise-grade data integrity:

| Control | Implementation |
|---------|----------------|
| **Referential Integrity** | All child tables use `FOREIGN KEY ... ON DELETE CASCADE` to prevent orphaned records |
| **Cascading Deletes** | Email deletion automatically removes: attachments, chunks, triage_log, labels, reply_tracking, working memory references |
| **CHECK Constraints** | Enum fields validated at DB level: `urgency IN ('immediate', 'today', 'this_week', 'someday')`, `extraction_status IN ('pending', 'extracting', 'completed', 'failed', 'skipped')`, etc. |
| **NOT NULL + Defaults** | Required fields enforced; JSON arrays default to `'[]'` to avoid null-check bugs |
| **Polymorphic FK Cleanup** | Triggers delete orphaned `chunks` when parent `emails` or `attachments` are removed |
| **WAL Mode** | Write-Ahead Logging enabled for concurrent read/write access |
| **Foreign Keys Enabled** | `PRAGMA foreign_keys=ON` enforced at connection time |

**Indexed Fields** (optimized for common queries):

- `emails`: conversation_id, sender, received_at, urgency, processed_at
- `attachments`: email_id, content_hash, extraction_status
- `chunks`: source_type + source_id (composite)
- `work_items`: status, type
- `wm_threads`: status, urgency, needs_reply
- `wm_contacts`: email, relationship
- `wm_observations`: type, observed_at
- `wm_decisions`: is_resolved, urgency
- `wm_commitments`: is_completed, due_by

### Working Memory Tables

| Table | Purpose |
|-------|---------|
| `wm_threads` | Active conversation threads |
| `wm_contacts` | Known contacts with interaction history |
| `wm_decisions` | Pending decisions awaiting response |
| `wm_commitments` | User's commitments to others |
| `wm_observations` | Passive learnings from CC'd emails |
| `wm_projects` | Inferred projects/initiatives |

## Standard Folders

The service manages emails into these prefixed folders (e.g., `aa_Work`):

- Work, Personal, Travel, Finance
- Newsletters, Social, Cold Outreach
- Urgent, Action Required, FYI, Should Delete
