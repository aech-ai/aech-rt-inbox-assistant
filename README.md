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

Triggers are written to `/triggers/outbox/*.json` (configurable via `RT_OUTBOX_DIR`) for the Agent Aech worker to consume. Each trigger includes routing metadata for Teams or webhook delivery.

### Email Classification Triggers

| Trigger | When | Payload |
|---------|------|---------|
| `urgent_email` | Email has `urgency == "immediate"` | subject, sender, message_id, reason |
| `reply_needed` | AI detected `requires_reply == true` | message_id, subject, sender, reason |
| `availability_requested` | Meeting scheduling request detected | time_window, duration, constraints |
| `availability_requested_enhanced` | Availability request + calendar context | above + actual_free_slots, proposed_slots |

### Executive Assistant Triggers

| Trigger | When | Payload |
|---------|------|---------|
| `daily_briefing` | Scheduled morning time | schedule overview, busy/free hours, alerts |
| `meeting_prep_ready` | N minutes before meeting | event details, attendee context |
| `weekly_digest_ready` | Configured day/time (e.g., Friday 8:30am) | week summary, top items, recommendations |
| `no_reply_after_n_days` | N days without reply (default: 2) | thread_id, subject, waiting_since |

### Working Memory Nudges

| Trigger | When | Nudge Type |
|---------|------|------------|
| `working_memory_nudge` | WM engine cycle | `reply_overdue` - User hasn't replied for N days |
| `working_memory_nudge` | WM engine cycle | `urgent_thread_stale` - Urgent thread no activity >24h |
| `working_memory_nudge` | WM engine cycle | `commitment_overdue` - User's promise past due |
| `working_memory_nudge` | WM engine cycle | `decision_pending` - Decision waiting >N days |

### Alert Rule Triggers

| Trigger | When | Payload |
|---------|------|---------|
| `alert_rule_triggered` | User-defined alert rule matches | rule_id, rule_text, event_type, match_reason |

See [Alert Rules](#alert-rules) for configuring custom triggers.

## Prerequisites

1. **aech-cli-msgraph**: Must be installed and available in PATH (version 0.1.22+)
2. **Delegate Access**: The Agent's M365 account must have delegate access to the target mailbox and calendar

## Delegated User Setup

For each user whose mailbox/calendar will be managed by Agent Aech, the following one-time setup steps are required:

### 1. Azure AD App Permissions

The Agent Aech app registration must have these **Delegated** permissions (with admin consent):

| Permission                   | Type      | Purpose                                  |
|------------------------------|-----------|------------------------------------------|
| `Mail.ReadWrite`             | Delegated | Read/write access to delegated mailboxes |
| `Mail.Send`                  | Delegated | Send emails on behalf of users           |
| `Calendars.ReadWrite`        | Delegated | Full access to user calendars            |
| `Calendars.ReadWrite.Shared` | Delegated | Access shared/delegated calendars        |

### 2. Mailbox Delegation (Exchange Admin)

Grant Agent Aech's M365 account (`agent@yourdomain.com`) delegate access to the user's mailbox:

```powershell
# Via Exchange Online PowerShell
Add-MailboxPermission -Identity "user@yourdomain.com" -User "agent@yourdomain.com" -AccessRights FullAccess -InheritanceType All
```

Or via Microsoft 365 Admin Center: **Users** → **Active users** → Select user → **Mail** → **Mailbox permissions** → **Read and manage (Full Access)** → Add agent account.

### 3. Calendar Delegation (User Action Required)

The user must delegate their calendar to Agent Aech. This **cannot** be done programmatically.

**User steps (Outlook Web):**

1. Go to **Calendar** → **Settings** (gear icon) → **Calendar** → **Shared calendars**
2. Under "Share a calendar", select their primary calendar
3. Enter `agent@yourdomain.com`
4. Set permission level to **Delegate** (can view, edit, and delete)
5. Click **Share**

### 4. Accept Calendar Share (Agent Account)

After the user shares their calendar, Agent Aech's account must accept the invitation:

**Agent steps (one-time per user):**

1. Log into Outlook (web or desktop) as `agent@yourdomain.com`
2. Open the calendar sharing invitation email from the user
3. Click **"Accept and add this calendar"**

> ⚠️ **This step cannot be automated** - Microsoft Graph API does not support programmatically accepting calendar share invitations. The invitation must be accepted through an Outlook client.

### 5. Verify Setup

After completing the above steps, verify the setup:

```bash
# Check that the shared calendar appears
docker compose exec inbox-assistant python3 -c "
from aech_cli_msgraph.graph import GraphClient
client = GraphClient()
for cal in client.list_calendars().get('value', []):
    owner = cal.get('owner', {}).get('address', 'self')
    print(f'{cal.get(\"name\")} (owner: {owner})')
"

# Test calendar sync
docker compose exec inbox-assistant python3 -c "
from src.calendar_sync import sync_calendar
result = sync_calendar()
print(f'Synced {result[\"events_synced\"]} events')
"
```

The delegated user's calendar should appear in the list with their email as the owner.

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

#### Core Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DELEGATED_USER` | (required) | Email of the mailbox to manage |
| `AZURE_CLIENT_ID` | (required) | Azure AD app registration client ID |
| `AZURE_TENANT_ID` | (required) | Azure AD tenant ID |

#### Polling & Sync Intervals

| Variable | Default | Description |
|----------|---------|-------------|
| `POLL_INTERVAL` | `60` | Seconds between main poll cycles |
| `DELTA_SYNC_INTERVAL` | `300` | Seconds between inbox delta syncs (detect deletions) |
| `CALENDAR_SYNC_INTERVAL` | `300` | Seconds between calendar syncs |
| `WM_ENGINE_INTERVAL` | `300` | Seconds between working memory engine cycles |
| `SENT_SYNC_INTERVAL` | `300` | Seconds between sent items sync (for alert rules) |

#### AI Model Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MODEL_NAME` | `openai:gpt-5` | Primary LLM for email categorization |
| `CLASSIFICATION_MODEL` | (MODEL_NAME) | Lighter model for classification |
| `WM_MODEL` | (MODEL_NAME) | Working memory analysis model |
| `EMBEDDING_MODEL` | `bge-m3` | Vector embedding model |
| `EMBEDDING_BATCH_SIZE` | `8` | Batch size for embedding generation |
| `OPENAI_API_KEY` | - | OpenAI API key (at least one LLM key required) |
| `ANTHROPIC_API_KEY` | - | Anthropic API key |

#### Working Memory Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `WM_STALE_THRESHOLD_DAYS` | `3` | Days without activity before thread marked stale |
| `WM_REPLY_NUDGE_DAYS` | `3` | Days without reply before nudge trigger |
| `WM_DECISION_NUDGE_DAYS` | `7` | Days without decision before nudge trigger |
| `WM_URGENCY_ESCALATION_DAYS` | `14` | Days before urgency escalates |
| `WM_OBSERVATION_RETENTION_DAYS` | `30` | Days to retain observation facts |

#### Email Management

| Variable | Default | Description |
|----------|---------|-------------|
| `CLEANUP_STRATEGY` | `medium` | `low`, `medium`, or `aggressive` |
| `FOLLOWUP_N_DAYS` | `2` | Days before follow-up reminder |
| `DEFAULT_TIMEZONE` | `UTC` | IANA timezone for scheduling |

#### Weekly Digest

| Variable | Default | Description |
|----------|---------|-------------|
| `ENABLE_WEEKLY_DIGEST` | `false` | Enable weekly digest triggers |
| `DIGEST_DAY` | `friday` | Day for weekly digest |
| `DIGEST_TIME_LOCAL` | `08:30` | Local time for digest (HH:MM) |
| `DIGEST_WINDOW_MINUTES` | `30` | Window size for digest scheduling |

#### RT Trigger Queue

| Variable | Default | Description |
|----------|---------|-------------|
| `RT_OUTBOX_DIR` | `/triggers/outbox` | Directory for trigger JSON files |
| `RT_DEDUPE_DIR` | `/triggers/dedupe` | Deduplication marker directory |
| `RT_DEDUPE_TTL_DAYS` | `7` | Dedupe marker TTL in days |

#### Paths & Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `AECH_HOST_DATA` | - | Host path to aech-main/data (Docker) |
| `AECH_USER_DIR` | `./data/users/<USER>` | User data directory |
| `INBOX_DB_PATH` | `~/.inbox-assistant/assistant.sqlite` | SQLite database path |
| `INBOX_STATE_DIR` | `~/.inbox-assistant` | State directory |
| `LLM_LOG_PATH` | (state_dir/llm.jsonl) | LLM observability log path |

## CLI Usage (aech-cli-inbox-assistant)

The CLI is the read-only interface for querying inbox state. All commands return JSON for agent consumption.

### Email Commands

```bash
# List recent emails
aech-cli-inbox-assistant list --limit 20 --include-read

# Search emails (FTS, vector, or hybrid)
aech-cli-inbox-assistant search "contract renewal" --mode hybrid --limit 10 --facts

# View emails needing reply
aech-cli-inbox-assistant reply-needed --limit 10 --include-stale

# View triage history
aech-cli-inbox-assistant history --limit 50

# Corpus statistics
aech-cli-inbox-assistant stats

# Sync status
aech-cli-inbox-assistant sync-status

# Attachment extraction status
aech-cli-inbox-assistant attachment-status --limit 20 --status pending
```

### Inbox Cleanup

```bash
aech-cli-inbox-assistant cleanup              # Show delete/archive suggestions
aech-cli-inbox-assistant cleanup delete       # Delete calendar accepts, receipts, etc.
aech-cli-inbox-assistant cleanup archive      # Archive read newsletters
aech-cli-inbox-assistant cleanup delete --dry-run  # Preview without executing
```

| Action | Email Types |
|--------|-------------|
| `delete` | Calendar accepts/declines, delivery receipts, read receipts, OOO replies, expired auth codes |
| `archive` | Read newsletters, FYI-only notifications, automated reports |
| `keep` | Real conversations, actionable items, unexpired auth codes |

### Calendar Commands

```bash
# View schedule
aech-cli-inbox-assistant calendar-today
aech-cli-inbox-assistant calendar-week
aech-cli-inbox-assistant calendar-upcoming --hours 48

# Check availability
aech-cli-inbox-assistant calendar-free 2025-01-20
aech-cli-inbox-assistant calendar-busy 2025-01-20T09:00:00 2025-01-20T17:00:00

# Search and query
aech-cli-inbox-assistant calendar-event AAMkAG...
aech-cli-inbox-assistant calendar-search "quarterly review" --limit 10
aech-cli-inbox-assistant calendar-meetings-with jane@example.com --limit 10

# Meeting prep
aech-cli-inbox-assistant calendar-prep --next
aech-cli-inbox-assistant calendar-prep AAMkAG...
```

### Calendar Actions (Queued for RT Execution)

Calendar modifications are queued in the `actions` table and executed by the RT service:

```bash
# Create events
aech-cli-inbox-assistant event-create "Team Sync" 2025-01-20T14:00:00 2025-01-20T15:00:00 \
  --attendees "jane@example.com,bob@example.com" \
  --location "Conference Room A" \
  --body "Weekly team sync meeting" \
  --online

# Update events
aech-cli-inbox-assistant event-update AAMkAG... \
  --subject "Updated: Team Sync" \
  --start 2025-01-20T15:00:00 \
  --end 2025-01-20T16:00:00

# Cancel events
aech-cli-inbox-assistant event-cancel AAMkAG... --notify

# Respond to invites
aech-cli-inbox-assistant event-respond AAMkAG... accept
aech-cli-inbox-assistant event-respond AAMkAG... tentative
aech-cli-inbox-assistant event-respond AAMkAG... decline
```

### Actions Queue

```bash
# View pending actions
aech-cli-inbox-assistant actions-pending

# View action history
aech-cli-inbox-assistant actions-history --limit 20
```

### Preferences

```bash
aech-cli-inbox-assistant prefs show
aech-cli-inbox-assistant prefs keys
aech-cli-inbox-assistant prefs set vip_senders '["ceo@company.com"]'
aech-cli-inbox-assistant prefs set followup_n_days 3
aech-cli-inbox-assistant prefs unset vip_senders
```

### System Commands

```bash
aech-cli-inbox-assistant dbpath      # Get database path
aech-cli-inbox-assistant schema      # Get database schema
aech-cli-inbox-assistant timezone    # Show timezone config
```

## Alert Rules

User-defined notification rules with natural language input. Alert rules can monitor multiple event types and route to Teams or webhooks.

### Managing Alert Rules

```bash
# List all rules
aech-cli-inbox-assistant alerts list
aech-cli-inbox-assistant alerts list --enabled-only

# Add a new rule
aech-cli-inbox-assistant alerts add "Alert me when CFO emails about budget" \
  --channel teams \
  --cooldown 60

# Add rule with webhook routing
aech-cli-inbox-assistant alerts add "Notify when urgent emails arrive" \
  --channel webhook \
  --target "https://hooks.example.com/inbox"

# View rule details
aech-cli-inbox-assistant alerts show rule-uuid-here

# Enable/disable rules
aech-cli-inbox-assistant alerts enable rule-uuid-here
aech-cli-inbox-assistant alerts disable rule-uuid-here

# Remove a rule
aech-cli-inbox-assistant alerts remove rule-uuid-here

# View trigger history
aech-cli-inbox-assistant alerts history --limit 20
aech-cli-inbox-assistant alerts history --rule-id rule-uuid-here
```

### Supported Event Types

Alert rules can monitor:

| Event Type | Description |
|------------|-------------|
| `email_received` | Inbound emails (default) |
| `email_sent` | Outbound emails |
| `calendar_event` | Calendar changes |
| `wm_thread` | Working memory thread updates |
| `wm_commitment` | Commitment status changes |
| `wm_decision` | Decision updates |

### Rule Conditions

Rules are parsed into structured conditions:

- **Sender patterns**: `*cfo*`, `*@legal.company.com`
- **Subject/body keywords**: `budget`, `urgent`, `deadline`
- **Urgency levels**: `immediate`, `today`, `this_week`
- **Labels**: `vip`, `billing`, `marketing`
- **Outlook categories**: Applied category names
- **Semantic matching**: LLM evaluation for complex rules

### Cooldown & Deduplication

- **Cooldown**: Configurable per-rule (default: 30 minutes)
- **Deduplication**: Same (rule_id, event_type, event_id) won't trigger twice

## Meeting Prep

Executive assistant features for meeting preparation:

### Features

- **Daily Briefing**: Schedule overview, busy/free hours, back-to-back alerts
- **Attendee Context**: Cross-references attendees with email corpus
- **Configurable Rules**: Which meetings get prep based on:
  - External attendees
  - Meeting size (5+ attendees)
  - Keywords in subject (interview, review, board, exec, client, partner)
  - VIP attendee list

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

```text
~/.inbox-assistant/
├── assistant.sqlite  # Main database
├── llm.jsonl         # LLM observability logs (configurable via LLM_LOG_PATH)
└── preferences.json  # User preferences (optional)
```

### Database Tables

#### Core Tables

| Table | Purpose |
|-------|---------|
| `emails` | All ingested messages with metadata, body, classification |
| `attachments` | Email attachments with extraction status |
| `chunks` | Searchable text segments for FTS and vector search |
| `labels` | Email ML classifications (vip, billing, marketing, etc.) |
| `triage_log` | Classification history |
| `user_preferences` | Key-value preferences storage |

#### Calendar Tables

| Table | Purpose |
|-------|---------|
| `calendar_events` | Synced calendar events cache |

#### Working Memory Tables

| Table | Purpose |
|-------|---------|
| `wm_threads` | Active conversation threads |
| `wm_contacts` | Known contacts with interaction history |
| `wm_decisions` | Pending decisions awaiting response |
| `wm_commitments` | User's commitments to others |
| `wm_observations` | Passive learnings from CC'd emails |
| `wm_projects` | Inferred projects/initiatives |
| `facts` | Unified business facts extraction (amounts, deadlines, tax IDs, etc.) |

#### Alert Rules Tables

| Table | Purpose |
|-------|---------|
| `alert_rules` | User-defined notification rules with parsed conditions |
| `alert_triggers` | Alert trigger history for deduplication |

#### System Tables

| Table | Purpose |
|-------|---------|
| `actions` | CLI-initiated actions queued for RT execution |
| `sync_state` | Delta sync tracking (delta links, last sync times) |
| `work_items` | Internal work queue |

#### Computed Views

| View | Purpose |
|------|---------|
| `active_threads` | Derived thread state from emails (last 30 days) |
| `contacts` | Derived contact statistics from senders |

### Data Integrity Controls

The SQLite schema enforces enterprise-grade data integrity:

| Control | Implementation |
|---------|----------------|
| **Referential Integrity** | All child tables use `FOREIGN KEY ... ON DELETE CASCADE` |
| **Cascading Deletes** | Email deletion removes: attachments, chunks, triage_log, labels, WM references |
| **CHECK Constraints** | Enum validation: `urgency`, `extraction_status`, `action_type`, etc. |
| **NOT NULL + Defaults** | Required fields enforced; JSON arrays default to `'[]'` |
| **Polymorphic FK Cleanup** | Triggers delete orphaned `chunks` when parents removed |
| **WAL Mode** | Write-Ahead Logging for concurrent access |
| **Foreign Keys Enabled** | `PRAGMA foreign_keys=ON` at connection time |

**Indexed Fields** (optimized for common queries):

- `emails`: conversation_id, sender, received_at, urgency, processed_at
- `attachments`: email_id, content_hash, extraction_status
- `chunks`: source_type + source_id (composite)
- `calendar_events`: start_at, end_at
- `actions`: status
- `alert_rules`: enabled
- `alert_triggers`: rule_id, (rule_id, event_type, event_id)
- `facts`: (source_type, source_id), (fact_type), status, due_date
- `wm_threads`: status, urgency, needs_reply
- `wm_contacts`: email, relationship
- `wm_observations`: type, observed_at
- `wm_decisions`: is_resolved, urgency
- `wm_commitments`: is_completed, due_by

## Standard Folders

The service manages emails into these prefixed folders (e.g., `aa_Work`):

- Work, Personal, Travel, Finance
- Newsletters, Social, Cold Outreach
- Urgent, Action Required, FYI, Should Delete
