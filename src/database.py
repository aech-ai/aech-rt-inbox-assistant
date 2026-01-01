import logging
import os
from pathlib import Path
from typing import Optional

try:
    import pysqlite3 as sqlite3  # type: ignore
except ImportError:  # pragma: no cover
    import sqlite3  # type: ignore

logger = logging.getLogger(__name__)

CAPABILITY_NAME = "inbox-assistant"


def get_user_root() -> Path:
    """
    Resolve the delegated user's mounted directory.

    In production the Worker mounts the user's directory at `/home/agentaech`.
    For local dev, this falls back to `./data/users/<DELEGATED_USER>/` when present.
    """
    configured = os.environ.get("AECH_USER_DIR")
    if configured:
        return Path(configured).expanduser().resolve()

    container_root = Path("/home/agentaech")
    if container_root.exists():
        return container_root

    delegated = os.environ.get("DELEGATED_USER")
    if delegated:
        local = (Path.cwd() / "data" / "users" / delegated).resolve()
        return local

    return (Path.home() / "agentaech").resolve()


def get_state_dir() -> Path:
    configured = os.environ.get("INBOX_STATE_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return get_user_root() / f".{CAPABILITY_NAME}"


def get_db_path() -> Path:
    """
    Get the path to the capability-owned SQLite state.

    Override with `INBOX_DB_PATH` if needed.
    """
    db_path_str = os.environ.get("INBOX_DB_PATH")
    if db_path_str:
        return Path(db_path_str).expanduser().resolve()
    return get_state_dir() / "assistant.sqlite"


def init_db(db_path: Optional[Path] = None) -> None:
    """Initialize the database schema."""
    db_path = (db_path or get_db_path()).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Enable WAL mode for concurrency
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA foreign_keys=ON;")
    
    # Emails table - categories mode (no folders)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS emails (
        id TEXT PRIMARY KEY,
        conversation_id TEXT,
        internet_message_id TEXT,
        subject TEXT,
        sender TEXT NOT NULL,
        to_emails TEXT NOT NULL DEFAULT '[]', -- JSON array
        cc_emails TEXT NOT NULL DEFAULT '[]', -- JSON array
        received_at DATETIME NOT NULL,
        body_preview TEXT,
        body_html TEXT,
        body_markdown TEXT,        -- Semantic markdown main content
        signature_block TEXT,      -- Preserved sender signature
        thread_summary TEXT,       -- LLM-generated thread summary
        body_hash TEXT,
        has_attachments BOOLEAN DEFAULT 0,
        is_read BOOLEAN DEFAULT 0,
        etag TEXT,
        web_link TEXT,
        -- Categories mode fields
        outlook_categories TEXT NOT NULL DEFAULT '[]', -- JSON array of applied Outlook categories
        urgency TEXT DEFAULT 'someday' CHECK(urgency IN ('immediate', 'today', 'this_week', 'someday')),
        suggested_action TEXT DEFAULT 'keep' CHECK(suggested_action IN ('keep', 'archive', 'delete')),
        processed_at DATETIME,
        wm_processed_at DATETIME,  -- When working memory analysis was done
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    # Indexes for common email queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_conversation ON emails(conversation_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_sender ON emails(sender)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_received ON emails(received_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_urgency ON emails(urgency)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_processed ON emails(processed_at)")
    # Migrate existing databases
    _ensure_columns(cursor, "emails", {"wm_processed_at": "DATETIME"})

    # Triage Log table - categories mode
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS triage_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email_id TEXT NOT NULL,
        outlook_categories TEXT NOT NULL DEFAULT '[]', -- JSON array of applied categories
        urgency TEXT CHECK(urgency IN ('immediate', 'today', 'this_week', 'someday')),
        reason TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(email_id) REFERENCES emails(id) ON DELETE CASCADE
    )
    """)

    # User preferences table for Executive Assistant
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_preferences (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Labels (e.g. vip, action_required, billing, marketing)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS labels (
        message_id TEXT NOT NULL,
        label TEXT NOT NULL,
        confidence REAL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(message_id, label),
        FOREIGN KEY(message_id) REFERENCES emails(id) ON DELETE CASCADE
    )
    """)

    # NOTE: reply_tracking table removed - Working Memory (wm_threads) is now the source of truth
    # for tracking which threads need replies. The WM Engine handles staleness and nudges.

    # NOTE: Calendar events are synced to calendar_events table by RT service.
    # See src/calendar_sync.py for sync implementation.

    # Internal work queue
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS work_items (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('pending', 'processing', 'completed', 'failed')),
        payload_json TEXT NOT NULL DEFAULT '{}',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_work_items_status ON work_items(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_work_items_type ON work_items(type)")

    # Sync state for delta sync tracking (per-folder)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sync_state (
        folder_id TEXT PRIMARY KEY,
        delta_link TEXT,
        last_sync_at DATETIME,
        sync_type TEXT,
        messages_synced INTEGER DEFAULT 0
    )
    """)

    # Attachments table for extracted content
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS attachments (
        id TEXT PRIMARY KEY,
        email_id TEXT NOT NULL,
        filename TEXT,
        content_type TEXT,
        size_bytes INTEGER,
        content_hash TEXT,
        extracted_text TEXT,
        extraction_status TEXT DEFAULT 'pending' CHECK(extraction_status IN ('pending', 'extracting', 'completed', 'failed', 'skipped')),
        extraction_error TEXT,
        downloaded_at DATETIME,
        extracted_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(email_id) REFERENCES emails(id) ON DELETE CASCADE
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_attachments_email ON attachments(email_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_attachments_hash ON attachments(content_hash)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_attachments_status ON attachments(extraction_status)")

    # Chunks table for searchable text segments
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS chunks (
        id TEXT PRIMARY KEY,
        source_type TEXT NOT NULL CHECK(source_type IN ('email', 'attachment')),
        source_id TEXT NOT NULL,
        chunk_index INTEGER NOT NULL CHECK(chunk_index >= 0),
        content TEXT NOT NULL,
        char_offset_start INTEGER,
        char_offset_end INTEGER,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        embedding BLOB,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source_type, source_id, chunk_index)
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_source ON chunks(source_type, source_id)")

    # Cascade delete triggers for chunks (polymorphic FK cleanup)
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS emails_delete_chunks
    AFTER DELETE ON emails BEGIN
        DELETE FROM chunks WHERE source_type = 'email' AND source_id = old.id;
    END;
    """)
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS attachments_delete_chunks
    AFTER DELETE ON attachments BEGIN
        DELETE FROM chunks WHERE source_type = 'attachment' AND source_id = old.id;
    END;
    """)

    # === Working Memory Tables ===

    # Active threads being tracked
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wm_threads (
        id TEXT PRIMARY KEY,
        conversation_id TEXT NOT NULL UNIQUE,
        subject TEXT,
        participants_json TEXT NOT NULL DEFAULT '[]',
        status TEXT DEFAULT 'active' CHECK(status IN ('active', 'awaiting_reply', 'awaiting_action', 'stale', 'resolved', 'archived')),
        urgency TEXT DEFAULT 'this_week' CHECK(urgency IN ('immediate', 'today', 'this_week', 'someday')),
        started_at DATETIME,
        last_activity_at DATETIME,
        user_last_action_at DATETIME,
        summary TEXT,
        key_points_json TEXT NOT NULL DEFAULT '[]',
        pending_questions_json TEXT NOT NULL DEFAULT '[]',
        message_count INTEGER DEFAULT 0,
        user_is_cc BOOLEAN DEFAULT 0,
        needs_reply BOOLEAN DEFAULT 0,
        reply_deadline DATETIME,
        labels_json TEXT NOT NULL DEFAULT '[]',
        project_refs_json TEXT NOT NULL DEFAULT '[]',
        latest_email_id TEXT,
        latest_web_link TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    _ensure_columns(cursor, "wm_threads", {"latest_email_id": "TEXT", "latest_web_link": "TEXT"})
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_threads_status ON wm_threads(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_threads_urgency ON wm_threads(urgency)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_threads_needs_reply ON wm_threads(needs_reply)")

    # Known contacts with interaction history
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wm_contacts (
        id TEXT PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        name TEXT,
        organization TEXT,
        relationship TEXT DEFAULT 'unknown' CHECK(relationship IN ('unknown', 'colleague', 'client', 'vendor', 'partner', 'personal', 'other')),
        first_seen_at DATETIME,
        last_interaction_at DATETIME,
        total_interactions INTEGER DEFAULT 0,
        user_initiated_count INTEGER DEFAULT 0,
        they_initiated_count INTEGER DEFAULT 0,
        cc_count INTEGER DEFAULT 0,
        topics_json TEXT NOT NULL DEFAULT '[]',
        notes TEXT,
        is_vip BOOLEAN DEFAULT 0,
        is_internal BOOLEAN DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_contacts_email ON wm_contacts(email)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_contacts_relationship ON wm_contacts(relationship)")

    # Inferred projects/initiatives
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wm_projects (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        related_threads_json TEXT NOT NULL DEFAULT '[]',
        participants_json TEXT NOT NULL DEFAULT '[]',
        status TEXT DEFAULT 'active' CHECK(status IN ('active', 'completed', 'on_hold', 'cancelled')),
        confidence REAL DEFAULT 0.5 CHECK(confidence >= 0.0 AND confidence <= 1.0),
        first_mentioned_at DATETIME,
        last_activity_at DATETIME,
        key_decisions_json TEXT NOT NULL DEFAULT '[]',
        deadlines_json TEXT NOT NULL DEFAULT '[]',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Observations from passive learning (CC emails)
    # Types match ObservationType enum in src/working_memory/models.py
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wm_observations (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL CHECK(type IN (
            'project_mention', 'decision_made', 'deadline_mentioned',
            'person_introduced', 'status_update', 'meeting_scheduled',
            'commitment_made', 'context_learned'
        )),
        content TEXT NOT NULL,
        source_email_id TEXT,
        source_thread_id TEXT,
        related_contacts_json TEXT NOT NULL DEFAULT '[]',
        related_projects_json TEXT NOT NULL DEFAULT '[]',
        importance REAL DEFAULT 0.5 CHECK(importance >= 0.0 AND importance <= 1.0),
        confidence REAL DEFAULT 0.5 CHECK(confidence >= 0.0 AND confidence <= 1.0),
        observed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        relevant_until DATETIME,
        FOREIGN KEY(source_email_id) REFERENCES emails(id) ON DELETE CASCADE
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_observations_type ON wm_observations(type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_observations_observed ON wm_observations(observed_at)")

    # Pending decisions requiring user response
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wm_decisions (
        id TEXT PRIMARY KEY,
        question TEXT NOT NULL,
        context TEXT,
        options_json TEXT NOT NULL DEFAULT '[]',
        source_email_id TEXT,
        source_thread_id TEXT,
        requester TEXT,
        urgency TEXT DEFAULT 'this_week' CHECK(urgency IN ('immediate', 'today', 'this_week', 'someday')),
        deadline DATETIME,
        is_resolved BOOLEAN DEFAULT 0,
        resolution TEXT,
        resolved_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(source_email_id) REFERENCES emails(id) ON DELETE CASCADE
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_decisions_resolved ON wm_decisions(is_resolved)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_decisions_urgency ON wm_decisions(urgency)")

    # User commitments to others
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wm_commitments (
        id TEXT PRIMARY KEY,
        description TEXT NOT NULL,
        to_whom TEXT,
        source_email_id TEXT,
        committed_at DATETIME,
        due_by DATETIME,
        is_completed BOOLEAN DEFAULT 0,
        completed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(source_email_id) REFERENCES emails(id) ON DELETE CASCADE
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_commitments_completed ON wm_commitments(is_completed)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_commitments_due ON wm_commitments(due_by)")

    # === Calendar Events Table ===
    # Synced from Microsoft Graph API for offline access by CLI
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS calendar_events (
        id TEXT PRIMARY KEY,
        subject TEXT,
        start_at TEXT,
        end_at TEXT,
        is_all_day INTEGER DEFAULT 0,
        location TEXT,
        is_online_meeting INTEGER DEFAULT 0,
        online_meeting_url TEXT,
        organizer_email TEXT,
        organizer_name TEXT,
        attendees_json TEXT NOT NULL DEFAULT '[]',
        body_preview TEXT,
        response_status TEXT,
        sensitivity TEXT,
        show_as TEXT,
        importance TEXT,
        is_cancelled INTEGER DEFAULT 0,
        web_link TEXT,
        last_modified_at TEXT,
        synced_at TEXT
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_calendar_events_start ON calendar_events(start_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_calendar_events_end ON calendar_events(end_at)")

    # === Actions Table ===
    # Queue for CLI-initiated actions executed by RT service
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS actions (
        id TEXT PRIMARY KEY,
        item_type TEXT NOT NULL,
        item_id TEXT,
        action_type TEXT NOT NULL,
        payload_json TEXT,
        status TEXT NOT NULL DEFAULT 'proposed',
        proposed_at TEXT,
        executed_at TEXT,
        result_json TEXT,
        error TEXT
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_actions_status ON actions(status)")

    # === Alert Rules Tables ===
    # User-defined alert rules for custom notifications
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS alert_rules (
        id TEXT PRIMARY KEY,
        natural_language_rule TEXT NOT NULL,
        parsed_conditions_json TEXT NOT NULL DEFAULT '{}',
        event_types TEXT NOT NULL DEFAULT '["email_received"]',
        channel TEXT NOT NULL DEFAULT 'teams',
        channel_target TEXT,
        enabled BOOLEAN DEFAULT 1,
        cooldown_minutes INTEGER DEFAULT 30,
        last_triggered_at DATETIME,
        trigger_count INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        created_by TEXT DEFAULT 'user'
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_alert_rules_enabled ON alert_rules(enabled)")

    # Alert trigger history for deduplication and auditing
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS alert_triggers (
        id TEXT PRIMARY KEY,
        rule_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        event_id TEXT NOT NULL,
        triggered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        match_reason TEXT,
        trigger_payload_json TEXT,
        FOREIGN KEY(rule_id) REFERENCES alert_rules(id) ON DELETE CASCADE,
        UNIQUE(rule_id, event_type, event_id)
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_alert_triggers_rule ON alert_triggers(rule_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_alert_triggers_event ON alert_triggers(event_type, event_id)")

    # === Unified Facts Table ===
    # Consolidates: wm_decisions, wm_commitments, wm_observations, plus key business facts
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS facts (
        id TEXT PRIMARY KEY,
        source_type TEXT NOT NULL CHECK(source_type IN ('email', 'attachment', 'calendar')),
        source_id TEXT NOT NULL,
        fact_type TEXT NOT NULL CHECK(fact_type IN (
            -- Action items (from WM)
            'decision',        -- Pending decision requiring response
            'commitment',      -- Promise made by user
            'action_item',     -- Task extracted from email

            -- Key details
            'tax_id', 'business_number', 'account_number',
            'amount', 'address', 'phone', 'deadline',
            'person_name', 'company_name', 'contract_number',

            -- Observations
            'preference',      -- User preference learned
            'relationship',    -- Org structure insight
            'pattern',         -- Recurring pattern

            'other'
        )),
        fact_value TEXT NOT NULL,
        context TEXT,                    -- Surrounding text for disambiguation
        confidence REAL DEFAULT 0.8 CHECK(confidence >= 0.0 AND confidence <= 1.0),
        entity_normalized TEXT,          -- Normalized form (dates, phones, etc)
        metadata_json TEXT,              -- Additional structured data
        status TEXT DEFAULT 'active' CHECK(status IN ('active', 'resolved', 'expired')),
        due_date DATETIME,               -- For deadlines, commitments
        extracted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        resolved_at DATETIME,
        FOREIGN KEY(source_id) REFERENCES emails(id) ON DELETE CASCADE
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_source ON facts(source_type, source_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_type ON facts(fact_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_status ON facts(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_due ON facts(due_date)")

    # === Derived Views (replace wm_threads and wm_contacts) ===

    # Active threads view - computed from emails on demand
    # Note: needs_reply logic should be done in application code (requires user_email)
    cursor.execute("""
    CREATE VIEW IF NOT EXISTS active_threads AS
    SELECT
        e.conversation_id,
        MAX(e.received_at) as last_activity,
        COUNT(*) as message_count,
        GROUP_CONCAT(DISTINCT e.sender) as participants,
        (SELECT e2.subject FROM emails e2
         WHERE e2.conversation_id = e.conversation_id
         ORDER BY e2.received_at DESC LIMIT 1) as subject,
        (SELECT e3.sender FROM emails e3
         WHERE e3.conversation_id = e.conversation_id
         ORDER BY e3.received_at DESC LIMIT 1) as last_sender,
        (SELECT e4.id FROM emails e4
         WHERE e4.conversation_id = e.conversation_id
         ORDER BY e4.received_at DESC LIMIT 1) as latest_email_id,
        (SELECT e5.web_link FROM emails e5
         WHERE e5.conversation_id = e.conversation_id
         ORDER BY e5.received_at DESC LIMIT 1) as latest_web_link,
        (SELECT e6.urgency FROM emails e6
         WHERE e6.conversation_id = e.conversation_id
         ORDER BY e6.received_at DESC LIMIT 1) as urgency,
        EXISTS(SELECT 1 FROM facts f
               WHERE f.source_id IN (SELECT id FROM emails WHERE conversation_id = e.conversation_id)
               AND f.fact_type IN ('decision', 'commitment', 'action_item')
               AND f.status = 'active') as has_action_items
    FROM emails e
    WHERE datetime(e.received_at) > datetime('now', '-30 days')
      AND e.conversation_id IS NOT NULL
    GROUP BY e.conversation_id
    """)

    # Contacts view - computed from emails on demand
    cursor.execute("""
    CREATE VIEW IF NOT EXISTS contacts AS
    SELECT
        e.sender as email,
        CASE
            WHEN e.sender LIKE '%<%>' THEN TRIM(SUBSTR(e.sender, 1, INSTR(e.sender, '<')-1))
            ELSE e.sender
        END as name,
        COUNT(*) as email_count,
        MAX(e.received_at) as last_interaction,
        MIN(e.received_at) as first_interaction,
        COUNT(DISTINCT e.conversation_id) as thread_count
    FROM emails e
    GROUP BY e.sender
    """)

    conn.commit()
    _ensure_fts(cursor)
    conn.commit()
    conn.close()

    setup_query_library(db_path)

def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Get a connection to the database."""
    db_path = (db_path or get_db_path()).expanduser().resolve()
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    # NOTE: SQLite pragma settings are per-connection.
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    return conn


def _ensure_columns(cursor: sqlite3.Cursor, table: str, columns: dict[str, str]) -> None:
    existing = {row[1] for row in cursor.execute(f"PRAGMA table_info({table})")}
    for name, column_type in columns.items():
        if name in existing:
            continue
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {name} {column_type}")


def _ensure_fts(cursor: sqlite3.Cursor) -> None:
    """
    Create FTS5 indexes over email subject/body and chunks for search.
    This is idempotent and safe to call at startup.
    """
    # Create FTS5 index for emails
    cursor.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS emails_fts
    USING fts5(
        id UNINDEXED,
        subject,
        body_markdown,
        sender,
        tokenize = 'porter'
    )
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS emails_ai_fts
    AFTER INSERT ON emails BEGIN
        INSERT OR REPLACE INTO emails_fts(id, subject, body_markdown, sender)
        VALUES (new.id, new.subject, COALESCE(new.body_markdown, new.body_preview), new.sender);
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS emails_ad_fts
    AFTER DELETE ON emails BEGIN
        DELETE FROM emails_fts WHERE id = old.id;
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS emails_au_fts
    AFTER UPDATE ON emails BEGIN
        DELETE FROM emails_fts WHERE id = old.id;
        INSERT OR REPLACE INTO emails_fts(id, subject, body_markdown, sender)
        VALUES (new.id, new.subject, COALESCE(new.body_markdown, new.body_preview), new.sender);
    END;
    """)

    # Create FTS5 index for chunks
    cursor.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts
    USING fts5(
        id UNINDEXED,
        content,
        tokenize = 'porter'
    )
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS chunks_ai_fts
    AFTER INSERT ON chunks BEGIN
        INSERT OR REPLACE INTO chunks_fts(id, content)
        VALUES (new.id, new.content);
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS chunks_ad_fts
    AFTER DELETE ON chunks BEGIN
        DELETE FROM chunks_fts WHERE id = old.id;
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS chunks_au_fts
    AFTER UPDATE ON chunks BEGIN
        DELETE FROM chunks_fts WHERE id = old.id;
        INSERT OR REPLACE INTO chunks_fts(id, content)
        VALUES (new.id, new.content);
    END;
    """)

    # Create FTS5 index for facts
    cursor.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts
    USING fts5(
        id UNINDEXED,
        fact_value,
        context,
        entity_normalized,
        tokenize = 'porter'
    )
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS facts_ai_fts
    AFTER INSERT ON facts BEGIN
        INSERT OR REPLACE INTO facts_fts(id, fact_value, context, entity_normalized)
        VALUES (new.id, new.fact_value, new.context, new.entity_normalized);
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS facts_ad_fts
    AFTER DELETE ON facts BEGIN
        DELETE FROM facts_fts WHERE id = old.id;
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS facts_au_fts
    AFTER UPDATE ON facts BEGIN
        DELETE FROM facts_fts WHERE id = old.id;
        INSERT OR REPLACE INTO facts_fts(id, fact_value, context, entity_normalized)
        VALUES (new.id, new.fact_value, new.context, new.entity_normalized);
    END;
    """)


def setup_query_library(db_path: Path) -> None:
    """
    Create the queries folder and populate with starter SQL templates.
    """
    queries_dir = db_path.parent / "queries"
    queries_dir.mkdir(exist_ok=True)

    # Define starter query templates - categories mode
    templates = {
        "urgent_emails.sql": """-- Get urgent emails (immediate or today urgency)
SELECT
    id,
    subject,
    sender,
    received_at,
    body_preview,
    outlook_categories,
    urgency
FROM emails
WHERE urgency IN ('immediate', 'today')
  AND datetime(received_at) > datetime('now', '-24 hours')
ORDER BY
    CASE urgency WHEN 'immediate' THEN 1 WHEN 'today' THEN 2 END,
    received_at DESC;""",

        "emails_by_urgency.sql": """-- Count emails by urgency level
SELECT
    urgency,
    COUNT(*) as count,
    COUNT(CASE WHEN is_read = 0 THEN 1 END) as unread_count
FROM emails
WHERE processed_at IS NOT NULL
GROUP BY urgency
ORDER BY
    CASE urgency
        WHEN 'immediate' THEN 1
        WHEN 'today' THEN 2
        WHEN 'this_week' THEN 3
        ELSE 4
    END;""",

        "unprocessed_emails.sql": """-- Get unprocessed emails
SELECT
    id,
    subject,
    sender,
    received_at,
    body_preview,
    is_read
FROM emails
WHERE processed_at IS NULL
ORDER BY received_at DESC;""",

        "recent_triage_decisions.sql": """-- Get recent triage decisions
SELECT
    t.timestamp,
    t.outlook_categories,
    t.urgency,
    e.subject,
    e.sender,
    t.reason
FROM triage_log t
JOIN emails e ON t.email_id = e.id
ORDER BY t.timestamp DESC
LIMIT 20;""",

        "action_required.sql": """-- Emails needing action
SELECT
    id,
    subject,
    sender,
    received_at,
    urgency,
    web_link
FROM emails
WHERE outlook_categories LIKE '%Action Required%'
  AND processed_at IS NOT NULL
ORDER BY
    CASE urgency WHEN 'immediate' THEN 1 WHEN 'today' THEN 2 WHEN 'this_week' THEN 3 ELSE 4 END,
    received_at DESC;"""
    }

    # Write templates to files (overwrite to update with new schema)
    for filename, content in templates.items():
        filepath = queries_dir / filename
        filepath.write_text(content)
