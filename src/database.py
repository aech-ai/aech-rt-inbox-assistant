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
    return get_state_dir() / "inbox.sqlite"


def init_db(db_path: Optional[Path] = None) -> None:
    """Initialize (or migrate) the database schema."""
    db_path = (db_path or get_db_path()).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Enable WAL mode for concurrency
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA foreign_keys=ON;")
    
    # Emails table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS emails (
        id TEXT PRIMARY KEY,
        conversation_id TEXT,
        internet_message_id TEXT,
        subject TEXT,
        sender TEXT,
        to_emails TEXT, -- JSON array
        cc_emails TEXT, -- JSON array
        received_at DATETIME,
        body_preview TEXT,
        has_attachments BOOLEAN,
        is_read BOOLEAN,
        folder_id TEXT,
        etag TEXT,
        category TEXT,
        processed_at DATETIME
    )
    """)
    _ensure_columns(
        cursor,
        "emails",
        {
            "conversation_id": "TEXT",
            "internet_message_id": "TEXT",
            "to_emails": "TEXT",
            "cc_emails": "TEXT",
            "has_attachments": "BOOLEAN",
            "etag": "TEXT",
            "body_text": "TEXT",
            "body_html": "TEXT",
            "body_hash": "TEXT",
        },
    )
    
    # Triage Log table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS triage_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email_id TEXT,
        action TEXT,
        destination_folder TEXT,
        reason TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(email_id) REFERENCES emails(id)
    )
    """)
    
    # Folders cache
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS folders (
        id TEXT PRIMARY KEY,
        display_name TEXT,
        parent_folder_id TEXT
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

    # Reply tracking for follow-ups
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS reply_tracking (
        message_id TEXT PRIMARY KEY,
        requires_reply BOOLEAN NOT NULL,
        reason TEXT,
        last_activity_at DATETIME,
        nudge_scheduled_at DATETIME,
        follow_up_sent_at DATETIME,
        FOREIGN KEY(message_id) REFERENCES emails(id) ON DELETE CASCADE
    )
    """)

    # Calendar events cache (for scheduling/conflict detection)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS calendar_events (
        event_id TEXT PRIMARY KEY,
        starts_at DATETIME,
        ends_at DATETIME,
        title TEXT,
        location TEXT,
        organizer TEXT,
        attendees_json TEXT,
        priority INTEGER
    )
    """)

    # Internal work queue
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS work_items (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        status TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

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
        extraction_status TEXT DEFAULT 'pending',
        extraction_error TEXT,
        downloaded_at DATETIME,
        extracted_at DATETIME,
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
        source_type TEXT NOT NULL,
        source_id TEXT NOT NULL,
        chunk_index INTEGER NOT NULL,
        content TEXT NOT NULL,
        char_offset_start INTEGER,
        char_offset_end INTEGER,
        metadata_json TEXT,
        embedding BLOB,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source_type, source_id, chunk_index)
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_source ON chunks(source_type, source_id)")

    conn.commit()
    _ensure_fts(cursor)
    conn.commit()
    conn.close()

    setup_query_library(db_path)

def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Get a connection to the database."""
    db_path = (db_path or get_db_path()).expanduser().resolve()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
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
    # Check current emails_fts schema to see if we need to migrate
    fts_info = cursor.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='emails_fts'"
    ).fetchone()

    # If old FTS exists with body_preview, drop and recreate with body_text
    if fts_info and "body_preview" in (fts_info[0] or ""):
        cursor.execute("DROP TRIGGER IF EXISTS emails_ai_fts")
        cursor.execute("DROP TRIGGER IF EXISTS emails_ad_fts")
        cursor.execute("DROP TRIGGER IF EXISTS emails_au_fts")
        cursor.execute("DROP TABLE IF EXISTS emails_fts")
        fts_info = None

    existed = fts_info is not None

    # Create FTS5 index for emails with full body_text
    cursor.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS emails_fts
    USING fts5(
        id UNINDEXED,
        subject,
        body_text,
        sender,
        tokenize = 'porter'
    )
    """)

    if not existed:
        # Populate from existing data - use body_text if available, fallback to body_preview
        cursor.execute("""
            INSERT INTO emails_fts(id, subject, body_text, sender)
            SELECT id, subject, COALESCE(body_text, body_preview), sender FROM emails
        """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS emails_ai_fts
    AFTER INSERT ON emails BEGIN
        INSERT OR REPLACE INTO emails_fts(id, subject, body_text, sender)
        VALUES (new.id, new.subject, COALESCE(new.body_text, new.body_preview), new.sender);
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
        INSERT OR REPLACE INTO emails_fts(id, subject, body_text, sender)
        VALUES (new.id, new.subject, COALESCE(new.body_text, new.body_preview), new.sender);
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

def setup_query_library(db_path: Path) -> None:
    """
    Create the queries folder and populate with starter SQL templates.
    """
    queries_dir = db_path.parent / "queries"
    queries_dir.mkdir(exist_ok=True)
    
    # Define starter query templates
    templates = {
        "urgent_emails_last_24h.sql": """-- Get urgent emails from the last 24 hours
SELECT 
    id,
    subject,
    sender,
    received_at,
    body_preview,
    category
FROM emails
WHERE category = 'Urgent'
  AND datetime(received_at) > datetime('now', '-24 hours')
ORDER BY received_at DESC;""",
        
        "newsletters_last_week.sql": """-- Get newsletters from the last week
SELECT 
    id,
    subject,
    sender,
    received_at,
    body_preview
FROM emails
WHERE category = 'Newsletters'
  AND datetime(received_at) > datetime('now', '-7 days')
ORDER BY received_at DESC;""",
        
        "emails_by_category.sql": """-- Count emails by category
SELECT 
    category,
    COUNT(*) as count,
    COUNT(CASE WHEN is_read = 0 THEN 1 END) as unread_count
FROM emails
WHERE category IS NOT NULL
GROUP BY category
ORDER BY count DESC;""",
        
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
    t.action,
    t.destination_folder,
    e.subject,
    e.sender,
    t.reason
FROM triage_log t
JOIN emails e ON t.email_id = e.id
ORDER BY t.timestamp DESC
LIMIT 20;"""
    }
    
    # Write templates to files (only if they don't exist)
    for filename, content in templates.items():
        filepath = queries_dir / filename
        if not filepath.exists():
            filepath.write_text(content)
