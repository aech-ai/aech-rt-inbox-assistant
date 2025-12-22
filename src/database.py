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
    return get_user_root() / CAPABILITY_NAME


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
    Create an FTS5 index over email subject/body for search and keep it in sync.
    This is idempotent and safe to call at startup.
    """
    existed = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='emails_fts'"
    ).fetchone()

    cursor.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS emails_fts
    USING fts5(
        id UNINDEXED,
        subject,
        body_preview,
        tokenize = 'porter'
    )
    """)

    if not existed:
        cursor.execute("""
            INSERT INTO emails_fts(id, subject, body_preview)
            SELECT id, subject, body_preview FROM emails
        """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS emails_ai_fts
    AFTER INSERT ON emails BEGIN
        INSERT OR REPLACE INTO emails_fts(id, subject, body_preview)
        VALUES (new.id, new.subject, new.body_preview);
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
        INSERT OR REPLACE INTO emails_fts(id, subject, body_preview)
        VALUES (new.id, new.subject, new.body_preview);
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
