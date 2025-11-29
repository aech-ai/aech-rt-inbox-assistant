import logging
from pathlib import Path
from datetime import datetime
import json

import pysqlite3 as sqlite3

logger = logging.getLogger(__name__)

def get_db_path(user_email: str) -> Path:
    """Get the path to the user's database file."""
    # Assuming data is mounted at /data inside the container
    # or relative to the project root for local dev
    base_path = Path("data/users") / user_email / "inbox-assistant"
    base_path.mkdir(parents=True, exist_ok=True)
    return base_path / "inbox.sqlite"

def init_db(db_path: Path):
    """Initialize the database schema."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Enable WAL mode for concurrency
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA foreign_keys=ON;")
    
    # Emails table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS emails (
        id TEXT PRIMARY KEY,
        subject TEXT,
        sender TEXT,
        received_at DATETIME,
        body_preview TEXT,
        is_read BOOLEAN,
        folder_id TEXT,
        category TEXT,
        processed_at DATETIME
    )
    """)
    
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
    
    conn.commit()
    _ensure_fts(cursor)
    conn.commit()
    conn.close()

def get_connection(db_path: Path) -> sqlite3.Connection:
    """Get a connection to the database."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_fts(cursor: sqlite3.Cursor) -> None:
    """
    Create an FTS5 index over email subject/body for search and keep it in sync.
    This is idempotent and safe to call at startup.
    """
    # Clean up any stale/broken FTS artifacts first
    cursor.execute("DROP TRIGGER IF EXISTS emails_ai_fts")
    cursor.execute("DROP TRIGGER IF EXISTS emails_ad_fts")
    cursor.execute("DROP TRIGGER IF EXISTS emails_au_fts")
    cursor.execute("DROP TABLE IF EXISTS emails_fts")

    cursor.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS emails_fts
    USING fts5(
        id UNINDEXED,
        subject,
        body_preview,
        tokenize = 'porter'
    )
    """)

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
