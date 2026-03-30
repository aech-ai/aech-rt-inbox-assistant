import logging
import os
from pathlib import Path
from typing import Any, Optional

try:
    import pysqlite3 as sqlite3  # type: ignore
except ImportError:  # pragma: no cover
    import sqlite3  # type: ignore

logger = logging.getLogger(__name__)

CAPABILITY_NAME = "inbox-assistant"
STATE_DB_NAME = "assistant.sqlite"
STATE_HINT_DIRS = ("attachments", "queries")
SHARED_INBOX_ROOT_ENV = "AECH_SHARED_INBOX_ROOT"
DEFAULT_SHARED_INBOX_ROOT = Path("/shared-inbox-root")

FACTS_COLUMNS = [
    "id",
    "source_type",
    "source_id",
    "fact_type",
    "fact_value",
    "context",
    "confidence",
    "entity_normalized",
    "metadata_json",
    "status",
    "due_date",
    "extracted_at",
    "resolved_at",
]

FACTS_COLUMN_DEFAULTS = {
    "context": "NULL",
    "confidence": "0.8",
    "entity_normalized": "NULL",
    "metadata_json": "NULL",
    "status": "'active'",
    "due_date": "NULL",
    "extracted_at": "CURRENT_TIMESTAMP",
    "resolved_at": "NULL",
}


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


def _configured_db_path() -> Path | None:
    configured = os.environ.get("INBOX_DB_PATH")
    if configured:
        return Path(configured).expanduser().resolve()
    return None


def _configured_state_root() -> Path:
    configured = os.environ.get("INBOX_STATE_DIR")
    if configured:
        return Path(configured).expanduser().resolve()

    configured_db_path = _configured_db_path()
    if configured_db_path is not None:
        return configured_db_path.parent

    shared_root = os.environ.get(SHARED_INBOX_ROOT_ENV)
    if shared_root:
        return Path(shared_root).expanduser().resolve()

    if DEFAULT_SHARED_INBOX_ROOT.exists():
        return DEFAULT_SHARED_INBOX_ROOT.resolve()

    return get_user_root() / f".{CAPABILITY_NAME}"


def _delegated_user() -> str | None:
    delegated = os.environ.get("DELEGATED_USER", "").strip().lower()
    return delegated or None


def _mailbox_selector() -> str | None:
    for name in ("DELEGATED_USER", "DELEGATED_INBOX_USER", "AECH_SHARED_INBOX_MAILBOX"):
        value = os.environ.get(name, "").strip().lower()
        if value:
            return value
    return None


def _looks_like_mailbox_state_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    if (path / STATE_DB_NAME).exists():
        return True
    return any((path / name).is_dir() for name in STATE_HINT_DIRS)


def _available_mailbox_state_dirs(root: Path) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []

    mailbox_dirs: list[Path] = []
    for child in sorted(root.iterdir(), key=lambda item: item.name.lower()):
        if child.is_dir() and _looks_like_mailbox_state_dir(child):
            mailbox_dirs.append(child.resolve())
    return mailbox_dirs


def _resolve_state_binding() -> dict[str, Any]:
    mailbox_selector = _mailbox_selector()
    explicit_db_path = _configured_db_path()
    root_state_dir = _configured_state_root()
    available_mailbox_dirs = _available_mailbox_state_dirs(root_state_dir)
    available_mailboxes = [path.name for path in available_mailbox_dirs]

    if _looks_like_mailbox_state_dir(root_state_dir):
        state_dir = root_state_dir
    elif mailbox_selector:
        candidate = (root_state_dir / mailbox_selector).resolve()
        if _looks_like_mailbox_state_dir(candidate):
            state_dir = candidate
        elif available_mailboxes:
            raise RuntimeError(
                f"Configured mailbox selector '{mailbox_selector}' does not match a mailbox state under "
                f"{root_state_dir}. Available mailboxes: {', '.join(available_mailboxes)}"
            )
        else:
            state_dir = root_state_dir
    elif len(available_mailbox_dirs) == 1:
        state_dir = available_mailbox_dirs[0]
    elif len(available_mailbox_dirs) > 1:
        raise RuntimeError(
            f"Ambiguous inbox state root at {root_state_dir}. "
            "Set INBOX_STATE_DIR to a mailbox-scoped directory, set INBOX_DB_PATH directly, "
            f"or set DELEGATED_USER to select one of: {', '.join(available_mailboxes)}"
        )
    else:
        state_dir = root_state_dir

    return {
        "state_dir": state_dir,
        "db_path": explicit_db_path or (state_dir / STATE_DB_NAME),
    }


def get_state_dir() -> Path:
    return _resolve_state_binding()["state_dir"]


def get_attachment_store_dir() -> Path:
    """Directory for canonical attachment blobs owned by inbox-assistant."""
    return get_state_dir() / "attachments"


def resolve_state_path(path_str: str) -> Path:
    """Resolve a stored state-relative or absolute path."""
    path = Path(path_str).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (get_state_dir() / path).resolve()


def get_db_path() -> Path:
    """
    Get the path to the capability-owned SQLite state.

    Override with `INBOX_DB_PATH` if needed.
    """
    return _resolve_state_binding()["db_path"]


def init_db(db_path: Optional[Path] = None) -> None:
    """Initialize the database schema."""
    db_path = (db_path or get_db_path()).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Enable WAL mode for concurrency
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA foreign_keys=ON;")
    
    # Canonical email corpus
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS emails (
        id TEXT PRIMARY KEY,
        conversation_id TEXT,
        internet_message_id TEXT,
        subject TEXT,
        sender TEXT NOT NULL,
        to_emails TEXT NOT NULL DEFAULT '[]', -- JSON array
        cc_emails TEXT NOT NULL DEFAULT '[]', -- JSON array
        bcc_emails TEXT NOT NULL DEFAULT '[]', -- JSON array
        received_at DATETIME NOT NULL,
        body_preview TEXT,
        body_html TEXT,
        body_markdown TEXT,        -- Semantic markdown main content
        signature_block TEXT,      -- Preserved sender signature
        thread_summary TEXT,       -- LLM-generated thread summary
        body_hash TEXT,
        has_attachments BOOLEAN DEFAULT 0,
        is_draft BOOLEAN DEFAULT 0,
        is_read BOOLEAN DEFAULT 0,
        etag TEXT,
        web_link TEXT,
        mail_folder_id TEXT,
        mail_folder_name TEXT,
        outlook_categories TEXT NOT NULL DEFAULT '[]', -- JSON array of applied Outlook categories
        urgency TEXT DEFAULT 'someday' CHECK(urgency IN ('immediate', 'today', 'this_week', 'someday')),
        processed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    # Indexes for common email queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_conversation ON emails(conversation_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_sender ON emails(sender)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_received ON emails(received_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_urgency ON emails(urgency)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_processed ON emails(processed_at)")
    _ensure_columns(cursor, "emails", {
        "bcc_emails": "TEXT NOT NULL DEFAULT '[]'",
        "is_draft": "BOOLEAN DEFAULT 0",
        "mail_folder_id": "TEXT",
        "mail_folder_name": "TEXT",
        "updated_at": "DATETIME DEFAULT CURRENT_TIMESTAMP",
    })

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
        storage_path TEXT,
        downloaded_at DATETIME,
        stored_at DATETIME,
        extracted_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(email_id) REFERENCES emails(id) ON DELETE CASCADE
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_attachments_email ON attachments(email_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_attachments_hash ON attachments(content_hash)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_attachments_status ON attachments(extraction_status)")
    _ensure_columns(cursor, "attachments", {
        "storage_path": "TEXT",
        "stored_at": "DATETIME",
        "updated_at": "DATETIME DEFAULT CURRENT_TIMESTAMP",
    })

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

    # Unified structured facts extracted from email and attachment content.
    _ensure_facts_table(cursor)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_source ON facts(source_type, source_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_type ON facts(fact_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_status ON facts(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_due ON facts(due_date)")
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS emails_delete_facts
    AFTER DELETE ON emails BEGIN
        DELETE FROM facts WHERE source_type = 'email' AND source_id = old.id;
    END;
    """)
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS attachments_delete_facts
    AFTER DELETE ON attachments BEGIN
        DELETE FROM facts WHERE source_type = 'attachment' AND source_id = old.id;
    END;
    """)

    # Compliance-safe retained intelligence not tied directly to a single email row.
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS derived_learnings (
        id TEXT PRIMARY KEY,
        learning_key TEXT NOT NULL UNIQUE,
        learning_type TEXT NOT NULL CHECK(learning_type IN ('interest_signal', 'preference', 'relationship', 'pattern', 'other')),
        summary TEXT NOT NULL,
        confidence REAL DEFAULT 0.5 CHECK(confidence >= 0.0 AND confidence <= 1.0),
        metadata_json TEXT,
        seen_count INTEGER DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_derived_learnings_type ON derived_learnings(learning_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_derived_learnings_seen ON derived_learnings(last_seen_at)")

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


def _create_facts_table(cursor: sqlite3.Cursor) -> None:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS facts (
        id TEXT PRIMARY KEY,
        source_type TEXT NOT NULL CHECK(source_type IN ('email', 'attachment')),
        source_id TEXT NOT NULL,
        fact_type TEXT NOT NULL CHECK(fact_type IN (
            'decision',
            'commitment',
            'action_item',
            'tax_id', 'business_number', 'account_number',
            'amount', 'address', 'phone', 'deadline',
            'person_name', 'company_name', 'contract_number',
            'preference',
            'relationship',
            'pattern',
            'other'
        )),
        fact_value TEXT NOT NULL,
        context TEXT,
        confidence REAL DEFAULT 0.8 CHECK(confidence >= 0.0 AND confidence <= 1.0),
        entity_normalized TEXT,
        metadata_json TEXT,
        status TEXT DEFAULT 'active' CHECK(status IN ('active', 'resolved', 'expired')),
        due_date DATETIME,
        extracted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        resolved_at DATETIME
    )
    """)


def _ensure_facts_table(cursor: sqlite3.Cursor) -> None:
    existing = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'facts'"
    ).fetchone()
    if not existing:
        _create_facts_table(cursor)
        return

    legacy_fk = cursor.execute("PRAGMA foreign_key_list(facts)").fetchall()
    if not legacy_fk:
        return

    logger.info("Migrating facts table to polymorphic source references")
    cursor.execute("DROP TRIGGER IF EXISTS facts_ai_fts")
    cursor.execute("DROP TRIGGER IF EXISTS facts_ad_fts")
    cursor.execute("DROP TRIGGER IF EXISTS facts_au_fts")
    cursor.execute("DROP TABLE IF EXISTS facts_fts")
    cursor.execute("ALTER TABLE facts RENAME TO facts_legacy")
    _create_facts_table(cursor)

    existing_columns = {
        row[1] for row in cursor.execute("PRAGMA table_info(facts_legacy)")
    }
    select_columns = [
        column if column in existing_columns else FACTS_COLUMN_DEFAULTS[column]
        for column in FACTS_COLUMNS
    ]
    cursor.execute(
        f"""
        INSERT INTO facts ({", ".join(FACTS_COLUMNS)})
        SELECT {", ".join(select_columns)}
        FROM facts_legacy
        """
    )
    cursor.execute("DROP TABLE facts_legacy")


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

    # Create FTS5 index for derived learnings (survives email deletion).
    cursor.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS derived_learnings_fts
    USING fts5(
        id UNINDEXED,
        summary,
        metadata_json,
        tokenize = 'porter'
    )
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS derived_learnings_ai_fts
    AFTER INSERT ON derived_learnings BEGIN
        INSERT OR REPLACE INTO derived_learnings_fts(id, summary, metadata_json)
        VALUES (new.id, new.summary, new.metadata_json);
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS derived_learnings_ad_fts
    AFTER DELETE ON derived_learnings BEGIN
        DELETE FROM derived_learnings_fts WHERE id = old.id;
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS derived_learnings_au_fts
    AFTER UPDATE ON derived_learnings BEGIN
        DELETE FROM derived_learnings_fts WHERE id = old.id;
        INSERT OR REPLACE INTO derived_learnings_fts(id, summary, metadata_json)
        VALUES (new.id, new.summary, new.metadata_json);
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
