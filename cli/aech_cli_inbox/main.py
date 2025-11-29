import typer
import pysqlite3 as sqlite3
import json
import os
from pathlib import Path
from typing import Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

app = typer.Typer()

# Determine DB path based on environment or default
# In the worker container, the user's data is mounted at /data/users/{email}/inbox-assistant/
# But the CLI runs in the worker, which might not have the same mount structure as the RT service.
# The spec says: "Mounts that user's data directory - data/users/user@example.com/inbox-assistant/"
# And "CLI is installed in Agent Aech and reads from the same SQLite database".
# So we need to know where the DB is.
# Assuming the agent worker mounts the user's home or data directory.
# Let's assume a standard path or pass it via env/flag.
# For now, let's look for it in a standard location relative to the user's home.

def init_db(db_path: Path):
    """Initialize the database schema."""
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

def _ensure_fts(cursor: sqlite3.Cursor) -> None:
    """
    Create an FTS5 index over email subject/body for search and keep it in sync.
    """
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

def get_db_connection(user_email: str):
    # Try to find the DB
    # Option 1: Env var
    db_path_str = os.environ.get("INBOX_DB_PATH")
    
    if not db_path_str:
        # Check common locations
        candidates = [
            # Container/Standard path
            Path(f"/data/users/{user_email}/inbox-assistant/inbox.sqlite"),
            # Local dev relative to current dir
            Path(f"data/users/{user_email}/inbox-assistant/inbox.sqlite"),
            # Local dev relative to repo root (sibling of cli)
            Path(f"../data/users/{user_email}/inbox-assistant/inbox.sqlite"),
            # Local dev relative to repo root (if running from root)
            Path(f"../data/users/{user_email}/inbox-assistant/inbox.sqlite").resolve()
        ]
        
        for candidate in candidates:
            if candidate.exists():
                db_path_str = str(candidate)
                break
        
        # If still not found, default to the local dev path (sibling) or standard
        if not db_path_str:
            # Default to creating it in ../data if we are in the repo
            # Or /data if we are in root
            if Path("../data").exists():
                 db_path_str = f"../data/users/{user_email}/inbox-assistant/inbox.sqlite"
            else:
                 db_path_str = f"data/users/{user_email}/inbox-assistant/inbox.sqlite"

    db_path = Path(db_path_str)

    if not db_path.exists():
        typer.echo(f"Database not found at {db_path}. Creating it...", err=True)
        init_db(db_path)
        
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

@app.command()
def list(
    user: str = typer.Option(..., help="Email of the user"),
    limit: int = typer.Option(20, help="Number of emails to list"),
    all_senders: bool = typer.Option(False, "--all-senders", help="Include all senders (not just whitelisted)"),
    include_read: bool = typer.Option(False, "--include-read", help="Include read emails"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON")
):
    """List ingested emails."""
    conn = get_db_connection(user)
    cursor = conn.cursor()
    
    query = "SELECT * FROM emails WHERE 1=1"
    params = []
    
    if not include_read:
        query += " AND is_read = 0"
        
    # Note: all_senders logic depends on how we ingest. 
    # If ingestion filters, then DB only has whitelisted.
    # If ingestion takes everything, we might filter here.
    # For now, assuming DB has what we want to show, or we just show everything in DB.
    
    query += " ORDER BY received_at DESC LIMIT ?"
    params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    emails = [dict(row) for row in rows]
    
    if json_output:
        typer.echo(json.dumps(emails, default=str))
    else:
        if not emails:
            typer.echo("No emails found.")
        for email in emails:
            typer.echo(f"[{email['id']}] {email['subject']} ({email['category']})")

@app.command()
def history(
    user: str = typer.Option(..., help="Email of the user"),
    limit: int = typer.Option(20, help="Number of entries to list"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON")
):
    """View triage history."""
    conn = get_db_connection(user)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT t.*, e.subject 
        FROM triage_log t 
        JOIN emails e ON t.email_id = e.id 
        ORDER BY t.timestamp DESC LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    
    logs = [dict(row) for row in rows]
    
    if json_output:
        typer.echo(json.dumps(logs, default=str))
    else:
        for log in logs:
            typer.echo(f"{log['timestamp']} - {log['action']} - {log['subject']} -> {log['destination_folder']} ({log['reason']})")

@app.command()
def move(
    message_id: str,
    folder: str,
    user: str = typer.Option(..., help="Email of the user"),
):
    """Manually move an email (override)."""
    # This just updates the DB to say we want to move it, or we need to actually move it?
    # The CLI should probably just update the DB or trigger an action.
    # But since the RT service is the one with the token, the CLI might not be able to move it directly unless it also has a token.
    # The spec says: "CLI is read/write to DB - can query and update status (approve/reject)"
    # So maybe we write a "pending_action" to the DB and the RT service picks it up?
    # Or we just update the category/folder in the DB and let the RT service sync?
    # For now, let's just print a message that this is not fully implemented in this phase.
    typer.echo("Manual move not yet implemented. Please use the Outlook client or wait for the next update.")

@app.command()
def search(
    query: str,
    user: str = typer.Option(..., help="Email of the user"),
    limit: int = typer.Option(20, help="Number of results to return"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON")
):
    """Search emails in the local DB."""
    conn = get_db_connection(user)
    cursor = conn.cursor()
    results = []

    # Prefer FTS if available
    try:
        cursor.execute(
            """
            SELECT e.id, e.subject, e.body_preview, e.received_at, e.category, e.is_read,
                   bm25(emails_fts) AS rank
            FROM emails_fts
            JOIN emails e ON emails_fts.id = e.id
            WHERE emails_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (query, limit),
        )
        rows = cursor.fetchall()
        results = [dict(row) for row in rows]
    except sqlite3.Error:
        # Fallback to LIKE search
        sql_query = f"%{query}%"
        cursor.execute(
            """
            SELECT id, subject, body_preview, received_at, category, is_read
            FROM emails
            WHERE subject LIKE ? OR body_preview LIKE ?
            ORDER BY received_at DESC
            LIMIT ?
            """,
            (sql_query, sql_query, limit),
        )
        rows = cursor.fetchall()
        results = [dict(row) for row in rows]

    if json_output:
        typer.echo(json.dumps(results, default=str))
    else:
        if not results:
            typer.echo("No results.")
        for email in results:
            typer.echo(f"[{email['id']}] {email['subject']} ({email.get('category')})")

def run():
    app()

if __name__ == "__main__":
    app()
