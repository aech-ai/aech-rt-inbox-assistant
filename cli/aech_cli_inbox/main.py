import typer
import sqlite3
import json
import logging

from .state import (
    connect_db,
    get_db_path,
    read_preferences,
    set_preference_from_string,
    write_preferences,
)

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Query Inbox Assistant state and preferences.",
    no_args_is_help=True,
    add_completion=False,
)
prefs_app = typer.Typer(help="Manage `/home/agentaech/preferences.json`.", add_completion=False)
app.add_typer(prefs_app, name="prefs")

@app.command()
def list(
    limit: int = typer.Option(20, help="Number of emails to list"),
    include_read: bool = typer.Option(False, "--include-read", help="Include read emails"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON")
):
    """List ingested emails."""
    conn = connect_db()
    cursor = conn.cursor()
    
    query = "SELECT * FROM emails WHERE 1=1"
    params = []
    
    if not include_read:
        query += " AND is_read = 0"
        
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
    limit: int = typer.Option(20, help="Number of entries to list"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON")
):
    """View triage history."""
    conn = connect_db()
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
    limit: int = typer.Option(20, help="Number of results to return"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON")
):
    """Search emails in the local DB."""
    conn = connect_db()
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

@app.command()
def dbpath():
    """Get the absolute path to the user's database."""
    typer.echo(get_db_path())

@app.command()
def schema():
    """Get the database schema (CREATE TABLE statements)."""
    conn = connect_db()
    cursor = conn.cursor()
    
    # Get all table schemas
    cursor.execute("""
        SELECT sql FROM sqlite_master 
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    
    schemas = cursor.fetchall()
    conn.close()
    
    for row in schemas:
        if row[0]:
            typer.echo(row[0] + ";\n")

@app.command("reply-needed")
def reply_needed(
    limit: int = typer.Option(20, help="Number of messages to list"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """List messages currently marked as requiring a reply."""
    conn = connect_db()
    rows = conn.execute(
        """
        SELECT rt.message_id, rt.reason, rt.last_activity_at, e.subject, e.sender
        FROM reply_tracking rt
        JOIN emails e ON e.id = rt.message_id
        WHERE rt.requires_reply = 1
        ORDER BY rt.last_activity_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    items = [dict(r) for r in rows]
    if json_output:
        typer.echo(json.dumps(items, default=str))
        return
    if not items:
        typer.echo("No reply-needed items.")
        return
    for item in items:
        typer.echo(f"[{item['message_id']}] {item['subject']} ({item['sender']}) - {item.get('reason')}")


@prefs_app.command("show")
def prefs_show():
    """Show the current preferences.json."""
    typer.echo(json.dumps(read_preferences(), indent=2, sort_keys=True))


@prefs_app.command("set")
def prefs_set(
    key: str = typer.Argument(..., help="Preference key"),
    value: str = typer.Argument(..., help="Preference value (string/number/bool/JSON)"),
):
    """Set a preference key in preferences.json."""
    path = set_preference_from_string(key, value)
    typer.echo(str(path))


@prefs_app.command("unset")
def prefs_unset(
    key: str = typer.Argument(..., help="Preference key"),
):
    """Remove a preference key from preferences.json."""
    prefs = read_preferences()
    if key in prefs:
        prefs.pop(key, None)
        path = write_preferences(prefs)
        typer.echo(str(path))
    else:
        raise typer.Exit(1)

def run():
    import sys
    from pathlib import Path

    # Agent installer expects `--help` to return JSON manifest.
    if len(sys.argv) == 2 and sys.argv[1] in {"--help", "-h"}:
        typer.echo(Path(__file__).with_name("manifest.json").read_text())
        return

    app()

if __name__ == "__main__":
    app()
