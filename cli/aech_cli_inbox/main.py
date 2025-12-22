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
    mode: str = typer.Option("fts", help="Search mode: fts, vector, or hybrid"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON")
):
    """Search emails and attachments in the local DB."""
    # Use hybrid search if mode is vector or hybrid
    if mode in ("vector", "hybrid"):
        try:
            # Import from src module
            import sys
            from pathlib import Path

            # Add repo root to path if needed
            repo_root = Path(__file__).parent.parent.parent
            if repo_root.exists() and str(repo_root) not in sys.path:
                sys.path.insert(0, str(repo_root))

            from src.search import search_with_source_details

            results = search_with_source_details(query, limit, mode)

            if json_output:
                typer.echo(json.dumps(results, default=str))
            else:
                if not results:
                    typer.echo("No results.")
                    return

                for r in results:
                    score_info = f"score={r['score']:.3f}"
                    if r.get('fts_rank'):
                        score_info += f" fts={r['fts_rank']}"
                    if r.get('vector_rank'):
                        score_info += f" vec={r['vector_rank']}"

                    if r['source_type'] == 'email':
                        typer.echo(f"[EMAIL] {r.get('email_subject', 'N/A')} ({score_info})")
                        typer.echo(f"  From: {r.get('email_sender', 'N/A')} | {r.get('email_date', '')}")
                    elif r['source_type'] == 'virtual_email':
                        typer.echo(f"[VIRTUAL] {r.get('email_subject', '(from forward)')} ({score_info})")
                        typer.echo(f"  Original sender: {r.get('email_sender', 'N/A')} | {r.get('email_date', '')}")
                        if r.get('forwarded_by'):
                            typer.echo(f"  Forwarded by: {r.get('forwarded_by')} on {r.get('forwarded_at', '')}")
                    else:
                        typer.echo(f"[ATTACHMENT] {r.get('filename', 'N/A')} ({score_info})")
                        typer.echo(f"  In: {r.get('email_subject', 'N/A')}")

                    typer.echo(f"  Preview: {r['content_preview'][:100]}...")
                    typer.echo("")
            return

        except ImportError as e:
            typer.echo(f"Hybrid search not available: {e}", err=True)
            typer.echo("Falling back to FTS search.", err=True)

    # FTS-only search (original behavior)
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


@app.command("init-db")
def init_db_cmd():
    """Initialize or migrate the database schema."""
    import sys
    from pathlib import Path

    repo_root = Path(__file__).parent.parent.parent
    if repo_root.exists() and str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from src.database import init_db

    db_path = get_db_path()
    typer.echo(f"Initializing database at {db_path}...")
    init_db(db_path)
    typer.echo("Database schema initialized/migrated successfully.")


@app.command("sync-status")
def sync_status(
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Show the sync status for all folders."""
    conn = connect_db()
    cursor = conn.cursor()

    # Get sync state
    cursor.execute("""
        SELECT folder_id, last_sync_at, sync_type, messages_synced,
               CASE WHEN delta_link IS NOT NULL THEN 1 ELSE 0 END as has_delta_link
        FROM sync_state
        ORDER BY last_sync_at DESC
    """)
    rows = cursor.fetchall()
    conn.close()

    status = [dict(r) for r in rows]

    if json_output:
        typer.echo(json.dumps(status, default=str))
    else:
        if not status:
            typer.echo("No sync state found. Run a full sync first.")
            return
        typer.echo(f"{'Folder ID':<40} {'Last Sync':<20} {'Type':<8} {'Messages':<10} {'Delta'}")
        typer.echo("-" * 90)
        for s in status:
            delta = "Yes" if s["has_delta_link"] else "No"
            typer.echo(f"{s['folder_id'][:38]:<40} {str(s['last_sync_at']):<20} {s['sync_type']:<8} {s['messages_synced']:<10} {delta}")


@app.command()
def stats(
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Show corpus statistics."""
    conn = connect_db()
    cursor = conn.cursor()

    stats_data = {}

    # Email counts
    cursor.execute("SELECT COUNT(*) FROM emails")
    stats_data["total_emails"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM emails WHERE body_text IS NOT NULL")
    stats_data["emails_with_body"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM emails WHERE has_attachments = 1")
    stats_data["emails_with_attachments"] = cursor.fetchone()[0]

    # Attachment counts
    cursor.execute("SELECT COUNT(*) FROM attachments")
    stats_data["total_attachments"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM attachments WHERE extraction_status = 'success'")
    stats_data["attachments_extracted"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM attachments WHERE extraction_status = 'pending'")
    stats_data["attachments_pending"] = cursor.fetchone()[0]

    # Chunk counts
    cursor.execute("SELECT COUNT(*) FROM chunks")
    stats_data["total_chunks"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM chunks WHERE embedding IS NOT NULL")
    stats_data["chunks_with_embeddings"] = cursor.fetchone()[0]

    # Sync state
    cursor.execute("SELECT COUNT(*) FROM sync_state")
    stats_data["folders_synced"] = cursor.fetchone()[0]

    cursor.execute("SELECT SUM(messages_synced) FROM sync_state")
    result = cursor.fetchone()[0]
    stats_data["total_synced_messages"] = result if result else 0

    conn.close()

    if json_output:
        typer.echo(json.dumps(stats_data))
    else:
        typer.echo("=== Email Corpus Statistics ===")
        typer.echo(f"Total emails:              {stats_data['total_emails']:,}")
        typer.echo(f"Emails with full body:     {stats_data['emails_with_body']:,}")
        typer.echo(f"Emails with attachments:   {stats_data['emails_with_attachments']:,}")
        typer.echo("")
        typer.echo("=== Attachments ===")
        typer.echo(f"Total attachments:         {stats_data['total_attachments']:,}")
        typer.echo(f"Extracted:                 {stats_data['attachments_extracted']:,}")
        typer.echo(f"Pending extraction:        {stats_data['attachments_pending']:,}")
        typer.echo("")
        typer.echo("=== Chunks & Embeddings ===")
        typer.echo(f"Total chunks:              {stats_data['total_chunks']:,}")
        typer.echo(f"Chunks with embeddings:    {stats_data['chunks_with_embeddings']:,}")
        typer.echo("")
        typer.echo("=== Sync State ===")
        typer.echo(f"Folders synced:            {stats_data['folders_synced']:,}")
        typer.echo(f"Total synced messages:     {stats_data['total_synced_messages']:,}")


@app.command("attachment-status")
def attachment_status(
    limit: int = typer.Option(20, help="Number of attachments to list"),
    status_filter: str = typer.Option(None, "--status", help="Filter by status (pending/success/failed/unsupported)"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Show attachment extraction status."""
    conn = connect_db()
    cursor = conn.cursor()

    query = """
        SELECT a.id, a.email_id, a.filename, a.content_type, a.size_bytes,
               a.extraction_status, a.extraction_error, a.extracted_at,
               e.subject as email_subject
        FROM attachments a
        LEFT JOIN emails e ON a.email_id = e.id
    """
    params = []

    if status_filter:
        query += " WHERE a.extraction_status = ?"
        params.append(status_filter)

    query += " ORDER BY a.downloaded_at DESC LIMIT ?"
    params.append(limit)

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    attachments = [dict(r) for r in rows]

    if json_output:
        typer.echo(json.dumps(attachments, default=str))
    else:
        if not attachments:
            typer.echo("No attachments found.")
            return

        # Show summary by status first
        status_counts = {}
        for a in attachments:
            s = a["extraction_status"] or "unknown"
            status_counts[s] = status_counts.get(s, 0) + 1

        typer.echo("Status summary: " + ", ".join(f"{k}: {v}" for k, v in status_counts.items()))
        typer.echo("")

        for a in attachments:
            size_kb = (a["size_bytes"] or 0) / 1024
            typer.echo(f"[{a['extraction_status']}] {a['filename']} ({size_kb:.1f} KB)")
            typer.echo(f"  Email: {a['email_subject'][:50] if a['email_subject'] else 'N/A'}...")
            if a["extraction_error"]:
                typer.echo(f"  Error: {a['extraction_error']}")
            typer.echo("")

@app.command()
def sync(
    mode: str = typer.Option("delta", help="Sync mode: full or delta"),
    folder: str = typer.Option(None, help="Specific folder ID to sync (defaults to all)"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Sync emails from M365 mailbox."""
    import sys
    from pathlib import Path

    # Add repo root to path if needed
    repo_root = Path(__file__).parent.parent.parent
    if repo_root.exists() and str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from src.poller import GraphPoller

    try:
        poller = GraphPoller()
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        typer.echo("Set DELEGATED_USER environment variable.", err=True)
        raise typer.Exit(1)

    if folder:
        # Sync specific folder
        if mode == "full":
            typer.echo(f"Running full sync for folder {folder}...")
            count = poller.full_sync_folder(folder, folder)
            result = {"folder_id": folder, "mode": "full", "messages_synced": count}
        else:
            typer.echo(f"Running delta sync for folder {folder}...")
            added, removed = poller.delta_sync_folder(folder, folder)
            result = {"folder_id": folder, "mode": "delta", "added": added, "removed": removed}
    else:
        # Sync all folders
        typer.echo(f"Running {mode} sync for all folders...")
        result = poller.sync_all_folders()

    if json_output:
        typer.echo(json.dumps(result, default=str))
    else:
        if folder:
            if mode == "full":
                typer.echo(f"Synced {result['messages_synced']} messages from {folder}")
            else:
                typer.echo(f"Delta sync: +{result['added']} added, -{result['removed']} removed")
        else:
            typer.echo(f"Sync complete: {result.get('total_synced', 0)} messages across {result.get('folders_synced', 0)} folders")


@app.command()
def process(
    limit: int = typer.Option(100, help="Max items to process per batch"),
    skip_embeddings: bool = typer.Option(False, "--skip-embeddings", help="Skip embedding generation"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Process emails for chunking and embedding generation."""
    import sys
    from pathlib import Path

    repo_root = Path(__file__).parent.parent.parent
    if repo_root.exists() and str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from src.chunker import process_unindexed_emails, process_unindexed_attachments

    results = {}

    typer.echo("Processing unindexed emails...")
    email_result = process_unindexed_emails(limit)
    results["emails"] = email_result
    typer.echo(f"  Emails: {email_result['processed']} processed, {email_result.get('chunks_created', 0)} chunks, {email_result.get('virtual_emails', 0)} virtual")

    typer.echo("Processing unindexed attachments...")
    attach_result = process_unindexed_attachments(limit)
    results["attachments"] = attach_result
    typer.echo(f"  Attachments: {attach_result['processed']} processed, {attach_result.get('chunks_created', 0)} chunks")

    if not skip_embeddings:
        try:
            from src.embeddings import embed_pending_chunks

            typer.echo("Generating embeddings for new chunks...")
            embed_result = embed_pending_chunks(limit)
            results["embeddings"] = embed_result
            typer.echo(f"  Embeddings: {embed_result['processed']} generated, {embed_result['failed']} failed")
        except ImportError as e:
            typer.echo(f"  Embeddings skipped (sentence-transformers not installed): {e}", err=True)
            results["embeddings"] = {"skipped": True, "reason": str(e)}

    if json_output:
        typer.echo(json.dumps(results, default=str))


@app.command("extract-attachments")
def extract_attachments(
    limit: int = typer.Option(50, help="Max attachments to process"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Download and extract text from pending attachments."""
    import sys
    from pathlib import Path

    repo_root = Path(__file__).parent.parent.parent
    if repo_root.exists() and str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    try:
        from src.attachments import AttachmentProcessor

        processor = AttachmentProcessor()
        typer.echo(f"Processing up to {limit} pending attachments...")
        result = processor.process_pending_attachments(limit)

        if json_output:
            typer.echo(json.dumps(result, default=str))
        else:
            typer.echo(f"Processed: {result['processed']}, Failed: {result['failed']}, Skipped: {result['skipped']}")

    except ImportError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        typer.echo("Set DELEGATED_USER environment variable.", err=True)
        raise typer.Exit(1)


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
