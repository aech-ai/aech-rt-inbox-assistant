import os
import typer
import sqlite3
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional

from .state import (
    connect_db,
    get_db_path,
    read_preferences,
    set_preference_from_string,
    write_preferences,
)
from src.database import init_db

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Query Inbox Assistant state and preferences.",
    no_args_is_help=True,
    add_completion=False,
)


@app.callback()
def startup():
    """Initialize database schema on every command."""
    init_db()


prefs_app = typer.Typer(help="Manage `/home/agentaech/preferences.json`.", add_completion=False)
app.add_typer(prefs_app, name="prefs")

categories_app = typer.Typer(
    help="Manage Outlook categories for email organization.",
    add_completion=False,
)
app.add_typer(categories_app, name="categories")

@app.command()
def list(
    limit: int = typer.Option(20, help="Number of emails to list"),
    include_read: bool = typer.Option(False, "--include-read", help="Include read emails"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON")
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

    if human:
        if not emails:
            typer.echo("No emails found.")
        for email in emails:
            typer.echo(f"Subject: {email['subject']}")
            typer.echo(f"  From: {email['sender']}")
            typer.echo(f"  Received: {email['received_at']}")
            typer.echo(f"  Category: {email['category']}")
            # Use web_link (folder-agnostic) if available, otherwise construct from id
            link = email.get('web_link')
            if not link and email.get('id'):
                from urllib.parse import quote
                link = f"https://outlook.office365.com/mail/inbox/id/{quote(email['id'], safe='')}"
            if link:
                typer.echo(f"  Link: {link}")
            typer.echo()
    else:
        typer.echo(json.dumps(emails, default=str))

@app.command()
def history(
    limit: int = typer.Option(20, help="Number of entries to list"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON")
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

    if human:
        for log in logs:
            typer.echo(f"{log['timestamp']} - {log['action']} - {log['subject']} -> {log['destination_folder']} ({log['reason']})")
    else:
        typer.echo(json.dumps(logs, default=str))

@app.command()
def search(
    query: str,
    limit: int = typer.Option(20, help="Number of results to return"),
    mode: str = typer.Option("fts", help="Search mode: fts, vector, or hybrid"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON")
):
    """Search emails and attachments in the local DB."""
    # Use hybrid search if mode is vector or hybrid
    if mode in ("vector", "hybrid"):
        try:
            from src.search import search_with_source_details

            results = search_with_source_details(query, limit, mode)

            if human:
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

                    # Show link for email-based results
                    if r['source_type'] in ('email', 'virtual_email'):
                        link = r.get('web_link')
                        if not link and r.get('email_id'):
                            from urllib.parse import quote
                            link = f"https://outlook.office365.com/mail/inbox/id/{quote(r['email_id'], safe='')}"
                        if link:
                            typer.echo(f"  Link: {link}")

                    typer.echo(f"  Preview: {r['content_preview'][:100]}...")
                    typer.echo("")
            else:
                typer.echo(json.dumps(results, default=str))
            return

        except ImportError as e:
            typer.echo(f"Hybrid search not available: {e}", err=True)
            typer.echo("Falling back to FTS search.", err=True)

    # FTS-only search (original behavior) - search both emails and attachments
    conn = connect_db()
    cursor = conn.cursor()
    email_results = []
    attachment_results = []

    # Search emails via FTS
    try:
        cursor.execute(
            """
            SELECT e.id, e.subject, e.body_preview, e.received_at, e.category, e.is_read,
                   e.sender, e.web_link, bm25(emails_fts) AS rank, 'email' as result_type
            FROM emails_fts
            JOIN emails e ON emails_fts.id = e.id
            WHERE emails_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (query, limit),
        )
        email_results = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error:
        # Fallback to LIKE search for emails
        sql_query = f"%{query}%"
        cursor.execute(
            """
            SELECT id, subject, body_preview, received_at, category, is_read, sender, web_link,
                   0 as rank, 'email' as result_type
            FROM emails
            WHERE subject LIKE ? OR body_preview LIKE ?
            ORDER BY received_at DESC
            LIMIT ?
            """,
            (sql_query, sql_query, limit),
        )
        email_results = [dict(row) for row in cursor.fetchall()]

    # Also search attachment extracted_text
    sql_query = f"%{query}%"
    try:
        cursor.execute(
            """
            SELECT a.id, a.filename, a.extracted_text, e.id as email_id, e.subject as email_subject,
                   e.sender, e.received_at, e.web_link, 'attachment' as result_type
            FROM attachments a
            JOIN emails e ON a.email_id = e.id
            WHERE a.extracted_text LIKE ?
            LIMIT ?
            """,
            (sql_query, limit),
        )
        attachment_results = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error:
        pass  # Attachment search failed, continue with email results only

    conn.close()

    if human:
        if not email_results and not attachment_results:
            typer.echo("No results.")

        for email in email_results:
            typer.echo(f"[EMAIL] {email['subject']}")
            typer.echo(f"  From: {email.get('sender', 'N/A')}")
            typer.echo(f"  Received: {email['received_at']}")
            typer.echo(f"  Category: {email.get('category', 'N/A')}")
            link = email.get('web_link')
            if not link and email.get('id'):
                from urllib.parse import quote
                link = f"https://outlook.office365.com/mail/inbox/id/{quote(email['id'], safe='')}"
            if link:
                typer.echo(f"  Link: {link}")
            typer.echo()

        for att in attachment_results:
            typer.echo(f"[ATTACHMENT] {att['filename']}")
            typer.echo(f"  In email: {att.get('email_subject', 'N/A')}")
            typer.echo(f"  From: {att.get('sender', 'N/A')}")
            typer.echo(f"  Date: {att.get('received_at', 'N/A')}")
            # Show preview of extracted text
            text_preview = (att.get('extracted_text') or '')[:200].replace('\n', ' ')
            if text_preview:
                typer.echo(f"  Preview: {text_preview}...")
            link = att.get('web_link')
            if not link and att.get('email_id'):
                from urllib.parse import quote
                link = f"https://outlook.office365.com/mail/inbox/id/{quote(att['email_id'], safe='')}"
            if link:
                typer.echo(f"  Link: {link}")
            typer.echo()
    else:
        typer.echo(json.dumps({"emails": email_results, "attachments": attachment_results}, default=str))

@app.command()
def dbpath():
    """Get the absolute path to the user's database."""
    typer.echo(get_db_path())


@app.command("backfill-bodies")
def backfill_bodies(
    limit: int = typer.Option(100, help="Number of emails to backfill"),
    concurrency: int = typer.Option(10, "--concurrency", help="Number of parallel fetches"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """
    Backfill missing email bodies from Graph API.

    Fetches full body content for emails where body_text is NULL.
    This is needed if emails were synced before body fetching was enabled.
    """
    import asyncio
    import hashlib
    import threading

    try:
        from src.poller import GraphPoller
    except ImportError:
        typer.echo("Error: GraphPoller not available. Run from inbox-assistant repo.", err=True)
        raise typer.Exit(1)

    conn = connect_db()

    # Find emails with missing bodies
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, subject FROM emails
        WHERE body_text IS NULL OR body_text = ''
        ORDER BY received_at DESC
        LIMIT ?
    """, (limit,))
    emails_to_backfill = cursor.fetchall()
    conn.close()

    if not emails_to_backfill:
        if human:
            typer.echo("All emails already have body content.")
        else:
            typer.echo(json.dumps({"backfilled": 0, "failed": 0}))
        return

    if human:
        typer.echo(f"Backfilling {len(emails_to_backfill)} emails (concurrency: {concurrency})...")

    poller = GraphPoller()
    total = len(emails_to_backfill)

    # Thread-safe counters and results queue
    lock = threading.Lock()
    counters = {"backfilled": 0, "failed": 0, "completed": 0}
    updates = []  # Collect updates for batch commit

    async def fetch_one(email_id: str, subject: str, semaphore: asyncio.Semaphore):
        async with semaphore:
            try:
                # Run sync method in thread pool
                body_text, body_html = await asyncio.to_thread(
                    poller._get_message_body, email_id
                )
                if body_text:
                    body_hash = hashlib.sha256(body_text.encode()).hexdigest()[:16]
                    with lock:
                        updates.append((body_text, body_html, body_hash, email_id))
                        counters["backfilled"] += 1
                else:
                    with lock:
                        counters["failed"] += 1
            except Exception:
                with lock:
                    counters["failed"] += 1

            with lock:
                counters["completed"] += 1
                if human:
                    pct = int(counters["completed"] / total * 100)
                    bar_len = 30
                    filled = int(bar_len * counters["completed"] / total)
                    bar = "█" * filled + "░" * (bar_len - filled)
                    subj = (subject or "(no subject)")[:25]
                    print(f"\r  [{bar}] {pct}% ({counters['completed']}/{total}) {subj}...", end="", flush=True)

    async def run_all():
        semaphore = asyncio.Semaphore(concurrency)
        tasks = [
            fetch_one(email_id, subject, semaphore)
            for email_id, subject in emails_to_backfill
        ]
        await asyncio.gather(*tasks)

    asyncio.run(run_all())

    if human:
        print("\r\033[K", end="")  # Clear progress line

    # Batch commit all updates
    if updates:
        conn = connect_db()
        conn.executemany("""
            UPDATE emails SET body_text = ?, body_html = ?, body_hash = ?
            WHERE id = ?
        """, updates)
        conn.commit()

        # Also update FTS index
        if human:
            typer.echo("Updating FTS index...")

        try:
            conn.execute("INSERT INTO emails_fts(emails_fts) VALUES('rebuild')")
            conn.commit()
        except Exception as e:
            if human:
                typer.echo(f"FTS rebuild failed: {e}")

        conn.close()

    results = {"backfilled": counters["backfilled"], "failed": counters["failed"]}

    if human:
        typer.echo(f"\nResults:")
        typer.echo(f"  Backfilled: {results['backfilled']}")
        typer.echo(f"  Failed:     {results['failed']}")
    else:
        typer.echo(json.dumps(results))


@app.command("sync-status")
def sync_status(
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
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

    if human:
        if not status:
            typer.echo("No sync state found. Run a full sync first.")
            return
        typer.echo(f"{'Folder ID':<40} {'Last Sync':<20} {'Type':<8} {'Messages':<10} {'Delta'}")
        typer.echo("-" * 90)
        for s in status:
            delta = "Yes" if s["has_delta_link"] else "No"
            typer.echo(f"{s['folder_id'][:38]:<40} {str(s['last_sync_at']):<20} {s['sync_type']:<8} {s['messages_synced']:<10} {delta}")
    else:
        typer.echo(json.dumps(status, default=str))


@app.command("sync")
def sync_emails(
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
    no_bodies: bool = typer.Option(False, "--no-bodies", help="Skip fetching full bodies during sync (metadata only)"),
    full: bool = typer.Option(False, "--full", help="Force full sync instead of delta"),
):
    """
    Sync emails from Microsoft Graph.

    Uses delta sync to efficiently fetch only changes since last sync.
    Handles new emails, updates, and deletions.
    Use --full to force a complete resync.
    """
    try:
        from src.poller import GraphPoller
    except ImportError:
        typer.echo("Error: GraphPoller not available. Run from inbox-assistant repo.", err=True)
        raise typer.Exit(1)

    poller = GraphPoller()

    if human:
        typer.echo("Syncing emails from Microsoft Graph...")

    if full:
        # Reset sync state to force full sync
        conn = connect_db()
        conn.execute("DELETE FROM sync_state")
        conn.commit()
        conn.close()
        if human:
            typer.echo("  Cleared sync state, forcing full sync...")

    # Track current folder for message callback context
    current_folder_info = {"name": "", "num": 0, "total": 0}

    def progress(current: int, total: int, folder_name: str):
        if human:
            current_folder_info["name"] = folder_name
            current_folder_info["num"] = current
            current_folder_info["total"] = total
            pct = int(current / total * 100)
            # Clear line and show folder progress
            print(f"\r\033[K  Syncing folder {current}/{total}: {folder_name[:30]:<30} ({pct}%)", end="", flush=True)

    def message_progress(count: int, subject: str):
        if human:
            folder = current_folder_info["name"][:20]
            # Truncate subject to fit on line
            subj = subject[:35] if subject else "(no subject)"
            print(f"\r\033[K  [{folder}] {count} msgs - {subj}...", end="", flush=True)

    results = poller.sync_all_folders(
        fetch_body=not no_bodies,
        progress_callback=progress if human else None,
        message_callback=message_progress if human else None,
    )

    if human:
        print("\r\033[K", end="")  # Clear progress line

    if human:
        typer.echo(f"\nSync complete:")
        typer.echo(f"  Folders synced: {results['folders_synced']}")
        typer.echo(f"  Total messages: {results['total_messages']}")
        typer.echo(f"  Deleted:        {results.get('total_deleted', 0)}")
        if results.get('folder_results'):
            typer.echo(f"\nPer folder:")
            for fr in results['folder_results']:
                typer.echo(f"  {fr['folder_name']}: {fr['messages']} msgs ({fr['sync_type']})")
    else:
        typer.echo(json.dumps(results, default=str))


@app.command()
def stats(
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
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

    cursor.execute("SELECT COUNT(*) FROM attachments WHERE extraction_status = 'completed'")
    stats_data["attachments_extracted"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM attachments WHERE extraction_status = 'pending'")
    stats_data["attachments_pending"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM attachments WHERE extraction_status = 'failed'")
    stats_data["attachments_failed"] = cursor.fetchone()[0]

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

    if human:
        typer.echo("=== Email Corpus Statistics ===")
        typer.echo(f"Total emails:              {stats_data['total_emails']:,}")
        typer.echo(f"Emails with full body:     {stats_data['emails_with_body']:,}")
        typer.echo(f"Emails with attachments:   {stats_data['emails_with_attachments']:,}")
        typer.echo("")
        typer.echo("=== Attachments ===")
        typer.echo(f"Total attachments:         {stats_data['total_attachments']:,}")
        typer.echo(f"Extracted:                 {stats_data['attachments_extracted']:,}")
        typer.echo(f"Pending extraction:        {stats_data['attachments_pending']:,}")
        typer.echo(f"Failed extraction:         {stats_data['attachments_failed']:,}")
        typer.echo("")
        typer.echo("=== Chunks & Embeddings ===")
        typer.echo(f"Total chunks:              {stats_data['total_chunks']:,}")
        typer.echo(f"Chunks with embeddings:    {stats_data['chunks_with_embeddings']:,}")
        typer.echo("")
        typer.echo("=== Sync State ===")
        typer.echo(f"Folders synced:            {stats_data['folders_synced']:,}")
        typer.echo(f"Total synced messages:     {stats_data['total_synced_messages']:,}")
    else:
        typer.echo(json.dumps(stats_data))


@app.command("attachment-status")
def attachment_status(
    limit: int = typer.Option(20, help="Number of attachments to list"),
    status_filter: str = typer.Option(None, "--status", help="Filter by status (pending/completed/failed/skipped/extracting)"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
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

    if human:
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
    else:
        typer.echo(json.dumps(attachments, default=str))


@app.command("extract-attachments")
def extract_attachments(
    limit: int = typer.Option(50, help="Number of attachments to process"),
    concurrency: int = typer.Option(5, "--concurrency", help="Number of parallel workers"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """
    Extract text from pending attachments.

    Downloads attachments from Graph API and extracts text using aech-cli-documents.
    Extracted text is stored in the database for full-text search.
    """
    import asyncio

    try:
        from src.attachments import AttachmentProcessor
    except ImportError:
        typer.echo("Error: AttachmentProcessor not available. Run from inbox-assistant repo.", err=True)
        raise typer.Exit(1)

    processor = AttachmentProcessor()

    if human:
        typer.echo(f"Processing up to {limit} pending attachments (concurrency: {concurrency})...")

        def progress(current: int, total: int, filename: str):
            pct = int(current / total * 100) if total > 0 else 0
            bar_len = 30
            filled = int(bar_len * current / total) if total > 0 else 0
            bar = "█" * filled + "░" * (bar_len - filled)
            name = (filename or "unknown")[:25]
            print(f"\r  [{bar}] {pct}% ({current}/{total}) {name}...", end="", flush=True)

        results = asyncio.run(
            processor.process_pending_attachments_async(
                limit=limit, concurrency=concurrency, progress_callback=progress
            )
        )
        print("\r\033[K", end="")  # Clear progress line
    else:
        results = asyncio.run(
            processor.process_pending_attachments_async(limit=limit, concurrency=concurrency)
        )

    if human:
        typer.echo(f"\nResults:")
        typer.echo(f"  Completed:   {results['completed']}")
        typer.echo(f"  Failed:      {results['failed']}")
        typer.echo(f"  Skipped:     {results['skipped']}")
    else:
        typer.echo(json.dumps(results))


@app.command("index")
def index_content(
    limit: int = typer.Option(500, help="Number of items to process per type"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """
    Index emails and attachments for full-text search.

    Creates searchable chunks from unindexed emails and attachment text.
    Run this after extract-attachments to make content searchable.
    """
    try:
        from src.chunker import process_unindexed_emails, process_unindexed_attachments
    except ImportError:
        typer.echo("Error: Chunker not available. Run from inbox-assistant repo.", err=True)
        raise typer.Exit(1)

    if human:
        typer.echo(f"Indexing up to {limit} items per type...")
        print("  Processing emails...", end="", flush=True)

    email_results = process_unindexed_emails(limit=limit)

    if human:
        print(f"\r\033[K  Emails: {email_results['processed']} processed, {email_results['chunks_created']} chunks")
        print("  Processing attachments...", end="", flush=True)

    attachment_results = process_unindexed_attachments(limit=limit)

    if human:
        print(f"\r\033[K  Attachments: {attachment_results['processed']} processed, {attachment_results['chunks_created']} chunks")

    combined = {
        "emails_processed": email_results["processed"],
        "emails_skipped": email_results["skipped"],
        "email_chunks": email_results["chunks_created"],
        "virtual_emails": email_results.get("virtual_emails", 0),
        "attachments_processed": attachment_results["processed"],
        "attachment_chunks": attachment_results["chunks_created"],
    }

    if human:
        if combined['emails_skipped'] > 0 or combined['virtual_emails'] > 0:
            typer.echo(f"\n  Skipped: {combined['emails_skipped']}, Virtual: {combined['virtual_emails']} (from forwards)")
    else:
        typer.echo(json.dumps(combined))


@app.command("extract-content")
def extract_content(
    limit: int = typer.Option(100, help="Number of emails to process"),
    concurrency: int = typer.Option(10, "--concurrency", help="Number of parallel LLM calls"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """
    Extract clean content from emails using LLM.

    Runs content extraction on emails where extracted_body is NULL.
    This is required before 'index' since chunking requires extracted content.
    """
    import asyncio
    import threading

    conn = connect_db()
    cursor = conn.cursor()

    # Find emails without extracted_body
    cursor.execute("""
        SELECT id, conversation_id, subject, sender, received_at,
               body_text, body_preview, to_emails, cc_emails
        FROM emails
        WHERE extracted_body IS NULL
          AND (body_text IS NOT NULL OR body_preview IS NOT NULL)
        LIMIT ?
    """, (limit,))

    emails = [dict(row) for row in cursor.fetchall()]
    conn.close()

    if not emails:
        if human:
            typer.echo("No emails need content extraction.")
        else:
            typer.echo(json.dumps({"processed": 0, "failed": 0, "remaining": 0}))
        return

    # Count remaining
    conn = connect_db()
    remaining = conn.execute("""
        SELECT COUNT(*) FROM emails
        WHERE extracted_body IS NULL
          AND (body_text IS NOT NULL OR body_preview IS NOT NULL)
    """).fetchone()[0]
    conn.close()

    if human:
        typer.echo(f"Processing {len(emails)} emails ({remaining} total without extracted content)...")
        typer.echo(f"Concurrency: {concurrency}")

    try:
        from src.working_memory.updater import WorkingMemoryUpdater
    except ImportError:
        typer.echo("Error: WorkingMemoryUpdater not available.", err=True)
        raise typer.Exit(1)

    user_email = os.getenv("DELEGATED_USER", "")
    updater = WorkingMemoryUpdater(user_email)

    # Thread-safe counters
    lock = threading.Lock()
    counters = {"success": 0, "failed": 0, "completed": 0}
    total = len(emails)

    async def process_one(email: dict, semaphore: asyncio.Semaphore):
        async with semaphore:
            try:
                await updater.process_email(email)
                with lock:
                    counters["success"] += 1
                    counters["completed"] += 1
                    if human:
                        pct = int(counters["completed"] / total * 100)
                        bar_len = 30
                        filled = int(bar_len * counters["completed"] / total)
                        bar = "█" * filled + "░" * (bar_len - filled)
                        print(f"\r  [{bar}] {pct}% ({counters['completed']}/{total})", end="", flush=True)
            except Exception as e:
                with lock:
                    counters["failed"] += 1
                    counters["completed"] += 1
                    if human:
                        print()  # newline before error
                        typer.echo(f"  ✗ {email.get('subject', 'Unknown')[:50]}... ({e})")

    async def process_all():
        semaphore = asyncio.Semaphore(concurrency)
        tasks = [process_one(email, semaphore) for email in emails]
        await asyncio.gather(*tasks)

    asyncio.run(process_all())

    if human:
        print()  # newline after progress bar

    results = {
        "processed": counters["success"],
        "failed": counters["failed"],
        "remaining": remaining - counters["success"],
    }

    if human:
        typer.echo(f"Results:")
        typer.echo(f"  Processed: {results['processed']}")
        typer.echo(f"  Failed:    {results['failed']}")
        typer.echo(f"  Remaining: {results['remaining']}")

        if results['remaining'] > 0:
            typer.echo(f"\nNote: Run again to process remaining emails.")
    else:
        typer.echo(json.dumps(results))


@app.command("embed")
def embed_chunks(
    limit: int = typer.Option(1000, help="Number of chunks to embed"),
    batch_size: int = typer.Option(16, help="Batch size for model inference and progress updates"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """
    Generate embeddings for vector similarity search.

    Creates embeddings for chunks that don't have them yet.
    Uses BAAI/bge-m3 by default (configurable via EMBEDDING_MODEL env var).
    Run this after 'index' to enable semantic search.

    Note: Embeddings use batch processing for efficiency. Concurrency won't help
    since the model inference is the bottleneck, not I/O.
    """
    try:
        from src.embeddings import embed_pending_chunks, MODEL_NAME
    except ImportError:
        typer.echo("Error: Embeddings module not available. Run from inbox-assistant repo.", err=True)
        raise typer.Exit(1)

    if human:
        typer.echo(f"Generating embeddings using model: {MODEL_NAME}")
        typer.echo(f"Processing up to {limit} chunks (batch size: {batch_size})...")

        def show_progress(processed: int, total: int):
            pct = int(processed / total * 100) if total > 0 else 0
            bar_len = 30
            filled = int(bar_len * processed / total) if total > 0 else 0
            bar = "█" * filled + "░" * (bar_len - filled)
            print(f"\r  [{bar}] {pct}% ({processed}/{total})", end="", flush=True)

        results = embed_pending_chunks(
            limit=limit,
            enrich=True,
            batch_size=batch_size,
            progress_callback=show_progress,
        )
        print()  # newline after progress bar

        typer.echo(f"Results:")
        typer.echo(f"  Processed: {results['processed']}")
        typer.echo(f"  Failed:    {results['failed']}")
        typer.echo(f"  Remaining: {results['total_pending']}")

        if results['total_pending'] > 0:
            typer.echo(f"\nNote: Run again to process remaining chunks.")
    else:
        results = embed_pending_chunks(limit=limit, enrich=True, batch_size=batch_size)
        typer.echo(json.dumps(results))


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
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """List messages currently marked as requiring a reply."""
    conn = connect_db()
    rows = conn.execute(
        """
        SELECT rt.message_id, rt.reason, rt.last_activity_at, e.subject, e.sender, e.web_link
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

    if human:
        if not items:
            typer.echo("No reply-needed items.")
            return
        for item in items:
            typer.echo(f"Subject: {item['subject']}")
            typer.echo(f"  From: {item['sender']}")
            typer.echo(f"  Reason: {item.get('reason', 'N/A')}")
            typer.echo(f"  Last activity: {item['last_activity_at']}")
            # Use web_link if available
            link = item.get('web_link')
            if not link and item.get('message_id'):
                from urllib.parse import quote
                link = f"https://outlook.office365.com/mail/inbox/id/{quote(item['message_id'], safe='')}"
            if link:
                typer.echo(f"  Link: {link}")
            typer.echo()
    else:
        typer.echo(json.dumps(items, default=str))


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


# =============================================================================
# Category Setup Commands
# =============================================================================

@app.command("setup-categories")
def setup_categories(
    create_in_outlook: bool = typer.Option(
        True, "--create-in-outlook/--no-create-in-outlook",
        help="Create master categories in Outlook via msgraph CLI"
    ),
    reset_defaults: bool = typer.Option(
        False, "--reset-defaults",
        help="Reset categories to defaults in preferences.json"
    ),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Setup Outlook categories for email organization.

    This command:
    1. Ensures default categories are in preferences.json (if not already set)
    2. Optionally creates master categories in Outlook with correct colors

    Categories keep emails in your Inbox while applying color-coded labels.
    """
    import subprocess
    from src.categories_config import (
        DEFAULT_CATEGORIES, get_categories, COLOR_PRESETS,
        ensure_categories_initialized, is_categories_mode_enabled, NAMESPACE
    )

    prefs = read_preferences()

    # Reset categories if requested
    if reset_defaults:
        if NAMESPACE not in prefs:
            prefs[NAMESPACE] = {}
        prefs[NAMESPACE]["categories"] = [cat.copy() for cat in DEFAULT_CATEGORIES]
        write_preferences(prefs)
        if human:
            typer.echo("Reset categories to defaults in preferences.json")
    else:
        # Ensure categories are initialized (handles migration from legacy location)
        categories, was_initialized = ensure_categories_initialized(prefs)
        if was_initialized:
            write_preferences(prefs)
            if human:
                typer.echo("Initialized categories in preferences.json")

    categories = get_categories(prefs)

    # Optionally create categories in Outlook
    created = []
    failed = []

    if create_in_outlook:
        for cat in categories:
            name = cat["name"]
            color = cat.get("preset") or COLOR_PRESETS.get(cat.get("color", "blue"), "preset7")

            try:
                result = subprocess.run(
                    ["aech-cli-msgraph", "create-category", name, "--color", color, "--json"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if result.returncode == 0:
                    created.append(name)
                else:
                    # Category might already exist - that's OK
                    if "already exists" in result.stderr.lower() or "conflict" in result.stderr.lower():
                        created.append(f"{name} (exists)")
                    else:
                        failed.append({"name": name, "error": result.stderr or result.stdout})
            except FileNotFoundError:
                if human:
                    typer.echo(
                        "Warning: aech-cli-msgraph not found. "
                        "Install msgraph CLI to create categories in Outlook.",
                        err=True
                    )
                failed.append({"name": name, "error": "msgraph CLI not found"})
                break
            except Exception as e:
                failed.append({"name": name, "error": str(e)})

    result_data = {
        "categories": categories,
        "created_in_outlook": created,
        "failed": failed,
        "preferences_path": str(get_db_path().parent / "preferences.json"),
    }

    if human:
        typer.echo("\nConfigured Categories:")
        for cat in categories:
            flag_info = f" (auto-flag: {cat.get('flag_urgency')})" if cat.get("flag_urgency") else ""
            typer.echo(f"  - {cat['name']} ({cat.get('color', 'blue')}){flag_info}")
            typer.echo(f"    {cat.get('description', '')}")

        if created:
            typer.echo(f"\nCreated in Outlook: {', '.join(created)}")
        if failed:
            typer.echo("\nFailed to create:")
            for f in failed:
                typer.echo(f"  - {f['name']}: {f['error']}")

        typer.echo(f"\nCategories mode: {'enabled' if is_categories_mode_enabled(prefs) else 'disabled'}")
        typer.echo("Set 'inbox_assistant.use_categories_mode' to false in preferences to use legacy folder mode.")
    else:
        typer.echo(json.dumps(result_data, default=str))


@app.command("list-categories")
def list_outlook_categories(
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """List configured Outlook categories from preferences."""
    from src.categories_config import get_categories

    prefs = read_preferences()
    categories = get_categories(prefs)

    if human:
        typer.echo("Configured Categories:")
        for cat in categories:
            flag_info = f" (auto-flag: {cat.get('flag_urgency')})" if cat.get("flag_urgency") else ""
            typer.echo(f"  - {cat['name']} ({cat.get('color', 'blue')}){flag_info}")
            if cat.get("description"):
                typer.echo(f"    {cat['description']}")
    else:
        typer.echo(json.dumps(categories, default=str))


# =============================================================================
# Categories Subcommand Group
# =============================================================================

@categories_app.command("list")
def categories_list(
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """List all configured categories.

    Categories are stored in your user profile (preferences.json) under the
    'inbox_assistant' namespace and auto-populated with defaults on first use.
    """
    from src.categories_config import ensure_categories_initialized, NAMESPACE
    from .state import get_preferences_path

    prefs = read_preferences()
    categories, was_initialized = ensure_categories_initialized(prefs)

    if was_initialized:
        write_preferences(prefs)

    if human:
        if was_initialized:
            typer.echo("(Initialized with default categories)\n")
        typer.echo("Your Categories (inbox_assistant.categories):")
        for cat in categories:
            flag_info = f" [auto-flag: {cat.get('flag_urgency')}]" if cat.get("flag_urgency") else ""
            typer.echo(f"  {cat['name']} ({cat.get('color', 'blue')}){flag_info}")
            if cat.get("description"):
                typer.echo(f"    → {cat['description']}")
        typer.echo(f"\nProfile: {get_preferences_path()}")
    else:
        typer.echo(json.dumps({
            "namespace": NAMESPACE,
            "categories": categories,
            "initialized": was_initialized,
            "profile_path": str(get_preferences_path()),
        }, default=str))


@categories_app.command("add")
def categories_add(
    name: str = typer.Argument(..., help="Category name (must match what you create in Outlook)"),
    color: str = typer.Option("blue", "--color", "-c", help="Color: red, orange, yellow, green, blue, purple, gray, etc."),
    description: str = typer.Option("", "--description", "-d", help="Description of when to use this category"),
    flag_urgency: Optional[str] = typer.Option(None, "--flag", "-f", help="Auto-flag urgency: immediate, today, this_week, someday"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Add a new category.

    The category name must exactly match what you create in Outlook's master categories
    for colors to display correctly. Names are case-sensitive.

    Example:
        aech-cli-inbox categories add "Urgent" --color red --flag today -d "Time-sensitive items"
    """
    from src.categories_config import add_category

    prefs = read_preferences()

    try:
        new_cat = add_category(prefs, name, color, description, flag_urgency)
        write_preferences(prefs)

        if human:
            typer.echo(f"Added category: {name} ({color})")
            if description:
                typer.echo(f"  → {description}")
            if flag_urgency:
                typer.echo(f"  Auto-flag: {flag_urgency}")
            typer.echo(f"\nRemember to create '{name}' in Outlook with {color} color!")
        else:
            typer.echo(json.dumps({"added": new_cat}, default=str))

    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        if "already exists" in str(e):
            typer.echo(f"Use 'categories edit \"{name}\"' to modify it.", err=True)
        raise typer.Exit(1)


@categories_app.command("remove")
def categories_remove(
    name: str = typer.Argument(..., help="Category name to remove"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Remove a category.

    This removes the category from your profile. Emails already tagged with this
    category will keep the tag, but new emails won't be assigned to it.
    """
    from src.categories_config import remove_category

    prefs = read_preferences()

    try:
        removed = remove_category(prefs, name)
        write_preferences(prefs)

        if human:
            typer.echo(f"Removed category: {removed['name']}")
        else:
            typer.echo(json.dumps({"removed": removed}, default=str))

    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@categories_app.command("edit")
def categories_edit(
    name: str = typer.Argument(..., help="Category name to edit"),
    new_name: Optional[str] = typer.Option(None, "--name", "-n", help="New name for the category"),
    color: Optional[str] = typer.Option(None, "--color", "-c", help="New color"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="New description"),
    flag_urgency: Optional[str] = typer.Option(None, "--flag", "-f", help="New flag urgency (use 'none' to clear)"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Edit an existing category.

    Only specified fields will be updated. Use --flag none to remove auto-flagging.

    Example:
        aech-cli-inbox categories edit "Work" --color teal --flag this_week
    """
    from src.categories_config import edit_category

    prefs = read_preferences()

    try:
        updated = edit_category(prefs, name, new_name, color, description, flag_urgency)
        write_preferences(prefs)

        if human:
            display_name = new_name if new_name else name
            typer.echo(f"Updated category: {display_name}")
            typer.echo(f"  Color: {updated.get('color', 'blue')}")
            if updated.get('description'):
                typer.echo(f"  Description: {updated['description']}")
            typer.echo(f"  Auto-flag: {updated.get('flag_urgency') or 'none'}")
            if new_name and new_name != name:
                typer.echo(f"\nRemember to rename '{name}' to '{new_name}' in Outlook!")
        else:
            typer.echo(json.dumps({"updated": updated}, default=str))

    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@categories_app.command("reset")
def categories_reset(
    confirm: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Reset categories to defaults.

    This will replace all custom categories with the default set:
    Action Required, Follow Up, Work, Personal
    """
    from src.categories_config import DEFAULT_CATEGORIES

    if not confirm:
        typer.confirm("Reset all categories to defaults?", abort=True)

    prefs = read_preferences()
    prefs["outlook_categories"] = DEFAULT_CATEGORIES.copy()
    write_preferences(prefs)

    if human:
        typer.echo("Categories reset to defaults:")
        for cat in DEFAULT_CATEGORIES:
            typer.echo(f"  - {cat['name']} ({cat['color']})")
    else:
        typer.echo(json.dumps({"reset": True, "categories": DEFAULT_CATEGORIES}, default=str))


@categories_app.command("strip")
def categories_strip(
    name: str = typer.Argument(..., help="Category name to remove from all messages"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be done without making changes"),
    concurrency: int = typer.Option(10, "--concurrency", help="Number of parallel API calls"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Remove a category from all messages that have it.

    This command updates messages in Outlook to remove the specified category,
    useful when retiring a category (like FYI) from your workflow.

    Example:
        aech-cli-inbox-assistant categories strip "FYI" --dry-run
        aech-cli-inbox-assistant categories strip "FYI"
    """
    import subprocess
    import os
    import concurrent.futures

    user_email = os.environ.get("DELEGATED_USER")
    if not user_email:
        typer.echo("Error: DELEGATED_USER environment variable not set", err=True)
        raise typer.Exit(1)

    conn = connect_db()
    cursor = conn.cursor()

    # Find all emails with this category in our DB
    cursor.execute("""
        SELECT id, subject, outlook_categories
        FROM emails
        WHERE outlook_categories LIKE ?
    """, (f'%"{name}"%',))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        if human:
            typer.echo(f"No emails found with category '{name}'")
        else:
            typer.echo(json.dumps({"found": 0, "updated": 0, "failed": 0}))
        return

    # Filter to only those that actually have the category
    messages_to_update = []
    for row in rows:
        try:
            categories = json.loads(row["outlook_categories"] or "[]")
            if name in categories:
                new_categories = [c for c in categories if c != name]
                messages_to_update.append({
                    "id": row["id"],
                    "subject": row["subject"],
                    "old_categories": categories,
                    "new_categories": new_categories,
                })
        except json.JSONDecodeError:
            continue

    if not messages_to_update:
        if human:
            typer.echo(f"No emails found with category '{name}'")
        else:
            typer.echo(json.dumps({"found": 0, "updated": 0, "failed": 0}))
        return

    if human:
        typer.echo(f"Found {len(messages_to_update)} emails with category '{name}'")

    if dry_run:
        if human:
            typer.echo(f"\n[DRY RUN] Would update {len(messages_to_update)} messages:")
            for msg in messages_to_update[:10]:
                typer.echo(f"  - {msg['subject'][:60]}...")
                typer.echo(f"    {msg['old_categories']} → {msg['new_categories']}")
            if len(messages_to_update) > 10:
                typer.echo(f"  ... and {len(messages_to_update) - 10} more")
        else:
            typer.echo(json.dumps({"dry_run": True, "found": len(messages_to_update), "messages": messages_to_update[:10]}))
        return

    results = {"found": len(messages_to_update), "updated": 0, "failed": 0, "errors": []}

    def update_message(msg):
        """Update a single message's categories."""
        cmd = [
            "aech-cli-msgraph", "update-message", msg["id"],
            "--user", user_email,
        ]
        # Set categories (empty string clears all categories)
        if msg["new_categories"]:
            cmd.extend(["--categories", ",".join(msg["new_categories"])])
        else:
            cmd.extend(["--categories", ""])

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            return result.returncode == 0, msg["id"], result.stderr if result.returncode != 0 else None
        except Exception as e:
            return False, msg["id"], str(e)

    if human:
        typer.echo(f"Updating messages (concurrency={concurrency})...")

    # Process in parallel
    successful_updates = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(update_message, msg): msg for msg in messages_to_update}

        for future in concurrent.futures.as_completed(futures):
            msg = futures[future]
            success, msg_id, error = future.result()
            if success:
                results["updated"] += 1
                successful_updates.append(msg)
            else:
                results["failed"] += 1
                if error:
                    results["errors"].append(f"{msg_id}: {error}")

    # Update local DB only for successful updates
    if successful_updates:
        conn = connect_db()
        for msg in successful_updates:
            conn.execute(
                "UPDATE emails SET outlook_categories = ? WHERE id = ?",
                (json.dumps(msg["new_categories"]), msg["id"])
            )
        conn.commit()
        conn.close()

    if human:
        typer.echo(f"\nResults:")
        typer.echo(f"  Updated: {results['updated']}")
        typer.echo(f"  Failed:  {results['failed']}")
        if results["errors"]:
            typer.echo(f"\nErrors:")
            for err in results["errors"][:5]:
                typer.echo(f"  - {err}")
    else:
        typer.echo(json.dumps(results, default=str))


@categories_app.command("colors")
def categories_colors(
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """List available colors for categories."""
    from src.categories_config import COLOR_PRESETS

    if human:
        typer.echo("Available colors:")
        for color, preset in COLOR_PRESETS.items():
            typer.echo(f"  {color} ({preset})")
    else:
        typer.echo(json.dumps(COLOR_PRESETS, default=str))


@app.command("migrate-to-categories")
def migrate_to_categories(
    folder_prefix: str = typer.Option("aa_", "--prefix", help="Folder prefix to migrate from"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be done without making changes"),
    recategorize: bool = typer.Option(True, "--recategorize/--no-recategorize", help="Recategorize emails after moving"),
    batch_size: int = typer.Option(100, "--batch-size", help="Number of emails to fetch per API call"),
    concurrency: int = typer.Option(10, "--concurrency", help="Number of parallel move operations"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Migrate emails from folder-based organization to categories.

    This command:
    1. Finds all folders matching the prefix (e.g., aa_Work, aa_Finance)
    2. Moves all emails from those folders back to Inbox (in parallel)
    3. Optionally recategorizes them using the new categories system

    Use --dry-run to see what would be done without making changes.
    Use --concurrency to control parallel API calls (default: 10).
    """
    import asyncio
    import subprocess
    import os
    import threading

    user_email = os.environ.get("DELEGATED_USER")
    if not user_email:
        typer.echo("Error: DELEGATED_USER environment variable not set", err=True)
        raise typer.Exit(1)

    results = {
        "folders_found": [],
        "emails_moved": 0,
        "emails_failed": 0,
        "dry_run": dry_run,
        "errors": [],
    }

    # Step 1: List all mail folders
    if human:
        typer.echo(f"Finding folders with prefix '{folder_prefix}'...")

    try:
        list_result = subprocess.run(
            ["aech-cli-msgraph", "list-folders", "--user", user_email, "--json"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if list_result.returncode != 0:
            typer.echo(f"Error listing folders: {list_result.stderr}", err=True)
            raise typer.Exit(1)

        folders_data = json.loads(list_result.stdout)
        # Handle both direct list and {"value": [...]} format
        folders = folders_data.get("value", folders_data) if isinstance(folders_data, dict) else folders_data

    except FileNotFoundError:
        typer.echo("Error: aech-cli-msgraph not found. Install msgraph CLI first.", err=True)
        raise typer.Exit(1)
    except json.JSONDecodeError as e:
        typer.echo(f"Error parsing folder list: {e}", err=True)
        raise typer.Exit(1)

    # Find folders matching prefix
    matching_folders = []
    for folder in folders:
        name = folder.get("displayName", "")
        if name.startswith(folder_prefix):
            matching_folders.append({
                "id": folder.get("id"),
                "name": name,
                "total_count": folder.get("totalItemCount", 0),
                "unread_count": folder.get("unreadItemCount", 0),
            })

    results["folders_found"] = matching_folders

    if not matching_folders:
        if human:
            typer.echo(f"No folders found with prefix '{folder_prefix}'")
        else:
            typer.echo(json.dumps(results, default=str))
        return

    total_expected = sum(f["total_count"] for f in matching_folders)

    if human:
        typer.echo(f"Found {len(matching_folders)} folders with {total_expected} total emails:")
        for f in matching_folders:
            typer.echo(f"  - {f['name']}: {f['total_count']} emails")

    if dry_run:
        if human:
            typer.echo(f"\n[DRY RUN] Would move {total_expected} emails to Inbox")
            typer.echo(f"[DRY RUN] Using concurrency={concurrency}")
            if recategorize:
                typer.echo("[DRY RUN] Would recategorize emails after moving")
        else:
            typer.echo(json.dumps(results, default=str))
        return

    # Step 2: Get Inbox folder ID
    inbox_id = None
    for folder in folders:
        if folder.get("displayName", "").lower() == "inbox":
            inbox_id = folder.get("id")
            break

    if not inbox_id:
        typer.echo("Error: Could not find Inbox folder", err=True)
        raise typer.Exit(1)

    # Step 3: Collect all messages from all folders first
    if human:
        typer.echo(f"\nCollecting messages from {len(matching_folders)} folders...")

    all_messages = []  # List of (folder_name, msg_id, subject)

    for folder_info in matching_folders:
        folder_name = folder_info["name"]
        folder_count = folder_info["total_count"]

        if folder_count == 0:
            continue

        if human:
            typer.echo(f"  Listing {folder_name}...", nl=False)

        # Fetch all messages from folder (use folder name, not ID)
        # Note: list-messages uses --folder (name) and --count (max messages)
        try:
            messages_result = subprocess.run(
                [
                    "aech-cli-msgraph", "list-messages",
                    "--folder", folder_name,
                    "--user", user_email,
                    "--count", str(max(folder_count + 10, batch_size)),  # Fetch all + buffer
                    "--json",
                ],
                capture_output=True,
                text=True,
                timeout=300,  # Longer timeout for large folders
            )

            if messages_result.returncode != 0:
                results["errors"].append(f"Failed to list messages in {folder_name}: {messages_result.stderr}")
                if human:
                    typer.echo(f" ERROR")
                continue

            messages_data = json.loads(messages_result.stdout)
            messages = messages_data.get("value", messages_data) if isinstance(messages_data, dict) else messages_data

            for msg in messages:
                all_messages.append({
                    "folder": folder_name,
                    "id": msg.get("id"),
                    "subject": msg.get("subject", "")[:50],
                })

            if human:
                typer.echo(f" {len(messages)} messages")

        except Exception as e:
            results["errors"].append(f"Error listing messages in {folder_name}: {e}")
            if human:
                typer.echo(f" ERROR: {e}")

    if not all_messages:
        if human:
            typer.echo("No messages to migrate.")
        else:
            typer.echo(json.dumps(results, default=str))
        return

    if human:
        typer.echo(f"\nMoving {len(all_messages)} emails with concurrency={concurrency}...")

    # Step 4: Move all messages in parallel
    lock = threading.Lock()
    counters = {"moved": 0, "failed": 0, "completed": 0}
    total = len(all_messages)
    errors_list: List[str] = []

    async def move_one(msg_info: dict, semaphore: asyncio.Semaphore):
        async with semaphore:
            msg_id = msg_info["id"]
            subject = msg_info["subject"]
            folder = msg_info["folder"]

            try:
                proc = await asyncio.create_subprocess_exec(
                    "aech-cli-msgraph", "move-email",
                    msg_id,
                    "Inbox",  # folder name, not ID
                    "--user", user_email,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)

                with lock:
                    counters["completed"] += 1
                    if proc.returncode == 0:
                        counters["moved"] += 1
                    else:
                        counters["failed"] += 1
                        errors_list.append(f"Failed to move '{subject}' from {folder}")

                    # Progress update every 50 or at the end
                    completed = counters["completed"]
                    if completed % 50 == 0 or completed == total:
                        pct = int(completed / total * 100)
                        if human:
                            typer.echo(f"  [{pct:3d}%] {completed}/{total} moved...")

            except asyncio.TimeoutError:
                with lock:
                    counters["completed"] += 1
                    counters["failed"] += 1
                    errors_list.append(f"Timeout moving '{subject}'")
            except Exception as e:
                with lock:
                    counters["completed"] += 1
                    counters["failed"] += 1
                    errors_list.append(f"Error moving '{subject}': {e}")

    async def run_parallel():
        semaphore = asyncio.Semaphore(concurrency)
        tasks = [move_one(msg, semaphore) for msg in all_messages]
        await asyncio.gather(*tasks)

    asyncio.run(run_parallel())

    results["emails_moved"] = counters["moved"]
    results["emails_failed"] = counters["failed"]
    results["errors"] = errors_list[:100]  # Limit stored errors

    # Summary
    if human:
        typer.echo(f"\n=== Migration Complete ===")
        typer.echo(f"Emails moved: {results['emails_moved']}")
        typer.echo(f"Emails failed: {results['emails_failed']}")
        if errors_list:
            typer.echo(f"\nErrors ({len(errors_list)}):")
            for err in errors_list[:10]:
                typer.echo(f"  - {err}")

        if recategorize and results["emails_moved"] > 0:
            typer.echo("\nTo recategorize moved emails, run:")
            typer.echo("  python -m src.main --process-inbox-once")
    else:
        typer.echo(json.dumps(results, default=str))


# =============================================================================
# Calendar Commands (Graph API - no local sync)
# =============================================================================

calendar_app = typer.Typer(help="Calendar operations via Microsoft Graph API.", add_completion=False)
app.add_typer(calendar_app, name="calendar")


def _get_calendar_client():
    """Lazy import and create CalendarClient."""
    from src.calendar import CalendarClient
    return CalendarClient()


@calendar_app.command("today")
def calendar_today(
    tz: Optional[str] = typer.Option(None, "--tz", help="Override timezone (IANA format)"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Show today's agenda."""
    client = _get_calendar_client()
    events = client.get_todays_agenda(timezone=tz)

    events_data = [e.model_dump() for e in events]

    if human:
        if not events_data:
            typer.echo("No events today.")
            return
        tz_str = tz or client.get_user_timezone()
        typer.echo(f"=== Today's Agenda ({len(events)} events) ===")
        typer.echo(f"Timezone: {tz_str}\n")
        for e in events:
            start_str = e.start.strftime("%H:%M")
            end_str = e.end.strftime("%H:%M")
            loc = f" @ {e.location}" if e.location else ""
            online = " [Online]" if e.is_online_meeting else ""
            typer.echo(f"{start_str}-{end_str}  {e.subject}{loc}{online}")
            if e.attendees:
                attendee_names = [a.name or a.email for a in e.attendees[:3]]
                if attendee_names:
                    typer.echo(f"           With: {', '.join(attendee_names)}")
    else:
        typer.echo(json.dumps(events_data, default=str))


@calendar_app.command("upcoming")
def calendar_upcoming(
    hours: int = typer.Option(24, help="Hours to look ahead"),
    limit: int = typer.Option(10, help="Maximum events to show"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Show upcoming events in the next N hours."""
    client = _get_calendar_client()
    events = client.get_upcoming_events(hours=hours, limit=limit)

    events_data = [e.model_dump() for e in events]

    if human:
        if not events_data:
            typer.echo("No upcoming events.")
            return
        typer.echo(f"=== Next {hours} hours ({len(events)} events) ===\n")
        for e in events:
            dt_str = e.start.strftime("%a %H:%M")
            duration = int((e.end - e.start).total_seconds() / 60)
            typer.echo(f"{dt_str} ({duration}min) - {e.subject}")
    else:
        typer.echo(json.dumps(events_data, default=str))


@calendar_app.command("view")
def calendar_view(
    start: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD or ISO datetime)"),
    end: str = typer.Option(..., "--end", help="End date (YYYY-MM-DD or ISO datetime)"),
    limit: int = typer.Option(100, help="Maximum events to return"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """View calendar events in a date range."""
    client = _get_calendar_client()

    # Parse dates
    try:
        if "T" in start:
            start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        else:
            start_dt = datetime.strptime(start, "%Y-%m-%d")
    except ValueError:
        typer.echo(f"Invalid start date format: {start}", err=True)
        raise typer.Exit(1)

    try:
        if "T" in end:
            end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
        else:
            end_dt = datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)
    except ValueError:
        typer.echo(f"Invalid end date format: {end}", err=True)
        raise typer.Exit(1)

    events = client.get_calendar_view(start_dt, end_dt, max_results=limit)
    events_data = [e.model_dump() for e in events]

    if human:
        if not events_data:
            typer.echo("No events in range.")
            return
        typer.echo(f"=== {start} to {end} ({len(events)} events) ===\n")
        current_day = None
        for e in events:
            event_day = e.start.strftime("%Y-%m-%d")
            if event_day != current_day:
                current_day = event_day
                typer.echo(f"\n{e.start.strftime('%A, %B %d')}:")
            start_str = e.start.strftime("%H:%M")
            end_str = e.end.strftime("%H:%M")
            typer.echo(f"  {start_str}-{end_str}  {e.subject}")
    else:
        typer.echo(json.dumps(events_data, default=str))


@calendar_app.command("free-busy")
def calendar_free_busy(
    start: str = typer.Option(..., "--start", help="Start datetime (YYYY-MM-DD or ISO)"),
    end: str = typer.Option(..., "--end", help="End datetime (YYYY-MM-DD or ISO)"),
    emails: Optional[str] = typer.Option(None, "--emails", help="Comma-separated emails to check"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Check free/busy status for one or more users."""
    client = _get_calendar_client()

    # Parse dates
    try:
        if "T" in start:
            start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        else:
            start_dt = datetime.strptime(start, "%Y-%m-%d")
    except ValueError:
        typer.echo(f"Invalid start date format: {start}", err=True)
        raise typer.Exit(1)

    try:
        if "T" in end:
            end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
        else:
            end_dt = datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)
    except ValueError:
        typer.echo(f"Invalid end date format: {end}", err=True)
        raise typer.Exit(1)

    email_list = [e.strip() for e in emails.split(",")] if emails else None
    results = client.get_schedule(start_dt, end_dt, emails=email_list)
    results_data = [r.model_dump() for r in results]

    if human:
        if not results_data:
            typer.echo("No schedule data returned.")
            return
        for result in results:
            typer.echo(f"\n=== {result.email} ===")
            if result.availability_view:
                typer.echo(f"Availability view: {result.availability_view}")
            if result.schedule_items:
                typer.echo("Busy periods:")
                for item in result.schedule_items:
                    start_str = item.start.strftime("%Y-%m-%d %H:%M")
                    end_str = item.end.strftime("%H:%M")
                    subj = f" - {item.subject}" if item.subject else ""
                    typer.echo(f"  [{item.status}] {start_str} to {end_str}{subj}")
            else:
                typer.echo("No busy periods in range.")
    else:
        typer.echo(json.dumps(results_data, default=str))


@calendar_app.command("find-times")
def calendar_find_times(
    attendees: str = typer.Option(..., "--attendees", help="Comma-separated attendee emails"),
    duration: int = typer.Option(30, "--duration", "-d", help="Meeting duration in minutes"),
    start: Optional[str] = typer.Option(None, "--start", help="Start date constraint (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, "--end", help="End date constraint (YYYY-MM-DD)"),
    max_results: int = typer.Option(10, help="Maximum suggestions to return"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Find meeting times that work for all attendees."""
    client = _get_calendar_client()

    attendee_list = [e.strip() for e in attendees.split(",")]

    # Parse optional date constraints
    start_dt = None
    end_dt = None
    if start:
        try:
            start_dt = datetime.strptime(start, "%Y-%m-%d")
        except ValueError:
            typer.echo(f"Invalid start date: {start}", err=True)
            raise typer.Exit(1)
    if end:
        try:
            end_dt = datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)
        except ValueError:
            typer.echo(f"Invalid end date: {end}", err=True)
            raise typer.Exit(1)

    suggestions = client.find_meeting_times(
        attendees=attendee_list,
        duration_minutes=duration,
        start=start_dt,
        end=end_dt,
        max_candidates=max_results,
    )

    suggestions_data = [s.model_dump() for s in suggestions]

    if human:
        if not suggestions_data:
            typer.echo("No meeting times found that work for all attendees.")
            return
        typer.echo(f"=== Meeting Time Suggestions ({len(suggestions)}) ===")
        typer.echo(f"Duration: {duration} minutes")
        typer.echo(f"Attendees: {', '.join(attendee_list)}\n")
        for i, s in enumerate(suggestions, 1):
            start_str = s.start.strftime("%a %b %d, %I:%M %p")
            end_str = s.end.strftime("%I:%M %p")
            conf = f" (confidence: {s.confidence:.0%})" if s.confidence else ""
            typer.echo(f"{i}. {start_str} - {end_str}{conf}")
    else:
        typer.echo(json.dumps(suggestions_data, default=str))


@calendar_app.command("create-event")
def calendar_create_event(
    subject: str = typer.Option(..., "--subject", "-s", help="Event subject/title"),
    start: str = typer.Option(..., "--start", help="Start datetime (ISO format)"),
    duration: int = typer.Option(30, "--duration", "-d", help="Duration in minutes"),
    end: Optional[str] = typer.Option(None, "--end", help="End datetime (overrides duration)"),
    attendees: Optional[str] = typer.Option(None, "--attendees", help="Comma-separated attendee emails"),
    location: Optional[str] = typer.Option(None, "--location", "-l", help="Event location"),
    body: Optional[str] = typer.Option(None, "--body", "-b", help="Event body/description"),
    online: bool = typer.Option(False, "--online", help="Create as Teams meeting"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Create a calendar event (no invitations sent by default)."""
    client = _get_calendar_client()

    # Parse start time
    try:
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
    except ValueError:
        typer.echo(f"Invalid start datetime: {start}", err=True)
        raise typer.Exit(1)

    # Parse end time if provided
    end_dt = None
    if end:
        try:
            end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
        except ValueError:
            typer.echo(f"Invalid end datetime: {end}", err=True)
            raise typer.Exit(1)

    attendee_list = [e.strip() for e in attendees.split(",")] if attendees else None

    event = client.create_event(
        subject=subject,
        start=start_dt,
        end=end_dt,
        duration_minutes=duration,
        attendees=attendee_list,
        location=location,
        body=body,
        is_online_meeting=online,
        send_invitations=False,  # Don't send invites
    )

    if human:
        typer.echo("Event created successfully:")
        typer.echo(f"  Subject: {event.subject}")
        typer.echo(f"  Start: {event.start.strftime('%Y-%m-%d %H:%M')}")
        typer.echo(f"  End: {event.end.strftime('%Y-%m-%d %H:%M')}")
        if event.location:
            typer.echo(f"  Location: {event.location}")
        if event.is_online_meeting and event.online_meeting_url:
            typer.echo(f"  Teams URL: {event.online_meeting_url}")
        typer.echo(f"  Event ID: {event.event_id}")
        typer.echo("\nNote: Invitations were NOT sent. Review in Outlook to send.")
    else:
        typer.echo(json.dumps(event.model_dump(), default=str))


@calendar_app.command("working-hours")
def calendar_working_hours(
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Get user's working hours from mailbox settings."""
    client = _get_calendar_client()
    working_hours = client.get_working_hours()

    if human:
        typer.echo("=== Working Hours ===")
        typer.echo(f"Timezone: {working_hours.timezone}")
        typer.echo(f"Days: {', '.join(d.capitalize() for d in working_hours.days_of_week)}")
        typer.echo(f"Hours: {working_hours.start_time} - {working_hours.end_time}")
    else:
        typer.echo(json.dumps(working_hours.model_dump(), default=str))


# =============================================================================
# Meeting Prep Commands
# =============================================================================

def _get_meeting_prep_service():
    """Lazy import and create MeetingPrepService."""
    from src.meeting_prep import MeetingPrepService
    return MeetingPrepService()


@calendar_app.command("prep")
def calendar_prep(
    event_id: Optional[str] = typer.Option(None, "--event-id", help="Specific event ID to prepare"),
    next_meeting: bool = typer.Option(False, "--next", "-n", help="Prepare for the next meeting that needs prep"),
    hours: int = typer.Option(8, "--hours", "-h", help="Lookahead hours when using --next"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """
    Generate meeting preparation briefing.

    Use --next to prepare for your next meeting, or --event-id for a specific event.
    """
    service = _get_meeting_prep_service()

    if event_id:
        # Get specific event and prepare
        events = service.calendar.get_upcoming_events(hours=48, limit=50)
        target = None
        for e in events:
            if e.event_id == event_id:
                target = e
                break
        if not target:
            typer.echo(f"Event not found: {event_id}", err=True)
            raise typer.Exit(1)
        prep = service.prepare_meeting(target)
    elif next_meeting:
        # Override the default lookahead in the service
        events = service.calendar.get_upcoming_events(hours=hours, limit=20)
        prep = None
        for e in events:
            if service._should_prepare(e):
                prep = service.prepare_meeting(e)
                break
        if not prep:
            if human:
                typer.echo("No upcoming meetings need preparation based on your rules.")
            else:
                typer.echo(json.dumps({"message": "No meetings need preparation"}))
            return
    else:
        typer.echo("Specify --next or --event-id", err=True)
        raise typer.Exit(1)

    if human:
        typer.echo(f"=== Meeting Prep: {prep.subject} ===")
        typer.echo(f"When: {prep.start.strftime('%A, %B %d at %I:%M %p')}")
        duration = int((prep.end - prep.start).total_seconds() / 60)
        typer.echo(f"Duration: {duration} minutes")

        if prep.location:
            typer.echo(f"Location: {prep.location}")
        if prep.is_online and prep.join_url:
            typer.echo(f"Teams Link: {prep.join_url}")

        typer.echo(f"\n{prep.briefing_summary}")

        if prep.preparation_notes:
            typer.echo("\n--- Preparation Notes ---")
            for note in prep.preparation_notes:
                typer.echo(f"  - {note}")

        if prep.attendee_context:
            external = [a for a in prep.attendee_context if a.is_external]
            if external:
                typer.echo(f"\n--- External Attendees ({len(external)}) ---")
                for a in external:
                    name = a.name or a.email
                    if a.recent_email_count > 0:
                        typer.echo(f"  - {name}: {a.recent_email_count} recent emails")
                    else:
                        typer.echo(f"  - {name}: No recent correspondence")

        if prep.body_preview:
            typer.echo(f"\n--- Meeting Description ---")
            typer.echo(f"  {prep.body_preview[:300]}...")

        if prep.rule_matched:
            typer.echo(f"\n(Matched rule: {prep.rule_matched})")
    else:
        typer.echo(json.dumps(prep.model_dump(), default=str))


@calendar_app.command("briefing")
def calendar_briefing(
    date: Optional[str] = typer.Option(None, "--date", "-d", help="Date for briefing (YYYY-MM-DD, default: today)"),
    tz: Optional[str] = typer.Option(None, "--tz", help="Override timezone (IANA format)"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """
    Generate a daily briefing with schedule overview and meeting prep.

    Shows the day's schedule, highlights meetings needing attention, and
    provides preparation notes for important meetings.
    """
    service = _get_meeting_prep_service()

    # Parse date if provided
    date_dt = None
    if date:
        try:
            from zoneinfo import ZoneInfo
            tz_str = tz or service.calendar.get_user_timezone()
            date_dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=ZoneInfo(tz_str))
        except ValueError:
            typer.echo(f"Invalid date format: {date}", err=True)
            raise typer.Exit(1)

    briefing = service.generate_daily_briefing(date=date_dt, timezone=tz)

    if human:
        typer.echo(f"=== Daily Briefing for {briefing.date} ===")
        typer.echo(f"Timezone: {briefing.timezone}\n")

        typer.echo(briefing.schedule_summary)

        if briefing.alerts:
            typer.echo("\n--- Alerts ---")
            for alert in briefing.alerts:
                typer.echo(f"  ! {alert}")

        if briefing.meeting_preps:
            typer.echo(f"\n--- Meetings Needing Prep ({len(briefing.meeting_preps)}) ---")
            for prep in briefing.meeting_preps:
                start_str = prep.start.strftime("%I:%M %p").lstrip("0")
                duration = int((prep.end - prep.start).total_seconds() / 60)
                typer.echo(f"\n  [{start_str}] {prep.subject} ({duration}min)")
                if prep.briefing_summary:
                    typer.echo(f"    {prep.briefing_summary}")
                if prep.preparation_notes:
                    for note in prep.preparation_notes[:3]:
                        typer.echo(f"    - {note}")
                if prep.rule_matched:
                    typer.echo(f"    (Rule: {prep.rule_matched})")
        else:
            typer.echo("\nNo meetings require special preparation today.")

        typer.echo(f"\nGenerated at: {briefing.generated_at}")
    else:
        typer.echo(json.dumps(briefing.model_dump(), default=str))


@calendar_app.command("prep-config")
def calendar_prep_config(
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Show current meeting prep configuration and rules."""
    service = _get_meeting_prep_service()
    config = service.config

    if human:
        typer.echo("=== Meeting Prep Configuration ===")
        typer.echo(f"Enabled: {config.enabled}")
        typer.echo(f"Morning briefing: {config.morning_briefing_enabled} at {config.morning_briefing_time}")
        typer.echo(f"Individual prep: {config.individual_prep_enabled}")
        typer.echo(f"Default prep lead time: {config.default_prep_minutes} minutes")

        typer.echo("\n--- Exclusions ---")
        typer.echo(f"  Skip all-day events: {config.skip_all_day_events}")
        typer.echo(f"  Skip declined: {config.skip_declined_events}")
        typer.echo(f"  Skip tentative: {config.skip_tentative_events}")
        typer.echo(f"  Min duration: {config.min_duration_minutes} minutes")

        typer.echo(f"\n--- Rules ({len(config.rules)}) ---")
        for rule in config.rules:
            status = "enabled" if rule.enabled else "disabled"
            typer.echo(f"\n  [{status}] {rule.name}")
            typer.echo(f"    Prep {rule.prep_minutes_before} min before")
            if rule.external_only:
                typer.echo("    Matches: external attendees only")
            if rule.min_attendees:
                typer.echo(f"    Matches: {rule.min_attendees}+ attendees")
            if rule.keywords:
                typer.echo(f"    Matches keywords: {', '.join(rule.keywords)}")
            if rule.vip_attendees:
                typer.echo(f"    Matches VIPs: {', '.join(rule.vip_attendees)}")
    else:
        typer.echo(json.dumps(config.model_dump(), default=str))


# =============================================================================
# Working Memory Commands (EA State Awareness)
# =============================================================================

wm_app = typer.Typer(
    help="Query EA working memory - threads, decisions, commitments, contacts, projects.",
    add_completion=False,
)
app.add_typer(wm_app, name="wm")


def _format_snapshot_llm(snapshot: dict) -> str:
    """Format snapshot as LLM-optimized markdown."""
    lines = ["# Working Memory Snapshot", ""]

    # Summary with actionable framing
    s = snapshot["summary"]
    lines.append("## Summary")
    if s["threads_needing_reply"] > 0:
        lines.append(f"- **{s['threads_needing_reply']} threads need your reply**")
    if s["urgent_threads"] > 0:
        lines.append(f"- {s['urgent_threads']} urgent threads (immediate/today priority)")
    if s["pending_decisions_count"] > 0:
        lines.append(f"- **{s['pending_decisions_count']} pending decisions** awaiting your response")
    if s["open_commitments_count"] > 0:
        lines.append(f"- **{s['open_commitments_count']} open commitments** you made to others")
    if all(v == 0 for v in s.values()):
        lines.append("- All clear - no urgent items or pending actions")
    lines.append("")

    # Threads needing reply first (most actionable)
    threads = snapshot["active_threads"]
    reply_threads = [t for t in threads if t.get("needs_reply")]
    other_threads = [t for t in threads if not t.get("needs_reply")]

    if reply_threads:
        lines.append(f"## Threads Needing Reply ({len(reply_threads)})")
        for i, t in enumerate(reply_threads, 1):
            urgency = (t.get("urgency") or "someday").upper()
            status = t.get("status") or "active"
            subject = t.get("subject") or "(no subject)"
            summary = t.get("summary") or ""
            participants = t.get("participants_json") or "[]"
            last_activity = (t.get("last_activity_at") or "")[:10]  # Just date

            lines.append(f"### {i}. [{urgency}] {subject}")
            if status == "stale":
                lines.append("- **Status**: STALE (no activity for 3+ days)")
            if summary:
                lines.append(f"- **Summary**: {summary}")
            lines.append(f"- **Participants**: {participants}")
            if last_activity:
                lines.append(f"- **Last activity**: {last_activity}")
            # Include link
            link = t.get("latest_web_link")
            if not link and t.get("latest_email_id"):
                from urllib.parse import quote
                link = f"https://outlook.office365.com/mail/inbox/id/{quote(t['latest_email_id'], safe='')}"
            if link:
                lines.append(f"- **Link**: {link}")
            lines.append("")

    if other_threads:
        lines.append(f"## Other Active Threads ({len(other_threads)})")
        for t in other_threads:
            urgency = (t.get("urgency") or "someday").upper()
            subject = t.get("subject") or "(no subject)"
            summary = t.get("summary") or ""
            lines.append(f"- [{urgency}] **{subject}**")
            if summary:
                lines.append(f"  - {summary[:150]}")
        lines.append("")

    # Pending decisions
    decisions = snapshot["pending_decisions"]
    if decisions:
        lines.append(f"## Pending Decisions ({len(decisions)})")
        for i, d in enumerate(decisions, 1):
            urgency = (d.get("urgency") or "someday").upper()
            question = d.get("question") or "(unknown question)"
            requester = d.get("requester") or "unknown"
            context = d.get("context") or ""
            created = (d.get("created_at") or "")[:10]

            lines.append(f"### {i}. [{urgency}] {question[:80]}")
            lines.append(f"- **From**: {requester}")
            if context:
                lines.append(f"- **Context**: {context}")
            if created:
                lines.append(f"- **Received**: {created}")
            lines.append("")
    else:
        lines.append("## Pending Decisions")
        lines.append("No pending decisions.")
        lines.append("")

    # Open commitments
    commitments = snapshot["open_commitments"]
    if commitments:
        lines.append(f"## Open Commitments ({len(commitments)})")
        for c in commitments:
            desc = c.get("description") or "(no description)"
            to_whom = c.get("to_whom") or "unknown"
            due = c.get("due_by")
            due_str = f" **DUE: {due[:10]}**" if due else ""
            lines.append(f"- {desc} (to {to_whom}){due_str}")
        lines.append("")
    else:
        lines.append("## Open Commitments")
        lines.append("No open commitments.")
        lines.append("")

    # Recent observations (condensed)
    observations = snapshot["recent_observations"]
    if observations:
        lines.append(f"## Recent Observations ({len(observations)})")
        for o in observations:
            obs_type = o.get("type") or "context_learned"
            content = o.get("content") or ""
            if content:
                lines.append(f"- [{obs_type}] {content[:100]}")
        lines.append("")

    return "\n".join(lines)


def _output_wm_error(llm: bool, human: bool, message: str):
    """Output a graceful error message when working memory is unavailable."""
    if llm:
        typer.echo(f"""# Working Memory Unavailable

{message}

**Alternative**: Use `aech-cli-inbox-assistant search 'query'` to find specific emails.
""")
    elif human:
        typer.echo(f"=== Working Memory Unavailable ===\n{message}")
    else:
        typer.echo(json.dumps({"error": message, "working_memory_available": False}))


@wm_app.command("snapshot")
def wm_snapshot(
    limit_threads: int = typer.Option(10, help="Max active threads to include"),
    limit_decisions: int = typer.Option(5, help="Max pending decisions"),
    limit_observations: int = typer.Option(10, help="Max recent observations"),
    human: bool = typer.Option(False, "--human", help="Human-readable terminal output"),
    llm: bool = typer.Option(False, "--llm", help="LLM-optimized markdown output"),
):
    """
    Get a complete snapshot of current working memory state.

    This is the primary tool for understanding "what's going on" across
    all tracked threads, decisions, commitments, and observations.
    """
    import sqlite3 as sqlite3_module

    try:
        conn = connect_db()
    except FileNotFoundError as e:
        # Database doesn't exist yet - return minimal context
        _output_wm_error(llm, human, f"Inbox not synced yet: {e}")
        return

    try:
        conn_check = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='wm_threads'"
        ).fetchone()
        if not conn_check:
            conn.close()
            _output_wm_error(llm, human, "Working memory not initialized. Use 'aech-cli-inbox-assistant search' to query emails directly.")
            return
    except sqlite3_module.OperationalError as e:
        conn.close()
        _output_wm_error(llm, human, f"Database error checking tables: {e}")
        return

    # Active threads
    threads = conn.execute(
        """
        SELECT * FROM wm_threads
        WHERE status NOT IN ('resolved')
        ORDER BY
            CASE urgency
                WHEN 'immediate' THEN 1
                WHEN 'today' THEN 2
                WHEN 'this_week' THEN 3
                ELSE 4
            END,
            last_activity_at DESC
        LIMIT ?
        """,
        (limit_threads,),
    ).fetchall()

    # Pending decisions
    decisions = conn.execute(
        """
        SELECT * FROM wm_decisions
        WHERE is_resolved = 0
        ORDER BY
            CASE urgency
                WHEN 'immediate' THEN 1
                WHEN 'today' THEN 2
                WHEN 'this_week' THEN 3
                ELSE 4
            END,
            created_at DESC
        LIMIT ?
        """,
        (limit_decisions,),
    ).fetchall()

    # Open commitments
    commitments = conn.execute(
        """
        SELECT * FROM wm_commitments
        WHERE is_completed = 0
        ORDER BY due_by ASC NULLS LAST
        LIMIT 10
        """
    ).fetchall()

    # Recent observations
    observations = conn.execute(
        """
        SELECT * FROM wm_observations
        ORDER BY observed_at DESC
        LIMIT ?
        """,
        (limit_observations,),
    ).fetchall()

    conn.close()

    snapshot = {
        "active_threads": [dict(t) for t in threads],
        "pending_decisions": [dict(d) for d in decisions],
        "open_commitments": [dict(c) for c in commitments],
        "recent_observations": [dict(o) for o in observations],
        "summary": {
            "threads_needing_reply": sum(1 for t in threads if t["needs_reply"]),
            "urgent_threads": sum(
                1 for t in threads if t["urgency"] in ("immediate", "today")
            ),
            "pending_decisions_count": len(decisions),
            "open_commitments_count": len(commitments),
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    if llm:
        typer.echo(_format_snapshot_llm(snapshot))
    elif human:
        typer.echo("=== Working Memory Snapshot ===\n")

        typer.echo("SUMMARY:")
        typer.echo(f"  - {snapshot['summary']['threads_needing_reply']} threads need reply")
        typer.echo(f"  - {snapshot['summary']['urgent_threads']} urgent threads")
        typer.echo(f"  - {snapshot['summary']['pending_decisions_count']} pending decisions")
        typer.echo(f"  - {snapshot['summary']['open_commitments_count']} open commitments")

        if threads:
            typer.echo(f"\nACTIVE THREADS ({len(threads)}):")
            for t in threads:
                urgency = (t["urgency"] or "").upper()
                reply = " [NEEDS REPLY]" if t["needs_reply"] else ""
                subj = (t["subject"] or "")[:50]
                typer.echo(f"  [{urgency}] {subj}{reply}")
                # Include link (use latest_web_link if available)
                link = dict(t).get('latest_web_link')
                if not link and dict(t).get('latest_email_id'):
                    from urllib.parse import quote
                    link = f"https://outlook.office365.com/mail/inbox/id/{quote(t['latest_email_id'], safe='')}"
                if link:
                    typer.echo(f"    Link: {link}")

        if decisions:
            typer.echo(f"\nPENDING DECISIONS ({len(decisions)}):")
            for d in decisions:
                question = (d["question"] or "")[:60]
                typer.echo(f"  - {question}")
                typer.echo(f"    From: {d['requester']}")

        if commitments:
            typer.echo(f"\nOPEN COMMITMENTS ({len(commitments)}):")
            for c in commitments:
                due = f" (due: {c['due_by']})" if c["due_by"] else ""
                desc = (c["description"] or "")[:60]
                typer.echo(f"  - {desc}{due}")
    else:
        typer.echo(json.dumps(snapshot, default=str))


@wm_app.command("threads")
def wm_threads(
    status: Optional[str] = typer.Option(None, help="Filter by status"),
    urgency: Optional[str] = typer.Option(None, help="Filter by urgency"),
    needs_reply: bool = typer.Option(False, "--needs-reply", help="Only threads needing reply"),
    limit: int = typer.Option(20, help="Max results"),
    human: bool = typer.Option(False, "--human"),
):
    """
    Query active threads with optional filters.

    Use this to find specific threads or get detailed thread information.
    """
    conn = connect_db()

    query = "SELECT * FROM wm_threads WHERE 1=1"
    params: List[Any] = []

    if status:
        query += " AND status = ?"
        params.append(status)
    else:
        query += " AND status != 'resolved'"

    if urgency:
        query += " AND urgency = ?"
        params.append(urgency)

    if needs_reply:
        query += " AND needs_reply = 1"

    query += " ORDER BY last_activity_at DESC LIMIT ?"
    params.append(limit)

    threads = conn.execute(query, params).fetchall()
    conn.close()

    results = [dict(t) for t in threads]

    if human:
        if not results:
            typer.echo("No threads found matching criteria.")
            return
        for t in results:
            typer.echo(f"[{t['urgency']}] {t['subject']}")
            typer.echo(f"  Status: {t['status']} | Messages: {t['message_count']}")
            typer.echo(f"  Last activity: {t['last_activity_at']}")
            if t.get("summary"):
                summary = t["summary"][:100]
                typer.echo(f"  Summary: {summary}...")
            # Use latest_web_link (folder-agnostic) if available
            link = t.get('latest_web_link')
            if not link and t.get('latest_email_id'):
                from urllib.parse import quote
                link = f"https://outlook.office365.com/mail/inbox/id/{quote(t['latest_email_id'], safe='')}"
            if link:
                typer.echo(f"  Link: {link}")
            typer.echo("")
    else:
        typer.echo(json.dumps(results, default=str))


@wm_app.command("contacts")
def wm_contacts(
    relationship: Optional[str] = typer.Option(None, help="Filter by relationship type"),
    vip_only: bool = typer.Option(False, "--vip", help="Only VIP contacts"),
    external_only: bool = typer.Option(False, "--external", help="Only external contacts"),
    search: Optional[str] = typer.Option(None, help="Search by email or name"),
    limit: int = typer.Option(20, help="Max results"),
    human: bool = typer.Option(False, "--human"),
):
    """
    Query known contacts and their interaction history.

    Useful for understanding relationships and communication patterns.
    """
    conn = connect_db()

    query = "SELECT * FROM wm_contacts WHERE 1=1"
    params: List[Any] = []

    if relationship:
        query += " AND relationship = ?"
        params.append(relationship)

    if vip_only:
        query += " AND is_vip = 1"

    if external_only:
        query += " AND is_internal = 0"

    if search:
        query += " AND (email LIKE ? OR name LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])

    query += " ORDER BY total_interactions DESC LIMIT ?"
    params.append(limit)

    contacts = conn.execute(query, params).fetchall()
    conn.close()

    results = [dict(c) for c in contacts]

    if human:
        if not results:
            typer.echo("No contacts found.")
            return
        for c in results:
            vip = " [VIP]" if c["is_vip"] else ""
            internal = " (internal)" if c["is_internal"] else " (external)"
            name = c["name"] or c["email"]
            typer.echo(f"{name}{vip}{internal}")
            typer.echo(f"  Email: {c['email']}")
            typer.echo(f"  Interactions: {c['total_interactions']} total")
            typer.echo(f"  Relationship: {c['relationship']}")
            typer.echo("")
    else:
        typer.echo(json.dumps(results, default=str))


@wm_app.command("decisions")
def wm_decisions(
    include_resolved: bool = typer.Option(False, "--include-resolved"),
    limit: int = typer.Option(10, help="Max results"),
    human: bool = typer.Option(False, "--human"),
):
    """
    Query pending decisions requiring user input.

    Decisions are extracted from emails asking for choices or approvals.
    """
    conn = connect_db()

    query = "SELECT * FROM wm_decisions"
    params: List[Any] = []

    if not include_resolved:
        query += " WHERE is_resolved = 0"

    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    decisions = conn.execute(query, params).fetchall()
    conn.close()

    results = [dict(d) for d in decisions]

    if human:
        if not results:
            typer.echo("No pending decisions.")
            return
        for d in results:
            urgency = f"[{d['urgency'].upper()}]" if d["urgency"] else ""
            resolved = " (RESOLVED)" if d["is_resolved"] else ""
            question = (d["question"] or "")[:60]
            typer.echo(f"{urgency} {question}{resolved}")
            typer.echo(f"  From: {d['requester']}")
            if d.get("context"):
                context = d["context"][:80]
                typer.echo(f"  Context: {context}...")
            if d.get("deadline"):
                typer.echo(f"  Deadline: {d['deadline']}")
            typer.echo("")
    else:
        typer.echo(json.dumps(results, default=str))


@wm_app.command("commitments")
def wm_commitments(
    include_completed: bool = typer.Option(False, "--include-completed"),
    overdue_only: bool = typer.Option(False, "--overdue"),
    limit: int = typer.Option(10, help="Max results"),
    human: bool = typer.Option(False, "--human"),
):
    """
    Query commitments the user has made.

    Commitments are tracked when the user promises to do something.
    """
    conn = connect_db()

    query = "SELECT * FROM wm_commitments WHERE 1=1"
    params: List[Any] = []

    if not include_completed:
        query += " AND is_completed = 0"

    if overdue_only:
        query += " AND due_by < datetime('now') AND is_completed = 0"

    query += " ORDER BY due_by ASC NULLS LAST LIMIT ?"
    params.append(limit)

    commitments = conn.execute(query, params).fetchall()
    conn.close()

    results = [dict(c) for c in commitments]

    if human:
        if not results:
            typer.echo("No commitments found.")
            return
        for c in results:
            completed = " (DONE)" if c["is_completed"] else ""
            due = f" [Due: {c['due_by']}]" if c["due_by"] else ""
            desc = (c["description"] or "")[:60]
            typer.echo(f"- {desc}{due}{completed}")
            typer.echo(f"  To: {c['to_whom']}")
            typer.echo("")
    else:
        typer.echo(json.dumps(results, default=str))


@wm_app.command("observations")
def wm_observations(
    type_filter: Optional[str] = typer.Option(None, "--type", help="Filter by observation type"),
    days: int = typer.Option(7, help="Look back N days"),
    limit: int = typer.Option(20, help="Max results"),
    human: bool = typer.Option(False, "--human"),
):
    """
    Query observations from passive learning (CC emails, etc).

    Observations capture context without requiring action.
    """
    conn = connect_db()

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    query = "SELECT * FROM wm_observations WHERE observed_at > ?"
    params: List[Any] = [cutoff]

    if type_filter:
        query += " AND type = ?"
        params.append(type_filter)

    query += " ORDER BY observed_at DESC LIMIT ?"
    params.append(limit)

    observations = conn.execute(query, params).fetchall()
    conn.close()

    results = [dict(o) for o in observations]

    if human:
        if not results:
            typer.echo("No observations in the specified period.")
            return
        for o in results:
            content = (o["content"] or "")[:80]
            typer.echo(f"[{o['type']}] {content}...")
            typer.echo(f"  Observed: {o['observed_at']}")
            typer.echo("")
    else:
        typer.echo(json.dumps(results, default=str))


@wm_app.command("projects")
def wm_projects(
    status: Optional[str] = typer.Option(None, help="Filter by status"),
    limit: int = typer.Option(10, help="Max results"),
    human: bool = typer.Option(False, "--human"),
):
    """
    Query inferred projects and initiatives.

    Projects are automatically detected from email patterns.
    """
    conn = connect_db()

    query = "SELECT * FROM wm_projects WHERE 1=1"
    params: List[Any] = []

    if status:
        query += " AND status = ?"
        params.append(status)

    query += " ORDER BY last_activity_at DESC LIMIT ?"
    params.append(limit)

    projects = conn.execute(query, params).fetchall()
    conn.close()

    results = [dict(p) for p in projects]

    if human:
        if not results:
            typer.echo("No projects tracked.")
            return
        for p in results:
            threads = json.loads(p["related_threads_json"] or "[]")
            typer.echo(f"{p['name']} [{p['status']}]")
            typer.echo(f"  {len(threads)} related threads")
            typer.echo(f"  Last activity: {p['last_activity_at']}")
            if p.get("description"):
                desc = p["description"][:80]
                typer.echo(f"  {desc}...")
            typer.echo("")
    else:
        typer.echo(json.dumps(results, default=str))


@app.command()
def convert_bodies(
    limit: int = typer.Option(0, help="Max emails to process (0 = all)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be processed"),
):
    """
    Backfill body_markdown and signature_block for existing emails.

    Processes emails that have body_html but no body_markdown.
    """
    from src.body_parser import parse_email_body

    conn = connect_db()
    cursor = conn.cursor()

    # Find emails needing conversion
    query = """
        SELECT id, subject, body_html
        FROM emails
        WHERE body_html IS NOT NULL AND body_markdown IS NULL
    """
    if limit > 0:
        query += f" LIMIT {limit}"

    cursor.execute(query)
    rows = cursor.fetchall()

    if not rows:
        typer.echo("No emails need body conversion.")
        return

    typer.echo(f"Found {len(rows)} emails to convert.")

    if dry_run:
        for row in rows[:10]:
            typer.echo(f"  Would convert: {row['subject'][:60]}...")
        if len(rows) > 10:
            typer.echo(f"  ... and {len(rows) - 10} more")
        return

    converted = 0
    errors = 0

    for row in rows:
        try:
            parsed = parse_email_body(row["body_html"])
            cursor.execute(
                """
                UPDATE emails
                SET body_markdown = ?, signature_block = ?
                WHERE id = ?
                """,
                (parsed.main_content, parsed.signature_block, row["id"]),
            )
            converted += 1

            if converted % 100 == 0:
                conn.commit()
                typer.echo(f"  Converted {converted} emails...")
        except Exception as e:
            errors += 1
            logger.error(f"Error converting {row['id']}: {e}")

    conn.commit()
    conn.close()

    typer.echo(f"Done: {converted} converted, {errors} errors.")


@app.command("cleanup")
def cleanup_inbox(
    action: str = typer.Argument(
        None,
        help="Action to take: 'delete' (move to Deleted Items), 'archive', or omit to just show summary"
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be done without making changes"),
    limit: int = typer.Option(100, "--limit", help="Maximum number of emails to process"),
    concurrency: int = typer.Option(10, "--concurrency", help="Number of parallel API calls"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Clean up inbox based on LLM-suggested actions.

    After running 'extract-content', emails are classified with suggested_action:
    - 'delete': calendar accepts, delivery receipts, expired auth codes
    - 'archive': read newsletters, FYI notifications
    - 'keep': real conversations, actionable items

    Examples:
        aech-cli-inbox-assistant cleanup --human           # Show summary
        aech-cli-inbox-assistant cleanup delete --dry-run  # Preview deletions
        aech-cli-inbox-assistant cleanup delete            # Move to Deleted Items
    """
    import subprocess
    import concurrent.futures

    conn = connect_db()
    cursor = conn.cursor()

    # Get summary by suggested_action
    cursor.execute("""
        SELECT suggested_action, COUNT(*) as count
        FROM emails
        WHERE suggested_action IS NOT NULL
        GROUP BY suggested_action
    """)
    summary = {row["suggested_action"]: row["count"] for row in cursor.fetchall()}

    if action is None:
        # Just show summary
        if human:
            typer.echo("=== Inbox Cleanup Summary ===")
            typer.echo(f"  Keep:    {summary.get('keep', 0):,} emails")
            typer.echo(f"  Archive: {summary.get('archive', 0):,} emails")
            typer.echo(f"  Delete:  {summary.get('delete', 0):,} emails")
            typer.echo("")
            typer.echo("To clean up, run extract-content first, then:")
            typer.echo("  aech-cli-inbox-assistant cleanup delete --dry-run")
        else:
            typer.echo(json.dumps(summary))
        conn.close()
        return

    if action not in ("delete", "archive"):
        typer.echo(f"Error: action must be 'delete' or 'archive', got '{action}'", err=True)
        raise typer.Exit(1)

    # Get emails to process
    cursor.execute("""
        SELECT id, subject, sender, received_at
        FROM emails
        WHERE suggested_action = ?
        ORDER BY received_at DESC
        LIMIT ?
    """, (action, limit))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        if human:
            typer.echo(f"No emails with suggested_action='{action}' found.")
            typer.echo("Run 'extract-content' first to classify emails.")
        else:
            typer.echo(json.dumps({"action": action, "found": 0, "processed": 0}))
        return

    if human:
        typer.echo(f"Found {len(rows)} emails to {action}")

    if dry_run:
        if human:
            typer.echo(f"\n[DRY RUN] Would {action} {len(rows)} emails:")
            for row in rows[:20]:
                typer.echo(f"  - {row['subject'][:60]}...")
                typer.echo(f"    From: {row['sender']}")
            if len(rows) > 20:
                typer.echo(f"  ... and {len(rows) - 20} more")
        else:
            typer.echo(json.dumps({
                "dry_run": True,
                "action": action,
                "found": len(rows),
                "emails": [dict(r) for r in rows[:20]]
            }, default=str))
        return

    # Get user email for Graph API
    user_email = os.environ.get("DELEGATED_USER")
    if not user_email:
        typer.echo("Error: DELEGATED_USER environment variable not set", err=True)
        raise typer.Exit(1)

    # Determine destination folder
    dest_folder = "deleteditems" if action == "delete" else "archive"

    results = {"processed": 0, "failed": 0, "errors": []}

    def move_email(email_id: str, subject: str) -> bool:
        try:
            cmd = [
                "aech-cli-msgraph", "move-email",
                email_id,
                dest_folder,
                "--user", user_email,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            return result.returncode == 0
        except Exception as e:
            results["errors"].append(f"{subject[:40]}: {e}")
            return False

    if human:
        typer.echo(f"\nMoving emails to {dest_folder}...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(move_email, row["id"], row["subject"]): row
            for row in rows
        }

        for future in concurrent.futures.as_completed(futures):
            row = futures[future]
            if future.result():
                results["processed"] += 1
                # Update local DB to reflect the move
                conn = connect_db()
                if action == "delete":
                    conn.execute("DELETE FROM emails WHERE id = ?", (row["id"],))
                else:
                    conn.execute("UPDATE emails SET suggested_action = 'archived' WHERE id = ?", (row["id"],))
                conn.commit()
                conn.close()
            else:
                results["failed"] += 1

            if human and (results["processed"] + results["failed"]) % 25 == 0:
                typer.echo(f"  Progress: {results['processed'] + results['failed']}/{len(rows)}")

    if human:
        typer.echo(f"\nResults:")
        typer.echo(f"  Processed: {results['processed']}")
        typer.echo(f"  Failed:    {results['failed']}")
        if results["errors"]:
            typer.echo(f"\nFirst few errors:")
            for err in results["errors"][:5]:
                typer.echo(f"  - {err}")
    else:
        typer.echo(json.dumps(results, default=str))


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
