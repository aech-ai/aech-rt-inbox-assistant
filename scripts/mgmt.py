#!/usr/bin/env python3
"""
Management CLI for Inbox Assistant pipeline operations.

This CLI provides admin/pipeline commands for managing the inbox assistant.
These are NOT agent-facing commands - they are for operators running pipelines.

Usage (from repo root):
    python scripts/mgmt.py --help
    python scripts/mgmt.py stats --human
    python scripts/mgmt.py sync --since 2025-01-01 --human
"""

import json
import sys
from pathlib import Path

# Add repo root to path so we can import from src.*
_repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(_repo_root))

import typer

app = typer.Typer(
    help="Management CLI for Inbox Assistant pipeline operations.",
    no_args_is_help=True,
    add_completion=False,
)


@app.command("embed")
def embed_chunks(
    limit: int = typer.Option(1000, help="Number of chunks to embed"),
    batch_size: int = typer.Option(64, help="Batch size for model inference"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """
    Generate embeddings for vector similarity search.

    Creates embeddings for chunks that don't have them yet.
    Uses BAAI/bge-m3 by default (configurable via EMBEDDING_MODEL env var).
    """
    try:
        from src.embeddings import embed_pending_chunks, MODEL_NAME
    except ImportError:
        typer.echo("Error: Embeddings module not available.", err=True)
        raise typer.Exit(1)

    if human:
        typer.echo(f"Generating embeddings using model: {MODEL_NAME}")
        typer.echo(f"Processing up to {limit} chunks (batch size: {batch_size})...")

        def show_progress(processed: int, total: int):
            pct = int(processed / total * 100) if total > 0 else 0
            bar_len = 30
            filled = int(bar_len * processed / total) if total > 0 else 0
            bar = "#" * filled + "-" * (bar_len - filled)
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

        if results["total_pending"] > 0:
            typer.echo(f"\nNote: Run again to process remaining chunks.")
    else:
        results = embed_pending_chunks(limit=limit, enrich=True, batch_size=batch_size)
        typer.echo(json.dumps(results))


@app.command("index")
def index_chunks(
    limit: int = typer.Option(1000, help="Number of items to process"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """
    Create search chunks from emails and attachments.

    Processes emails and attachments that haven't been chunked yet.
    Run this before 'embed' to prepare content for vector search.
    """
    try:
        from src.chunker import process_unindexed_emails, process_unindexed_attachments
    except ImportError:
        typer.echo("Error: Chunker module not available.", err=True)
        raise typer.Exit(1)

    email_results = process_unindexed_emails(limit=limit)
    att_results = process_unindexed_attachments(limit=limit)

    results = {
        "emails_processed": email_results["processed"],
        "emails_skipped": email_results["skipped"],
        "email_chunks": email_results["chunks_created"],
        "attachments_processed": att_results["processed"],
        "attachment_chunks": att_results["chunks_created"],
        "total_chunks": email_results["chunks_created"] + att_results["chunks_created"],
    }

    if human:
        typer.echo("=== Search Index Results ===")
        typer.echo(f"Emails processed:     {results['emails_processed']}")
        typer.echo(f"Emails skipped:       {results['emails_skipped']}")
        typer.echo(f"Email chunks created: {results['email_chunks']}")
        typer.echo(f"Attachments processed: {results['attachments_processed']}")
        typer.echo(f"Attachment chunks:    {results['attachment_chunks']}")
        typer.echo(f"Total chunks created: {results['total_chunks']}")
    else:
        typer.echo(json.dumps(results))


@app.command("extract-attachments")
def extract_attachments(
    limit: int = typer.Option(50, help="Number of attachments to process"),
    concurrency: int = typer.Option(5, help="Number of concurrent workers"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """
    Extract text from pending attachments.

    Downloads attachments from Graph API and extracts text using
    aech-cli-documents. Supports PDFs, Word docs, Excel, PowerPoint, etc.
    """
    import asyncio

    try:
        from src.attachments import AttachmentProcessor
    except ImportError:
        typer.echo("Error: Attachments module not available.", err=True)
        raise typer.Exit(1)

    processor = AttachmentProcessor()

    if human:
        typer.echo(f"Processing up to {limit} attachments (concurrency: {concurrency})...")

        def show_progress(current: int, total: int, filename: str):
            pct = int(current / total * 100) if total > 0 else 0
            fname = filename[:30] + "..." if len(filename) > 30 else filename
            print(f"\r  [{pct:3d}%] ({current}/{total}) {fname:<35}", end="", flush=True)

        results = asyncio.run(
            processor.process_pending_attachments_async(
                limit=limit,
                concurrency=concurrency,
                progress_callback=show_progress,
            )
        )
        print()  # newline after progress

        typer.echo(f"\nResults:")
        typer.echo(f"  Completed: {results['completed']}")
        typer.echo(f"  Failed:    {results['failed']}")
        typer.echo(f"  Skipped:   {results['skipped']}")
    else:
        results = asyncio.run(
            processor.process_pending_attachments_async(limit=limit, concurrency=concurrency)
        )
        typer.echo(json.dumps(results))


@app.command("backfill-bodies")
def backfill_bodies(
    limit: int = typer.Option(100, help="Number of emails to process"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """
    Fetch and convert email bodies for emails missing body_markdown.

    Downloads full HTML bodies from Graph API and converts to markdown.
    """
    try:
        from src.database import get_connection
        from src.poller import GraphPoller
        from src.body_parser import html_to_markdown
    except ImportError as e:
        typer.echo(f"Error: Required module not available: {e}", err=True)
        raise typer.Exit(1)

    conn = get_connection()
    emails = conn.execute(
        """
        SELECT id FROM emails
        WHERE (body_markdown IS NULL OR body_markdown = '')
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    if not emails:
        if human:
            typer.echo("No emails need body backfill.")
        else:
            typer.echo(json.dumps({"processed": 0, "failed": 0}))
        return

    poller = GraphPoller()
    processed = 0
    failed = 0

    if human:
        typer.echo(f"Processing {len(emails)} emails...")

    for i, row in enumerate(emails):
        email_id = row["id"]
        if human:
            pct = int((i + 1) / len(emails) * 100)
            print(f"\r  [{pct:3d}%] ({i + 1}/{len(emails)})", end="", flush=True)

        try:
            body_html = poller._get_message_body(email_id)
            if body_html:
                body_markdown = html_to_markdown(body_html)
                conn = get_connection()
                conn.execute(
                    "UPDATE emails SET body_markdown = ?, body_html = ? WHERE id = ?",
                    (body_markdown, body_html, email_id),
                )
                conn.commit()
                conn.close()
                processed += 1
            else:
                failed += 1
        except Exception as e:
            if human:
                typer.echo(f"\n  Error processing {email_id}: {e}")
            failed += 1

    if human:
        print()  # newline after progress
        typer.echo(f"\nResults:")
        typer.echo(f"  Processed: {processed}")
        typer.echo(f"  Failed:    {failed}")
    else:
        typer.echo(json.dumps({"processed": processed, "failed": failed}))


@app.command("sync")
def sync_emails(
    folder: str = typer.Option(None, help="Specific folder to sync (default: all)"),
    since: str = typer.Option(None, help="Only sync emails since date (YYYY-MM-DD)"),
    no_bodies: bool = typer.Option(False, "--no-bodies", help="Skip fetching email bodies"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """
    Sync emails from Microsoft Graph API.

    Fetches email metadata and optionally bodies from all mail folders.
    """
    from datetime import datetime

    try:
        from src.database import init_db
        from src.poller import GraphPoller
    except ImportError as e:
        typer.echo(f"Error: Required module not available: {e}", err=True)
        raise typer.Exit(1)

    init_db()
    poller = GraphPoller()

    since_date = None
    if since:
        try:
            since_date = datetime.strptime(since, "%Y-%m-%d")
        except ValueError:
            typer.echo(f"Invalid date format: {since}. Use YYYY-MM-DD.", err=True)
            raise typer.Exit(1)

    if folder:
        # Sync specific folder
        folders = poller.get_all_folders()
        target = next((f for f in folders if f.get("displayName", "").lower() == folder.lower()), None)
        if not target:
            typer.echo(f"Folder not found: {folder}", err=True)
            raise typer.Exit(1)

        if human:
            typer.echo(f"Syncing folder: {folder}...")

        count = poller.full_sync_folder(
            target["id"],
            target["displayName"],
            fetch_body=not no_bodies,
        )

        if human:
            typer.echo(f"Synced {count} messages from {folder}")
        else:
            typer.echo(json.dumps({"folder": folder, "messages": count}))
    else:
        # Sync all folders
        if human:
            typer.echo("Syncing all folders...")

            def folder_progress(current: int, total: int, name: str):
                print(f"\r  Folder {current}/{total}: {name:<30}", end="", flush=True)

            def message_progress(count: int, folder_name: str):
                print(f"\r  {folder_name}: {count} messages synced", end="", flush=True)

            results = poller.sync_all_folders(
                fetch_body=not no_bodies,
                progress_callback=folder_progress,
                message_callback=message_progress,
                since_date=since_date,
            )
            print()  # newline after progress

            typer.echo(f"\nResults:")
            typer.echo(f"  Folders synced:   {results['folders_synced']}")
            typer.echo(f"  Folders skipped:  {results['folders_skipped']}")
            typer.echo(f"  Total messages:   {results['total_messages']}")
        else:
            results = poller.sync_all_folders(
                fetch_body=not no_bodies,
                since_date=since_date,
            )
            typer.echo(json.dumps(results))


@app.command("stats")
def show_stats(
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """
    Show pipeline statistics.

    Displays counts for emails, attachments, chunks, and embeddings.
    """
    try:
        from src.database import get_connection
    except ImportError:
        typer.echo("Error: Database module not available.", err=True)
        raise typer.Exit(1)

    conn = get_connection()
    cursor = conn.cursor()

    stats = {}

    # Email counts
    cursor.execute("SELECT COUNT(*) FROM emails")
    stats["total_emails"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM emails WHERE body_markdown IS NOT NULL")
    stats["emails_with_body"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM emails WHERE body_markdown IS NULL OR body_markdown = ''")
    stats["emails_missing_body"] = cursor.fetchone()[0]

    # Attachment counts
    cursor.execute("SELECT COUNT(*) FROM attachments")
    stats["total_attachments"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM attachments WHERE extraction_status = 'pending'")
    stats["attachments_pending"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM attachments WHERE extraction_status = 'completed'")
    stats["attachments_completed"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM attachments WHERE extraction_status = 'failed'")
    stats["attachments_failed"] = cursor.fetchone()[0]

    # Chunk counts
    cursor.execute("SELECT COUNT(*) FROM chunks")
    stats["total_chunks"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM chunks WHERE embedding IS NOT NULL")
    stats["chunks_with_embedding"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM chunks WHERE embedding IS NULL")
    stats["chunks_pending_embedding"] = cursor.fetchone()[0]

    conn.close()

    if human:
        typer.echo("=== Pipeline Statistics ===")
        typer.echo("")
        typer.echo("Emails:")
        typer.echo(f"  Total:           {stats['total_emails']:,}")
        typer.echo(f"  With body:       {stats['emails_with_body']:,}")
        typer.echo(f"  Missing body:    {stats['emails_missing_body']:,}")
        typer.echo("")
        typer.echo("Attachments:")
        typer.echo(f"  Total:           {stats['total_attachments']:,}")
        typer.echo(f"  Pending:         {stats['attachments_pending']:,}")
        typer.echo(f"  Completed:       {stats['attachments_completed']:,}")
        typer.echo(f"  Failed:          {stats['attachments_failed']:,}")
        typer.echo("")
        typer.echo("Chunks:")
        typer.echo(f"  Total:           {stats['total_chunks']:,}")
        typer.echo(f"  With embedding:  {stats['chunks_with_embedding']:,}")
        typer.echo(f"  Pending embed:   {stats['chunks_pending_embedding']:,}")
    else:
        typer.echo(json.dumps(stats))


def run():
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    run()
