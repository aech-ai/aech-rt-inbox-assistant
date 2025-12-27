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

    # FTS-only search (original behavior)
    conn = connect_db()
    cursor = conn.cursor()
    results = []

    # Prefer FTS if available
    try:
        cursor.execute(
            """
            SELECT e.id, e.subject, e.body_preview, e.received_at, e.category, e.is_read,
                   e.sender, e.web_link, bm25(emails_fts) AS rank
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
            SELECT id, subject, body_preview, received_at, category, is_read, sender, web_link
            FROM emails
            WHERE subject LIKE ? OR body_preview LIKE ?
            ORDER BY received_at DESC
            LIMIT ?
            """,
            (sql_query, sql_query, limit),
        )
        rows = cursor.fetchall()
        results = [dict(row) for row in rows]

    if human:
        if not results:
            typer.echo("No results.")
        for email in results:
            typer.echo(f"Subject: {email['subject']}")
            typer.echo(f"  From: {email.get('sender', 'N/A')}")
            typer.echo(f"  Received: {email['received_at']}")
            typer.echo(f"  Category: {email.get('category', 'N/A')}")
            # Use web_link if available
            link = email.get('web_link')
            if not link and email.get('id'):
                from urllib.parse import quote
                link = f"https://outlook.office365.com/mail/inbox/id/{quote(email['id'], safe='')}"
            if link:
                typer.echo(f"  Link: {link}")
            typer.echo()
    else:
        typer.echo(json.dumps(results, default=str))

@app.command()
def dbpath():
    """Get the absolute path to the user's database."""
    typer.echo(get_db_path())


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
    status_filter: str = typer.Option(None, "--status", help="Filter by status (pending/success/failed/unsupported)"),
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
    from src.categories_config import DEFAULT_CATEGORIES, get_categories, COLOR_PRESETS

    prefs = read_preferences()

    # Reset or initialize categories in preferences
    if reset_defaults or "outlook_categories" not in prefs:
        prefs["outlook_categories"] = DEFAULT_CATEGORIES
        write_preferences(prefs)
        if human:
            typer.echo("Initialized default categories in preferences.json")

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

        typer.echo(f"\nCategories mode: {'enabled' if prefs.get('use_outlook_categories', True) else 'disabled'}")
        typer.echo("Set 'use_outlook_categories' to false in preferences to use legacy folder mode.")
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


@wm_app.command("snapshot")
def wm_snapshot(
    limit_threads: int = typer.Option(10, help="Max active threads to include"),
    limit_decisions: int = typer.Option(5, help="Max pending decisions"),
    limit_observations: int = typer.Option(10, help="Max recent observations"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """
    Get a complete snapshot of current working memory state.

    This is the primary tool for understanding "what's going on" across
    all tracked threads, decisions, commitments, and observations.
    """
    conn = connect_db()

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

    if human:
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
                link = t.get('latest_web_link')
                if not link and t.get('latest_email_id'):
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
