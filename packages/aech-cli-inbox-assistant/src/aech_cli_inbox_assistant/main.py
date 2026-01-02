"""
CLI for Agent Aech to query Inbox Assistant state.

This CLI is a read-only query interface to the SQLite database
populated by the RT (real-time) inbox assistant service.
"""
import os
import typer
import sqlite3
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .state import (
    connect_db,
    get_db_path,
    read_preferences,
    set_preference_from_string,
    write_preferences,
    InvalidPreferenceKeyError,
    VALID_PREFERENCE_KEYS,
)


def get_user_timezone() -> ZoneInfo:
    """Get the user's timezone from environment or preferences.

    Priority:
    1. DEFAULT_TIMEZONE environment variable
    2. 'timezone' key in preferences.json
    3. Fall back to UTC
    """
    # Check environment first
    tz_name = os.environ.get("DEFAULT_TIMEZONE")

    # Then check preferences
    if not tz_name:
        prefs = read_preferences()
        tz_name = prefs.get("timezone")

    # Fall back to UTC
    if not tz_name:
        tz_name = "UTC"

    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo("UTC")


def now_in_user_tz() -> datetime:
    """Get current datetime in user's timezone."""
    return datetime.now(get_user_timezone())


def today_in_user_tz():
    """Get today's date in user's timezone."""
    return now_in_user_tz().date()

app = typer.Typer(
    help="Query Inbox Assistant state and preferences.",
    no_args_is_help=True,
    add_completion=False,
)

prefs_app = typer.Typer(help="Manage `/home/agentaech/preferences.json`.", add_completion=False)
app.add_typer(prefs_app, name="prefs")

wm_app = typer.Typer(help="Working Memory - EA cognitive state.", add_completion=False)
app.add_typer(wm_app, name="wm")

alerts_app = typer.Typer(help="Manage user-defined alert rules.", add_completion=False)
app.add_typer(alerts_app, name="alerts")


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
            typer.echo(f"  Urgency: {email.get('urgency', 'N/A')}")
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
            typer.echo(f"{log['timestamp']} - {log['subject']} - urgency: {log.get('urgency', 'N/A')} ({log.get('reason', 'N/A')})")
    else:
        typer.echo(json.dumps(logs, default=str))


@app.command()
def search(
    query: str,
    limit: int = typer.Option(20, help="Number of results to return"),
    mode: str = typer.Option("hybrid", help="Search mode: hybrid, fts, or vector"),
    include_facts: bool = typer.Option(True, "--facts/--no-facts", help="Include facts in search"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON")
):
    """Search emails, attachments, and facts using unified search.

    Combines full-text search (FTS) with vector similarity search for best results.
    """
    import sys
    from pathlib import Path

    # Add src to path for imports
    src_path = Path(__file__).parent.parent.parent.parent.parent / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    try:
        from search import unified_search
    except ImportError:
        # Fallback to basic FTS if unified search unavailable
        typer.echo("Warning: Unified search unavailable, using basic FTS", err=True)
        _search_fallback(query, limit, human)
        return

    results = unified_search(
        query=query,
        limit=limit,
        mode=mode,
        include_facts=include_facts,
        recency_weight=True,
    )

    if human:
        if not results:
            typer.echo("No results found.")
            return

        for r in results:
            result_type = r.result_type.upper()
            if result_type == "FACT":
                typer.echo(f"[FACT:{r.fact_type}] {r.fact_value}")
                if r.email_subject:
                    typer.echo(f"  From email: {r.email_subject}")
                if r.email_sender:
                    typer.echo(f"  Sender: {r.email_sender}")
            elif result_type == "ATTACHMENT":
                typer.echo(f"[ATTACHMENT] {r.filename or 'Unknown'}")
                if r.email_subject:
                    typer.echo(f"  In email: {r.email_subject}")
                if r.email_sender:
                    typer.echo(f"  From: {r.email_sender}")
                if r.content_preview:
                    preview = r.content_preview[:200].replace('\n', ' ')
                    typer.echo(f"  Preview: {preview}...")
            else:  # EMAIL or VIRTUAL_EMAIL
                typer.echo(f"[EMAIL] {r.email_subject or '(no subject)'}")
                if r.email_sender:
                    typer.echo(f"  From: {r.email_sender}")
                if r.email_date:
                    typer.echo(f"  Date: {r.email_date}")
                if r.content_preview:
                    preview = r.content_preview[:200].replace('\n', ' ')
                    typer.echo(f"  Preview: {preview}...")

            # Show link for all types
            link = r.web_link
            if not link and r.source_id:
                from urllib.parse import quote
                link = f"https://outlook.office365.com/mail/inbox/id/{quote(r.source_id, safe='')}"
            if link:
                typer.echo(f"  Link: {link}")
            typer.echo(f"  Score: {r.score:.3f}")
            typer.echo()
    else:
        # JSON output
        output = []
        for r in results:
            item = {
                "id": r.id,
                "result_type": r.result_type,
                "source_id": r.source_id,
                "content_preview": r.content_preview,
                "score": r.score,
            }
            if r.email_subject:
                item["email_subject"] = r.email_subject
            if r.email_sender:
                item["email_sender"] = r.email_sender
            if r.email_date:
                item["email_date"] = r.email_date
            if r.conversation_id:
                item["conversation_id"] = r.conversation_id
            if r.filename:
                item["filename"] = r.filename
            if r.fact_type:
                item["fact_type"] = r.fact_type
            if r.fact_value:
                item["fact_value"] = r.fact_value
            if r.web_link:
                item["web_link"] = r.web_link
            output.append(item)
        typer.echo(json.dumps(output, default=str))


def _search_fallback(query: str, limit: int, human: bool):
    """Fallback search using basic FTS when unified search is unavailable."""
    conn = connect_db()
    cursor = conn.cursor()
    results = []

    try:
        cursor.execute(
            """
            SELECT e.id, e.subject, e.body_preview, e.received_at, e.sender, e.web_link
            FROM emails_fts
            JOIN emails e ON emails_fts.id = e.id
            WHERE emails_fts MATCH ?
            ORDER BY bm25(emails_fts)
            LIMIT ?
            """,
            (query, limit),
        )
        results = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error:
        pass

    conn.close()

    if human:
        if not results:
            typer.echo("No results found.")
            return
        for r in results:
            typer.echo(f"[EMAIL] {r['subject']}")
            typer.echo(f"  From: {r.get('sender', 'N/A')}")
            typer.echo(f"  Date: {r['received_at']}")
            typer.echo()
    else:
        typer.echo(json.dumps(results, default=str))


@app.command()
def dbpath():
    """Get the absolute path to the user's database."""
    typer.echo(get_db_path())


@app.command("timezone")
def show_timezone():
    """Show the current timezone being used for calendar queries."""
    tz = get_user_timezone()
    now = now_in_user_tz()
    typer.echo(f"Timezone: {tz}")
    typer.echo(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    typer.echo(f"Today's date: {today_in_user_tz()}")


@app.command("sync-status")
def sync_status(
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Show the sync status for all folders."""
    conn = connect_db()
    cursor = conn.cursor()

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

    cursor.execute("SELECT COUNT(*) FROM emails WHERE body_markdown IS NOT NULL")
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
    limit: int = typer.Option(20, help="Number of threads to list"),
    include_stale: bool = typer.Option(False, "--include-stale", help="Include stale threads"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """List threads currently marked as requiring a reply.

    Queries Working Memory threads. By default excludes stale threads (no activity for 3+ days).
    """
    conn = connect_db()

    # Build query - exclude stale by default
    status_filter = "" if include_stale else "AND status != 'stale'"

    rows = conn.execute(
        f"""
        SELECT id, conversation_id, subject, last_activity_at, urgency, summary, status
        FROM wm_threads
        WHERE needs_reply = 1
          {status_filter}
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
        (limit,),
    ).fetchall()
    conn.close()

    items = [dict(r) for r in rows]

    if human:
        if not items:
            typer.echo("No threads needing reply.")
            return
        for item in items:
            urgency = item.get('urgency', 'someday')
            status = item.get('status', 'active')
            typer.echo(f"[{urgency.upper()}] {item['subject']}")
            if item.get('summary'):
                typer.echo(f"  Summary: {item['summary'][:100]}...")
            typer.echo(f"  Last activity: {item['last_activity_at']}")
            typer.echo(f"  Status: {status}")
            typer.echo()
    else:
        typer.echo(json.dumps(items, default=str))


# =============================================================================
# Calendar Commands (read from calendar_events table)
# =============================================================================

def _format_event(event: dict, human: bool = True) -> str:
    """Format a calendar event for display."""
    start = event.get("start_at", "")[:16].replace("T", " ")
    end = event.get("end_at", "")[:16].replace("T", " ")
    subject = event.get("subject") or "(No subject)"
    location = event.get("location") or ""
    show_as = event.get("show_as", "busy")

    if human:
        lines = [f"{start} - {end[-5:]}  {subject}"]
        if location:
            lines.append(f"  Location: {location}")
        if event.get("is_online_meeting"):
            url = event.get("online_meeting_url") or "Teams meeting"
            lines.append(f"  Online: {url}")
        if show_as != "busy":
            lines.append(f"  Show as: {show_as}")
        return "\n".join(lines)
    return json.dumps(event, default=str)


@app.command("calendar-today")
def calendar_today(
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Show today's calendar events (in user's timezone)."""
    conn = connect_db()
    cursor = conn.cursor()

    # Get today's date range in user's timezone
    today = today_in_user_tz()
    start = today.isoformat() + "T00:00:00"
    end = (today + timedelta(days=1)).isoformat() + "T00:00:00"

    cursor.execute(
        """
        SELECT * FROM calendar_events
        WHERE start_at >= ? AND start_at < ?
          AND is_cancelled = 0
        ORDER BY start_at
        """,
        (start, end),
    )
    rows = cursor.fetchall()
    conn.close()

    events = [dict(r) for r in rows]

    if human:
        if not events:
            typer.echo(f"No events today ({today}, {get_user_timezone()}).")
            return
        typer.echo(f"=== Today's Agenda ({today}, {get_user_timezone()}) ===\n")
        for event in events:
            typer.echo(_format_event(event, human=True))
            typer.echo()
    else:
        typer.echo(json.dumps(events, default=str))


@app.command("calendar-week")
def calendar_week(
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Show this week's calendar events (in user's timezone)."""
    conn = connect_db()
    cursor = conn.cursor()

    today = today_in_user_tz()
    # Start from today, go 7 days forward
    start = today.isoformat() + "T00:00:00"
    end = (today + timedelta(days=7)).isoformat() + "T00:00:00"

    cursor.execute(
        """
        SELECT * FROM calendar_events
        WHERE start_at >= ? AND start_at < ?
          AND is_cancelled = 0
        ORDER BY start_at
        """,
        (start, end),
    )
    rows = cursor.fetchall()
    conn.close()

    events = [dict(r) for r in rows]

    if human:
        if not events:
            typer.echo(f"No events this week ({get_user_timezone()}).")
            return
        typer.echo(f"=== This Week ({today} to {today + timedelta(days=6)}, {get_user_timezone()}) ===\n")
        current_date = None
        for event in events:
            event_date = event["start_at"][:10]
            if event_date != current_date:
                current_date = event_date
                typer.echo(f"\n--- {current_date} ---")
            typer.echo(_format_event(event, human=True))
    else:
        typer.echo(json.dumps(events, default=str))


@app.command("calendar-upcoming")
def calendar_upcoming(
    hours: int = typer.Option(24, help="Number of hours to look ahead"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Show upcoming events in the next N hours (in user's timezone)."""
    conn = connect_db()
    cursor = conn.cursor()

    now = now_in_user_tz()
    end = now + timedelta(hours=hours)

    # DB stores naive datetimes, so strip timezone for comparison
    now_naive = now.strftime("%Y-%m-%dT%H:%M:%S")
    end_naive = end.strftime("%Y-%m-%dT%H:%M:%S")

    cursor.execute(
        """
        SELECT * FROM calendar_events
        WHERE start_at >= ? AND start_at < ?
          AND is_cancelled = 0
        ORDER BY start_at
        """,
        (now_naive, end_naive),
    )
    rows = cursor.fetchall()
    conn.close()

    events = [dict(r) for r in rows]

    if human:
        if not events:
            typer.echo(f"No events in the next {hours} hours ({get_user_timezone()}).")
            return
        typer.echo(f"=== Next {hours} Hours ({get_user_timezone()}) ===\n")
        for event in events:
            typer.echo(_format_event(event, human=True))
            typer.echo()
    else:
        typer.echo(json.dumps(events, default=str))


@app.command("calendar-free")
def calendar_free(
    date: str = typer.Argument(..., help="Date to check (YYYY-MM-DD)"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Show free time slots on a given date (in user's timezone)."""
    conn = connect_db()
    cursor = conn.cursor()

    # Parse date
    try:
        check_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        typer.echo(f"Invalid date format: {date}. Use YYYY-MM-DD.", err=True)
        raise typer.Exit(1)

    start = check_date.isoformat() + "T00:00:00"
    end = (check_date + timedelta(days=1)).isoformat() + "T00:00:00"

    # Get busy periods (only events that block time)
    cursor.execute(
        """
        SELECT start_at, end_at, subject, show_as FROM calendar_events
        WHERE start_at >= ? AND start_at < ?
          AND is_cancelled = 0
          AND show_as IN ('busy', 'tentative', 'oof')
        ORDER BY start_at
        """,
        (start, end),
    )
    busy_rows = cursor.fetchall()
    conn.close()

    # Calculate free slots (assuming 9am-5pm work hours)
    work_start = datetime.combine(check_date, datetime.strptime("09:00", "%H:%M").time())
    work_end = datetime.combine(check_date, datetime.strptime("17:00", "%H:%M").time())

    busy_periods = []
    for row in busy_rows:
        busy_start = datetime.fromisoformat(row["start_at"].replace("Z", "+00:00").replace("+00:00", ""))
        busy_end = datetime.fromisoformat(row["end_at"].replace("Z", "+00:00").replace("+00:00", ""))
        busy_periods.append((busy_start, busy_end, row["subject"]))

    # Find free slots
    free_slots = []
    current = work_start

    for busy_start, busy_end, subject in sorted(busy_periods):
        if busy_start > current:
            free_slots.append({
                "start": current.isoformat(),
                "end": busy_start.isoformat(),
                "duration_minutes": int((busy_start - current).total_seconds() / 60),
            })
        current = max(current, busy_end)

    if current < work_end:
        free_slots.append({
            "start": current.isoformat(),
            "end": work_end.isoformat(),
            "duration_minutes": int((work_end - current).total_seconds() / 60),
        })

    if human:
        if not free_slots:
            typer.echo(f"No free time on {date} (within 9am-5pm).")
            return
        typer.echo(f"=== Free Time on {date} ===\n")
        for slot in free_slots:
            start_time = slot["start"][11:16]
            end_time = slot["end"][11:16]
            typer.echo(f"  {start_time} - {end_time} ({slot['duration_minutes']} min)")
    else:
        typer.echo(json.dumps(free_slots, default=str))


@app.command("calendar-busy")
def calendar_busy(
    start: str = typer.Argument(..., help="Start datetime (YYYY-MM-DDTHH:MM or YYYY-MM-DD HH:MM)"),
    end: str = typer.Argument(..., help="End datetime"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Check if busy during a time range (times interpreted in user's timezone)."""
    conn = connect_db()
    cursor = conn.cursor()

    # Normalize datetime format
    start = start.replace(" ", "T")
    end = end.replace(" ", "T")

    cursor.execute(
        """
        SELECT id, subject, start_at, end_at, show_as FROM calendar_events
        WHERE start_at < ? AND end_at > ?
          AND is_cancelled = 0
          AND show_as IN ('busy', 'tentative', 'oof')
        ORDER BY start_at
        """,
        (end, start),  # Overlap check: event.start < query.end AND event.end > query.start
    )
    rows = cursor.fetchall()
    conn.close()

    conflicts = [dict(r) for r in rows]

    if human:
        if not conflicts:
            typer.echo(f"FREE: No conflicts between {start} and {end}")
        else:
            typer.echo(f"BUSY: {len(conflicts)} conflicting event(s):")
            for c in conflicts:
                typer.echo(f"  - {c['subject']} ({c['start_at'][:16]} to {c['end_at'][:16]})")
    else:
        typer.echo(json.dumps({
            "is_busy": len(conflicts) > 0,
            "conflicts": conflicts,
        }, default=str))


@app.command("calendar-event")
def calendar_event(
    event_id: str = typer.Argument(..., help="Event ID"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Get details of a specific calendar event."""
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM calendar_events WHERE id = ?", (event_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        typer.echo(f"Event not found: {event_id}", err=True)
        raise typer.Exit(1)

    event = dict(row)

    if human:
        typer.echo(f"Subject: {event['subject']}")
        typer.echo(f"Start: {event['start_at']}")
        typer.echo(f"End: {event['end_at']}")
        if event.get("location"):
            typer.echo(f"Location: {event['location']}")
        if event.get("is_online_meeting"):
            typer.echo(f"Online Meeting: {event.get('online_meeting_url', 'Yes')}")
        typer.echo(f"Organizer: {event.get('organizer_name', '')} <{event.get('organizer_email', '')}>")
        if event.get("attendees_json"):
            attendees = json.loads(event["attendees_json"])
            if attendees:
                typer.echo("Attendees:")
                for att in attendees:
                    typer.echo(f"  - {att.get('name', att['email'])} ({att.get('response', 'none')})")
        if event.get("body_preview"):
            typer.echo(f"\nDescription:\n{event['body_preview']}")
    else:
        typer.echo(json.dumps(event, default=str))


@app.command("calendar-search")
def calendar_search(
    query: str = typer.Argument(..., help="Search query (subject or attendee email)"),
    limit: int = typer.Option(20, help="Number of results"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Search calendar events by subject or attendee."""
    conn = connect_db()
    cursor = conn.cursor()

    like_query = f"%{query}%"
    cursor.execute(
        """
        SELECT * FROM calendar_events
        WHERE (subject LIKE ? OR attendees_json LIKE ? OR organizer_email LIKE ?)
          AND is_cancelled = 0
        ORDER BY start_at DESC
        LIMIT ?
        """,
        (like_query, like_query, like_query, limit),
    )
    rows = cursor.fetchall()
    conn.close()

    events = [dict(r) for r in rows]

    if human:
        if not events:
            typer.echo(f"No events matching '{query}'.")
            return
        typer.echo(f"=== Events matching '{query}' ===\n")
        for event in events:
            typer.echo(_format_event(event, human=True))
            typer.echo()
    else:
        typer.echo(json.dumps(events, default=str))


@app.command("calendar-meetings-with")
def calendar_meetings_with(
    email: str = typer.Argument(..., help="Email address to search for"),
    limit: int = typer.Option(50, help="Number of results"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """List meetings with a specific person."""
    conn = connect_db()
    cursor = conn.cursor()

    like_email = f"%{email}%"
    cursor.execute(
        """
        SELECT * FROM calendar_events
        WHERE (attendees_json LIKE ? OR organizer_email LIKE ?)
          AND is_cancelled = 0
        ORDER BY start_at DESC
        LIMIT ?
        """,
        (like_email, like_email, limit),
    )
    rows = cursor.fetchall()
    conn.close()

    events = [dict(r) for r in rows]

    if human:
        if not events:
            typer.echo(f"No meetings found with '{email}'.")
            return
        typer.echo(f"=== Meetings with {email} ({len(events)} found) ===\n")
        for event in events:
            date = event["start_at"][:10]
            time = event["start_at"][11:16]
            typer.echo(f"  {date} {time}  {event['subject']}")
    else:
        typer.echo(json.dumps(events, default=str))


@app.command("calendar-prep")
def calendar_prep(
    event_id: str = typer.Argument(None, help="Event ID (or use --next for next meeting)"),
    next_meeting: bool = typer.Option(False, "--next", help="Prepare for next upcoming meeting"),
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Prepare briefing for a meeting - includes attendee email history."""
    conn = connect_db()
    cursor = conn.cursor()

    # Get the event
    if next_meeting:
        now_naive = now_in_user_tz().strftime("%Y-%m-%dT%H:%M:%S")
        cursor.execute(
            """
            SELECT * FROM calendar_events
            WHERE start_at >= ? AND is_cancelled = 0
            ORDER BY start_at
            LIMIT 1
            """,
            (now_naive,),
        )
    elif event_id:
        cursor.execute("SELECT * FROM calendar_events WHERE id = ?", (event_id,))
    else:
        typer.echo("Provide --event-id or use --next", err=True)
        raise typer.Exit(1)

    row = cursor.fetchone()
    if not row:
        typer.echo("Event not found.", err=True)
        raise typer.Exit(1)

    event = dict(row)

    # Get attendee emails
    attendee_emails = []
    if event.get("attendees_json"):
        attendees = json.loads(event["attendees_json"])
        attendee_emails = [att["email"] for att in attendees if att.get("email")]
    if event.get("organizer_email"):
        attendee_emails.append(event["organizer_email"])

    # Get recent emails from/to attendees
    email_context = []
    if attendee_emails:
        placeholders = ",".join("?" * len(attendee_emails))
        cursor.execute(
            f"""
            SELECT id, subject, sender, received_at, body_preview
            FROM emails
            WHERE sender IN ({placeholders})
            ORDER BY received_at DESC
            LIMIT 10
            """,
            attendee_emails,
        )
        email_context = [dict(r) for r in cursor.fetchall()]

    conn.close()

    prep = {
        "event": event,
        "attendee_emails": attendee_emails,
        "recent_emails": email_context,
    }

    if human:
        typer.echo("=== Meeting Prep ===\n")
        typer.echo(f"Subject: {event['subject']}")
        typer.echo(f"When: {event['start_at'][:16]} - {event['end_at'][11:16]}")
        if event.get("location"):
            typer.echo(f"Where: {event['location']}")
        if event.get("is_online_meeting"):
            typer.echo(f"Online: {event.get('online_meeting_url', 'Teams')}")

        typer.echo(f"\nAttendees ({len(attendee_emails)}):")
        for email in attendee_emails:
            typer.echo(f"  - {email}")

        if email_context:
            typer.echo(f"\n=== Recent Emails from Attendees ===")
            for em in email_context:
                typer.echo(f"\n{em['received_at'][:10]} - {em['sender']}")
                typer.echo(f"  Subject: {em['subject']}")
                if em.get("body_preview"):
                    preview = em["body_preview"][:200].replace("\n", " ")
                    typer.echo(f"  Preview: {preview}...")
        else:
            typer.echo("\nNo recent emails from attendees.")
    else:
        typer.echo(json.dumps(prep, default=str))


# =============================================================================
# Action Commands (write to actions table, executed by RT service)
# =============================================================================

def _create_action(
    item_type: str,
    item_id: str | None,
    action_type: str,
    payload: dict,
) -> str:
    """Create an action in the actions table."""
    import uuid
    from datetime import datetime

    action_id = str(uuid.uuid4())
    conn = connect_db()
    conn.execute(
        """
        INSERT INTO actions (id, item_type, item_id, action_type, payload_json, status, proposed_at)
        VALUES (?, ?, ?, ?, ?, 'proposed', ?)
        """,
        (action_id, item_type, item_id, action_type, json.dumps(payload), datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()
    return action_id


@app.command("event-create")
def event_create(
    subject: str = typer.Argument(..., help="Event subject"),
    start: str = typer.Argument(..., help="Start datetime (YYYY-MM-DDTHH:MM)"),
    end: str = typer.Argument(..., help="End datetime (YYYY-MM-DDTHH:MM)"),
    attendees: str = typer.Option(None, help="Comma-separated attendee emails"),
    location: str = typer.Option(None, help="Location"),
    body: str = typer.Option(None, help="Event description"),
    online: bool = typer.Option(False, "--online", help="Create as Teams meeting"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """Schedule a new calendar event (queued for RT service execution)."""
    # Normalize datetime format
    start = start.replace(" ", "T")
    end = end.replace(" ", "T")

    payload = {
        "subject": subject,
        "start": start,
        "end": end,
        "attendees": [e.strip() for e in attendees.split(",")] if attendees else [],
        "location": location,
        "body": body,
        "is_online_meeting": online,
    }

    action_id = _create_action("calendar_event", None, "create_event", payload)

    if human:
        typer.echo(f"Action queued: create_event")
        typer.echo(f"  ID: {action_id}")
        typer.echo(f"  Subject: {subject}")
        typer.echo(f"  When: {start} to {end}")
        typer.echo("\nRT service will execute this action shortly.")
    else:
        typer.echo(json.dumps({"action_id": action_id, "status": "proposed", "action_type": "create_event"}))


@app.command("event-update")
def event_update(
    event_id: str = typer.Argument(..., help="Event ID to update"),
    subject: str = typer.Option(None, help="New subject"),
    start: str = typer.Option(None, help="New start datetime"),
    end: str = typer.Option(None, help="New end datetime"),
    location: str = typer.Option(None, help="New location"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """Update/reschedule a calendar event (queued for RT service execution)."""
    payload = {"event_id": event_id}
    if subject:
        payload["subject"] = subject
    if start:
        payload["start"] = start.replace(" ", "T")
    if end:
        payload["end"] = end.replace(" ", "T")
    if location:
        payload["location"] = location

    action_id = _create_action("calendar_event", event_id, "update_event", payload)

    if human:
        typer.echo(f"Action queued: update_event")
        typer.echo(f"  ID: {action_id}")
        typer.echo(f"  Event: {event_id}")
        typer.echo("\nRT service will execute this action shortly.")
    else:
        typer.echo(json.dumps({"action_id": action_id, "status": "proposed", "action_type": "update_event"}))


@app.command("event-cancel")
def event_cancel(
    event_id: str = typer.Argument(..., help="Event ID to cancel"),
    notify: bool = typer.Option(True, help="Send cancellation to attendees"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """Cancel a calendar event (queued for RT service execution)."""
    payload = {
        "event_id": event_id,
        "notify_attendees": notify,
    }

    action_id = _create_action("calendar_event", event_id, "cancel_event", payload)

    if human:
        typer.echo(f"Action queued: cancel_event")
        typer.echo(f"  ID: {action_id}")
        typer.echo(f"  Event: {event_id}")
        typer.echo(f"  Notify attendees: {notify}")
        typer.echo("\nRT service will execute this action shortly.")
    else:
        typer.echo(json.dumps({"action_id": action_id, "status": "proposed", "action_type": "cancel_event"}))


@app.command("event-respond")
def event_respond(
    event_id: str = typer.Argument(..., help="Event ID"),
    response: str = typer.Argument(..., help="Response: accept, tentative, or decline"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """Respond to a meeting invite (queued for RT service execution)."""
    if response not in ("accept", "tentative", "decline"):
        typer.echo(f"Invalid response: {response}. Use: accept, tentative, decline", err=True)
        raise typer.Exit(1)

    payload = {
        "event_id": event_id,
        "response": response,
    }

    action_id = _create_action("calendar_event", event_id, "respond_event", payload)

    if human:
        typer.echo(f"Action queued: respond_event")
        typer.echo(f"  ID: {action_id}")
        typer.echo(f"  Event: {event_id}")
        typer.echo(f"  Response: {response}")
        typer.echo("\nRT service will execute this action shortly.")
    else:
        typer.echo(json.dumps({"action_id": action_id, "status": "proposed", "action_type": "respond_event"}))


@app.command("actions-pending")
def actions_pending(
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """List pending actions awaiting execution."""
    conn = connect_db()
    rows = conn.execute(
        """
        SELECT id, item_type, item_id, action_type, payload_json, status, proposed_at
        FROM actions
        WHERE status = 'proposed'
        ORDER BY proposed_at DESC
        """
    ).fetchall()
    conn.close()

    actions = [dict(r) for r in rows]

    if human:
        if not actions:
            typer.echo("No pending actions.")
            return
        typer.echo(f"=== Pending Actions ({len(actions)}) ===\n")
        for a in actions:
            typer.echo(f"[{a['action_type']}] {a['id'][:8]}...")
            typer.echo(f"  Item: {a['item_type']} / {a['item_id'] or 'new'}")
            typer.echo(f"  Proposed: {a['proposed_at']}")
            typer.echo()
    else:
        typer.echo(json.dumps(actions, default=str))


@app.command("actions-history")
def actions_history(
    limit: int = typer.Option(20, help="Number of actions to show"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """Show action execution history."""
    conn = connect_db()
    rows = conn.execute(
        """
        SELECT id, item_type, item_id, action_type, status, proposed_at, executed_at, error
        FROM actions
        ORDER BY proposed_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    actions = [dict(r) for r in rows]

    if human:
        if not actions:
            typer.echo("No action history.")
            return
        typer.echo(f"=== Action History ===\n")
        for a in actions:
            status_icon = {"proposed": "⏳", "executed": "✓", "failed": "✗"}.get(a["status"], "?")
            typer.echo(f"{status_icon} [{a['action_type']}] {a['id'][:8]}... - {a['status']}")
            if a.get("error"):
                typer.echo(f"    Error: {a['error']}")
    else:
        typer.echo(json.dumps(actions, default=str))


# =============================================================================
# Working Memory Commands (wm)
# =============================================================================


def _get_wm_snapshot(conn) -> dict:
    """Query working memory state from database.

    Uses the new architecture:
    - active_threads view: computed thread state
    - facts table: decisions, commitments, observations
    """
    cursor = conn.cursor()

    # Get timezone info
    tz = get_user_timezone()
    now = now_in_user_tz()
    today = today_in_user_tz()

    # Get user email for needs_reply computation
    user_email = os.environ.get("DELEGATED_USER", "")

    # Active threads from view
    threads = []
    try:
        cursor.execute("""
            SELECT conversation_id, subject, urgency, last_activity, message_count,
                   participants, last_sender, latest_email_id, latest_web_link,
                   has_action_items
            FROM active_threads
            ORDER BY
                CASE urgency
                    WHEN 'immediate' THEN 1
                    WHEN 'today' THEN 2
                    WHEN 'this_week' THEN 3
                    ELSE 4
                END,
                last_activity DESC
            LIMIT 20
        """)
        for r in cursor.fetchall():
            thread = dict(r)
            # Compute needs_reply: last message not from user
            thread["needs_reply"] = thread.get("last_sender", "") != user_email
            thread["last_activity_at"] = thread.pop("last_activity", None)
            thread["id"] = thread["conversation_id"]  # For compatibility
            threads.append(thread)
    except Exception:
        # Fall back to wm_threads if view doesn't exist yet
        cursor.execute("""
            SELECT id, conversation_id, subject, status, urgency, needs_reply,
                   last_activity_at, summary, latest_email_id, latest_web_link
            FROM wm_threads
            WHERE status NOT IN ('resolved', 'stale')
            ORDER BY
                CASE urgency
                    WHEN 'immediate' THEN 1
                    WHEN 'today' THEN 2
                    WHEN 'this_week' THEN 3
                    ELSE 4
                END,
                last_activity_at DESC
            LIMIT 20
        """)
        threads = [dict(r) for r in cursor.fetchall()]

    # Threads needing reply
    threads_needing_reply = [t for t in threads if t.get("needs_reply")]

    # Pending decisions from facts table
    decisions = []
    try:
        cursor.execute("""
            SELECT f.id, f.fact_value as question, f.context, e.sender as requester,
                   e.urgency, f.due_date as deadline
            FROM facts f
            LEFT JOIN emails e ON f.source_id = e.id
            WHERE f.fact_type = 'decision' AND f.status = 'active'
            ORDER BY
                CASE e.urgency
                    WHEN 'immediate' THEN 1
                    WHEN 'today' THEN 2
                    WHEN 'this_week' THEN 3
                    ELSE 4
                END,
                f.extracted_at DESC
            LIMIT 10
        """)
        decisions = [dict(r) for r in cursor.fetchall()]
    except Exception:
        # Fall back to wm_decisions if facts table doesn't exist yet
        cursor.execute("""
            SELECT id, question, context, requester, urgency, deadline
            FROM wm_decisions
            WHERE is_resolved = 0
            ORDER BY
                CASE urgency
                    WHEN 'immediate' THEN 1
                    WHEN 'today' THEN 2
                    WHEN 'this_week' THEN 3
                    ELSE 4
                END,
                created_at DESC
            LIMIT 10
        """)
        decisions = [dict(r) for r in cursor.fetchall()]

    # Open commitments from facts table
    commitments = []
    try:
        cursor.execute("""
            SELECT f.id, f.fact_value as description,
                   f.metadata_json, f.due_date as due_by, f.extracted_at as committed_at
            FROM facts f
            WHERE f.fact_type = 'commitment' AND f.status = 'active'
            ORDER BY f.due_date ASC NULLS LAST, f.extracted_at DESC
            LIMIT 10
        """)
        for r in cursor.fetchall():
            c = dict(r)
            # Extract to_whom from metadata
            if c.get("metadata_json"):
                try:
                    meta = json.loads(c["metadata_json"])
                    c["to_whom"] = meta.get("to_whom", "unknown")
                except Exception:
                    c["to_whom"] = "unknown"
            else:
                c["to_whom"] = "unknown"
            commitments.append(c)
    except Exception:
        # Fall back to wm_commitments
        cursor.execute("""
            SELECT id, description, to_whom, due_by, committed_at
            FROM wm_commitments
            WHERE is_completed = 0
            ORDER BY due_by ASC NULLS LAST, committed_at DESC
            LIMIT 10
        """)
        commitments = [dict(r) for r in cursor.fetchall()]

    # Overdue commitments count
    overdue_count = 0
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM facts
            WHERE fact_type = 'commitment'
            AND status = 'active'
            AND due_date IS NOT NULL
            AND due_date < ?
        """, (now.isoformat(),))
        overdue_count = cursor.fetchone()[0]
    except Exception:
        cursor.execute("""
            SELECT COUNT(*) FROM wm_commitments
            WHERE is_completed = 0 AND due_by IS NOT NULL AND due_by < ?
        """, (now.isoformat(),))
        overdue_count = cursor.fetchone()[0]

    # Recent observations from facts table
    observations = []
    try:
        cursor.execute("""
            SELECT fact_type as type, fact_value as content, extracted_at as observed_at
            FROM facts
            WHERE fact_type IN ('preference', 'relationship', 'pattern')
            AND status = 'active'
            AND extracted_at > datetime('now', '-7 days')
            ORDER BY extracted_at DESC
            LIMIT 10
        """)
        observations = [dict(r) for r in cursor.fetchall()]
    except Exception:
        cursor.execute("""
            SELECT type, content, observed_at
            FROM wm_observations
            WHERE observed_at > datetime('now', '-7 days')
            ORDER BY observed_at DESC
            LIMIT 10
        """)
        observations = [dict(r) for r in cursor.fetchall()]

    # Today's calendar (if available)
    today_events = []
    try:
        start = today.isoformat() + "T00:00:00"
        end = (today + timedelta(days=1)).isoformat() + "T00:00:00"
        cursor.execute("""
            SELECT subject, start_at, end_at, is_online_meeting, location
            FROM calendar_events
            WHERE start_at >= ? AND start_at < ? AND is_cancelled = 0
            ORDER BY start_at
        """, (start, end))
        today_events = [dict(r) for r in cursor.fetchall()]
    except Exception:
        pass  # Calendar table may not exist

    return {
        "timezone": str(tz),
        "current_time": now.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "today": str(today),
        "active_threads": threads,
        "threads_needing_reply": len(threads_needing_reply),
        "pending_decisions": decisions,
        "open_commitments": commitments,
        "overdue_commitments": overdue_count,
        "recent_observations": observations,
        "today_calendar": today_events,
        "urgent_items": len([t for t in threads if t.get("urgency") in ("immediate", "today")]),
    }


@wm_app.command("snapshot")
def wm_snapshot(
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
    llm: bool = typer.Option(False, "--llm", help="LLM-optimized output for context injection"),
):
    """Get complete working memory snapshot (for context injection)."""
    conn = connect_db()
    snapshot = _get_wm_snapshot(conn)
    conn.close()

    if llm:
        # LLM-optimized format for context injection
        lines = [
            f"=== EA Working Memory (as of {snapshot['current_time']}) ===",
            f"Timezone: {snapshot['timezone']} | Today: {snapshot['today']}",
            "",
        ]

        # Today's schedule
        if snapshot["today_calendar"]:
            lines.append("## Today's Schedule")
            for event in snapshot["today_calendar"]:
                time = event["start_at"][11:16]
                end_time = event["end_at"][11:16]
                loc = " (Teams)" if event.get("is_online_meeting") else ""
                lines.append(f"  {time}-{end_time} {event['subject']}{loc}")
            lines.append("")

        # Urgent attention
        if snapshot["threads_needing_reply"] > 0 or snapshot["pending_decisions"]:
            lines.append("## Needs Attention")
            if snapshot["threads_needing_reply"] > 0:
                lines.append(f"  - {snapshot['threads_needing_reply']} threads awaiting reply")
            if snapshot["pending_decisions"]:
                lines.append(f"  - {len(snapshot['pending_decisions'])} pending decisions")
            if snapshot["overdue_commitments"] > 0:
                lines.append(f"  - {snapshot['overdue_commitments']} overdue commitments")
            lines.append("")

        # Active threads (brief)
        if snapshot["active_threads"]:
            lines.append("## Active Threads")
            for t in snapshot["active_threads"][:10]:
                urgency = t.get("urgency", "")[:4]
                reply = "[REPLY NEEDED]" if t.get("needs_reply") else ""
                link = t.get("latest_web_link") or ""
                if link:
                    lines.append(f"  - [{urgency}] {t['subject'][:50]} {reply}")
                    lines.append(f"    Link: {link}")
                else:
                    lines.append(f"  - [{urgency}] {t['subject'][:50]} {reply}")
                if t.get("summary"):
                    lines.append(f"    {t['summary'][:100]}")
            lines.append("")

        # Pending decisions
        if snapshot["pending_decisions"]:
            lines.append("## Pending Decisions")
            for d in snapshot["pending_decisions"][:5]:
                lines.append(f"  - [{d['urgency'][:4]}] {d['question'][:60]}")
                lines.append(f"    From: {d['requester']}")
            lines.append("")

        # Commitments
        if snapshot["open_commitments"]:
            lines.append("## Open Commitments")
            for c in snapshot["open_commitments"][:5]:
                due = f" (due: {c['due_by'][:10]})" if c.get("due_by") else ""
                lines.append(f"  - {c['description'][:50]} → {c['to_whom']}{due}")
            lines.append("")

        typer.echo("\n".join(lines))

    elif human:
        # Human-readable format
        typer.echo(f"=== Working Memory Snapshot ===")
        typer.echo(f"Timezone: {snapshot['timezone']}")
        typer.echo(f"Current Time: {snapshot['current_time']}")
        typer.echo(f"Today: {snapshot['today']}")
        typer.echo("")

        if snapshot["today_calendar"]:
            typer.echo("--- Today's Calendar ---")
            for event in snapshot["today_calendar"]:
                time = event["start_at"][11:16]
                typer.echo(f"  {time} - {event['subject']}")
            typer.echo("")

        typer.echo("--- Summary ---")
        typer.echo(f"  Active threads: {len(snapshot['active_threads'])}")
        typer.echo(f"  Threads needing reply: {snapshot['threads_needing_reply']}")
        typer.echo(f"  Pending decisions: {len(snapshot['pending_decisions'])}")
        typer.echo(f"  Open commitments: {len(snapshot['open_commitments'])}")
        typer.echo(f"  Overdue commitments: {snapshot['overdue_commitments']}")
        typer.echo(f"  Urgent items: {snapshot['urgent_items']}")
        typer.echo("")

        if snapshot["active_threads"]:
            typer.echo("--- Active Threads (top 10) ---")
            for t in snapshot["active_threads"][:10]:
                reply = " [REPLY NEEDED]" if t.get("needs_reply") else ""
                typer.echo(f"  [{t['urgency']}] {t['subject'][:50]}{reply}")
            typer.echo("")

        if snapshot["pending_decisions"]:
            typer.echo("--- Pending Decisions ---")
            for d in snapshot["pending_decisions"]:
                typer.echo(f"  [{d['urgency']}] {d['question'][:60]}")
                typer.echo(f"    From: {d['requester']}")
            typer.echo("")

        if snapshot["open_commitments"]:
            typer.echo("--- Open Commitments ---")
            for c in snapshot["open_commitments"]:
                due = f" (due: {c['due_by'][:10]})" if c.get("due_by") else ""
                typer.echo(f"  - {c['description'][:50]} → {c['to_whom']}{due}")
            typer.echo("")

    else:
        # JSON format
        typer.echo(json.dumps(snapshot, default=str))


@wm_app.command("threads")
def wm_threads(
    needs_reply: bool = typer.Option(False, "--needs-reply", help="Only show threads needing reply"),
    urgency: str = typer.Option(None, help="Filter by urgency (immediate/today/this_week/someday)"),
    limit: int = typer.Option(20, help="Number of threads to show"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """Query active threads."""
    conn = connect_db()
    cursor = conn.cursor()

    query = """
        SELECT id, conversation_id, subject, status, urgency, needs_reply,
               last_activity_at, summary, participants_json, latest_web_link
        FROM wm_threads
        WHERE status NOT IN ('resolved')
    """
    params = []

    if needs_reply:
        query += " AND needs_reply = 1"
    if urgency:
        query += " AND urgency = ?"
        params.append(urgency)

    query += " ORDER BY last_activity_at DESC LIMIT ?"
    params.append(limit)

    cursor.execute(query, params)
    threads = [dict(r) for r in cursor.fetchall()]
    conn.close()

    if human:
        if not threads:
            typer.echo("No matching threads.")
            return
        for t in threads:
            reply = " [REPLY NEEDED]" if t.get("needs_reply") else ""
            typer.echo(f"[{t['urgency']}] {t['subject']}{reply}")
            typer.echo(f"  Status: {t['status']} | Last: {t['last_activity_at'][:16]}")
            if t.get("summary"):
                typer.echo(f"  {t['summary'][:100]}")
            if t.get("latest_web_link"):
                typer.echo(f"  Link: {t['latest_web_link']}")
            typer.echo("")
    else:
        typer.echo(json.dumps(threads, default=str))


@wm_app.command("decisions")
def wm_decisions(
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """List pending decisions awaiting response."""
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, question, context, requester, urgency, deadline, created_at
        FROM wm_decisions
        WHERE is_resolved = 0
        ORDER BY
            CASE urgency
                WHEN 'immediate' THEN 1
                WHEN 'today' THEN 2
                WHEN 'this_week' THEN 3
                ELSE 4
            END,
            created_at DESC
    """)
    decisions = [dict(r) for r in cursor.fetchall()]
    conn.close()

    if human:
        if not decisions:
            typer.echo("No pending decisions.")
            return
        typer.echo(f"=== Pending Decisions ({len(decisions)}) ===\n")
        for d in decisions:
            typer.echo(f"[{d['urgency']}] {d['question']}")
            typer.echo(f"  From: {d['requester']}")
            if d.get("context"):
                typer.echo(f"  Context: {d['context'][:100]}")
            if d.get("deadline"):
                typer.echo(f"  Deadline: {d['deadline']}")
            typer.echo("")
    else:
        typer.echo(json.dumps(decisions, default=str))


@wm_app.command("commitments")
def wm_commitments(
    overdue: bool = typer.Option(False, "--overdue", help="Only show overdue commitments"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """List open commitments."""
    conn = connect_db()
    cursor = conn.cursor()
    now = now_in_user_tz()

    query = """
        SELECT id, description, to_whom, due_by, committed_at
        FROM wm_commitments
        WHERE is_completed = 0
    """
    params = []

    if overdue:
        query += " AND due_by IS NOT NULL AND due_by < ?"
        params.append(now.isoformat())

    query += " ORDER BY due_by ASC NULLS LAST, committed_at DESC"

    cursor.execute(query, params)
    commitments = [dict(r) for r in cursor.fetchall()]
    conn.close()

    if human:
        if not commitments:
            typer.echo("No open commitments." if not overdue else "No overdue commitments.")
            return
        title = "Overdue Commitments" if overdue else "Open Commitments"
        typer.echo(f"=== {title} ({len(commitments)}) ===\n")
        for c in commitments:
            due = f" [DUE: {c['due_by'][:10]}]" if c.get("due_by") else ""
            typer.echo(f"- {c['description']}")
            typer.echo(f"  To: {c['to_whom']}{due}")
            typer.echo("")
    else:
        typer.echo(json.dumps(commitments, default=str))


@wm_app.command("contacts")
def wm_contacts(
    external: bool = typer.Option(False, "--external", help="Only external contacts"),
    search: str = typer.Option(None, "--search", help="Search by email or name"),
    limit: int = typer.Option(20, help="Number of contacts to show"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """Query known contacts."""
    conn = connect_db()
    cursor = conn.cursor()

    query = "SELECT * FROM wm_contacts WHERE 1=1"
    params = []

    if external:
        query += " AND is_internal = 0"
    if search:
        query += " AND (email LIKE ? OR name LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])

    query += " ORDER BY last_interaction_at DESC LIMIT ?"
    params.append(limit)

    cursor.execute(query, params)
    contacts = [dict(r) for r in cursor.fetchall()]
    conn.close()

    if human:
        if not contacts:
            typer.echo("No matching contacts.")
            return
        for c in contacts:
            name = c.get("name") or c["email"]
            internal = " [internal]" if c.get("is_internal") else ""
            typer.echo(f"{name}{internal}")
            typer.echo(f"  Email: {c['email']}")
            typer.echo(f"  Interactions: {c['total_interactions']} (last: {c['last_interaction_at'][:10]})")
            typer.echo("")
    else:
        typer.echo(json.dumps(contacts, default=str))


@wm_app.command("observations")
def wm_observations(
    days: int = typer.Option(7, "--days", help="Days of observations to show"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """View recent passive observations."""
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT type, content, importance, observed_at
        FROM wm_observations
        WHERE observed_at > datetime('now', ? || ' days')
        ORDER BY observed_at DESC
        LIMIT 50
    """, (f"-{days}",))
    observations = [dict(r) for r in cursor.fetchall()]
    conn.close()

    if human:
        if not observations:
            typer.echo(f"No observations in the last {days} days.")
            return
        typer.echo(f"=== Observations (last {days} days) ===\n")
        for o in observations:
            typer.echo(f"[{o['type']}] {o['content'][:80]}")
            typer.echo(f"  {o['observed_at'][:16]}")
            typer.echo("")
    else:
        typer.echo(json.dumps(observations, default=str))


@wm_app.command("projects")
def wm_projects(
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """View inferred projects."""
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT name, description, status, confidence, first_mentioned_at, last_activity_at
        FROM wm_projects
        WHERE status = 'active'
        ORDER BY last_activity_at DESC
        LIMIT 20
    """)
    projects = [dict(r) for r in cursor.fetchall()]
    conn.close()

    if human:
        if not projects:
            typer.echo("No active projects found.")
            return
        typer.echo("=== Inferred Projects ===\n")
        for p in projects:
            conf = f" ({int(p['confidence']*100)}% confidence)" if p.get("confidence") else ""
            typer.echo(f"- {p['name']}{conf}")
            if p.get("description"):
                typer.echo(f"  {p['description'][:80]}")
            typer.echo(f"  Last activity: {p['last_activity_at'][:10]}")
            typer.echo("")
    else:
        typer.echo(json.dumps(projects, default=str))


# =============================================================================
# Preferences Commands
# =============================================================================

@prefs_app.command("show")
def prefs_show():
    """Show the current preferences.json."""
    typer.echo(json.dumps(read_preferences(), indent=2, sort_keys=True))


@prefs_app.command("set")
def prefs_set(
    key: str = typer.Argument(..., help="Preference key (use 'prefs keys' to see valid keys)"),
    value: str = typer.Argument(..., help="Preference value (string/number/bool/JSON)"),
):
    """Set a preference key in preferences.json.

    Only known preference keys are allowed. Use 'prefs keys' to see valid keys.
    """
    try:
        path = set_preference_from_string(key, value)
        typer.echo(str(path))
    except InvalidPreferenceKeyError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@prefs_app.command("keys")
def prefs_keys():
    """List all valid preference keys."""
    typer.echo("Valid preference keys:\n")
    for key in sorted(VALID_PREFERENCE_KEYS):
        typer.echo(f"  {key}")


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
# Alerts Commands
# =============================================================================


@alerts_app.command("list")
def alerts_list(
    enabled_only: bool = typer.Option(False, "--enabled-only", help="Only show enabled rules"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """List all alert rules."""
    conn = connect_db()
    query = "SELECT * FROM alert_rules"
    if enabled_only:
        query += " WHERE enabled = 1"
    query += " ORDER BY created_at DESC"

    rows = conn.execute(query).fetchall()
    conn.close()

    rules = [dict(r) for r in rows]

    if human:
        if not rules:
            typer.echo("No alert rules configured.")
            typer.echo("\nCreate one with: alerts add \"alert me when CFO emails about budget\"")
            return
        typer.echo(f"=== Alert Rules ({len(rules)}) ===\n")
        for r in rules:
            status = "ENABLED" if r.get("enabled") else "DISABLED"
            typer.echo(f"[{status}] {r['id'][:8]}...")
            typer.echo(f"  Rule: {r['natural_language_rule']}")
            typer.echo(f"  Channel: {r.get('channel', 'teams')}")
            event_types = r.get("event_types", '["email_received"]')
            typer.echo(f"  Events: {event_types}")
            typer.echo(f"  Triggers: {r.get('trigger_count', 0)}")
            if r.get("last_triggered_at"):
                typer.echo(f"  Last triggered: {r['last_triggered_at']}")
            typer.echo()
    else:
        typer.echo(json.dumps(rules, default=str))


@alerts_app.command("add")
def alerts_add(
    rule: str = typer.Argument(..., help="Natural language alert rule"),
    channel: str = typer.Option("teams", help="Notification channel: teams, email"),
    target: str = typer.Option(None, help="Channel target (chat ID, email address)"),
    cooldown: int = typer.Option(30, help="Cooldown between triggers (minutes)"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """Add a new alert rule.

    Examples:
        alerts add "alert me when CFO emails about budget"
        alerts add "notify when I send email to legal@" --channel email
        alerts add "alert when commitment is overdue" --cooldown 60
    """
    import asyncio
    import sys
    from pathlib import Path

    # Add src to path for imports
    src_path = Path(__file__).parent.parent.parent.parent.parent / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    try:
        from alerts import create_alert_rule

        result = asyncio.run(create_alert_rule(
            natural_language_rule=rule,
            channel=channel,
            channel_target=target,
            cooldown_minutes=cooldown,
            created_by="user",
        ))

        if human:
            typer.echo(f"Created alert rule: {result['id']}")
            typer.echo(f"  Rule: {rule}")
            typer.echo(f"  Channel: {channel}")
            typer.echo(f"  Event types: {result.get('event_types', ['email_received'])}")
            parsed = result.get("parsed_conditions", {})
            if parsed:
                typer.echo(f"  Parsed conditions:")
                if parsed.get("sender_patterns"):
                    typer.echo(f"    Sender patterns: {parsed['sender_patterns']}")
                if parsed.get("subject_keywords"):
                    typer.echo(f"    Subject keywords: {parsed['subject_keywords']}")
                if parsed.get("body_keywords"):
                    typer.echo(f"    Body keywords: {parsed['body_keywords']}")
                if parsed.get("urgency_levels"):
                    typer.echo(f"    Urgency levels: {parsed['urgency_levels']}")
        else:
            typer.echo(json.dumps(result, default=str))
    except Exception as e:
        typer.echo(f"Error creating rule: {e}", err=True)
        raise typer.Exit(1)


@alerts_app.command("remove")
def alerts_remove(
    rule_id: str = typer.Argument(..., help="Rule ID to remove (can be partial)"),
):
    """Remove an alert rule."""
    conn = connect_db()

    # Support partial ID matching
    if len(rule_id) < 36:
        row = conn.execute(
            "SELECT id FROM alert_rules WHERE id LIKE ?",
            (f"{rule_id}%",)
        ).fetchone()
        if row:
            rule_id = row["id"]

    conn.execute("DELETE FROM alert_rules WHERE id = ?", (rule_id,))
    conn.commit()
    deleted = conn.total_changes > 0
    conn.close()

    if deleted:
        typer.echo(json.dumps({"status": "deleted", "id": rule_id}))
    else:
        typer.echo(json.dumps({"status": "not_found", "id": rule_id}))
        raise typer.Exit(1)


@alerts_app.command("enable")
def alerts_enable(
    rule_id: str = typer.Argument(..., help="Rule ID to enable (can be partial)"),
):
    """Enable an alert rule."""
    conn = connect_db()

    # Support partial ID matching
    if len(rule_id) < 36:
        row = conn.execute(
            "SELECT id FROM alert_rules WHERE id LIKE ?",
            (f"{rule_id}%",)
        ).fetchone()
        if row:
            rule_id = row["id"]

    conn.execute(
        "UPDATE alert_rules SET enabled = 1, updated_at = ? WHERE id = ?",
        (datetime.now().isoformat(), rule_id)
    )
    conn.commit()
    updated = conn.total_changes > 0
    conn.close()

    typer.echo(json.dumps({"status": "enabled" if updated else "not_found", "id": rule_id}))


@alerts_app.command("disable")
def alerts_disable(
    rule_id: str = typer.Argument(..., help="Rule ID to disable (can be partial)"),
):
    """Disable an alert rule."""
    conn = connect_db()

    # Support partial ID matching
    if len(rule_id) < 36:
        row = conn.execute(
            "SELECT id FROM alert_rules WHERE id LIKE ?",
            (f"{rule_id}%",)
        ).fetchone()
        if row:
            rule_id = row["id"]

    conn.execute(
        "UPDATE alert_rules SET enabled = 0, updated_at = ? WHERE id = ?",
        (datetime.now().isoformat(), rule_id)
    )
    conn.commit()
    updated = conn.total_changes > 0
    conn.close()

    typer.echo(json.dumps({"status": "disabled" if updated else "not_found", "id": rule_id}))


@alerts_app.command("history")
def alerts_history(
    rule_id: str = typer.Option(None, "--rule-id", help="Filter by rule ID"),
    limit: int = typer.Option(20, help="Number of entries"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """View alert trigger history."""
    conn = connect_db()

    query = """
        SELECT at.*, ar.natural_language_rule
        FROM alert_triggers at
        JOIN alert_rules ar ON at.rule_id = ar.id
    """
    params = []

    if rule_id:
        # Support partial ID matching
        if len(rule_id) < 36:
            query += " WHERE at.rule_id LIKE ?"
            params.append(f"{rule_id}%")
        else:
            query += " WHERE at.rule_id = ?"
            params.append(rule_id)

    query += " ORDER BY at.triggered_at DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()

    triggers = [dict(r) for r in rows]

    if human:
        if not triggers:
            typer.echo("No alert triggers found.")
            return
        typer.echo(f"=== Alert History ({len(triggers)} entries) ===\n")
        for t in triggers:
            rule_text = t.get("natural_language_rule", "Unknown")[:50]
            typer.echo(f"{t['triggered_at']} - {rule_text}")
            typer.echo(f"  Event: {t.get('event_type', 'email_received')} / {t.get('event_id', 'N/A')[:20]}...")
            typer.echo(f"  Reason: {t.get('match_reason', 'N/A')}")
            typer.echo()
    else:
        typer.echo(json.dumps(triggers, default=str))


@alerts_app.command("show")
def alerts_show(
    rule_id: str = typer.Argument(..., help="Rule ID to show (can be partial)"),
    human: bool = typer.Option(False, "--human", help="Human-readable output"),
):
    """Show details of a specific alert rule."""
    conn = connect_db()

    # Support partial ID matching
    if len(rule_id) < 36:
        row = conn.execute(
            "SELECT * FROM alert_rules WHERE id LIKE ?",
            (f"{rule_id}%",)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM alert_rules WHERE id = ?",
            (rule_id,)
        ).fetchone()

    conn.close()

    if not row:
        typer.echo(f"Rule not found: {rule_id}", err=True)
        raise typer.Exit(1)

    rule = dict(row)

    if human:
        typer.echo(f"=== Alert Rule ===\n")
        typer.echo(f"ID: {rule['id']}")
        typer.echo(f"Rule: {rule['natural_language_rule']}")
        typer.echo(f"Status: {'ENABLED' if rule.get('enabled') else 'DISABLED'}")
        typer.echo(f"Channel: {rule.get('channel', 'teams')}")
        if rule.get("channel_target"):
            typer.echo(f"Target: {rule['channel_target']}")
        typer.echo(f"Event types: {rule.get('event_types', '[]')}")
        typer.echo(f"Cooldown: {rule.get('cooldown_minutes', 30)} minutes")
        typer.echo(f"Trigger count: {rule.get('trigger_count', 0)}")
        if rule.get("last_triggered_at"):
            typer.echo(f"Last triggered: {rule['last_triggered_at']}")
        typer.echo(f"\nParsed conditions:")
        try:
            conditions = json.loads(rule.get("parsed_conditions_json", "{}"))
            for key, value in conditions.items():
                if value and value != [] and value != False:
                    typer.echo(f"  {key}: {value}")
        except Exception:
            typer.echo(f"  {rule.get('parsed_conditions_json', '{}')}")
    else:
        typer.echo(json.dumps(rule, default=str))


def run():
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    run()
