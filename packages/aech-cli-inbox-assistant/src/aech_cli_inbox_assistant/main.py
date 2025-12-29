"""
CLI for Agent Aech to query Inbox Assistant state.

This CLI is a read-only query interface to the SQLite database
populated by the RT (real-time) inbox assistant service.
"""
import typer
import sqlite3
import json

from .state import (
    connect_db,
    get_db_path,
    read_preferences,
    set_preference_from_string,
    write_preferences,
)

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
            typer.echo(f"  Category: {email.get('category', 'N/A')}")
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
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON")
):
    """Search emails and attachments using full-text search."""
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
            link = item.get('web_link')
            if not link and item.get('message_id'):
                from urllib.parse import quote
                link = f"https://outlook.office365.com/mail/inbox/id/{quote(item['message_id'], safe='')}"
            if link:
                typer.echo(f"  Link: {link}")
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
    """Show today's calendar events."""
    from datetime import datetime, timedelta

    conn = connect_db()
    cursor = conn.cursor()

    # Get today's date range
    today = datetime.now().date()
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
            typer.echo("No events today.")
            return
        typer.echo(f"=== Today's Agenda ({today}) ===\n")
        for event in events:
            typer.echo(_format_event(event, human=True))
            typer.echo()
    else:
        typer.echo(json.dumps(events, default=str))


@app.command("calendar-week")
def calendar_week(
    human: bool = typer.Option(False, "--human", help="Human-readable output instead of JSON"),
):
    """Show this week's calendar events."""
    from datetime import datetime, timedelta

    conn = connect_db()
    cursor = conn.cursor()

    today = datetime.now().date()
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
            typer.echo("No events this week.")
            return
        typer.echo(f"=== This Week ({today} to {today + timedelta(days=6)}) ===\n")
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
    """Show upcoming events in the next N hours."""
    from datetime import datetime, timedelta

    conn = connect_db()
    cursor = conn.cursor()

    now = datetime.now()
    end = now + timedelta(hours=hours)

    cursor.execute(
        """
        SELECT * FROM calendar_events
        WHERE start_at >= ? AND start_at < ?
          AND is_cancelled = 0
        ORDER BY start_at
        """,
        (now.isoformat(), end.isoformat()),
    )
    rows = cursor.fetchall()
    conn.close()

    events = [dict(r) for r in rows]

    if human:
        if not events:
            typer.echo(f"No events in the next {hours} hours.")
            return
        typer.echo(f"=== Next {hours} Hours ===\n")
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
    """Show free time slots on a given date."""
    from datetime import datetime, timedelta

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
    """Check if busy during a time range."""
    from datetime import datetime

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
    from datetime import datetime

    conn = connect_db()
    cursor = conn.cursor()

    # Get the event
    if next_meeting:
        cursor.execute(
            """
            SELECT * FROM calendar_events
            WHERE start_at >= ? AND is_cancelled = 0
            ORDER BY start_at
            LIMIT 1
            """,
            (datetime.now().isoformat(),),
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
# Preferences Commands
# =============================================================================

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
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    run()
