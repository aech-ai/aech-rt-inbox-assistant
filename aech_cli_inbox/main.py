import typer
import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

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
            typer.echo(f"[{email['id']}] {email['subject']} ({email['category']})")
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

    if human:
        if not results:
            typer.echo("No results.")
        for email in results:
            typer.echo(f"[{email['id']}] {email['subject']} ({email.get('category')})")
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

    if human:
        if not items:
            typer.echo("No reply-needed items.")
            return
        for item in items:
            typer.echo(f"[{item['message_id']}] {item['subject']} ({item['sender']}) - {item.get('reason')}")
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
