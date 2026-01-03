"""
CLI for Agent Aech to query Inbox Assistant state.

This CLI is a read-only query interface to the SQLite database
populated by the RT (real-time) inbox assistant service.

ALL OUTPUT IS JSON. No human-readable format option exists.
This ensures predictable machine-parseable output for agents.
"""
import os
import sys
import json
import click
import sqlite3
from typing import Any
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


# =============================================================================
# JSON Output Helpers
# =============================================================================


def output_json(data: Any) -> None:
    """Output data as JSON to stdout."""
    click.echo(json.dumps(data, indent=2, default=str))


def output_error(message: str, code: str = "error") -> None:
    """Output error as JSON to stderr."""
    click.echo(json.dumps({"error": code, "message": message}), err=True)


# =============================================================================
# JSON Help Classes
# =============================================================================


def get_param_info(param: click.Parameter) -> dict:
    """Extract parameter info from a Click parameter."""
    param_info: dict[str, Any] = {
        "name": param.name,
        "type": param.type.name if hasattr(param.type, "name") else str(param.type),
        "required": param.required if hasattr(param, "required") else False,
    }
    if isinstance(param, click.Option):
        if param.help:
            param_info["help"] = param.help
        if param.is_flag:
            param_info["is_flag"] = True
    if isinstance(param, click.Argument):
        param_info["argument"] = True
    # Only include JSON-serializable defaults
    if param.default is not None and param.default != ():
        try:
            json.dumps(param.default)
            param_info["default"] = param.default
        except (TypeError, ValueError):
            pass  # Skip non-serializable defaults
    return param_info


def get_command_help(cmd: click.Command) -> dict:
    """Get help dict for a single command."""
    return {
        "help": cmd.help or "",
        "options": [get_param_info(p) for p in cmd.params],
    }


class JSONGroup(click.Group):
    """Custom Click Group that outputs help as JSON."""

    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        pass

    def get_help(self, ctx: click.Context) -> str:
        """Return help as JSON for this group and its direct subcommands."""
        commands = {}
        for name, cmd in self.commands.items():
            cmd_info = get_command_help(cmd)
            if isinstance(cmd, click.Group):
                cmd_info["subcommands"] = list(cmd.commands.keys())
            commands[name] = cmd_info

        help_data = {
            "name": ctx.info_name,
            "help": self.help or "",
            "options": [get_param_info(p) for p in self.params],
            "commands": commands,
        }
        return json.dumps(help_data, indent=2)


class JSONCommand(click.Command):
    """Custom Click Command that outputs help as JSON."""

    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        pass

    def get_help(self, ctx: click.Context) -> str:
        help_data = {
            "name": ctx.info_name,
            "help": self.help or "",
            "options": [get_param_info(p) for p in self.params],
        }
        return json.dumps(help_data, indent=2)


# =============================================================================
# Timezone Helpers
# =============================================================================


def get_user_timezone() -> ZoneInfo:
    """Get the user's timezone from environment or preferences."""
    tz_name = os.environ.get("DEFAULT_TIMEZONE")
    if not tz_name:
        prefs = read_preferences()
        tz_name = prefs.get("timezone")
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


# =============================================================================
# CLI Groups
# =============================================================================


@click.group(cls=JSONGroup, help="Query Inbox Assistant state and preferences.")
def app():
    pass


@app.group(cls=JSONGroup, name="prefs", help="Manage /home/agentaech/preferences.json.")
def prefs_app():
    pass


@app.group(cls=JSONGroup, name="wm", help="Working Memory - EA cognitive state.")
def wm_app():
    pass


@app.group(cls=JSONGroup, name="alerts", help="Manage user-defined alert rules.")
def alerts_app():
    pass


# =============================================================================
# Email Commands
# =============================================================================


@app.command(cls=JSONCommand, name="list")
@click.option("--limit", default=20, help="Number of emails to list")
@click.option("--include-read", is_flag=True, help="Include read emails")
def list_emails(limit: int, include_read: bool):
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
    conn.close()

    emails = [dict(row) for row in rows]
    output_json(emails)


@app.command(cls=JSONCommand)
@click.option("--limit", default=20, help="Number of entries to list")
def history(limit: int):
    """View triage history."""
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT t.*, e.subject
        FROM triage_log t
        JOIN emails e ON t.email_id = e.id
        ORDER BY t.timestamp DESC LIMIT ?
    """,
        (limit,),
    )
    rows = cursor.fetchall()
    conn.close()

    logs = [dict(row) for row in rows]
    output_json(logs)


@app.command(cls=JSONCommand)
@click.argument("query")
@click.option("--limit", default=20, help="Number of results to return")
@click.option("--mode", default="hybrid", help="Search mode: hybrid, fts, or vector")
@click.option("--facts/--no-facts", default=True, help="Include facts in search")
def search(query: str, limit: int, mode: str, facts: bool):
    """Search emails, attachments, and facts using unified search."""
    from pathlib import Path

    src_path = Path(__file__).parent.parent.parent.parent.parent / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    try:
        from search import unified_search
    except ImportError:
        _search_fallback(query, limit)
        return

    results = unified_search(
        query=query,
        limit=limit,
        mode=mode,
        include_facts=facts,
        recency_weight=True,
    )

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
    output_json(output)


def _search_fallback(query: str, limit: int):
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
    output_json(results)


@app.command(cls=JSONCommand)
def dbpath():
    """Get the absolute path to the user's database."""
    output_json({"path": get_db_path()})


@app.command(cls=JSONCommand, name="timezone")
def show_timezone():
    """Show the current timezone being used for calendar queries."""
    tz = get_user_timezone()
    now = now_in_user_tz()
    output_json({
        "timezone": str(tz),
        "current_time": now.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "today": str(today_in_user_tz()),
    })


@app.command(cls=JSONCommand, name="sync-status")
def sync_status():
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
    output_json(status)


@app.command(cls=JSONCommand)
def stats():
    """Show corpus statistics."""
    conn = connect_db()
    cursor = conn.cursor()

    stats_data = {}

    cursor.execute("SELECT COUNT(*) FROM emails")
    stats_data["total_emails"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM emails WHERE body_markdown IS NOT NULL")
    stats_data["emails_with_body"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM emails WHERE has_attachments = 1")
    stats_data["emails_with_attachments"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM attachments")
    stats_data["total_attachments"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM attachments WHERE extraction_status = 'completed'")
    stats_data["attachments_extracted"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM attachments WHERE extraction_status = 'pending'")
    stats_data["attachments_pending"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM attachments WHERE extraction_status = 'failed'")
    stats_data["attachments_failed"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM chunks")
    stats_data["total_chunks"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM chunks WHERE embedding IS NOT NULL")
    stats_data["chunks_with_embeddings"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM sync_state")
    stats_data["folders_synced"] = cursor.fetchone()[0]

    cursor.execute("SELECT SUM(messages_synced) FROM sync_state")
    result = cursor.fetchone()[0]
    stats_data["total_synced_messages"] = result if result else 0

    conn.close()
    output_json(stats_data)


@app.command(cls=JSONCommand, name="attachment-status")
@click.option("--limit", default=20, help="Number of attachments to list")
@click.option("--status", "status_filter", default=None, help="Filter by status")
def attachment_status(limit: int, status_filter: str | None):
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
    output_json(attachments)


@app.command(cls=JSONCommand)
def schema():
    """Get the database schema (CREATE TABLE statements)."""
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT name, sql FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)

    schemas = cursor.fetchall()
    conn.close()

    result = [{"table": row["name"], "sql": row["sql"]} for row in schemas if row["sql"]]
    output_json(result)


@app.command(cls=JSONCommand, name="reply-needed")
@click.option("--limit", default=20, help="Number of threads to list")
@click.option("--include-stale", is_flag=True, help="Include stale threads")
def reply_needed(limit: int, include_stale: bool):
    """List threads currently marked as requiring a reply."""
    conn = connect_db()

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
    output_json(items)


# =============================================================================
# Calendar Commands
# =============================================================================


@app.command(cls=JSONCommand, name="calendar-today")
def calendar_today():
    """Show today's calendar events (in user's timezone)."""
    conn = connect_db()
    cursor = conn.cursor()

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
    output_json(events)


@app.command(cls=JSONCommand, name="calendar-week")
def calendar_week():
    """Show this week's calendar events (in user's timezone)."""
    conn = connect_db()
    cursor = conn.cursor()

    today = today_in_user_tz()
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
    output_json(events)


@app.command(cls=JSONCommand, name="calendar-upcoming")
@click.option("--hours", default=24, help="Number of hours to look ahead")
def calendar_upcoming(hours: int):
    """Show upcoming events in the next N hours (in user's timezone)."""
    conn = connect_db()
    cursor = conn.cursor()

    now = now_in_user_tz()
    end = now + timedelta(hours=hours)

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
    output_json(events)


@app.command(cls=JSONCommand, name="calendar-free")
@click.argument("date")
def calendar_free(date: str):
    """Show free time slots on a given date (in user's timezone)."""
    conn = connect_db()
    cursor = conn.cursor()

    try:
        check_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        output_error(f"Invalid date format: {date}. Use YYYY-MM-DD.", "invalid_date")
        sys.exit(1)

    start = check_date.isoformat() + "T00:00:00"
    end = (check_date + timedelta(days=1)).isoformat() + "T00:00:00"

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

    work_start = datetime.combine(check_date, datetime.strptime("09:00", "%H:%M").time())
    work_end = datetime.combine(check_date, datetime.strptime("17:00", "%H:%M").time())

    busy_periods = []
    for row in busy_rows:
        busy_start = datetime.fromisoformat(row["start_at"].replace("Z", "+00:00").replace("+00:00", ""))
        busy_end = datetime.fromisoformat(row["end_at"].replace("Z", "+00:00").replace("+00:00", ""))
        busy_periods.append((busy_start, busy_end, row["subject"]))

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

    output_json(free_slots)


@app.command(cls=JSONCommand, name="calendar-busy")
@click.argument("start")
@click.argument("end")
def calendar_busy(start: str, end: str):
    """Check if busy during a time range (times interpreted in user's timezone)."""
    conn = connect_db()
    cursor = conn.cursor()

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
        (end, start),
    )
    rows = cursor.fetchall()
    conn.close()

    conflicts = [dict(r) for r in rows]
    output_json({
        "is_busy": len(conflicts) > 0,
        "conflicts": conflicts,
    })


@app.command(cls=JSONCommand, name="calendar-event")
@click.argument("event_id")
def calendar_event(event_id: str):
    """Get details of a specific calendar event."""
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM calendar_events WHERE id = ?", (event_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        output_error(f"Event not found: {event_id}", "not_found")
        sys.exit(1)

    event = dict(row)
    output_json(event)


@app.command(cls=JSONCommand, name="calendar-search")
@click.argument("query")
@click.option("--limit", default=20, help="Number of results")
def calendar_search(query: str, limit: int):
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
    output_json(events)


@app.command(cls=JSONCommand, name="calendar-meetings-with")
@click.argument("email")
@click.option("--limit", default=50, help="Number of results")
def calendar_meetings_with(email: str, limit: int):
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
    output_json(events)


@app.command(cls=JSONCommand, name="calendar-prep")
@click.argument("event_id", required=False)
@click.option("--next", "next_meeting", is_flag=True, help="Prepare for next upcoming meeting")
def calendar_prep(event_id: str | None, next_meeting: bool):
    """Prepare briefing for a meeting - includes attendee email history."""
    conn = connect_db()
    cursor = conn.cursor()

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
        output_error("Provide event_id or use --next", "missing_argument")
        sys.exit(1)

    row = cursor.fetchone()
    if not row:
        output_error("Event not found.", "not_found")
        sys.exit(1)

    event = dict(row)

    attendee_emails = []
    if event.get("attendees_json"):
        attendees = json.loads(event["attendees_json"])
        attendee_emails = [att["email"] for att in attendees if att.get("email")]
    if event.get("organizer_email"):
        attendee_emails.append(event["organizer_email"])

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
    output_json(prep)


# =============================================================================
# Action Commands
# =============================================================================


def _create_action(
    item_type: str,
    item_id: str | None,
    action_type: str,
    payload: dict,
) -> str:
    """Create an action in the actions table."""
    import uuid

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


@app.command(cls=JSONCommand, name="event-create")
@click.argument("subject")
@click.argument("start")
@click.argument("end")
@click.option("--attendees", default=None, help="Comma-separated attendee emails")
@click.option("--location", default=None, help="Location")
@click.option("--body", default=None, help="Event description")
@click.option("--online", is_flag=True, help="Create as Teams meeting")
def event_create(subject: str, start: str, end: str, attendees: str | None, location: str | None, body: str | None, online: bool):
    """Schedule a new calendar event (queued for RT service execution)."""
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
    output_json({"action_id": action_id, "status": "proposed", "action_type": "create_event"})


@app.command(cls=JSONCommand, name="event-update")
@click.argument("event_id")
@click.option("--subject", default=None, help="New subject")
@click.option("--start", default=None, help="New start datetime")
@click.option("--end", default=None, help="New end datetime")
@click.option("--location", default=None, help="New location")
def event_update(event_id: str, subject: str | None, start: str | None, end: str | None, location: str | None):
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
    output_json({"action_id": action_id, "status": "proposed", "action_type": "update_event"})


@app.command(cls=JSONCommand, name="event-cancel")
@click.argument("event_id")
@click.option("--notify/--no-notify", default=True, help="Send cancellation to attendees")
def event_cancel(event_id: str, notify: bool):
    """Cancel a calendar event (queued for RT service execution)."""
    payload = {
        "event_id": event_id,
        "notify_attendees": notify,
    }

    action_id = _create_action("calendar_event", event_id, "cancel_event", payload)
    output_json({"action_id": action_id, "status": "proposed", "action_type": "cancel_event"})


@app.command(cls=JSONCommand, name="event-respond")
@click.argument("event_id")
@click.argument("response")
def event_respond(event_id: str, response: str):
    """Respond to a meeting invite (queued for RT service execution)."""
    if response not in ("accept", "tentative", "decline"):
        output_error(f"Invalid response: {response}. Use: accept, tentative, decline", "invalid_response")
        sys.exit(1)

    payload = {
        "event_id": event_id,
        "response": response,
    }

    action_id = _create_action("calendar_event", event_id, "respond_event", payload)
    output_json({"action_id": action_id, "status": "proposed", "action_type": "respond_event"})


@app.command(cls=JSONCommand, name="actions-pending")
def actions_pending():
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
    output_json(actions)


@app.command(cls=JSONCommand, name="actions-history")
@click.option("--limit", default=20, help="Number of actions to show")
def actions_history(limit: int):
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
    output_json(actions)


# =============================================================================
# Working Memory Commands
# =============================================================================


def _get_wm_snapshot(conn) -> dict:
    """Query working memory state from database."""
    cursor = conn.cursor()

    tz = get_user_timezone()
    now = now_in_user_tz()
    today = today_in_user_tz()

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
            thread["needs_reply"] = thread.get("last_sender", "") != user_email
            thread["last_activity_at"] = thread.pop("last_activity", None)
            thread["id"] = thread["conversation_id"]
            threads.append(thread)
    except Exception:
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

    # Today's calendar
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
        pass

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


@wm_app.command(cls=JSONCommand, name="snapshot")
@click.option("--llm", is_flag=True, help="LLM-optimized output for context injection")
def wm_snapshot(llm: bool):
    """Get complete working memory snapshot (for context injection)."""
    conn = connect_db()
    snapshot = _get_wm_snapshot(conn)
    conn.close()

    if llm:
        # LLM-optimized format - still JSON but structured for injection
        llm_output = {
            "format": "llm_context",
            "timestamp": snapshot["current_time"],
            "timezone": snapshot["timezone"],
            "today": snapshot["today"],
            "summary": {
                "threads_needing_reply": snapshot["threads_needing_reply"],
                "pending_decisions": len(snapshot["pending_decisions"]),
                "overdue_commitments": snapshot["overdue_commitments"],
                "urgent_items": snapshot["urgent_items"],
            },
            "today_calendar": snapshot["today_calendar"],
            "active_threads": [
                {
                    "subject": t["subject"][:50],
                    "urgency": t.get("urgency"),
                    "needs_reply": t.get("needs_reply"),
                    "web_link": t.get("latest_web_link"),
                }
                for t in snapshot["active_threads"][:10]
            ],
            "pending_decisions": [
                {
                    "question": d["question"][:60],
                    "requester": d.get("requester"),
                    "urgency": d.get("urgency"),
                }
                for d in snapshot["pending_decisions"][:5]
            ],
            "open_commitments": [
                {
                    "description": c["description"][:50],
                    "to_whom": c.get("to_whom"),
                    "due_by": c.get("due_by"),
                }
                for c in snapshot["open_commitments"][:5]
            ],
        }
        output_json(llm_output)
    else:
        output_json(snapshot)


@wm_app.command(cls=JSONCommand, name="threads")
@click.option("--needs-reply", is_flag=True, help="Only show threads needing reply")
@click.option("--urgency", default=None, help="Filter by urgency")
@click.option("--limit", default=20, help="Number of threads to show")
def wm_threads(needs_reply: bool, urgency: str | None, limit: int):
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

    output_json(threads)


@wm_app.command(cls=JSONCommand, name="decisions")
def wm_decisions():
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

    output_json(decisions)


@wm_app.command(cls=JSONCommand, name="commitments")
@click.option("--overdue", is_flag=True, help="Only show overdue commitments")
def wm_commitments(overdue: bool):
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

    output_json(commitments)


@wm_app.command(cls=JSONCommand, name="contacts")
@click.option("--external", is_flag=True, help="Only external contacts")
@click.option("--search", default=None, help="Search by email or name")
@click.option("--limit", default=20, help="Number of contacts to show")
def wm_contacts(external: bool, search: str | None, limit: int):
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

    output_json(contacts)


@wm_app.command(cls=JSONCommand, name="observations")
@click.option("--days", default=7, help="Days of observations to show")
def wm_observations(days: int):
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

    output_json(observations)


@wm_app.command(cls=JSONCommand, name="projects")
def wm_projects():
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

    output_json(projects)


# =============================================================================
# Preferences Commands
# =============================================================================


@prefs_app.command(cls=JSONCommand, name="show")
def prefs_show():
    """Show the current preferences.json."""
    output_json(read_preferences())


@prefs_app.command(cls=JSONCommand, name="set")
@click.argument("key")
@click.argument("value")
def prefs_set(key: str, value: str):
    """Set a preference key in preferences.json."""
    try:
        path = set_preference_from_string(key, value)
        output_json({"status": "ok", "path": str(path), "key": key})
    except InvalidPreferenceKeyError as e:
        output_error(str(e), "invalid_key")
        sys.exit(1)


@prefs_app.command(cls=JSONCommand, name="keys")
def prefs_keys():
    """List all valid preference keys."""
    output_json({"keys": sorted(VALID_PREFERENCE_KEYS)})


@prefs_app.command(cls=JSONCommand, name="unset")
@click.argument("key")
def prefs_unset(key: str):
    """Remove a preference key from preferences.json."""
    prefs = read_preferences()
    if key in prefs:
        prefs.pop(key, None)
        path = write_preferences(prefs)
        output_json({"status": "ok", "path": str(path), "key": key})
    else:
        output_error(f"Key not found: {key}", "not_found")
        sys.exit(1)


# =============================================================================
# Alerts Commands
# =============================================================================


@alerts_app.command(cls=JSONCommand, name="list")
@click.option("--enabled-only", is_flag=True, help="Only show enabled rules")
def alerts_list(enabled_only: bool):
    """List all alert rules."""
    conn = connect_db()
    query = "SELECT * FROM alert_rules"
    if enabled_only:
        query += " WHERE enabled = 1"
    query += " ORDER BY created_at DESC"

    rows = conn.execute(query).fetchall()
    conn.close()

    rules = [dict(r) for r in rows]
    output_json(rules)


@alerts_app.command(cls=JSONCommand, name="add")
@click.argument("rule")
@click.option("--channel", default="teams", help="Notification channel: teams, email")
@click.option("--target", default=None, help="Channel target (chat ID, email address)")
@click.option("--cooldown", default=30, help="Cooldown between triggers (minutes)")
def alerts_add(rule: str, channel: str, target: str | None, cooldown: int):
    """Add a new alert rule."""
    import asyncio
    from pathlib import Path

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

        output_json(result)
    except Exception as e:
        output_error(f"Error creating rule: {e}", "create_failed")
        sys.exit(1)


@alerts_app.command(cls=JSONCommand, name="remove")
@click.argument("rule_id")
def alerts_remove(rule_id: str):
    """Remove an alert rule."""
    conn = connect_db()

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
        output_json({"status": "deleted", "id": rule_id})
    else:
        output_error(f"Rule not found: {rule_id}", "not_found")
        sys.exit(1)


@alerts_app.command(cls=JSONCommand, name="enable")
@click.argument("rule_id")
def alerts_enable(rule_id: str):
    """Enable an alert rule."""
    conn = connect_db()

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

    output_json({"status": "enabled" if updated else "not_found", "id": rule_id})


@alerts_app.command(cls=JSONCommand, name="disable")
@click.argument("rule_id")
def alerts_disable(rule_id: str):
    """Disable an alert rule."""
    conn = connect_db()

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

    output_json({"status": "disabled" if updated else "not_found", "id": rule_id})


@alerts_app.command(cls=JSONCommand, name="history")
@click.option("--rule-id", default=None, help="Filter by rule ID")
@click.option("--limit", default=20, help="Number of entries")
def alerts_history(rule_id: str | None, limit: int):
    """View alert trigger history."""
    conn = connect_db()

    query = """
        SELECT at.*, ar.natural_language_rule
        FROM alert_triggers at
        JOIN alert_rules ar ON at.rule_id = ar.id
    """
    params = []

    if rule_id:
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
    output_json(triggers)


@alerts_app.command(cls=JSONCommand, name="show")
@click.argument("rule_id")
def alerts_show(rule_id: str):
    """Show details of a specific alert rule."""
    conn = connect_db()

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
        output_error(f"Rule not found: {rule_id}", "not_found")
        sys.exit(1)

    rule = dict(row)
    output_json(rule)


# =============================================================================
# Entry Point
# =============================================================================


def run():
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    run()
