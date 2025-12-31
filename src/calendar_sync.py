"""
Calendar sync module for Executive Assistant.

Syncs calendar events from Microsoft Graph API to local SQLite database
for offline access by the CLI.
"""

import json
import logging
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Optional

from .calendar import CalendarClient, CalendarEvent
from .database import get_connection

logger = logging.getLogger(__name__)

# Sync configuration
DAYS_BACK = 30
DAYS_FORWARD = 60
SYNC_FOLDER_ID = "calendar"  # Used in sync_state table


def sync_calendar(
    days_back: int = DAYS_BACK,
    days_forward: int = DAYS_FORWARD,
) -> dict:
    """
    Sync calendar events from Graph API to local database.

    Args:
        days_back: Number of days in the past to sync
        days_forward: Number of days in the future to sync

    Returns:
        Dict with sync statistics
    """
    client = CalendarClient()
    now = datetime.now(dt_timezone.utc)

    start = now - timedelta(days=days_back)
    end = now + timedelta(days=days_forward)

    logger.info(f"Syncing calendar events from {start.date()} to {end.date()}")

    # Fetch all events in range
    events = client.get_calendar_view(start, end, max_results=1000)
    logger.info(f"Fetched {len(events)} events from Graph API")

    # Upsert events to database
    conn = get_connection()
    upserted = 0
    for event in events:
        _upsert_event(conn, event)
        upserted += 1

    # Update sync state
    conn.execute(
        """
        INSERT INTO sync_state (folder_id, last_sync_at, sync_type, messages_synced)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(folder_id) DO UPDATE SET
            last_sync_at = excluded.last_sync_at,
            sync_type = excluded.sync_type,
            messages_synced = excluded.messages_synced
        """,
        (SYNC_FOLDER_ID, datetime.now(dt_timezone.utc).isoformat(), "full", upserted),
    )

    conn.commit()
    conn.close()

    logger.info(f"Calendar sync complete: {upserted} events synced")

    # Evaluate alert rules for new/changed calendar events
    if upserted > 0:
        _evaluate_calendar_alerts(events)

    return {
        "events_synced": upserted,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }


def _upsert_event(conn, event: CalendarEvent) -> None:
    """Upsert a calendar event to the database."""
    # Serialize attendees to JSON
    attendees_json = json.dumps([
        {
            "email": att.email,
            "name": att.name,
            "response": att.response,
            "type": att.type,
        }
        for att in event.attendees
    ])

    conn.execute(
        """
        INSERT INTO calendar_events (
            id, subject, start_at, end_at, is_all_day, location,
            is_online_meeting, online_meeting_url, organizer_email, organizer_name,
            attendees_json, body_preview, response_status, sensitivity,
            show_as, importance, is_cancelled, web_link, last_modified_at, synced_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            subject = excluded.subject,
            start_at = excluded.start_at,
            end_at = excluded.end_at,
            is_all_day = excluded.is_all_day,
            location = excluded.location,
            is_online_meeting = excluded.is_online_meeting,
            online_meeting_url = excluded.online_meeting_url,
            organizer_email = excluded.organizer_email,
            organizer_name = excluded.organizer_name,
            attendees_json = excluded.attendees_json,
            body_preview = excluded.body_preview,
            response_status = excluded.response_status,
            sensitivity = excluded.sensitivity,
            show_as = excluded.show_as,
            importance = excluded.importance,
            is_cancelled = excluded.is_cancelled,
            synced_at = excluded.synced_at
        """,
        (
            event.event_id,
            event.subject,
            event.start.isoformat(),
            event.end.isoformat(),
            1 if event.is_all_day else 0,
            event.location,
            1 if event.is_online_meeting else 0,
            event.online_meeting_url,
            event.organizer_email,
            event.organizer_name,
            attendees_json,
            event.body_preview,
            event.response_status,
            event.sensitivity,
            event.show_as,
            event.importance,
            0,  # is_cancelled - Graph API doesn't return cancelled events in calendarView
            None,  # web_link - not in CalendarEvent model currently
            None,  # last_modified_at - not tracked currently
            datetime.now(dt_timezone.utc).isoformat(),
        ),
    )


def get_last_sync_time() -> Optional[datetime]:
    """Get the last calendar sync time."""
    conn = get_connection()
    row = conn.execute(
        "SELECT last_sync_at FROM sync_state WHERE folder_id = ?",
        (SYNC_FOLDER_ID,),
    ).fetchone()
    conn.close()

    if row and row["last_sync_at"]:
        return datetime.fromisoformat(row["last_sync_at"])
    return None


def needs_sync(interval_seconds: int = 300) -> bool:
    """Check if calendar needs to be synced based on interval."""
    last_sync = get_last_sync_time()
    if last_sync is None:
        return True

    # Ensure timezone-aware comparison
    if last_sync.tzinfo is None:
        last_sync = last_sync.replace(tzinfo=dt_timezone.utc)

    now = datetime.now(dt_timezone.utc)
    return (now - last_sync).total_seconds() >= interval_seconds


def _evaluate_calendar_alerts(events: list[CalendarEvent]) -> None:
    """Evaluate alert rules against calendar events."""
    import asyncio
    import os

    try:
        from .alerts import AlertRulesEngine

        user_email = os.environ.get("DELEGATED_USER", "")
        if not user_email:
            return

        alert_engine = AlertRulesEngine(user_email)

        async def evaluate_events():
            for event in events:
                # Convert CalendarEvent to dict for alert evaluation
                event_dict = {
                    "id": event.event_id,
                    "subject": event.subject,
                    "start_at": event.start.isoformat(),
                    "end_at": event.end.isoformat(),
                    "organizer_email": event.organizer_email,
                    "organizer_name": event.organizer_name,
                    "attendees": [
                        {"email": att.email, "name": att.name}
                        for att in event.attendees
                    ],
                    "attendee_count": len(event.attendees),
                    "is_online_meeting": event.is_online_meeting,
                    "location": event.location,
                }

                triggered = await alert_engine.evaluate_calendar_rules(event_dict)

                for t in triggered:
                    alert_engine.emit_alert_trigger(
                        t["rule"],
                        "calendar_event",
                        event.event_id,
                        event_dict,
                        t["match_reason"],
                    )

        asyncio.run(evaluate_events())

    except Exception as e:
        logger.warning(f"Calendar alert evaluation error: {e}")
