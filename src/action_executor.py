"""
Action executor for the RT service.

Polls the actions table for pending actions and executes them via Graph API.
"""

import json
import logging
from datetime import datetime, timezone as dt_timezone
from typing import Optional

from .database import get_connection
from .calendar import CalendarClient

logger = logging.getLogger(__name__)


def poll_and_execute_actions() -> dict:
    """
    Poll for pending actions and execute them.

    Returns:
        Dict with execution statistics
    """
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT id, item_type, item_id, action_type, payload_json
        FROM actions
        WHERE status = 'proposed'
        ORDER BY proposed_at ASC
        LIMIT 10
        """
    ).fetchall()

    if not rows:
        conn.close()
        return {"executed": 0, "failed": 0}

    executed = 0
    failed = 0
    calendar_client = None

    for row in rows:
        action_id = row["id"]
        action_type = row["action_type"]
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}

        try:
            # Lazy init calendar client
            if calendar_client is None:
                calendar_client = CalendarClient()

            result = _execute_action(calendar_client, action_type, payload)

            # Mark as executed
            conn.execute(
                """
                UPDATE actions
                SET status = 'executed', executed_at = ?, result_json = ?
                WHERE id = ?
                """,
                (datetime.now(dt_timezone.utc).isoformat(), json.dumps(result), action_id),
            )
            conn.commit()
            executed += 1
            logger.info(f"Executed action {action_id}: {action_type}")

        except Exception as e:
            # Mark as failed
            conn.execute(
                """
                UPDATE actions
                SET status = 'failed', executed_at = ?, error = ?
                WHERE id = ?
                """,
                (datetime.now(dt_timezone.utc).isoformat(), str(e), action_id),
            )
            conn.commit()
            failed += 1
            logger.warning(f"Action {action_id} failed: {e}")

    conn.close()
    return {"executed": executed, "failed": failed}


def _execute_action(client: CalendarClient, action_type: str, payload: dict) -> dict:
    """Execute a single action and return result."""

    if action_type == "create_event":
        event = client.create_event(
            subject=payload["subject"],
            start=datetime.fromisoformat(payload["start"]),
            end=datetime.fromisoformat(payload["end"]),
            attendees=payload.get("attendees"),
            location=payload.get("location"),
            body=payload.get("body"),
            is_online_meeting=payload.get("is_online_meeting", False),
        )
        return {"event_id": event.event_id, "subject": event.subject}

    elif action_type == "update_event":
        event_id = payload["event_id"]
        start = datetime.fromisoformat(payload["start"]) if payload.get("start") else None
        end = datetime.fromisoformat(payload["end"]) if payload.get("end") else None

        event = client.update_event(
            event_id=event_id,
            subject=payload.get("subject"),
            start=start,
            end=end,
            location=payload.get("location"),
        )
        return {"event_id": event.event_id, "updated": True}

    elif action_type == "cancel_event":
        event_id = payload["event_id"]
        success = client.delete_event(event_id)
        return {"event_id": event_id, "deleted": success}

    elif action_type == "respond_event":
        # Note: Graph API response to event requires different endpoint
        # For now, we'll log this as not implemented
        event_id = payload["event_id"]
        response = payload["response"]
        # TODO: Implement calendar event response
        # client.respond_to_event(event_id, response)
        logger.warning(f"respond_event not yet implemented: {event_id} -> {response}")
        return {"event_id": event_id, "response": response, "note": "not_implemented"}

    else:
        raise ValueError(f"Unknown action type: {action_type}")


def has_pending_actions() -> bool:
    """Check if there are any pending actions."""
    conn = get_connection()
    row = conn.execute(
        "SELECT COUNT(*) as count FROM actions WHERE status = 'proposed'"
    ).fetchone()
    conn.close()
    return row["count"] > 0
