"""
Action executor for the RT service.

Polls the actions table for pending actions and executes them via Graph API.
"""

import json
import logging
import os
from datetime import datetime, timezone as dt_timezone
from typing import Optional

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from .model_utils import parse_model_string, get_model_settings
from .database import get_connection
from .calendar import CalendarClient

logger = logging.getLogger(__name__)


class ActionGuardDecision(BaseModel):
    """Typed LLM decision on whether an action is valid and executable."""

    allow: bool = Field(description="True if action is valid and can proceed")
    normalized_action_type: str = Field(description="Canonical action type")
    required_fields: list[str] = Field(default_factory=list)
    reason: str = Field(default="")


_ACTION_GUARD_AGENT: Agent[None, ActionGuardDecision] | None = None


def _build_action_guard_agent() -> Agent[None, ActionGuardDecision]:
    model_string = os.getenv(
        "ACTION_GUARD_MODEL",
        os.getenv("MODEL_NAME", "openai-responses:gpt-5-mini"),
    )
    model_name, _ = parse_model_string(model_string)
    model_settings = get_model_settings(model_string)

    instructions = """
You validate queued calendar actions before execution.
Input is JSON with item_type, action_type, and payload.

Decide:
- allow: true only if action_type and payload are semantically valid
- normalized_action_type: one of create_event, update_event, cancel_event, respond_event
- required_fields: required payload fields for this action
- reason: short explanation

Validation rules:
- create_event requires subject, start, end
- update_event requires event_id and at least one mutating field among subject/start/end/location
- cancel_event requires event_id
- respond_event requires event_id and response in {accept, tentative, decline}
- Reject unknown action types
"""

    return Agent(
        model_name,
        output_type=ActionGuardDecision,
        instructions=instructions,
        model_settings=model_settings,
    )


def _get_action_guard_agent() -> Agent[None, ActionGuardDecision]:
    global _ACTION_GUARD_AGENT
    if _ACTION_GUARD_AGENT is None:
        _ACTION_GUARD_AGENT = _build_action_guard_agent()
    return _ACTION_GUARD_AGENT


def _validate_action_with_llm(
    item_type: str,
    action_type: str,
    payload: dict,
) -> ActionGuardDecision:
    context = {
        "item_type": item_type,
        "action_type": action_type,
        "payload": payload,
    }
    result = _get_action_guard_agent().run_sync(json.dumps(context, default=str))
    decision = result.output

    if not decision.allow:
        raise ValueError(f"Action rejected by LLM guard: {decision.reason}")
    if decision.normalized_action_type != action_type:
        raise ValueError(
            f"Action type mismatch from LLM guard: expected {action_type}, "
            f"got {decision.normalized_action_type}"
        )

    for field in decision.required_fields:
        value = payload.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ValueError(f"Missing required action payload field: {field}")

    return decision


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
        item_type = row["item_type"]
        action_type = row["action_type"]
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}

        try:
            # Lazy init calendar client
            if calendar_client is None:
                calendar_client = CalendarClient()

            guard_decision = _validate_action_with_llm(item_type, action_type, payload)
            result = _execute_action(calendar_client, action_type, payload)
            result["guard_reason"] = guard_decision.reason

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
        if not success:
            raise RuntimeError(f"Calendar delete failed for event_id={event_id}")
        return {"event_id": event_id, "deleted": success}

    elif action_type == "respond_event":
        raise NotImplementedError("respond_event execution is not implemented")

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
