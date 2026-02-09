"""
Alert Rules Engine - evaluates user-defined rules against events.

Supports natural language rules like:
- "Alert me when CFO emails about budget"
- "Notify when I send email to legal@"
- "Alert when commitment is overdue"
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from .database import get_connection
from .model_utils import parse_model_string, get_model_settings
from .triggers import make_dedupe_key, write_trigger

logger = logging.getLogger(__name__)

_ALERT_CHANNEL_TEAMS = "teams"


class ParsedConditions(BaseModel):
    """Structured conditions extracted from natural language rule."""

    event_types: list[str] = Field(
        default_factory=lambda: ["email_received"],
        description="Event types this rule applies to: email_received, email_sent, calendar_event, wm_thread, wm_commitment, wm_decision",
    )


class RuleMatchResult(BaseModel):
    """Result of semantic rule matching."""

    matches: bool = Field(default=False, description="Whether the event matches the rule")
    match_reason: str = Field(default="", description="Explanation of why it matched")
    confidence: float = Field(default=1.0, ge=0, le=1, description="Confidence score")


def _build_rule_parser_agent() -> Agent[None, ParsedConditions]:
    """Build agent to parse natural language rules into structured conditions."""
    model_string = os.getenv(
        "RULE_PARSER_MODEL",
        os.getenv("MODEL_NAME", "openai:gpt-4o-mini"),
    )
    model_name, _ = parse_model_string(model_string)
    model_settings = get_model_settings(model_string)

    system_prompt = """
You parse natural-language alert rules into event type scope only.

Return ParsedConditions with event_types based on rule intent:
- email_received: inbound email alerts (default)
- email_sent: outbound email alerts
- calendar_event: calendar event alerts
- wm_thread: working-memory thread alerts
- wm_commitment: commitment alerts
- wm_decision: decision alerts

Always include at least one event type.
"""

    return Agent(
        model_name,
        output_type=ParsedConditions,
        instructions=system_prompt,
        model_settings=model_settings,
    )


def _build_semantic_matcher_agent() -> Agent[None, RuleMatchResult]:
    """Build agent for semantic rule matching."""
    model_string = os.getenv(
        "ALERT_MODEL",
        os.getenv("MODEL_NAME", "openai:gpt-4o-mini"),
    )
    model_name, _ = parse_model_string(model_string)
    model_settings = get_model_settings(model_string)

    system_prompt = """
You determine if an event matches a user-defined alert rule semantically.

Given a rule and an event (email, calendar event, or working memory item),
evaluate whether the event matches the rule's intent.

Consider:
- The overall meaning and context, not just keywords
- Sender/recipient relationships and roles
- Tone and sentiment when relevant
- Subject matter relevance

Return:
- matches: true/false
- match_reason: Brief explanation of why it matched or didn't
- confidence: 0.0 to 1.0
"""

    return Agent(
        model_name,
        output_type=RuleMatchResult,
        instructions=system_prompt,
        model_settings=model_settings,
    )


class AlertRulesEngine:
    """Evaluates alert rules against events."""

    def __init__(self, user_email: str):
        self.user_email = user_email
        self._parser_agent: Agent[None, ParsedConditions] | None = None
        self._matcher_agent: Agent[None, RuleMatchResult] | None = None

    async def parse_rule(self, natural_language_rule: str) -> ParsedConditions:
        """Parse a natural language rule into structured conditions."""
        if self._parser_agent is None:
            self._parser_agent = _build_rule_parser_agent()

        result = await self._parser_agent.run(natural_language_rule)
        return result.output

    def _get_matcher_agent(self) -> Agent[None, RuleMatchResult]:
        if self._matcher_agent is None:
            self._matcher_agent = _build_semantic_matcher_agent()
        return self._matcher_agent

    @staticmethod
    def _parse_rule_event_types(rule: dict[str, Any]) -> list[str]:
        raw = rule.get("event_types")
        if raw is None:
            return ["email_received"]

        event_types = json.loads(raw)
        if not isinstance(event_types, list) or not all(isinstance(t, str) for t in event_types):
            raise ValueError(f"Invalid event_types for rule {rule.get('id')}: {raw}")
        return event_types

    @staticmethod
    def _cooldown_active(rule: dict[str, Any], now: datetime) -> bool:
        last_triggered = rule.get("last_triggered_at")
        if not last_triggered:
            return False

        last_dt = datetime.fromisoformat(str(last_triggered).replace("Z", "+00:00"))
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)

        cooldown_minutes = int(rule.get("cooldown_minutes") or 30)
        cooldown = timedelta(minutes=cooldown_minutes)
        return now - last_dt < cooldown

    async def _semantic_match(
        self,
        rule: dict[str, Any],
        event_type: str,
        event_payload: dict[str, Any],
        event_context: dict[str, Any] | None = None,
    ) -> RuleMatchResult:
        matcher = self._get_matcher_agent()
        prompt_payload = {
            "rule_text": rule["natural_language_rule"],
            "event_type": event_type,
            "event_payload": event_payload,
            "event_context": event_context or {},
            "principal_user_email": self.user_email,
        }
        result = await matcher.run(json.dumps(prompt_payload, default=str))
        return result.output

    async def _evaluate_rules_for_event(
        self,
        event_type: str,
        event_id: str,
        event_payload: dict[str, Any],
        event_context: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        conn = get_connection()
        try:
            rows = conn.execute("SELECT * FROM alert_rules WHERE enabled = 1").fetchall()
            now = datetime.now(timezone.utc)
            triggered: list[dict[str, Any]] = []

            for row in rows:
                rule = dict(row)
                rule_id = rule["id"]
                rule_event_types = self._parse_rule_event_types(rule)
                if event_type not in rule_event_types:
                    continue

                existing = conn.execute(
                    "SELECT 1 FROM alert_triggers WHERE rule_id = ? AND event_type = ? AND event_id = ?",
                    (rule_id, event_type, event_id),
                ).fetchone()
                if existing:
                    continue

                if self._cooldown_active(rule, now):
                    continue

                match_result = await self._semantic_match(
                    rule=rule,
                    event_type=event_type,
                    event_payload=event_payload,
                    event_context=event_context,
                )
                if not match_result.matches:
                    continue

                triggered.append(
                    {
                        "rule": rule,
                        "match_reason": match_result.match_reason or "Matched by semantic alert evaluation",
                        "match_confidence": match_result.confidence,
                    }
                )

            return triggered
        finally:
            conn.close()

    async def evaluate_email_rules(
        self,
        email: dict[str, Any],
        classification: dict[str, Any],
        event_type: str = "email_received",
    ) -> list[dict[str, Any]]:
        """Evaluate all enabled rules against an email. Returns list of triggered rules."""
        email_id = str(email.get("id") or "")
        if not email_id:
            raise ValueError("Email event is missing id for alert evaluation")

        return await self._evaluate_rules_for_event(
            event_type=event_type,
            event_id=email_id,
            event_payload=email,
            event_context={"classification": classification},
        )

    async def evaluate_wm_rules(
        self,
        wm_item: dict[str, Any],
        wm_type: str,
    ) -> list[dict[str, Any]]:
        """Evaluate rules against a working memory item."""
        item_id = str(
            wm_item.get("id")
            or wm_item.get("thread_id")
            or wm_item.get("commitment_id")
            or wm_item.get("decision_id")
            or wm_item.get("conversation_id")
            or ""
        )
        if not item_id:
            raise ValueError("Working memory event is missing id for alert evaluation")

        return await self._evaluate_rules_for_event(
            event_type=wm_type,
            event_id=item_id,
            event_payload=wm_item,
            event_context={"wm_type": wm_type},
        )

    async def evaluate_calendar_rules(
        self,
        event: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Evaluate all enabled rules against a calendar event. Returns list of triggered rules."""
        event_id = str(event.get("id") or "")
        if not event_id:
            raise ValueError("Calendar event is missing id for alert evaluation")

        return await self._evaluate_rules_for_event(
            event_type="calendar_event",
            event_id=event_id,
            event_payload=event,
        )

    def emit_alert_trigger(
        self,
        rule: dict[str, Any],
        event_type: str,
        event_id: str,
        payload: dict[str, Any],
        match_reason: str,
    ) -> None:
        """Emit a trigger for a matched alert rule."""
        rule_id = rule["id"]

        trigger_payload = {
            "rule_id": rule_id,
            "rule_text": rule["natural_language_rule"],
            "event_type": event_type,
            "event_id": event_id,
            "match_reason": match_reason,
            **{k: v for k, v in payload.items() if k in (
                "subject", "sender", "received_at", "web_link",
                "description", "to_whom", "due_by", "question",
            )},
        }

        routing: dict[str, Any] = {"channel": _ALERT_CHANNEL_TEAMS}

        dedupe_key = make_dedupe_key(
            "alert_rule_triggered",
            self.user_email,
            f"{rule_id}:{event_type}:{event_id}",
        )

        write_trigger(
            self.user_email,
            "alert_rule_triggered",
            trigger_payload,
            dedupe_key=dedupe_key,
            routing=routing,
        )

        # Record trigger in database
        conn = get_connection()
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            conn.execute(
                """
                INSERT OR IGNORE INTO alert_triggers
                (id, rule_id, event_type, event_id, match_reason, trigger_payload_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    rule_id,
                    event_type,
                    event_id,
                    match_reason,
                    json.dumps(trigger_payload),
                ),
            )
            conn.execute(
                """
                UPDATE alert_rules
                SET last_triggered_at = ?, trigger_count = trigger_count + 1, updated_at = ?
                WHERE id = ?
                """,
                (now_iso, now_iso, rule_id),
            )
            conn.commit()
            logger.info(f"Alert rule {rule_id} triggered for {event_type}:{event_id}")
        finally:
            conn.close()


# === CRUD Functions ===


async def create_alert_rule(
    natural_language_rule: str,
    cooldown_minutes: int = 30,
    created_by: str = "user",
) -> dict[str, Any]:
    """Create a new alert rule by parsing natural language."""
    engine = AlertRulesEngine("")
    conditions = await engine.parse_rule(natural_language_rule)

    rule_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT INTO alert_rules
            (id, natural_language_rule, event_types,
             cooldown_minutes, created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rule_id,
                natural_language_rule,
                json.dumps(conditions.event_types),
                cooldown_minutes,
                created_by,
                now,
                now,
            ),
        )
        conn.commit()

        return {
            "id": rule_id,
            "natural_language_rule": natural_language_rule,
            "event_types": conditions.event_types,
            "cooldown_minutes": cooldown_minutes,
            "enabled": True,
        }
    finally:
        conn.close()


def list_alert_rules(enabled_only: bool = False) -> list[dict[str, Any]]:
    """List all alert rules."""
    conn = get_connection()
    try:
        query = "SELECT * FROM alert_rules"
        if enabled_only:
            query += " WHERE enabled = 1"
        query += " ORDER BY created_at DESC"

        rows = conn.execute(query).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_alert_rule(rule_id: str) -> dict[str, Any] | None:
    """Get a single alert rule by ID."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM alert_rules WHERE id = ?",
            (rule_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def update_alert_rule(rule_id: str, **kwargs: Any) -> bool:
    """Update an alert rule."""
    conn = get_connection()
    try:
        updates = []
        params: list[Any] = []
        allowed_fields = {"enabled", "cooldown_minutes"}

        for key, value in kwargs.items():
            if key in allowed_fields:
                updates.append(f"{key} = ?")
                params.append(value)

        if not updates:
            return False

        updates.append("updated_at = ?")
        params.append(datetime.now(timezone.utc).isoformat())
        params.append(rule_id)

        conn.execute(
            f"UPDATE alert_rules SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        conn.commit()
        return conn.total_changes > 0
    finally:
        conn.close()


def delete_alert_rule(rule_id: str) -> bool:
    """Delete an alert rule."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM alert_rules WHERE id = ?", (rule_id,))
        conn.commit()
        return conn.total_changes > 0
    finally:
        conn.close()


def get_alert_trigger_history(
    rule_id: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Get alert trigger history."""
    conn = get_connection()
    try:
        query = """
            SELECT at.*, ar.natural_language_rule
            FROM alert_triggers at
            JOIN alert_rules ar ON at.rule_id = ar.id
        """
        params: list[Any] = []

        if rule_id:
            query += " WHERE at.rule_id = ?"
            params.append(rule_id)

        query += " ORDER BY at.triggered_at DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
