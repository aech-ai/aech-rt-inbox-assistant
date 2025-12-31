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
import re
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from .database import get_connection
from .triggers import make_dedupe_key, write_trigger

logger = logging.getLogger(__name__)


class ParsedConditions(BaseModel):
    """Structured conditions extracted from natural language rule."""

    event_types: list[str] = Field(
        default_factory=lambda: ["email_received"],
        description="Event types this rule applies to: email_received, email_sent, calendar_event, wm_thread, wm_commitment, wm_decision",
    )
    sender_patterns: list[str] = Field(
        default_factory=list,
        description="Email sender patterns (wildcards: *@legal.com, *cfo*)",
    )
    recipient_patterns: list[str] = Field(
        default_factory=list,
        description="Email recipient patterns for sent emails",
    )
    subject_keywords: list[str] = Field(
        default_factory=list,
        description="Keywords to match in email subject",
    )
    body_keywords: list[str] = Field(
        default_factory=list,
        description="Keywords to match in email body",
    )
    urgency_levels: list[str] = Field(
        default_factory=list,
        description="Urgency levels: immediate, today, this_week, someday",
    )
    labels: list[str] = Field(
        default_factory=list,
        description="Email labels to match: vip, billing, marketing, etc.",
    )
    categories: list[str] = Field(
        default_factory=list,
        description="Outlook categories: Action Required, Work, Personal, etc.",
    )
    # Calendar-specific
    min_attendees: int | None = Field(
        default=None,
        description="Minimum number of meeting attendees",
    )
    organizer_patterns: list[str] = Field(
        default_factory=list,
        description="Calendar event organizer patterns",
    )
    # Working memory-specific
    wm_types: list[str] = Field(
        default_factory=list,
        description="Working memory types: thread, commitment, decision",
    )
    overdue_only: bool = Field(
        default=False,
        description="Only match overdue items (for WM events)",
    )
    match_mode: str = Field(
        default="any",
        description="Match mode: 'any' (OR) or 'all' (AND)",
    )
    requires_semantic_match: bool = Field(
        default=False,
        description="True if rule needs LLM semantic matching beyond keywords",
    )


class RuleMatchResult(BaseModel):
    """Result of semantic rule matching."""

    matches: bool = Field(default=False, description="Whether the event matches the rule")
    match_reason: str = Field(default="", description="Explanation of why it matched")
    confidence: float = Field(default=1.0, ge=0, le=1, description="Confidence score")


def _build_rule_parser_agent() -> Agent[None, ParsedConditions]:
    """Build agent to parse natural language rules into structured conditions."""
    model_name = os.getenv(
        "RULE_PARSER_MODEL",
        os.getenv("MODEL_NAME", "openai:gpt-4o-mini"),
    )

    system_prompt = """
You parse natural language email/calendar/working-memory alert rules into structured conditions.

## Event Types
Determine which event type(s) the rule applies to:
- email_received: Incoming emails (default if not specified)
- email_sent: Outgoing emails ("when I send", "when I email")
- calendar_event: Calendar events ("meeting", "appointment")
- wm_thread: Email thread tracking ("thread is stale", "awaiting reply")
- wm_commitment: User commitments ("commitment overdue", "promised to")
- wm_decision: Pending decisions ("decision pending", "waiting for decision")

## Parsing Rules

### Email patterns:
- "from CFO" → sender_patterns: ["*cfo*"]
- "from legal@company.com" → sender_patterns: ["legal@company.com"]
- "from *@legal.company.com" → sender_patterns: ["*@legal.company.com"]
- "to legal@" → recipient_patterns: ["*legal@*"] (for sent emails)

### Keywords:
- "about budget" → subject_keywords: ["budget"], body_keywords: ["budget"]
- "mentions contract" → body_keywords: ["contract"]
- "subject contains urgent" → subject_keywords: ["urgent"]

### Urgency:
- "urgent emails" → urgency_levels: ["immediate", "today"]
- "high priority" → urgency_levels: ["immediate"]

### Labels/Categories:
- "VIP emails" → labels: ["vip"]
- "action required" → categories: ["Action Required"]

### Calendar:
- "meeting with >5 people" → min_attendees: 5, event_types: ["calendar_event"]
- "meeting organized by john@" → organizer_patterns: ["*john@*"]

### Working Memory:
- "commitment is overdue" → event_types: ["wm_commitment"], overdue_only: true
- "thread awaiting reply for >3 days" → event_types: ["wm_thread"], overdue_only: true
- "pending decision" → event_types: ["wm_decision"]

### Match mode:
- "emails from CFO about budget" → match_mode: "all" (both must match)
- "emails from CFO or about budget" → match_mode: "any" (either matches)

### Semantic matching:
- "when someone sounds frustrated" → requires_semantic_match: true
- "when email is complaining" → requires_semantic_match: true

Return structured ParsedConditions. Be precise with patterns - use wildcards (*) appropriately.
"""

    return Agent(
        model_name,
        output_type=ParsedConditions,
        system_prompt=system_prompt,
    )


def _build_semantic_matcher_agent() -> Agent[None, RuleMatchResult]:
    """Build agent for semantic rule matching."""
    model_name = os.getenv(
        "ALERT_MODEL",
        os.getenv("MODEL_NAME", "openai:gpt-4o-mini"),
    )

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
        system_prompt=system_prompt,
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

    def _pattern_matches(self, pattern: str, value: str) -> bool:
        """Check if a pattern matches a value. Supports * wildcards."""
        if not pattern or not value:
            return False
        pattern_lower = pattern.lower()
        value_lower = value.lower()

        if "*" in pattern_lower:
            # Convert glob pattern to regex
            regex = pattern_lower.replace(".", r"\.").replace("*", ".*")
            return bool(re.search(regex, value_lower))
        return pattern_lower in value_lower

    def _fast_match_email(
        self,
        conditions: ParsedConditions,
        email: dict[str, Any],
        classification: dict[str, Any],
        event_type: str,
    ) -> tuple[bool, str]:
        """Fast pre-filter for email events. Returns (matches, reason)."""
        matches: list[bool] = []
        reasons: list[str] = []

        sender = str(email.get("sender") or "").lower()
        subject = str(email.get("subject") or "").lower()
        body = str(email.get("body_preview") or email.get("body_markdown") or "").lower()
        to_emails = email.get("to_emails") or []
        if isinstance(to_emails, str):
            try:
                to_emails = json.loads(to_emails)
            except Exception:
                to_emails = [to_emails]

        labels = classification.get("labels") or []
        urgency = classification.get("urgency") or "someday"
        categories = classification.get("outlook_categories") or []

        # Check sender patterns
        for pattern in conditions.sender_patterns:
            if self._pattern_matches(pattern, sender):
                matches.append(True)
                reasons.append(f"Sender matches '{pattern}'")

        # Check recipient patterns (for sent emails)
        if event_type == "email_sent":
            for pattern in conditions.recipient_patterns:
                for recipient in to_emails:
                    if self._pattern_matches(pattern, str(recipient)):
                        matches.append(True)
                        reasons.append(f"Recipient matches '{pattern}'")

        # Check subject keywords
        for kw in conditions.subject_keywords:
            if kw.lower() in subject:
                matches.append(True)
                reasons.append(f"Subject contains '{kw}'")

        # Check body keywords
        for kw in conditions.body_keywords:
            if kw.lower() in body:
                matches.append(True)
                reasons.append(f"Body contains '{kw}'")

        # Check labels
        for label in conditions.labels:
            if label.lower() in [str(lbl).lower() for lbl in labels]:
                matches.append(True)
                reasons.append(f"Has label '{label}'")

        # Check urgency
        for level in conditions.urgency_levels:
            if urgency == level:
                matches.append(True)
                reasons.append(f"Urgency is '{level}'")

        # Check categories
        for cat in conditions.categories:
            if cat in categories:
                matches.append(True)
                reasons.append(f"Has category '{cat}'")

        if not matches:
            return False, "No conditions matched"

        if conditions.match_mode == "all":
            # Count expected matches
            expected = (
                len(conditions.sender_patterns)
                + len(conditions.recipient_patterns)
                + len(conditions.subject_keywords)
                + len(conditions.body_keywords)
                + len(conditions.labels)
                + len(conditions.urgency_levels)
                + len(conditions.categories)
            )
            if len(matches) >= expected:
                return True, "; ".join(reasons)
            return False, f"Only {len(matches)}/{expected} conditions matched"
        else:
            # Any condition matching is sufficient
            return True, "; ".join(reasons)

    def _fast_match_wm(
        self,
        conditions: ParsedConditions,
        wm_item: dict[str, Any],
        wm_type: str,
    ) -> tuple[bool, str]:
        """Fast pre-filter for working memory events."""
        matches: list[bool] = []
        reasons: list[str] = []

        # Check if WM type matches
        if conditions.wm_types:
            type_map = {
                "wm_thread": "thread",
                "wm_commitment": "commitment",
                "wm_decision": "decision",
            }
            expected_type = type_map.get(wm_type, wm_type)
            if expected_type not in [t.lower() for t in conditions.wm_types]:
                return False, f"WM type {wm_type} not in {conditions.wm_types}"
            matches.append(True)
            reasons.append(f"WM type is {wm_type}")

        # Check overdue status
        if conditions.overdue_only:
            is_overdue = False
            if wm_type == "wm_commitment":
                due_by = wm_item.get("due_by")
                if due_by:
                    try:
                        due_dt = datetime.fromisoformat(str(due_by).replace("Z", "+00:00"))
                        is_overdue = due_dt < datetime.now(timezone.utc)
                    except Exception:
                        pass
            elif wm_type == "wm_thread":
                # Thread is "overdue" if awaiting reply for too long
                is_overdue = wm_item.get("needs_reply", False)
            elif wm_type == "wm_decision":
                is_overdue = not wm_item.get("is_resolved", False)

            if is_overdue:
                matches.append(True)
                reasons.append("Item is overdue")
            else:
                return False, "Item is not overdue"

        # Check urgency for threads/decisions
        if conditions.urgency_levels:
            item_urgency = wm_item.get("urgency", "someday")
            if item_urgency in conditions.urgency_levels:
                matches.append(True)
                reasons.append(f"Urgency is {item_urgency}")

        if not matches:
            # If no specific conditions, match all items of this type
            return True, f"Matches {wm_type} event"

        return True, "; ".join(reasons)

    async def evaluate_email_rules(
        self,
        email: dict[str, Any],
        classification: dict[str, Any],
        event_type: str = "email_received",
    ) -> list[dict[str, Any]]:
        """Evaluate all enabled rules against an email. Returns list of triggered rules."""
        conn = get_connection()
        try:
            rows = conn.execute(
                "SELECT * FROM alert_rules WHERE enabled = 1"
            ).fetchall()

            triggered = []
            now = datetime.now(timezone.utc)
            email_id = email.get("id")

            for row in rows:
                rule = dict(row)
                rule_id = rule["id"]

                # Check if rule applies to this event type
                try:
                    rule_event_types = json.loads(rule.get("event_types") or '["email_received"]')
                except Exception:
                    rule_event_types = ["email_received"]

                if event_type not in rule_event_types:
                    continue

                # Check if already triggered for this event
                existing = conn.execute(
                    "SELECT 1 FROM alert_triggers WHERE rule_id = ? AND event_type = ? AND event_id = ?",
                    (rule_id, event_type, email_id),
                ).fetchone()
                if existing:
                    continue

                # Check cooldown
                last_triggered = rule.get("last_triggered_at")
                if last_triggered:
                    try:
                        last_dt = datetime.fromisoformat(str(last_triggered).replace("Z", "+00:00"))
                        cooldown = timedelta(minutes=rule.get("cooldown_minutes") or 30)
                        if now - last_dt < cooldown:
                            continue
                    except Exception:
                        pass

                # Parse conditions
                try:
                    conditions = ParsedConditions.model_validate_json(
                        rule.get("parsed_conditions_json") or "{}"
                    )
                except Exception:
                    conditions = ParsedConditions()

                # Fast match
                matches, reason = self._fast_match_email(conditions, email, classification, event_type)

                # Semantic match if needed
                if matches and conditions.requires_semantic_match:
                    if self._matcher_agent is None:
                        self._matcher_agent = _build_semantic_matcher_agent()

                    context = f"""
Rule: {rule['natural_language_rule']}

Email:
From: {email.get('sender')}
Subject: {email.get('subject')}
Body: {str(email.get('body_preview') or '')[:500]}
"""
                    try:
                        result = await self._matcher_agent.run(context)
                        if not result.output.matches:
                            continue
                        reason = result.output.match_reason
                    except Exception as e:
                        logger.warning(f"Semantic matching failed: {e}")
                        continue

                if matches:
                    triggered.append({
                        "rule": rule,
                        "match_reason": reason,
                    })

            return triggered
        finally:
            conn.close()

    async def evaluate_wm_rules(
        self,
        wm_item: dict[str, Any],
        wm_type: str,
    ) -> list[dict[str, Any]]:
        """Evaluate rules against a working memory item."""
        conn = get_connection()
        try:
            rows = conn.execute(
                "SELECT * FROM alert_rules WHERE enabled = 1"
            ).fetchall()

            triggered = []
            now = datetime.now(timezone.utc)
            item_id = wm_item.get("id")

            for row in rows:
                rule = dict(row)
                rule_id = rule["id"]

                # Check if rule applies to this event type
                try:
                    rule_event_types = json.loads(rule.get("event_types") or '["email_received"]')
                except Exception:
                    rule_event_types = ["email_received"]

                if wm_type not in rule_event_types:
                    continue

                # Check if already triggered
                existing = conn.execute(
                    "SELECT 1 FROM alert_triggers WHERE rule_id = ? AND event_type = ? AND event_id = ?",
                    (rule_id, wm_type, item_id),
                ).fetchone()
                if existing:
                    continue

                # Check cooldown
                last_triggered = rule.get("last_triggered_at")
                if last_triggered:
                    try:
                        last_dt = datetime.fromisoformat(str(last_triggered).replace("Z", "+00:00"))
                        cooldown = timedelta(minutes=rule.get("cooldown_minutes") or 30)
                        if now - last_dt < cooldown:
                            continue
                    except Exception:
                        pass

                # Parse conditions
                try:
                    conditions = ParsedConditions.model_validate_json(
                        rule.get("parsed_conditions_json") or "{}"
                    )
                except Exception:
                    conditions = ParsedConditions()

                # Fast match
                matches, reason = self._fast_match_wm(conditions, wm_item, wm_type)

                if matches:
                    triggered.append({
                        "rule": rule,
                        "match_reason": reason,
                    })

            return triggered
        finally:
            conn.close()

    def _fast_match_calendar(
        self,
        conditions: ParsedConditions,
        event: dict[str, Any],
    ) -> tuple[bool, str]:
        """Fast pre-filter for calendar events. Returns (matches, reason)."""
        matches: list[bool] = []
        reasons: list[str] = []

        subject = str(event.get("subject") or "").lower()
        organizer_email = str(event.get("organizer_email") or "").lower()
        organizer_name = str(event.get("organizer_name") or "").lower()
        attendee_count = event.get("attendee_count") or len(event.get("attendees") or [])
        location = str(event.get("location") or "").lower()

        # Check organizer patterns
        for pattern in conditions.organizer_patterns:
            if self._pattern_matches(pattern, organizer_email) or self._pattern_matches(pattern, organizer_name):
                matches.append(True)
                reasons.append(f"Organizer matches '{pattern}'")

        # Check subject keywords
        for kw in conditions.subject_keywords:
            if kw.lower() in subject:
                matches.append(True)
                reasons.append(f"Subject contains '{kw}'")

        # Check body keywords in location (calendar events don't have body typically)
        for kw in conditions.body_keywords:
            if kw.lower() in location:
                matches.append(True)
                reasons.append(f"Location contains '{kw}'")

        # Check minimum attendees
        if conditions.min_attendees is not None:
            if attendee_count >= conditions.min_attendees:
                matches.append(True)
                reasons.append(f"Has {attendee_count} attendees (>= {conditions.min_attendees})")
            elif conditions.match_mode == "all":
                return False, f"Only {attendee_count} attendees (< {conditions.min_attendees})"

        if not matches:
            # If no specific conditions matched but rule is for calendar events,
            # check if rule has any calendar-specific conditions at all
            has_calendar_conditions = (
                conditions.organizer_patterns or
                conditions.min_attendees is not None or
                conditions.subject_keywords or
                conditions.body_keywords
            )
            if not has_calendar_conditions:
                # Rule matches all calendar events (e.g., "alert on all meetings")
                return True, "Matches calendar event"
            return False, "No conditions matched"

        if conditions.match_mode == "all":
            # Count expected matches
            expected = (
                len(conditions.organizer_patterns)
                + len(conditions.subject_keywords)
                + len(conditions.body_keywords)
                + (1 if conditions.min_attendees is not None else 0)
            )
            if len(matches) >= expected:
                return True, "; ".join(reasons)
            return False, f"Only {len(matches)}/{expected} conditions matched"
        else:
            # Any condition matching is sufficient
            return True, "; ".join(reasons)

    async def evaluate_calendar_rules(
        self,
        event: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Evaluate all enabled rules against a calendar event. Returns list of triggered rules."""
        conn = get_connection()
        try:
            rows = conn.execute(
                "SELECT * FROM alert_rules WHERE enabled = 1"
            ).fetchall()

            triggered = []
            now = datetime.now(timezone.utc)
            event_id = event.get("id")

            for row in rows:
                rule = dict(row)
                rule_id = rule["id"]

                # Check if rule applies to calendar events
                try:
                    rule_event_types = json.loads(rule.get("event_types") or '["email_received"]')
                except Exception:
                    rule_event_types = ["email_received"]

                if "calendar_event" not in rule_event_types:
                    continue

                # Check if already triggered for this event
                existing = conn.execute(
                    "SELECT 1 FROM alert_triggers WHERE rule_id = ? AND event_type = ? AND event_id = ?",
                    (rule_id, "calendar_event", event_id),
                ).fetchone()
                if existing:
                    continue

                # Check cooldown
                last_triggered = rule.get("last_triggered_at")
                if last_triggered:
                    try:
                        last_dt = datetime.fromisoformat(str(last_triggered).replace("Z", "+00:00"))
                        cooldown = timedelta(minutes=rule.get("cooldown_minutes") or 30)
                        if now - last_dt < cooldown:
                            continue
                    except Exception:
                        pass

                # Parse conditions
                try:
                    conditions = ParsedConditions.model_validate_json(
                        rule.get("parsed_conditions_json") or "{}"
                    )
                except Exception:
                    conditions = ParsedConditions()

                # Fast match
                matches, reason = self._fast_match_calendar(conditions, event)

                # Semantic match if needed
                if matches and conditions.requires_semantic_match:
                    if self._matcher_agent is None:
                        self._matcher_agent = _build_semantic_matcher_agent()
                    assert self._matcher_agent is not None

                    attendees_str = ", ".join(
                        f"{a.get('name', '')} <{a.get('email', '')}>"
                        for a in (event.get("attendees") or [])[:5]
                    )
                    context = f"""
Rule: {rule['natural_language_rule']}

Calendar Event:
Subject: {event.get('subject')}
Organizer: {event.get('organizer_name')} <{event.get('organizer_email')}>
Start: {event.get('start_at')}
Location: {event.get('location')}
Attendees ({event.get('attendee_count', 0)}): {attendees_str}
"""
                    try:
                        result = await self._matcher_agent.run(context)
                        if not result.output.matches:
                            continue
                        reason = result.output.match_reason
                    except Exception as e:
                        logger.warning(f"Semantic matching failed for calendar: {e}")
                        continue

                if matches:
                    triggered.append({
                        "rule": rule,
                        "match_reason": reason,
                    })

            return triggered
        finally:
            conn.close()

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

        channel = rule.get("channel") or "teams"
        routing: dict[str, Any] = {"channel": channel}
        if rule.get("channel_target"):
            routing["target"] = rule["channel_target"]

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
    channel: str = "teams",
    channel_target: str | None = None,
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
            (id, natural_language_rule, parsed_conditions_json, event_types,
             channel, channel_target, cooldown_minutes, created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rule_id,
                natural_language_rule,
                conditions.model_dump_json(),
                json.dumps(conditions.event_types),
                channel,
                channel_target,
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
            "parsed_conditions": conditions.model_dump(),
            "event_types": conditions.event_types,
            "channel": channel,
            "channel_target": channel_target,
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
        allowed_fields = {"enabled", "channel", "channel_target", "cooldown_minutes"}

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
