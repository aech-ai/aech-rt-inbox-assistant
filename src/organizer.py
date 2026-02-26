import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional
import subprocess
from pydantic import BaseModel, Field
from pydantic_ai import Agent

DEFAULT_CONCURRENCY = 5

from .agent_profile import append_agent_profile
from .database import get_connection
from .poller import GraphPoller
from .preferences import read_preferences
from .triggers import make_dedupe_key, write_trigger
from .categories_config import (
    get_category_names,
    get_flag_settings,
    format_categories_for_prompt,
)
from .model_utils import parse_model_string, get_model_settings
from .working_memory.updater import WorkingMemoryUpdater

logger = logging.getLogger(__name__)


class AvailabilityRequestInfo(BaseModel):
    time_window: Optional[str] = Field(
        default=None,
        description="Requested time window (natural language or structured).",
    )
    duration_minutes: Optional[int] = Field(
        default=None,
        description="Requested meeting duration in minutes, if specified.",
    )
    timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone (e.g. America/Los_Angeles).",
    )
    constraints: Optional[str] = Field(
        default=None,
        description="Constraints like 'no fridays' or 'avoid mornings'.",
    )
    proposed_slots: list[str] = Field(
        default_factory=list,
        description="Optional ISO-8601 intervals if explicit times are suggested.",
    )


class EmailClassification(BaseModel):
    """AI classification result for an email."""

    outlook_categories: list[str] = Field(
        default_factory=list,
        description="Outlook categories to apply (e.g., 'Action Required', 'Work'). Can be empty for low-value content."
    )
    urgency: str = Field(
        default="someday",
        description="Urgency level: 'immediate', 'today', 'this_week', 'someday'. Determines flag due date."
    )
    reason: str = Field(description="Brief explanation of the classification")

    labels: list[str] = Field(
        default_factory=list,
        description="Short labels like: pitch, marketing, billing, vip, newsletter.",
    )
    confidence: float = Field(
        default=0.7,
        ge=0,
        le=1,
        description="Overall confidence score (0-1).",
    )

    requires_reply: bool = Field(
        default=False,
        description="True if the user should reply (direct question, decision, approval, etc).",
    )
    reply_reason: Optional[str] = Field(
        default=None,
        description="Short reason why a reply is needed (if requires_reply=true).",
    )

    availability_requested: bool = Field(
        default=False,
        description="True if the email is requesting meeting availability/scheduling.",
    )
    availability: Optional[AvailabilityRequestInfo] = Field(
        default=None,
        description="Structured scheduling request details (if availability_requested=true).",
    )


class NotificationDecision(BaseModel):
    """LLM policy decision for interrupting the user and preparing drafts."""

    importance: Literal["critical", "important", "routine", "low"] = Field(
        default="routine",
        description="Overall importance of this email to the principal.",
    )
    notify_now: bool = Field(
        default=False,
        description="Whether to send an immediate notification now.",
    )
    notification_channel: Literal["teams", "none"] = Field(
        default="none",
        description="Delivery channel for immediate notifications.",
    )
    notification_reason: str = Field(
        default="",
        description="Why this should (or should not) interrupt the user now.",
    )
    create_reply_draft: bool = Field(
        default=False,
        description="Whether to prepare a suggested reply draft for the user.",
    )
    reply_draft: Optional[str] = Field(
        default=None,
        description="Suggested reply draft text. Never auto-send.",
    )
    include_in_periodic_summary: bool = Field(
        default=True,
        description="Whether this item belongs in periodic inbox summary.",
    )
    summary_note: str = Field(
        default="",
        description="Short summary note for digest context.",
    )


class PeriodicInboxSummary(BaseModel):
    """Typed summary for periodic inbox digest trigger payloads."""

    summary: str = Field(description="High-level summary of recent inbox activity.")
    priority_items: list[str] = Field(default_factory=list)
    draft_ready_items: list[str] = Field(default_factory=list)
    newsletter_insights: list[str] = Field(default_factory=list)
    recommended_actions: list[str] = Field(default_factory=list)


def _build_agent(prefs: dict) -> Agent[None, EmailClassification]:
    """Create the AI agent for email classification."""
    # Use CLASSIFICATION_MODEL if set, otherwise fall back to MODEL_NAME
    # Default to nano model - classification is a simple pattern matching task
    model_string = os.getenv(
        "CLASSIFICATION_MODEL",
        os.getenv("MODEL_NAME", "openai-responses:gpt-5-mini")
    )
    model_name, _ = parse_model_string(model_string)
    model_settings = get_model_settings(model_string)
    cleanup_strategy = os.getenv("CLEANUP_STRATEGY", "medium").lower()

    category_names = get_category_names(prefs)
    categories_description = format_categories_for_prompt(prefs)

    system_prompt = f"""
You are an expert executive assistant. Your goal is to deeply understand the INTENT of each email and categorize it using Outlook categories.

### 1. Intent Analysis (CRITICAL)
- **Do not rely on keywords alone.** Look at the Subject, Sender, and Body together to determine the *primary purpose* of the email.
- **Analyze the underlying message**: Is this actionable? Informational? A notification?
- **Sender matters**: Who is sending this? Is it a service, a colleague, or a friend?

### 2. Available Categories
Choose from these Outlook categories (you can apply multiple):
{categories_description}

### 3. Category Selection Logic
Think step-by-step:

1. **Does it require MY action?**
   - Direct question to me → "Action Required" + urgency based on deadline
   - Approval/decision needed → "Action Required" + urgency "today" or "this_week"
   - Task assigned to me → "Action Required"

2. **Am I waiting on someone else?**
   - I sent and am awaiting reply → "Follow Up"
   - Someone promised to get back to me → "Follow Up"

3. **Is it work-related but informational?**
   - Status updates, meeting notes → "Work"
   - Internal announcements → "Work"

4. **Is it low-value content?**
   - Automated notifications, newsletters, digests, marketing → Leave UNCATEGORIZED (no category)
   - Only categorize emails that matter to the user's job, preferences, or require attention

5. **Is it personal?**
   - Non-work correspondence → "Personal"

### 4. Urgency Levels (determines flag due date)
- **immediate**: Needs attention NOW (rare - only for critical time-sensitive items)
- **today**: Should handle today (deadlines today, urgent requests)
- **this_week**: Can wait a few days but shouldn't be forgotten
- **someday**: Low priority, no action needed

### 5. Cleanup Strategy (Current Level: {cleanup_strategy.upper()})
- **Low**: Only mark spam/phishing for deletion
- **Medium**: Also delete old/irrelevant newsletters (> 3 months)
- **Aggressive**: Also delete cold outreach, old promos

### 6. Output Fields
- **outlook_categories**: List of category names to apply (from: {", ".join(category_names)}). Can be empty for low-value content.
- **urgency**: One of: immediate, today, this_week, someday
- **reason**: Brief explanation of your decision

### 7. Executive Assistant Signals
- **requires_reply**: true if user should respond to this email
- **reply_reason**: short reason (direct_question, asks_for_decision, approval_needed)
- **availability_requested**: true if scheduling a meeting
- **labels**: additional labels like vip, billing, marketing, newsletter, pitch, security
"""
    instructions = append_agent_profile(
        system_prompt,
        capability_name="classification",
    )

    return Agent(
        model_name,
        output_type=EmailClassification,
        instructions=instructions,
        model_settings=model_settings,
    )


def _build_notification_policy_agent() -> Agent[None, NotificationDecision]:
    model_string = os.getenv(
        "NOTIFICATION_POLICY_MODEL",
        os.getenv("MODEL_NAME", "openai-responses:gpt-5-mini"),
    )
    model_name, _ = parse_model_string(model_string)
    model_settings = get_model_settings(model_string)

    instructions = """
You are the inbox communication policy for an executive assistant.
Input is JSON describing an email, its triage decision, working-memory analysis,
and user communication preferences.

Your job is to decide if the principal should be interrupted immediately.

Rules:
- Prefer NO immediate interruption for routine newsletters, automated notices, and low-value updates.
- If immediate interruption is warranted, use notification_channel='teams'.
- Prefer immediate notifications when a ready-to-paste reply draft would materially save user time.
- Never send messages automatically. Drafts are suggestions only.
- create_reply_draft=true only when a concise draft is genuinely useful.
- If create_reply_draft=true, reply_draft must be non-empty and professional.
- include_in_periodic_summary should usually be true unless the item is irrelevant noise.

Return NotificationDecision only.
"""

    enriched_instructions = append_agent_profile(
        instructions,
        capability_name="notification_policy",
    )

    return Agent(
        model_name,
        output_type=NotificationDecision,
        instructions=enriched_instructions,
        model_settings=model_settings,
    )


def _build_periodic_summary_agent() -> Agent[None, PeriodicInboxSummary]:
    model_string = os.getenv(
        "INBOX_SUMMARY_MODEL",
        os.getenv("MODEL_NAME", "openai-responses:gpt-5-mini"),
    )
    model_name, _ = parse_model_string(model_string)
    model_settings = get_model_settings(model_string)

    instructions = """
You generate concise periodic inbox activity summaries for the principal.
Input is JSON with recent emails and retained interest signals.

Return PeriodicInboxSummary:
- summary: 2-4 sentences, practical and concrete.
- priority_items: short bullets for high-signal items.
- draft_ready_items: subjects where a prepared draft is likely useful.
- newsletter_insights: distilled insights from newsletters/automated feeds.
- recommended_actions: concrete next actions.

Do not invent facts not present in input.
"""

    enriched_instructions = append_agent_profile(
        instructions,
        capability_name="periodic_summary",
    )

    return Agent(
        model_name,
        output_type=PeriodicInboxSummary,
        instructions=enriched_instructions,
        model_settings=model_settings,
    )


class Organizer:
    def __init__(self, poller: GraphPoller, backfill: bool = False):
        self.poller = poller
        self.user_email: str = poller.user_email or ""
        self.agent: Optional[Agent[None, EmailClassification]] = None
        self._notification_agent: Optional[Agent[None, NotificationDecision]] = None
        self._periodic_summary_agent: Optional[Agent[None, PeriodicInboxSummary]] = None
        self.backfill = backfill

    def _get_agent(self, prefs: dict) -> Agent[None, EmailClassification]:
        """Get or build the classification agent."""
        if self.agent is None:
            logger.info("Building classification agent")
            self.agent = _build_agent(prefs)
        return self.agent

    def _get_notification_agent(self) -> Agent[None, NotificationDecision]:
        if self._notification_agent is None:
            logger.info("Building notification policy agent")
            self._notification_agent = _build_notification_policy_agent()
        return self._notification_agent

    def _get_periodic_summary_agent(self) -> Agent[None, PeriodicInboxSummary]:
        if self._periodic_summary_agent is None:
            logger.info("Building periodic summary agent")
            self._periodic_summary_agent = _build_periodic_summary_agent()
        return self._periodic_summary_agent

    @staticmethod
    def _inbox_assistant_prefs(prefs: dict[str, Any]) -> dict[str, Any]:
        raw = prefs.get("inbox_assistant")
        return raw if isinstance(raw, dict) else {}

    async def organize_emails(self, concurrency: int = DEFAULT_CONCURRENCY):
        """Iterate over unprocessed emails and organize them in parallel."""
        prefs = read_preferences()

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM emails WHERE processed_at IS NULL")
        emails = cursor.fetchall()
        conn.close()

        if not emails:
            logger.info("No unprocessed emails found")
            await self._emit_periodic_summary_trigger(prefs)
            self._emit_weekly_digest_trigger(prefs)
            return

        logger.info(f"Processing {len(emails)} emails with concurrency={concurrency}")

        semaphore = asyncio.Semaphore(concurrency)

        async def process_with_semaphore(email):
            async with semaphore:
                await self._process_email(email, prefs)

        tasks = [process_with_semaphore(email) for email in emails]
        await asyncio.gather(*tasks, return_exceptions=True)

        logger.info(f"Finished processing {len(emails)} emails")

        # Working Memory Engine handles staleness and follow-up nudges.
        await self._emit_periodic_summary_trigger(prefs)
        self._emit_weekly_digest_trigger(prefs)

    async def _process_email(self, email, prefs: dict):
        # Convert sqlite3.Row to dict if needed (Row doesn't support .get())
        if not isinstance(email, dict):
            email = dict(email)
        conn = get_connection()
        logger.info(f"Processing email {email['id']}: {email['subject']}")
        wm_suggested_action = "keep"
        notification_decision = NotificationDecision()

        # Construct context for AI
        vip_senders = {str(s).strip().lower() for s in (prefs.get("vip_senders") or []) if str(s).strip()}
        is_vip_sender = str(email["sender"] or "").strip().lower() in vip_senders
        email_content = (
            f"VIP_SENDER: {str(is_vip_sender).lower()}\n"
            f"Subject: {email['subject']}\nSender: {email['sender']}\nPreview: {email['body_preview']}"
        )

        try:
            result = await self._get_agent(prefs).run(email_content)
            decision = result.output

            # Log LLM usage for cost tracking
            try:
                usage = result.usage()
                model = os.getenv("CLASSIFICATION_MODEL", os.getenv("MODEL_NAME", "gpt-5-mini"))
                logger.info(
                    f"LLM_USAGE task=classification model={model} "
                    f"in={usage.request_tokens} out={usage.response_tokens}"
                )
            except Exception:
                pass  # Usage tracking is best-effort

            logger.info(f"AI Decision for {email['id']}: {decision}")

            # Enrich labels deterministically
            if is_vip_sender:
                decision.labels = list({*(decision.labels or []), "vip"})

            # Apply categories to Outlook
            self._apply_categories_and_flags(email['id'], decision)

            # Log to Triage Log
            conn.execute("""
            INSERT INTO triage_log (email_id, outlook_categories, urgency, reason)
            VALUES (?, ?, ?, ?)
            """, (
                email['id'],
                json.dumps(decision.outlook_categories),
                decision.urgency,
                decision.reason
            ))

            # Mark as processed
            row_exists = conn.execute("SELECT 1 FROM emails WHERE id = ?", (email['id'],)).fetchone()
            if not row_exists:
                logger.error(f"Email {email['id']} missing from DB; skipping update.")
            else:
                conn.execute(
                    "UPDATE emails SET processed_at = CURRENT_TIMESTAMP, outlook_categories = ?, urgency = ? WHERE id = ?",
                    (json.dumps(decision.outlook_categories), decision.urgency, email['id'])
                )
                logger.debug(f"Marked email {email['id']} processed with categories {decision.outlook_categories}")

            # Persist labels
            try:
                conn.execute("DELETE FROM labels WHERE message_id = ?", (email["id"],))
                for label in decision.labels or []:
                    label_str = str(label).strip()
                    if not label_str:
                        continue
                    conn.execute(
                        "INSERT OR REPLACE INTO labels (message_id, label, confidence) VALUES (?, ?, ?)",
                        (email["id"], label_str, float(decision.confidence or 0.0)),
                    )
            except Exception as e:
                logger.warning(f"Failed to persist labels for {email['id']}: {e}")

            conn.commit()

            # Update Working Memory (handles reply tracking via wm_threads)
            if self.user_email:
                try:
                    wm_updater = WorkingMemoryUpdater(self.user_email)
                    wm_analysis = await wm_updater.process_email(
                        dict(email),
                        {
                            "outlook_categories": decision.outlook_categories,
                            "urgency": decision.urgency,
                            "requires_reply": decision.requires_reply,
                            "labels": decision.labels,
                        },
                    )
                    wm_suggested_action = str(getattr(wm_analysis, "suggested_action", "keep") or "keep").lower()
                except Exception as wm_err:
                    logger.warning(f"Working memory update failed for {email['id']}: {wm_err}")

            if not self.backfill:
                notification_decision = await self._plan_notification(
                    email=email,
                    decision=decision,
                    prefs=prefs,
                    wm_suggested_action=wm_suggested_action,
                )

            # LLM-first cleanup action (after knowledge extraction has been persisted)
            if not self.backfill:
                self._apply_mailbox_cleanup_action(email["id"], wm_suggested_action)

            # Skip triggers during backfill/onboarding
            if not self.backfill:
                await self._emit_triggers_for_email(
                    email=email,
                    decision=decision,
                    prefs=prefs,
                    notification_decision=notification_decision,
                )

                # Evaluate user-defined alert rules
                await self._evaluate_alert_rules(email, decision)

        except Exception as e:
            conn.rollback()
            logger.error(f"Error processing email {email['id']}: {e}", exc_info=True)
        finally:
            conn.close()

    async def _plan_notification(
        self,
        *,
        email: dict[str, Any],
        decision: EmailClassification,
        prefs: dict[str, Any],
        wm_suggested_action: str,
    ) -> NotificationDecision:
        """Run LLM policy to decide whether to interrupt now and whether to prepare a draft."""
        inbox_prefs = self._inbox_assistant_prefs(prefs)
        context = {
            "email": {
                "id": email.get("id"),
                "subject": email.get("subject"),
                "sender": email.get("sender"),
                "received_at": email.get("received_at"),
                "body_preview": email.get("body_preview"),
            },
            "classification": {
                "urgency": decision.urgency,
                "reason": decision.reason,
                "labels": decision.labels,
                "requires_reply": decision.requires_reply,
                "reply_reason": decision.reply_reason,
                "availability_requested": decision.availability_requested,
                "availability": decision.availability.model_dump(mode="json") if decision.availability else None,
            },
            "working_memory": {"suggested_action": wm_suggested_action},
            "preferences": {
                "teams_only": bool(inbox_prefs.get("teams_only", True)),
                "alert_only_when_draft_ready": bool(
                    inbox_prefs.get("alert_only_when_draft_ready", True)
                ),
            },
            "principal_user_email": self.user_email,
        }

        result = await self._get_notification_agent().run(json.dumps(context, default=str))
        policy = result.output

        try:
            usage = result.usage()
            model = os.getenv("NOTIFICATION_POLICY_MODEL", os.getenv("MODEL_NAME", "gpt-5-mini"))
            logger.info(
                f"LLM_USAGE task=notification_policy model={model} "
                f"in={usage.request_tokens} out={usage.response_tokens}"
            )
        except Exception:
            pass

        if policy.notify_now and policy.notification_channel == "none":
            raise ValueError("Notification policy invalid: notify_now=true requires a delivery channel")

        if policy.create_reply_draft and not str(policy.reply_draft or "").strip():
            raise ValueError("Notification policy invalid: create_reply_draft=true but reply_draft is empty")

        if bool(inbox_prefs.get("teams_only", True)) and policy.notification_channel not in {"teams", "none"}:
            raise ValueError(f"Notification policy invalid channel: {policy.notification_channel}")

        if (
            bool(inbox_prefs.get("alert_only_when_draft_ready", True))
            and policy.notify_now
            and not policy.create_reply_draft
            and not decision.availability_requested
            and decision.urgency != "immediate"
        ):
            policy.notify_now = False
            policy.notification_channel = "none"
            if not policy.notification_reason:
                policy.notification_reason = "Deferred to summary because no useful draft was prepared."

        if not policy.notify_now:
            policy.notification_channel = "none"

        return policy

    async def _emit_triggers_for_email(
        self,
        *,
        email: dict[str, Any],
        decision: EmailClassification,
        prefs: dict[str, Any],
        notification_decision: NotificationDecision,
    ) -> None:
        """Emit policy-gated triggers (draft-first, Teams-only)."""
        inbox_prefs = self._inbox_assistant_prefs(prefs)
        alert_only_when_draft_ready = bool(inbox_prefs.get("alert_only_when_draft_ready", True))
        notify_now = bool(notification_decision.notify_now)
        draft_text = str(notification_decision.reply_draft or "").strip()

        # Availability requests can produce a high-value draft from calendar-aware reasoning.
        if decision.availability_requested:
            if not notify_now:
                return
            availability = decision.availability
            default_timezone = str(prefs.get("timezone") or os.getenv("DEFAULT_TIMEZONE", "UTC"))
            availability_payload = {
                "time_window": getattr(availability, "time_window", None) if availability else None,
                "duration_minutes": (getattr(availability, "duration_minutes", None) if availability else None) or 30,
                "timezone": (getattr(availability, "timezone", None) if availability else None) or default_timezone,
                "constraints": getattr(availability, "constraints", None) if availability else None,
                "proposed_slots": (getattr(availability, "proposed_slots", None) if availability else None) or [],
                "requester": email["sender"],
            }

            from .calendar_intelligence import CalendarIntelligence

            intelligence = CalendarIntelligence()
            enhanced_payload = await intelligence.enhance_availability_trigger(availability_payload)
            enhanced_payload["message_id"] = email.get("id")
            enhanced_payload["subject"] = email.get("subject")
            enhanced_payload["requester"] = email.get("sender")
            enhanced_payload["importance"] = notification_decision.importance
            enhanced_payload["notification_reason"] = notification_decision.notification_reason or decision.reason
            enhanced_payload["summary_note"] = notification_decision.summary_note

            enhanced_draft = str(enhanced_payload.get("suggested_reply") or "").strip()
            if not enhanced_draft and draft_text:
                enhanced_payload["suggested_reply"] = draft_text
                enhanced_draft = draft_text

            if notify_now and (not alert_only_when_draft_ready or enhanced_draft):
                write_trigger(
                    self.user_email,
                    "availability_requested_enhanced",
                    enhanced_payload,
                    dedupe_key=make_dedupe_key("availability_requested_enhanced", self.user_email, email["id"]),
                    routing={"channel": "teams"},
                )
            elif notify_now and alert_only_when_draft_ready and not enhanced_draft:
                logger.info(
                    "Suppressed availability notification for %s: draft required but unavailable",
                    email["id"],
                )
            return

        if not notify_now:
            return

        if notification_decision.notification_channel != "teams":
            raise ValueError(
                f"Notification policy requested unsupported immediate channel: "
                f"{notification_decision.notification_channel}"
            )

        has_draft = bool(draft_text)
        if alert_only_when_draft_ready and not has_draft and decision.urgency != "immediate":
            logger.info("Suppressed immediate notification for %s: no draft prepared", email["id"])
            return

        if decision.urgency == "immediate" and not has_draft:
            write_trigger(
                self.user_email,
                "urgent_email",
                {
                    "subject": email["subject"],
                    "sender": email["sender"],
                    "message_id": email["id"],
                    "received_at": email["received_at"],
                    "reason": notification_decision.notification_reason or decision.reason,
                    "importance": notification_decision.importance,
                    "summary_note": notification_decision.summary_note,
                },
                dedupe_key=make_dedupe_key("urgent_email", self.user_email, email["id"]),
                routing={"channel": "teams"},
            )
            return

        if has_draft:
            write_trigger(
                self.user_email,
                "reply_needed",
                {
                    "message_id": email["id"],
                    "subject": email["subject"],
                    "sender": email["sender"],
                    "received_at": email["received_at"],
                    "reason": notification_decision.notification_reason or decision.reply_reason or decision.reason,
                    "suggested_reply": draft_text,
                    "draft_ready": True,
                    "do_not_send_automatically": True,
                    "importance": notification_decision.importance,
                    "summary_note": notification_decision.summary_note,
                },
                dedupe_key=make_dedupe_key("reply_needed", self.user_email, email["id"]),
                routing={"channel": "teams"},
            )

    async def _evaluate_alert_rules(self, email: dict, decision: EmailClassification) -> None:
        """Evaluate user-defined alert rules against the email."""
        from .alerts import AlertRulesEngine

        alert_engine = AlertRulesEngine(self.user_email)
        classification = {
            "labels": decision.labels,
            "urgency": decision.urgency,
            "outlook_categories": decision.outlook_categories,
        }

        triggered = await alert_engine.evaluate_email_rules(
            email, classification, event_type="email_received"
        )

        for t in triggered:
            alert_engine.emit_alert_trigger(
                t["rule"],
                "email_received",
                email["id"],
                email,
                t["match_reason"],
            )

    def _apply_categories_and_flags(self, message_id: str, decision: EmailClassification):
        """Apply Outlook categories and flags via msgraph CLI."""
        categories = decision.outlook_categories or []
        urgency = decision.urgency or "someday"

        if not categories and urgency == "someday":
            logger.debug(f"No categories or flags to apply for {message_id}")
            return

        delegated_user = (self.poller.user_email or "").strip()
        if not delegated_user:
            raise RuntimeError("DELEGATED_USER is required for mailbox writes (categories/flags)")

        cmd = ["aech-cli-msgraph", "update-message", message_id]

        if categories:
            cmd.extend(["--categories", ",".join(categories)])

        flag_settings = get_flag_settings(urgency)
        if flag_settings:
            if "flag_status" in flag_settings:
                cmd.extend(["--flag", flag_settings["flag_status"]])
            if "flag_due" in flag_settings:
                cmd.extend(["--flag-due", flag_settings["flag_due"]])

        cmd.extend(["--user", delegated_user])

        try:
            logger.info(f"Applying categories/flags: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                logger.warning(f"Failed to apply categories for {message_id}: {result.stderr or result.stdout}")
            else:
                logger.debug(f"Applied categories for {message_id}")
        except FileNotFoundError:
            logger.warning(f"aech-cli-msgraph not found - categories not applied for {message_id}")
        except subprocess.TimeoutExpired:
            logger.warning(f"Timeout applying categories for {message_id}")
        except Exception as e:
            logger.warning(f"Error applying categories for {message_id}: {e}")

    @staticmethod
    def _upsert_user_preference(conn, key: str, value: str) -> None:
        conn.execute(
            """
            INSERT INTO user_preferences (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, value),
        )

    async def _emit_periodic_summary_trigger(self, prefs: dict[str, Any]) -> None:
        """Emit periodic inbox activity summary trigger using LLM synthesis."""
        inbox_prefs = self._inbox_assistant_prefs(prefs)
        enabled = bool(inbox_prefs.get("periodic_summary_enabled", True))
        if not enabled:
            return

        raw_interval = inbox_prefs.get(
            "periodic_summary_interval_minutes",
            os.getenv("INBOX_SUMMARY_INTERVAL_MINUTES", "240"),
        )
        try:
            interval_minutes = int(raw_interval)
        except Exception as exc:
            raise ValueError(
                f"Invalid periodic_summary_interval_minutes: {raw_interval}"
            ) from exc
        if interval_minutes <= 0:
            raise ValueError(f"periodic_summary_interval_minutes must be > 0, got {interval_minutes}")

        now_utc = datetime.now(timezone.utc)
        conn = get_connection()
        try:
            last_row = conn.execute(
                "SELECT value FROM user_preferences WHERE key = ?",
                ("_internal.last_periodic_inbox_summary_at",),
            ).fetchone()

            if last_row and str(last_row[0]).strip():
                try:
                    last_summary = datetime.fromisoformat(str(last_row[0]).replace("Z", "+00:00"))
                    if last_summary.tzinfo is None:
                        last_summary = last_summary.replace(tzinfo=timezone.utc)
                except Exception:
                    last_summary = now_utc - timedelta(minutes=interval_minutes + 1)
            else:
                last_summary = now_utc - timedelta(minutes=interval_minutes + 1)

            if now_utc - last_summary < timedelta(minutes=interval_minutes):
                return

            since_iso = last_summary.isoformat()
            email_rows = conn.execute(
                """
                SELECT id, subject, sender, received_at, urgency, outlook_categories,
                       wm_needs_reply, suggested_action, thread_summary, body_preview
                FROM emails
                WHERE received_at IS NOT NULL
                  AND datetime(received_at) >= datetime(?)
                ORDER BY received_at DESC
                LIMIT 120
                """,
                (since_iso,),
            ).fetchall()
            insight_rows = conn.execute(
                """
                SELECT summary, confidence, last_seen_at
                FROM derived_learnings
                WHERE learning_type = 'interest_signal'
                  AND datetime(last_seen_at) >= datetime(?)
                ORDER BY last_seen_at DESC
                LIMIT 30
                """,
                (since_iso,),
            ).fetchall()

            if not email_rows and not insight_rows:
                self._upsert_user_preference(conn, "_internal.last_periodic_inbox_summary_at", now_utc.isoformat())
                conn.commit()
                return

            summary_context = {
                "window_start": since_iso,
                "window_end": now_utc.isoformat(),
                "email_count": len(email_rows),
                "emails": [
                    {
                        "subject": row["subject"],
                        "sender": row["sender"],
                        "received_at": row["received_at"],
                        "urgency": row["urgency"],
                        "outlook_categories": row["outlook_categories"],
                        "wm_needs_reply": row["wm_needs_reply"],
                        "suggested_action": row["suggested_action"],
                        "thread_summary": row["thread_summary"],
                        "body_preview": row["body_preview"],
                    }
                    for row in email_rows
                ],
                "interest_signals": [
                    {
                        "summary": row["summary"],
                        "confidence": row["confidence"],
                        "last_seen_at": row["last_seen_at"],
                    }
                    for row in insight_rows
                ],
            }
            summary_result = await self._get_periodic_summary_agent().run(json.dumps(summary_context, default=str))
            summary = summary_result.output

            try:
                usage = summary_result.usage()
                model = os.getenv("INBOX_SUMMARY_MODEL", os.getenv("MODEL_NAME", "gpt-5-mini"))
                logger.info(
                    f"LLM_USAGE task=periodic_summary model={model} "
                    f"in={usage.request_tokens} out={usage.response_tokens}"
                )
            except Exception:
                pass

            slot_start = int(now_utc.timestamp() // (interval_minutes * 60))
            write_trigger(
                self.user_email,
                "inbox_activity_summary_ready",
                {
                    "window_start": since_iso,
                    "window_end": now_utc.isoformat(),
                    "email_count": len(email_rows),
                    "summary": summary.summary,
                    "priority_items": summary.priority_items,
                    "draft_ready_items": summary.draft_ready_items,
                    "newsletter_insights": summary.newsletter_insights,
                    "recommended_actions": summary.recommended_actions,
                },
                dedupe_key=make_dedupe_key(
                    "inbox_activity_summary_ready",
                    self.user_email,
                    str(slot_start),
                ),
                routing={"channel": "teams"},
            )

            self._upsert_user_preference(conn, "_internal.last_periodic_inbox_summary_at", now_utc.isoformat())
            conn.commit()
        finally:
            conn.close()

    def _emit_weekly_digest_trigger(self, prefs: dict) -> None:
        enabled = os.getenv("ENABLE_WEEKLY_DIGEST", "").strip().lower() in {"1", "true", "yes"}
        if not enabled and not (("digest_day" in prefs) or ("digest_time_local" in prefs)):
            return

        digest_day = str(prefs.get("digest_day") or os.getenv("DIGEST_DAY", "friday")).strip().lower()
        digest_time = str(prefs.get("digest_time_local") or os.getenv("DIGEST_TIME_LOCAL", "08:30")).strip()
        timezone_name = str(prefs.get("timezone") or os.getenv("DEFAULT_TIMEZONE", "UTC")).strip()

        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(timezone_name)
        except Exception:
            from datetime import timezone
            tz = timezone.utc

        from datetime import datetime, timedelta

        now_local = datetime.now(tz)
        weekday_map = {
            "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
            "friday": 4, "saturday": 5, "sunday": 6,
        }
        if digest_day not in weekday_map:
            return

        try:
            hour_str, minute_str = digest_time.split(":", 1)
            digest_hour = int(hour_str)
            digest_minute = int(minute_str)
        except Exception:
            return

        window_minutes = int(os.getenv("DIGEST_WINDOW_MINUTES", "30"))
        scheduled_today = now_local.replace(hour=digest_hour, minute=digest_minute, second=0, microsecond=0)
        in_window = (
            now_local.weekday() == weekday_map[digest_day]
            and scheduled_today <= now_local <= scheduled_today + timedelta(minutes=window_minutes)
        )
        if not in_window:
            return

        week_start = now_local.date() - timedelta(days=now_local.date().weekday())
        week_end = week_start + timedelta(days=6)
        week_start_iso = week_start.isoformat()

        conn = get_connection()
        try:
            last = conn.execute(
                "SELECT value FROM user_preferences WHERE key = ?",
                ("_internal.last_weekly_digest_week_start",),
            ).fetchone()
            if last and str(last[0]) == week_start_iso:
                return

            rows = conn.execute(
                """
                SELECT id, subject, sender, received_at, outlook_categories, urgency, body_preview
                FROM emails
                WHERE received_at IS NOT NULL
                ORDER BY received_at DESC
                LIMIT 500
                """
            ).fetchall()

            top_items: list[dict] = []
            recommended_actions: list[str] = []

            for row in rows:
                received_raw = row["received_at"]
                try:
                    received_dt = datetime.fromisoformat(str(received_raw).replace("Z", "+00:00"))
                    if received_dt.tzinfo is None:
                        received_dt = received_dt.replace(tzinfo=tz)
                    received_local = received_dt.astimezone(tz)
                except Exception:
                    continue

                if not (week_start <= received_local.date() <= week_end):
                    continue

                categories_str = row["outlook_categories"] or "[]"
                try:
                    categories = json.loads(categories_str)
                except Exception:
                    categories = []

                urgency = row["urgency"] or "someday"
                if urgency in ("immediate", "today") or "Action Required" in categories:
                    recommended_actions.append(row["subject"])

                if len(top_items) < 15:
                    top_items.append({
                        "title": row["subject"],
                        "why_it_matters": f"From {row['sender']} ({', '.join(categories) if categories else 'uncategorized'})",
                        "links": [row["id"]],
                    })

            write_trigger(
                self.user_email,
                "weekly_digest_ready",
                {
                    "week_start": week_start_iso,
                    "week_end": week_end.isoformat(),
                    "top_items": top_items,
                    "recommended_actions": recommended_actions[:20],
                },
                dedupe_key=make_dedupe_key("weekly_digest_ready", self.user_email, week_start_iso),
                routing={"channel": "teams"},
            )

            self._upsert_user_preference(
                conn, "_internal.last_weekly_digest_week_start", week_start_iso
            )
            conn.commit()
        finally:
            conn.close()

    def _apply_mailbox_cleanup_action(self, message_id: str, suggested_action: str) -> None:
        """Execute LLM-suggested mailbox cleanup action."""
        action = (suggested_action or "keep").strip().lower()
        if action == "keep":
            return

        if action not in {"archive", "delete"}:
            raise ValueError(f"Unsupported suggested_action '{suggested_action}' for {message_id}")

        if action == "archive":
            self.poller.archive_email(message_id)
        elif action == "delete":
            self.poller.delete_email(message_id)

        conn = get_connection()
        try:
            conn.execute(
                """
                DELETE FROM emails
                WHERE id = ?
                """,
                (message_id,),
            )
            conn.commit()
        finally:
            conn.close()
