import asyncio
import json
import logging
import os
import subprocess
from typing import Optional
from pydantic import BaseModel, Field
from pydantic_ai import Agent

DEFAULT_CONCURRENCY = 5

from .database import get_connection
from .poller import GraphPoller
from .preferences import read_preferences
from .triggers import make_dedupe_key, write_trigger
from .categories_config import (
    get_category_names,
    get_flag_settings,
    format_categories_for_prompt,
)
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
        description="Outlook categories to apply (e.g., 'Action Required', 'Work', 'FYI'). Must have at least one."
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


def _build_agent(prefs: dict) -> Agent[None, EmailClassification]:
    """Create the AI agent for email classification."""
    model_name = os.getenv("MODEL_NAME", "openai-responses:gpt-5-mini")
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
   - Internal announcements → "Work" or "FYI"

4. **Is it a newsletter/notification/update?**
   - Automated notifications → "FYI"
   - Newsletters, digests → "FYI"
   - Marketing/promotions → "FYI"

5. **Is it personal?**
   - Non-work correspondence → "Personal"

### 4. Urgency Levels (determines flag due date)
- **immediate**: Needs attention NOW (rare - only for critical time-sensitive items)
- **today**: Should handle today (deadlines today, urgent requests)
- **this_week**: Can wait a few days but shouldn't be forgotten
- **someday**: Low priority, FYI, no action needed

### 5. Cleanup Strategy (Current Level: {cleanup_strategy.upper()})
- **Low**: Only mark spam/phishing for deletion
- **Medium**: Also delete old/irrelevant newsletters (> 3 months)
- **Aggressive**: Also delete cold outreach, old promos

### 6. Output Fields
- **outlook_categories**: List of category names to apply (MUST have at least one from: {", ".join(category_names)})
- **urgency**: One of: immediate, today, this_week, someday
- **reason**: Brief explanation of your decision

### 7. Executive Assistant Signals
- **requires_reply**: true if user should respond to this email
- **reply_reason**: short reason (direct_question, asks_for_decision, approval_needed)
- **availability_requested**: true if scheduling a meeting
- **labels**: additional labels like vip, billing, marketing, newsletter, pitch, security
"""
    return Agent(
        model_name,
        output_type=EmailClassification,
        system_prompt=system_prompt,
    )


class Organizer:
    def __init__(self, poller: GraphPoller, backfill: bool = False):
        self.poller = poller
        self.user_email: str = poller.user_email or ""
        self.agent: Optional[Agent[None, EmailClassification]] = None
        self.backfill = backfill

    def _get_agent(self, prefs: dict) -> Agent[None, EmailClassification]:
        """Get or build the classification agent."""
        if self.agent is None:
            logger.info("Building classification agent")
            self.agent = _build_agent(prefs)
        return self.agent

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
            return

        logger.info(f"Processing {len(emails)} emails with concurrency={concurrency}")

        semaphore = asyncio.Semaphore(concurrency)

        async def process_with_semaphore(email):
            async with semaphore:
                await self._process_email(email, prefs)

        tasks = [process_with_semaphore(email) for email in emails]
        await asyncio.gather(*tasks, return_exceptions=True)

        logger.info(f"Finished processing {len(emails)} emails")

        self._emit_followup_triggers(prefs)
        self._emit_weekly_digest_trigger(prefs)

    async def _process_email(self, email, prefs: dict):
        conn = get_connection()
        logger.info(f"Processing email {email['id']}: {email['subject']}")

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

            # Persist reply tracking for follow-ups
            if decision.requires_reply:
                reply_reason = decision.reply_reason or decision.reason
                conn.execute(
                    """
                    INSERT INTO reply_tracking (message_id, requires_reply, reason, last_activity_at)
                    VALUES (?, 1, ?, ?)
                    ON CONFLICT(message_id) DO UPDATE SET
                        requires_reply=1,
                        reason=excluded.reason,
                        last_activity_at=excluded.last_activity_at
                    """,
                    (email["id"], reply_reason, email["received_at"]),
                )
            conn.commit()

            # Update Working Memory
            if self.user_email:
                try:
                    wm_updater = WorkingMemoryUpdater(self.user_email)
                    await wm_updater.process_email(
                        dict(email),
                        {
                            "outlook_categories": decision.outlook_categories,
                            "urgency": decision.urgency,
                            "requires_reply": decision.requires_reply,
                            "labels": decision.labels,
                        },
                    )
                except Exception as wm_err:
                    logger.warning(f"Working memory update failed for {email['id']}: {wm_err}")

            # Skip triggers during backfill/onboarding
            if not self.backfill:
                self._emit_triggers_for_email(email, decision, prefs)

        except Exception as e:
            conn.rollback()
            logger.error(f"Error processing email {email['id']}: {e}", exc_info=True)
        finally:
            conn.close()

    def _emit_triggers_for_email(self, email, decision: EmailClassification, prefs: dict):
        """Emit triggers based on classification."""
        # Urgent trigger
        if decision.urgency == "immediate":
            write_trigger(
                self.user_email,
                "urgent_email",
                {
                    "subject": email['subject'],
                    "sender": email["sender"],
                    "message_id": email["id"],
                    "received_at": email["received_at"],
                    "reason": decision.reason
                },
                dedupe_key=make_dedupe_key("urgent_email", self.user_email, email["id"]),
                routing={"channel": "teams"}
            )

        # Reply needed trigger
        if decision.requires_reply:
            write_trigger(
                self.user_email,
                "reply_needed",
                {
                    "message_id": email["id"],
                    "subject": email["subject"],
                    "sender": email["sender"],
                    "received_at": email["received_at"],
                    "reason": decision.reply_reason or decision.reason,
                },
                dedupe_key=make_dedupe_key("reply_needed", self.user_email, email["id"]),
                routing={"channel": "teams"},
            )

        # Availability request trigger
        if decision.availability_requested:
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

            try:
                from .calendar_intelligence import emit_enhanced_availability_trigger
                emit_enhanced_availability_trigger(
                    self.user_email,
                    {"id": email["id"], "subject": email["subject"], "sender": email["sender"]},
                    availability_payload,
                )
            except Exception as cal_err:
                logger.warning(f"Could not enhance availability trigger: {cal_err}")
                write_trigger(
                    self.user_email,
                    "availability_requested",
                    {"message_id": email["id"], "subject": email["subject"], **availability_payload},
                    dedupe_key=make_dedupe_key("availability_requested", self.user_email, email["id"]),
                    routing={"channel": "teams"},
                )

    def _apply_categories_and_flags(self, message_id: str, decision: EmailClassification):
        """Apply Outlook categories and flags via msgraph CLI."""
        categories = decision.outlook_categories or []
        urgency = decision.urgency or "someday"

        if not categories and urgency == "someday":
            logger.debug(f"No categories or flags to apply for {message_id}")
            return

        cmd = ["aech-cli-msgraph", "update-message", message_id]

        if categories:
            cmd.extend(["--categories", ",".join(categories)])

        flag_settings = get_flag_settings(urgency)
        if flag_settings:
            if "flag_status" in flag_settings:
                cmd.extend(["--flag", flag_settings["flag_status"]])
            if "flag_due" in flag_settings:
                cmd.extend(["--flag-due", flag_settings["flag_due"]])

        if self.user_email:
            cmd.extend(["--user", self.user_email])

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

    def _emit_followup_triggers(self, prefs: dict) -> None:
        followup_n_days = int(prefs.get("followup_n_days") or os.getenv("FOLLOWUP_N_DAYS", "2"))
        if followup_n_days <= 0:
            return

        conn = get_connection()
        try:
            rows = conn.execute(
                """
                SELECT rt.message_id, rt.last_activity_at, e.subject, e.sender
                FROM reply_tracking rt
                JOIN emails e ON e.id = rt.message_id
                WHERE rt.requires_reply = 1
                  AND rt.follow_up_sent_at IS NULL
                  AND rt.nudge_scheduled_at IS NULL
                  AND rt.last_activity_at IS NOT NULL
                ORDER BY rt.last_activity_at ASC
                LIMIT 50
                """
            ).fetchall()

            from datetime import datetime, timezone, timedelta

            now = datetime.now(timezone.utc)
            for row in rows:
                last_activity_raw = row["last_activity_at"]
                try:
                    last_activity = datetime.fromisoformat(str(last_activity_raw).replace("Z", "+00:00"))
                    if last_activity.tzinfo is None:
                        last_activity = last_activity.replace(tzinfo=timezone.utc)
                except Exception:
                    continue

                waiting = now - last_activity
                if waiting < timedelta(days=followup_n_days):
                    continue

                message_id = row["message_id"]
                days_waiting = max(followup_n_days, int(waiting.days))
                follow_up_draft = f"Following up on \"{row['subject']}\" — do you have an update?\n\nThanks!"

                write_trigger(
                    self.user_email,
                    "no_reply_after_n_days",
                    {
                        "message_id": message_id,
                        "subject": row["subject"],
                        "sender": row["sender"],
                        "last_activity_at": last_activity_raw,
                        "days_waiting": days_waiting,
                        "follow_up_draft": follow_up_draft,
                    },
                    dedupe_key=make_dedupe_key("no_reply_after_n_days", self.user_email, message_id),
                    routing={"channel": "teams"},
                )
                conn.execute(
                    "UPDATE reply_tracking SET nudge_scheduled_at = CURRENT_TIMESTAMP WHERE message_id = ?",
                    (message_id,),
                )
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

            conn.execute(
                """
                INSERT INTO user_preferences (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = CURRENT_TIMESTAMP
                """,
                ("_internal.last_weekly_digest_week_start", week_start_iso),
            )
            conn.commit()
        finally:
            conn.close()
