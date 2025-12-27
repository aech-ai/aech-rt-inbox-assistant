import logging
import os
import subprocess
from typing import Optional
from pydantic import BaseModel, Field
from pydantic_ai import Agent
from .database import get_connection
from .poller import GraphPoller
from .preferences import read_preferences
from .triggers import make_dedupe_key, write_trigger
from .folders_config import STANDARD_FOLDERS, FOLDER_ALIASES
from .categories_config import (
    get_categories,
    get_category_names,
    get_category_config,
    get_flag_settings,
    format_categories_for_prompt,
    is_categories_mode_enabled,
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

class EmailCategory(BaseModel):
    category: str = Field(description="Must be one of the standard folder categories")
    reason: str = Field(description="The reason for this categorization")
    action: str = Field(description="Recommended action: 'move', 'delete', 'mark_important', 'none'")
    destination_folder: Optional[str] = Field(
        default=None,
        description="Name of the folder to move to, if action is 'move' (legacy folder mode)"
    )

    # New category-based fields
    outlook_categories: list[str] = Field(
        default_factory=list,
        description="Outlook categories to apply (e.g., 'Action Required', 'Work', 'FYI'). Can be multiple."
    )
    urgency: str = Field(
        default="someday",
        description="Urgency level: 'immediate', 'today', 'this_week', 'someday'. Determines flag due date."
    )

    labels: list[str] = Field(
        default_factory=list,
        description="Short labels like: pitch, marketing, billing, vip, action_required, newsletter.",
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

def _build_agent(available_folders: list[str]) -> Agent:
    """Create the AI agent with dynamic folder list."""
    model_name = os.getenv("MODEL_NAME", "openai-responses:gpt-5-mini")

    # Use provided folders, falling back to standard if empty (shouldn't happen if poller works)
    folders_list = available_folders if available_folders else STANDARD_FOLDERS
    allowed_folders_str = ", ".join(folders_list)

    # The worker will add the prefix (e.g., aa_) automatically; model should NOT include it.
    folder_prefix = os.getenv("FOLDER_PREFIX", "aa_")

    cleanup_strategy = os.getenv("CLEANUP_STRATEGY", "medium").lower()

    allowed_categories = ", ".join(STANDARD_FOLDERS)
    system_prompt = f"""
You are an expert executive assistant. Your goal is to deeply understand the INTENT of each email and (a) triage it into a standard folder category, and (b) surface executive-assistant signals like "requires reply" and "availability request".

### 1. Intent Analysis (CRITICAL)
- **Do not rely on keywords alone.** Look at the Subject, Sender, and Body together to determine the *primary purpose* of the email.
- **Analyze the underlying message**: Is this a status update? A travel itinerary? A work request? A social notification?
- **Context matters**: A "Travel" keyword in a status update (e.g., "I am travelling") does not make the email a "Travel" document. "Travel" is for *your* bookings and itineraries.
- **Sender matters**: Who is sending this? Is it a service, a colleague, or a friend?

### 2. Few-Shot Examples (Guide your reasoning)

**Example 1: The "False Positive" Travel**
- **Subject**: Automatic reply: Project Roadmap Q4
- **Body**: I am currently travelling with limited access to email until Nov 27th.
- **Analysis**: The sender is unavailable. This information is temporary and has no long-term value.
- **Category**: "Should Delete".
- **Action**: move -> Should Delete.

**Example 2: Real Travel**
- **Subject**: Flight Confirmation: SFO to LHR
- **Body**: Your flight UA901 is confirmed. Seat 4A.
- **Analysis**: This is a booking for *you*.
- **Category**: "Travel".
- **Action**: move -> Travel.

**Example 3: Social Notification**
- **Subject**: You appeared in 5 searches this week
- **Sender**: LinkedIn <notifications@linkedin.com>
- **Analysis**: Automated platform notification.
- **Category**: "Social".
- **Action**: move -> Social.

**Example 4: Work via Social Platform**
- **Subject**: Project collaboration inquiry
- **Sender**: James Dolan via LinkedIn
- **Body**: Hi Steven, I'd like to discuss the Q4 roadmap...
- **Analysis**: Although from LinkedIn, the *content* is a direct work request.
- **Category**: "Work".
- **Action**: move -> Work.

**Example 5: Newsletter vs Promotion**
- **Subject**: The Daily Tech Digest: AI Agents on the rise
- **Body**: Here are the top stories in tech today...
- **Analysis**: Informational content, recurring.
- **Category**: "Newsletters".
- **Action**: move -> Newsletters.

### 3. Cleanup Strategy (Current Level: {cleanup_strategy.upper()})
- **Goal**: Suggest removal of clutter by moving it to the "Should Delete" folder. NEVER hard delete.
- **Low**: Only move obvious spam/phishing/junk to "Should Delete".
- **Medium**: Move spam + old/irrelevant newsletters (> 3 months) to "Should Delete".
- **Aggressive**: Move spam + any newsletter/promo > 1 month + cold outreach to "Should Delete".

### 4. Categories allowed (must match exactly):
- Must be exactly one of: {allowed_categories}
- Use 'Should Delete' for items that match the cleanup strategy.
- Use 'Cold Outreach' for unsolicited sales/networking emails.
- Use 'Urgent' only for truly time-sensitive items.

### 5. Rules for destination_folder:
- Must be exactly one of: {allowed_folders_str}
- Do NOT include the prefix "{folder_prefix}" in the folder name; the system will add it automatically.
- **CRITICAL**: You MUST choose a folder from the list above. Do NOT invent new folders.
- If nothing precise fits, choose the closest from the list.
- If no move is appropriate, set action='none' and leave destination_folder null.

### 6. Actions allowed:
- move: only when destination_folder is in the approved list above.
- delete: **DEPRECATED**. Do NOT use 'delete'. Instead, use 'move' with destination_folder='Should Delete'.
- mark_important: only for genuinely urgent/time-sensitive items.
- none: when no action is needed.

### 7. Executive assistant signals (set these fields explicitly):
- labels: include any of: vip, action_required, billing, marketing, newsletter, pitch, security.
- confidence: 0-1 overall confidence.
- requires_reply: true if the sender is asking a direct question, requesting a decision, or needs approval.
- reply_reason: short reason when requires_reply=true (e.g. direct_question, asks_for_decision, approval_needed).
- availability_requested: true if the email asks to schedule a meeting or requests availability.
- availability: if availability_requested=true, fill any of: duration_minutes, timezone, time_window, constraints, proposed_slots.
"""
    return Agent(
        model_name,
        output_type=EmailCategory,
        system_prompt=system_prompt,
    )


def _build_categories_agent(prefs: dict) -> Agent:
    """Create the AI agent that uses Outlook categories instead of folders.

    This is the new categorization system that keeps emails in Inbox
    and applies color-coded Outlook categories + flags for urgency.
    """
    model_name = os.getenv("MODEL_NAME", "openai-responses:gpt-5-mini")
    cleanup_strategy = os.getenv("CLEANUP_STRATEGY", "medium").lower()

    # Get configured categories
    category_names = get_category_names(prefs)
    categories_description = format_categories_for_prompt(prefs)

    system_prompt = f"""
You are an expert executive assistant. Your goal is to deeply understand the INTENT of each email and categorize it using Outlook categories that stay in the user's Inbox.

### 1. Intent Analysis (CRITICAL)
- **Do not rely on keywords alone.** Look at the Subject, Sender, and Body together to determine the *primary purpose* of the email.
- **Analyze the underlying message**: Is this actionable? Informational? A notification?
- **Sender matters**: Who is sending this? Is it a service, a colleague, or a friend?

### 2. Available Categories
Choose from these Outlook categories (you can apply multiple):
{categories_description}

### 3. Category Selection Logic
Think step-by-step:

1. **Is this spam/phishing?** → category: "FYI", urgency: "someday", action: "delete" (move to Should Delete)

2. **Does it require MY action?**
   - Direct question to me → "Action Required" + urgency based on deadline
   - Approval/decision needed → "Action Required" + urgency "today" or "this_week"
   - Task assigned to me → "Action Required"

3. **Am I waiting on someone else?**
   - I sent and am awaiting reply → "Follow Up"
   - Someone promised to get back to me → "Follow Up"

4. **Is it work-related but informational?**
   - Status updates, meeting notes → "Work"
   - Internal announcements → "Work" or "FYI"

5. **Is it a newsletter/notification/update?**
   - Automated notifications → "FYI"
   - Newsletters, digests → "FYI"
   - Marketing/promotions → "FYI" (or delete if cleanup strategy suggests)

6. **Is it personal?**
   - Non-work correspondence → "Personal"

### 4. Urgency Levels (determines flag due date)
- **immediate**: Needs attention NOW (rare - only for critical time-sensitive items)
- **today**: Should handle today (deadlines today, urgent requests)
- **this_week**: Can wait a few days but shouldn't be forgotten
- **someday**: Low priority, FYI, no action needed

### 5. Cleanup Strategy (Current Level: {cleanup_strategy.upper()})
- **Low**: Only suggest delete for obvious spam/phishing
- **Medium**: Also suggest delete for old/irrelevant newsletters (> 3 months)
- **Aggressive**: Also delete cold outreach, old promos

### 6. Output Fields
- **category**: Primary category name (one of: {", ".join(category_names)})
- **outlook_categories**: List of category names to apply (can be multiple)
- **urgency**: One of: immediate, today, this_week, someday
- **action**: "none" (categories stay in inbox), "delete" (move to Should Delete), or "mark_important"
- **reason**: Brief explanation of your decision

### 7. Executive Assistant Signals
- **requires_reply**: true if user should respond to this email
- **reply_reason**: short reason (direct_question, asks_for_decision, approval_needed)
- **availability_requested**: true if scheduling a meeting
- **labels**: additional labels like vip, billing, marketing, newsletter, pitch, security
"""
    return Agent(
        model_name,
        output_type=EmailCategory,
        system_prompt=system_prompt,
    )


class Organizer:
    def __init__(self, poller: GraphPoller):
        self.poller = poller
        self.user_email = poller.user_email
        self.agent: Optional[Agent] = None
        self.categories_agent: Optional[Agent] = None
        self.current_folders: list[str] = []
        self.use_categories_mode: bool = False  # Set per-run based on preferences

    def _canonicalize_folders(self, folders: list[str]) -> list[str]:
        """
        Normalize folder names to the standard set and strip any prefix (e.g., aa_).
        This prevents old folder names from being treated as distinct targets.
        """
        prefix = (getattr(self.poller, "folder_prefix", "") or "").lower()
        lower_standard = {name.lower(): name for name in STANDARD_FOLDERS}
        lower_aliases = {alias.lower(): target for alias, target in FOLDER_ALIASES.items()}

        canonical: set[str] = set()
        for folder in folders or []:
            if not folder:
                continue

            base_name = folder
            lower_name = folder.lower()

            if prefix and lower_name.startswith(prefix):
                base_name = folder[len(self.poller.folder_prefix):]
                lower_name = base_name.lower()

            if lower_name in lower_standard:
                canonical.add(lower_standard[lower_name])
            elif lower_name in lower_aliases:
                canonical.add(lower_aliases[lower_name])

        if not canonical:
            return STANDARD_FOLDERS

        return sorted(canonical)

    def _get_agent(self, folders: list[str]) -> Agent:
        # Rebuild agent if folders have changed or if agent is not initialized
        # Sort folders to ensure consistent comparison
        sorted_folders = sorted(folders)
        if self.agent is None or sorted_folders != self.current_folders:
            logger.info("Rebuilding agent with updated folder list")
            self.agent = _build_agent(folders)
            self.current_folders = sorted_folders
        return self.agent

    def _get_categories_agent(self, prefs: dict) -> Agent:
        """Get the categories-based agent, building if needed."""
        if self.categories_agent is None:
            logger.info("Building categories-based agent")
            self.categories_agent = _build_categories_agent(prefs)
        return self.categories_agent

    async def organize_emails(self):
        """Iterate over unprocessed emails and organize them."""
        prefs = read_preferences()

        # Check if we should use the new categories mode (default: True)
        # Set to False to use legacy folder-based system
        self.use_categories_mode = is_categories_mode_enabled(prefs)
        if self.use_categories_mode:
            logger.info("Using Outlook categories mode (emails stay in Inbox)")
        else:
            logger.info("Using legacy folder mode (emails moved to folders)")

        # Fetch current folders from mailbox (still needed for legacy mode and delete folder)
        user_folders = self.poller.get_user_folders()
        available_folders = self._canonicalize_folders(user_folders)

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM emails WHERE processed_at IS NULL")
        emails = cursor.fetchall()
        conn.close()

        for email in emails:
            await self._process_email(email, available_folders, prefs)

        self._emit_followup_triggers(prefs)
        self._emit_weekly_digest_trigger(prefs)

    async def _process_email(self, email, folders: list[str], prefs: dict):
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
            # Run AI Agent - use categories or folder mode based on preference
            if self.use_categories_mode:
                result = await self._get_categories_agent(prefs).run(email_content)
            else:
                result = await self._get_agent(folders).run(email_content)
            decision = result.output

            logger.info(f"AI Decision for {email['id']}: {decision}")

            # Enrich labels deterministically
            if is_vip_sender:
                decision.labels = list({*(getattr(decision, "labels", []) or []), "vip"})
            
            # Execute Action
            self._execute_action(email['id'], decision)
            
            # Log to Triage Log
            conn.execute("""
            INSERT INTO triage_log (email_id, action, destination_folder, reason)
            VALUES (?, ?, ?, ?)
            """, (email['id'], decision.action, decision.destination_folder, decision.reason))
            
            # Mark as processed
            row_exists = conn.execute("SELECT 1 FROM emails WHERE id = ?", (email['id'],)).fetchone()
            if not row_exists:
                logger.error(f"Email {email['id']} missing from DB; skipping update.")
            else:
                conn.execute(
                    "UPDATE emails SET processed_at = CURRENT_TIMESTAMP, category = ? WHERE id = ?", 
                    (decision.category, email['id'])
                )
                logger.debug(f"Marked email {email['id']} processed with category {decision.category}")

            # Persist labels
            try:
                conn.execute("DELETE FROM labels WHERE message_id = ?", (email["id"],))
                for label in getattr(decision, "labels", []) or []:
                    label_str = str(label).strip()
                    if not label_str:
                        continue
                    conn.execute(
                        "INSERT OR REPLACE INTO labels (message_id, label, confidence) VALUES (?, ?, ?)",
                        (email["id"], label_str, float(getattr(decision, "confidence", 0.0) or 0.0)),
                    )
            except Exception as e:
                logger.warning(f"Failed to persist labels for {email['id']}: {e}")

            # Persist reply tracking for follow-ups
            if getattr(decision, "requires_reply", False):
                reply_reason = getattr(decision, "reply_reason", None) or decision.reason
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

            # Update Working Memory (non-blocking - failures don't affect email processing)
            if self.user_email:
                try:
                    wm_updater = WorkingMemoryUpdater(self.user_email)
                    await wm_updater.process_email(
                        dict(email),
                        {
                            "category": getattr(decision, "category", ""),
                            "requires_reply": getattr(decision, "requires_reply", False),
                            "labels": getattr(decision, "labels", []),
                        },
                    )
                except Exception as wm_err:
                    logger.warning(f"Working memory update failed for {email['id']}: {wm_err}")

            # Create Trigger if Urgent - notify via Teams
            if decision.category.lower() == "urgent" or decision.action == "mark_important":
                write_trigger(self.user_email, "urgent_email", {
                    "subject": email['subject'],
                    "sender": email["sender"],
                    "message_id": email["id"],
                    "received_at": email["received_at"],
                    "reason": decision.reason
                },
                dedupe_key=make_dedupe_key("urgent_email", self.user_email, email["id"]),
                routing={"channel": "teams"})

            if getattr(decision, "requires_reply", False):
                write_trigger(
                    self.user_email,
                    "reply_needed",
                    {
                        "message_id": email["id"],
                        "subject": email["subject"],
                        "sender": email["sender"],
                        "received_at": email["received_at"],
                        "reason": getattr(decision, "reply_reason", None) or decision.reason,
                    },
                    dedupe_key=make_dedupe_key("reply_needed", self.user_email, email["id"]),
                    routing={"channel": "teams"},
                )

            if getattr(decision, "availability_requested", False):
                availability = getattr(decision, "availability", None)
                default_timezone = str(prefs.get("timezone") or os.getenv("DEFAULT_TIMEZONE", "UTC"))

                # Build base payload
                availability_payload = {
                    "time_window": getattr(availability, "time_window", None),
                    "duration_minutes": getattr(availability, "duration_minutes", None) or 30,
                    "timezone": getattr(availability, "timezone", None) or default_timezone,
                    "constraints": getattr(availability, "constraints", None),
                    "proposed_slots": getattr(availability, "proposed_slots", None) or [],
                    "requester": email["sender"],
                }

                # Try to enhance with real calendar data
                try:
                    from .calendar_intelligence import emit_enhanced_availability_trigger
                    emit_enhanced_availability_trigger(
                        self.user_email,
                        {"id": email["id"], "subject": email["subject"], "sender": email["sender"]},
                        availability_payload,
                    )
                except Exception as cal_err:
                    logger.warning(f"Could not enhance availability trigger with calendar: {cal_err}")
                    # Fall back to basic trigger without calendar data
                    write_trigger(
                        self.user_email,
                        "availability_requested",
                        {
                            "message_id": email["id"],
                            "subject": email["subject"],
                            **availability_payload,
                        },
                        dedupe_key=make_dedupe_key("availability_requested", self.user_email, email["id"]),
                        routing={"channel": "teams"},
                    )
                
        except Exception as e:
            conn.rollback()
            integrity = None
            try:
                integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
            except Exception as ic:
                integrity = f"integrity_check failed: {ic}"
            logger.error(
                f"Error processing email {email['id']}: {e} (args={getattr(e, 'args', None)})",
                exc_info=True,
                extra={"integrity_check": integrity},
            )
        finally:
            conn.close()

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
                    last_activity = datetime.fromisoformat(
                        str(last_activity_raw).replace("Z", "+00:00")
                    )
                    if last_activity.tzinfo is None:
                        last_activity = last_activity.replace(tzinfo=timezone.utc)
                except Exception:
                    continue

                waiting = now - last_activity
                if waiting < timedelta(days=followup_n_days):
                    continue

                message_id = row["message_id"]
                days_waiting = max(followup_n_days, int(waiting.days))
                follow_up_draft = (
                    f"Following up on \"{row['subject']}\" — do you have an update?\n\n"
                    "Thanks!"
                )

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
            "monday": 0,
            "tuesday": 1,
            "wednesday": 2,
            "thursday": 3,
            "friday": 4,
            "saturday": 5,
            "sunday": 6,
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

        week_start = (now_local.date() - timedelta(days=now_local.date().weekday()))
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
                SELECT id, subject, sender, received_at, category, body_preview
                FROM emails
                WHERE received_at IS NOT NULL
                ORDER BY received_at DESC
                LIMIT 500
                """
            ).fetchall()

            top_items: list[dict] = []
            newsletter_summaries: list[dict] = []
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

                category = row["category"] or ""
                if category == "Newsletters":
                    if len(newsletter_summaries) < 20:
                        newsletter_summaries.append(
                            {
                                "from": row["sender"],
                                "subject": row["subject"],
                                "summary": (row["body_preview"] or "")[:280],
                            }
                        )
                    continue

                if category in {"Urgent", "Action Required"} and row["subject"]:
                    recommended_actions.append(row["subject"])

                if len(top_items) < 15:
                    top_items.append(
                        {
                            "title": row["subject"],
                            "why_it_matters": f"From {row['sender']} ({category or 'unclassified'})",
                            "links": [row["id"]],
                        }
                    )

            write_trigger(
                self.user_email,
                "weekly_digest_ready",
                {
                    "week_start": week_start_iso,
                    "week_end": week_end.isoformat(),
                    "top_items": top_items,
                    "newsletter_summaries": newsletter_summaries,
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

    def _normalize_folder_name(self, folder_name: str) -> Optional[str]:
        """
        Normalize and validate folder name against standard folders.
        Returns standardized folder name if valid, None otherwise.
        """
        if not folder_name:
            return None

        prefix = (getattr(self.poller, "folder_prefix", "") or "").lower()
        lower_name = folder_name.lower()

        # Strip known prefix if present (e.g., aa_Work -> Work)
        if prefix and lower_name.startswith(prefix):
            folder_name = folder_name[len(self.poller.folder_prefix):]
            lower_name = folder_name.lower()

        # Check exact match (case-insensitive)
        for std_folder in STANDARD_FOLDERS:
            if lower_name == std_folder.lower():
                return std_folder

        # Check aliases
        if lower_name in FOLDER_ALIASES:
            return FOLDER_ALIASES[lower_name]

        # Not a valid standard folder
        logger.warning(f"Folder '{folder_name}' not in standard folder list. Skipping.")
        return None

    def _execute_action(self, message_id: str, decision: EmailCategory):
        # Categories mode: apply Outlook categories and flags, keep in Inbox
        if self.use_categories_mode:
            self._apply_categories_and_flags(message_id, decision)
            # Handle delete action (still moves to Should Delete folder)
            if decision.action == 'delete':
                self.poller.move_email(message_id, "Should Delete")
            return

        # Legacy folder mode: move emails to folders
        if decision.action == 'move' and decision.destination_folder:
            # Normalize folder name
            normalized_folder = self._normalize_folder_name(decision.destination_folder)
            if not normalized_folder:
                return

            self.poller.move_email(message_id, normalized_folder)
        elif decision.action == 'delete':
            self.poller.delete_email(message_id)

    def _apply_categories_and_flags(self, message_id: str, decision: EmailCategory):
        """Apply Outlook categories and flags via msgraph CLI.

        This keeps emails in the Inbox while applying color-coded categories
        and urgency-based flags for follow-up.
        """
        categories = getattr(decision, "outlook_categories", []) or []
        urgency = getattr(decision, "urgency", "someday") or "someday"

        if not categories and urgency == "someday":
            logger.debug(f"No categories or flags to apply for {message_id}")
            return

        # Build the msgraph CLI command
        # Format: aech-cli-msgraph update-message <message_id> [options]
        cmd = ["aech-cli-msgraph", "update-message", message_id]

        # Add categories if any
        if categories:
            cmd.extend(["--categories", ",".join(categories)])

        # Add flag based on urgency
        flag_settings = get_flag_settings(urgency)
        if flag_settings:
            if "flag_status" in flag_settings:
                cmd.extend(["--flag", flag_settings["flag_status"]])
            if "flag_due" in flag_settings:
                cmd.extend(["--flag-due", flag_settings["flag_due"]])

        # Add delegated user context if available
        if self.user_email:
            cmd.extend(["--user", self.user_email])

        # Execute the command
        try:
            logger.info(f"Applying categories/flags: {' '.join(cmd)}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                logger.warning(
                    f"Failed to apply categories/flags for {message_id}: "
                    f"{result.stderr or result.stdout}"
                )
            else:
                logger.debug(f"Successfully applied categories/flags for {message_id}")
        except FileNotFoundError:
            logger.warning(
                f"aech-cli-msgraph not found - categories/flags not applied for {message_id}. "
                "Install msgraph CLI to enable category support."
            )
        except subprocess.TimeoutExpired:
            logger.warning(f"Timeout applying categories/flags for {message_id}")
        except Exception as e:
            logger.warning(f"Error applying categories/flags for {message_id}: {e}")
