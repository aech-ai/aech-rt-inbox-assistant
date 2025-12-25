"""
Meeting Prep module for Executive Assistant.

Generates meeting preparation briefings by:
- Fetching upcoming calendar events
- Cross-referencing attendees with email corpus
- Applying configurable rules for which meetings get prep
- Emitting daily_briefing and meeting_prep_ready triggers
"""

import logging
import os
import json
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Dict, Any, List, Optional
from pathlib import Path

from pydantic import BaseModel, Field
from zoneinfo import ZoneInfo

from .calendar import CalendarClient, CalendarEvent
from .database import get_connection
from .triggers import make_dedupe_key, write_trigger

logger = logging.getLogger(__name__)


# =============================================================================
# Pydantic Models
# =============================================================================

class MeetingPrepRule(BaseModel):
    """A rule for determining which meetings get prep."""
    name: str
    enabled: bool = True
    # Match conditions (OR logic - any match triggers the rule)
    external_only: bool = False  # Only meetings with external attendees
    min_attendees: int = 0  # Minimum number of attendees
    keywords: List[str] = Field(default_factory=list)  # Keywords in subject
    sender_domains: List[str] = Field(default_factory=list)  # Organizer domain matches
    vip_attendees: List[str] = Field(default_factory=list)  # Specific email addresses
    # Actions
    prep_minutes_before: int = 15  # When to send prep notification


class MeetingPrepConfig(BaseModel):
    """Configuration for meeting prep features."""
    enabled: bool = True
    # Morning briefing settings
    morning_briefing_enabled: bool = True
    morning_briefing_time: str = "08:00"  # HH:MM in user's timezone
    morning_briefing_lookahead_hours: int = 12  # How far ahead to look
    # Individual prep settings
    individual_prep_enabled: bool = True
    default_prep_minutes: int = 15  # Default lead time for prep notifications
    # Rules (processed in order, first match wins)
    rules: List[MeetingPrepRule] = Field(default_factory=lambda: [
        MeetingPrepRule(
            name="external_meetings",
            external_only=True,
            prep_minutes_before=15,
        ),
        MeetingPrepRule(
            name="large_meetings",
            min_attendees=5,
            prep_minutes_before=30,
        ),
        MeetingPrepRule(
            name="important_keywords",
            keywords=["interview", "review", "board", "exec", "client", "partner"],
            prep_minutes_before=30,
        ),
    ])
    # Exclusions
    skip_all_day_events: bool = True
    skip_declined_events: bool = True
    skip_tentative_events: bool = False
    min_duration_minutes: int = 15  # Skip very short events


class AttendeeContext(BaseModel):
    """Context about an attendee from the email corpus."""
    email: str
    name: Optional[str] = None
    is_external: bool = False
    recent_email_count: int = 0
    last_email_date: Optional[str] = None
    last_email_subject: Optional[str] = None
    # Key topics from recent correspondence
    topics: List[str] = Field(default_factory=list)


class MeetingPrep(BaseModel):
    """Prepared briefing for a meeting."""
    event_id: str
    subject: str
    start: datetime
    end: datetime
    location: Optional[str] = None
    is_online: bool = False
    join_url: Optional[str] = None
    organizer: Optional[str] = None
    # Attendee analysis
    attendee_count: int = 0
    external_attendee_count: int = 0
    attendee_context: List[AttendeeContext] = Field(default_factory=list)
    # Meeting body/agenda if present
    body_preview: Optional[str] = None
    # Generated briefing
    briefing_summary: Optional[str] = None
    preparation_notes: List[str] = Field(default_factory=list)
    # Metadata
    prep_generated_at: str = ""
    rule_matched: Optional[str] = None


class DailyBriefing(BaseModel):
    """Morning briefing with day's schedule and prep."""
    date: str
    timezone: str
    total_meetings: int = 0
    meetings_needing_prep: int = 0
    # Schedule overview
    schedule_summary: str = ""
    first_meeting_time: Optional[str] = None
    busy_hours: float = 0.0
    free_hours: float = 0.0
    # Detailed prep for important meetings
    meeting_preps: List[MeetingPrep] = Field(default_factory=list)
    # Alerts
    alerts: List[str] = Field(default_factory=list)
    # Generated at
    generated_at: str = ""


# =============================================================================
# MeetingPrepService
# =============================================================================

class MeetingPrepService:
    """
    Service for generating meeting preparation briefings.

    Integrates calendar data with email corpus to provide context.
    """

    def __init__(
        self,
        calendar_client: Optional[CalendarClient] = None,
        config: Optional[MeetingPrepConfig] = None
    ):
        self.calendar = calendar_client or CalendarClient()
        self.user_email = self.calendar.user_email
        self.config = config or self._load_config()
        self._internal_domains = self._get_internal_domains()

    def _load_config(self) -> MeetingPrepConfig:
        """Load meeting prep config from preferences."""
        try:
            prefs_path = Path(os.environ.get(
                "AECH_PREFERENCES_PATH",
                Path(os.environ.get("AECH_USER_DIR", ".")) / "preferences.json"
            ))
            if prefs_path.exists():
                prefs = json.loads(prefs_path.read_text())
                if "meeting_prep" in prefs:
                    return MeetingPrepConfig(**prefs["meeting_prep"])
        except Exception as e:
            logger.debug(f"Could not load meeting prep config: {e}")
        return MeetingPrepConfig()

    def _get_internal_domains(self) -> set:
        """Get internal email domains from user's email."""
        domains = set()
        if self.user_email:
            domain = self.user_email.split("@")[-1].lower()
            domains.add(domain)
        return domains

    def _is_external(self, email: str) -> bool:
        """Check if an email address is external to the organization."""
        if not email:
            return False
        domain = email.split("@")[-1].lower()
        return domain not in self._internal_domains

    # =========================================================================
    # Rule Matching
    # =========================================================================

    def _should_prepare(self, event: CalendarEvent) -> Optional[MeetingPrepRule]:
        """
        Check if an event should get meeting prep based on configured rules.
        Returns the matching rule or None.
        """
        # Apply exclusions first
        if self.config.skip_all_day_events and event.is_all_day:
            return None
        if self.config.skip_declined_events and event.response_status == "declined":
            return None
        if self.config.skip_tentative_events and event.response_status == "tentative":
            return None

        duration_mins = (event.end - event.start).total_seconds() / 60
        if duration_mins < self.config.min_duration_minutes:
            return None

        # Check each rule
        for rule in self.config.rules:
            if not rule.enabled:
                continue

            matched = False

            # External only check
            if rule.external_only:
                external_count = sum(1 for a in event.attendees if self._is_external(a.email))
                if external_count > 0:
                    matched = True

            # Min attendees check
            if rule.min_attendees > 0 and len(event.attendees) >= rule.min_attendees:
                matched = True

            # Keywords in subject
            if rule.keywords:
                subject_lower = event.subject.lower()
                if any(kw.lower() in subject_lower for kw in rule.keywords):
                    matched = True

            # Sender domain check
            if rule.sender_domains and event.organizer_email:
                org_domain = event.organizer_email.split("@")[-1].lower()
                if org_domain in [d.lower() for d in rule.sender_domains]:
                    matched = True

            # VIP attendees
            if rule.vip_attendees:
                attendee_emails = {a.email.lower() for a in event.attendees}
                if any(vip.lower() in attendee_emails for vip in rule.vip_attendees):
                    matched = True

            if matched:
                return rule

        return None

    # =========================================================================
    # Email Corpus Integration
    # =========================================================================

    def _get_attendee_context(self, email: str, days_back: int = 30) -> AttendeeContext:
        """
        Get context about an attendee from the email corpus.
        """
        context = AttendeeContext(
            email=email,
            is_external=self._is_external(email)
        )

        try:
            conn = get_connection()
            cursor = conn.cursor()

            # Get recent emails with this person
            cutoff = (datetime.now() - timedelta(days=days_back)).isoformat()

            # Emails from or to this person
            rows = cursor.execute(
                """
                SELECT subject, sender, received_at, body_preview
                FROM emails
                WHERE (sender = ? OR to_emails LIKE ? OR cc_emails LIKE ?)
                AND received_at > ?
                ORDER BY received_at DESC
                LIMIT 10
                """,
                (email, f'%"{email}"%', f'%"{email}"%', cutoff)
            ).fetchall()

            context.recent_email_count = len(rows)

            if rows:
                context.last_email_date = rows[0]["received_at"]
                context.last_email_subject = rows[0]["subject"]

                # Extract topics from subjects (simple keyword extraction)
                topics = set()
                for row in rows:
                    subject = row["subject"] or ""
                    # Remove common prefixes
                    for prefix in ["Re:", "RE:", "Fwd:", "FW:", "Fw:"]:
                        subject = subject.replace(prefix, "").strip()
                    if subject and len(subject) < 100:
                        topics.add(subject)
                context.topics = list(topics)[:5]

            conn.close()

        except Exception as e:
            logger.debug(f"Could not get attendee context for {email}: {e}")

        return context

    # =========================================================================
    # Prep Generation
    # =========================================================================

    def prepare_meeting(self, event: CalendarEvent) -> MeetingPrep:
        """
        Generate a meeting prep briefing for a single event.
        """
        rule = self._should_prepare(event)

        prep = MeetingPrep(
            event_id=event.event_id,
            subject=event.subject,
            start=event.start,
            end=event.end,
            location=event.location,
            is_online=event.is_online_meeting,
            join_url=event.online_meeting_url,
            organizer=event.organizer_email,
            body_preview=event.body_preview,
            prep_generated_at=datetime.now(dt_timezone.utc).isoformat(),
            rule_matched=rule.name if rule else None,
        )

        # Analyze attendees
        prep.attendee_count = len(event.attendees)
        attendee_contexts = []

        for attendee in event.attendees:
            ctx = self._get_attendee_context(attendee.email)
            ctx.name = attendee.name
            attendee_contexts.append(ctx)
            if ctx.is_external:
                prep.external_attendee_count += 1

        prep.attendee_context = attendee_contexts

        # Generate preparation notes
        notes = []

        if prep.external_attendee_count > 0:
            notes.append(f"{prep.external_attendee_count} external attendee(s)")

        # Note attendees with recent correspondence
        active_contacts = [c for c in attendee_contexts if c.recent_email_count > 0]
        if active_contacts:
            for contact in active_contacts[:3]:
                notes.append(
                    f"Recent emails with {contact.name or contact.email}: "
                    f"{contact.recent_email_count} in last 30 days"
                )
                if contact.last_email_subject:
                    notes.append(f"  Last: \"{contact.last_email_subject}\"")

        # Note attendees with no recent contact
        no_contact = [c for c in attendee_contexts if c.recent_email_count == 0 and c.email != self.user_email]
        if no_contact and len(no_contact) <= 3:
            for contact in no_contact:
                notes.append(f"No recent emails with {contact.name or contact.email}")

        prep.preparation_notes = notes

        # Generate summary
        duration_mins = int((event.end - event.start).total_seconds() / 60)
        summary_parts = [f"{duration_mins}-minute meeting"]

        if event.is_online_meeting:
            summary_parts.append("(Teams)")
        elif event.location:
            summary_parts.append(f"at {event.location}")

        if prep.external_attendee_count > 0:
            summary_parts.append(f"with {prep.external_attendee_count} external attendee(s)")

        prep.briefing_summary = " ".join(summary_parts)

        return prep

    def prepare_next_meeting(self) -> Optional[MeetingPrep]:
        """Get prep for the next upcoming meeting that needs it."""
        events = self.calendar.get_upcoming_events(hours=8, limit=10)

        for event in events:
            if self._should_prepare(event):
                return self.prepare_meeting(event)

        return None

    def prepare_meetings_in_range(
        self,
        start: datetime,
        end: datetime,
        only_matching_rules: bool = True
    ) -> List[MeetingPrep]:
        """
        Prepare briefings for all meetings in a time range.
        """
        events = self.calendar.get_calendar_view(start, end)
        preps = []

        for event in events:
            if only_matching_rules and not self._should_prepare(event):
                continue
            preps.append(self.prepare_meeting(event))

        return preps

    # =========================================================================
    # Daily Briefing
    # =========================================================================

    def generate_daily_briefing(
        self,
        date: Optional[datetime] = None,
        timezone: Optional[str] = None
    ) -> DailyBriefing:
        """
        Generate a morning briefing for the day.
        """
        tz_str = timezone or self.calendar.get_user_timezone()
        tz = ZoneInfo(tz_str)

        if date is None:
            date = datetime.now(tz)

        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1)

        briefing = DailyBriefing(
            date=start_of_day.strftime("%Y-%m-%d"),
            timezone=tz_str,
            generated_at=datetime.now(dt_timezone.utc).isoformat(),
        )

        # Get today's events
        events = self.calendar.get_calendar_view(start_of_day, end_of_day)
        briefing.total_meetings = len([e for e in events if not e.is_all_day])

        # Calculate busy/free time
        working_hours = self.calendar.get_working_hours()
        work_start_parts = working_hours.start_time.split(":")
        work_end_parts = working_hours.end_time.split(":")
        work_start = int(work_start_parts[0])
        work_end = int(work_end_parts[0])
        total_work_hours = work_end - work_start

        busy_minutes = 0
        for event in events:
            if event.is_all_day:
                continue
            event_start = event.start.astimezone(tz) if event.start.tzinfo else event.start.replace(tzinfo=tz)
            event_end = event.end.astimezone(tz) if event.end.tzinfo else event.end.replace(tzinfo=tz)
            duration = (event_end - event_start).total_seconds() / 60
            busy_minutes += duration

        briefing.busy_hours = round(busy_minutes / 60, 1)
        briefing.free_hours = round(total_work_hours - briefing.busy_hours, 1)

        # Get first meeting
        non_all_day = [e for e in events if not e.is_all_day]
        if non_all_day:
            first = min(non_all_day, key=lambda e: e.start)
            first_start = first.start.astimezone(tz) if first.start.tzinfo else first.start.replace(tzinfo=tz)
            briefing.first_meeting_time = first_start.strftime("%I:%M %p").lstrip("0")

        # Generate schedule summary
        if briefing.total_meetings == 0:
            briefing.schedule_summary = "No meetings scheduled for today."
        elif briefing.total_meetings == 1:
            briefing.schedule_summary = f"1 meeting today, starting at {briefing.first_meeting_time}."
        else:
            briefing.schedule_summary = (
                f"{briefing.total_meetings} meetings today. "
                f"First at {briefing.first_meeting_time}. "
                f"~{briefing.busy_hours}h busy, ~{briefing.free_hours}h available."
            )

        # Prep important meetings
        for event in events:
            if self._should_prepare(event):
                prep = self.prepare_meeting(event)
                briefing.meeting_preps.append(prep)

        briefing.meetings_needing_prep = len(briefing.meeting_preps)

        # Generate alerts
        alerts = []

        # Check for back-to-back meetings
        sorted_events = sorted([e for e in events if not e.is_all_day], key=lambda e: e.start)
        for i in range(len(sorted_events) - 1):
            current = sorted_events[i]
            next_event = sorted_events[i + 1]
            gap = (next_event.start - current.end).total_seconds() / 60
            if gap < 5:  # Less than 5 minutes between meetings
                alerts.append(
                    f"Back-to-back: {current.subject} ends at "
                    f"{current.end.astimezone(tz).strftime('%I:%M %p').lstrip('0')}, "
                    f"{next_event.subject} starts immediately after"
                )

        # Early morning meeting alert
        if briefing.first_meeting_time and non_all_day:
            first = min(non_all_day, key=lambda e: e.start)
            first_start = first.start.astimezone(tz) if first.start.tzinfo else first.start.replace(tzinfo=tz)
            if first_start.hour < work_start:
                alerts.append(f"Early meeting at {briefing.first_meeting_time} (before working hours)")

        # Long day alert
        if briefing.busy_hours > 6:
            alerts.append(f"Heavy meeting load today ({briefing.busy_hours}h scheduled)")

        briefing.alerts = alerts

        return briefing


# =============================================================================
# Trigger Emission
# =============================================================================

def emit_daily_briefing(user_email: str) -> Optional[str]:
    """
    Emit a daily briefing trigger.
    Called by scheduler at configured morning time.
    """
    try:
        service = MeetingPrepService()
        briefing = service.generate_daily_briefing()

        # Convert to dict for trigger payload
        payload = briefing.model_dump(mode="json")

        today = datetime.now(dt_timezone.utc).strftime("%Y-%m-%d")
        dedupe_key = make_dedupe_key("daily_briefing", user_email, today)

        return write_trigger(
            user_email,
            "daily_briefing",
            payload,
            dedupe_key=dedupe_key,
            routing={"channel": "teams"},
        )

    except Exception as e:
        logger.error(f"Failed to emit daily briefing: {e}")
        return None


def emit_meeting_prep(user_email: str, event_id: str) -> Optional[str]:
    """
    Emit a meeting prep trigger for a specific event.
    """
    try:
        service = MeetingPrepService()

        # Get the specific event
        now = datetime.now(dt_timezone.utc)
        events = service.calendar.get_upcoming_events(hours=24, limit=50)

        target_event = None
        for event in events:
            if event.event_id == event_id:
                target_event = event
                break

        if not target_event:
            logger.warning(f"Event {event_id} not found in upcoming events")
            return None

        prep = service.prepare_meeting(target_event)
        payload = prep.model_dump(mode="json")

        dedupe_key = make_dedupe_key("meeting_prep_ready", user_email, event_id)

        return write_trigger(
            user_email,
            "meeting_prep_ready",
            payload,
            dedupe_key=dedupe_key,
            routing={"channel": "teams"},
        )

    except Exception as e:
        logger.error(f"Failed to emit meeting prep: {e}")
        return None


def check_and_emit_meeting_preps(user_email: str, lookahead_minutes: int = 30) -> List[str]:
    """
    Check for meetings starting soon and emit prep triggers.
    Called periodically by the poller/scheduler.

    Returns list of emitted trigger IDs.
    """
    emitted = []

    try:
        service = MeetingPrepService()

        if not service.config.individual_prep_enabled:
            return emitted

        now = datetime.now(dt_timezone.utc)
        lookahead = now + timedelta(minutes=lookahead_minutes)

        events = service.calendar.get_calendar_view(now, lookahead)

        for event in events:
            rule = service._should_prepare(event)
            if not rule:
                continue

            # Check if this event is within the prep window
            minutes_until = (event.start - now).total_seconds() / 60
            if minutes_until <= rule.prep_minutes_before:
                trigger_id = emit_meeting_prep(user_email, event.event_id)
                if trigger_id:
                    emitted.append(trigger_id)

    except Exception as e:
        logger.error(f"Error checking for meeting preps: {e}")

    return emitted
