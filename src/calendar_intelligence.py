"""
Calendar Intelligence module for Executive Assistant.

Enhances availability triggers from email detection with real calendar data.
"""

import logging
import os
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Dict, Any, List, Optional

from zoneinfo import ZoneInfo

from .calendar import CalendarClient, TimeSlot, MeetingTimeSuggestion
from .triggers import make_dedupe_key, write_trigger

logger = logging.getLogger(__name__)


class CalendarIntelligence:
    """
    Integrates calendar data with email availability requests.
    Enhances availability triggers with actual free slots from Graph API.
    """

    def __init__(self, calendar_client: Optional[CalendarClient] = None):
        self.calendar = calendar_client or CalendarClient()
        self.user_email = self.calendar.user_email

    def enhance_availability_trigger(
        self,
        original_payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Enhance an availability_requested payload with actual calendar availability.

        Adds:
        - actual_free_slots: List of available time windows from Graph API
        - proposed_slot_analysis: Conflict check on proposed times
        - recommendation: Suggested response
        """
        enhanced = original_payload.copy()

        # Parse request details
        time_window = original_payload.get("time_window")
        duration_minutes = original_payload.get("duration_minutes", 30)
        timezone_str = original_payload.get("timezone") or self.calendar.get_user_timezone()
        constraints = original_payload.get("constraints", "")
        proposed_slots = original_payload.get("proposed_slots", [])
        requester_email = original_payload.get("requester")

        try:
            tz = ZoneInfo(timezone_str)
        except Exception:
            tz = ZoneInfo("UTC")
            timezone_str = "UTC"

        now = datetime.now(tz)

        # Determine search window from natural language
        search_start, search_end = self._parse_time_window(time_window, now, tz)

        # Get free/busy from Graph API
        try:
            schedule_results = self.calendar.get_schedule(search_start, search_end)
            free_slots = self._extract_free_slots(
                schedule_results,
                search_start,
                search_end,
                duration_minutes,
                tz
            )

            # Apply natural language constraints
            if constraints:
                free_slots = self._apply_constraints(free_slots, constraints, tz)

        except Exception as e:
            logger.warning(f"Could not fetch schedule from Graph: {e}")
            free_slots = []

        # Check proposed slots for conflicts
        slot_analysis = []
        for slot_str in proposed_slots:
            try:
                slot_start = datetime.fromisoformat(slot_str.replace("Z", "+00:00"))
                slot_end = slot_start + timedelta(minutes=duration_minutes)
                is_available = self.calendar.check_availability(slot_start, slot_end)
                slot_analysis.append({
                    "proposed": slot_str,
                    "available": is_available,
                    "message": "Available" if is_available else "Conflicts with existing event"
                })
            except Exception as e:
                logger.warning(f"Could not parse proposed slot {slot_str}: {e}")

        # Try findMeetingTimes if requester email provided
        meeting_suggestions = []
        if requester_email:
            try:
                suggestions = self.calendar.find_meeting_times(
                    attendees=[requester_email],
                    duration_minutes=duration_minutes,
                    start=search_start,
                    end=search_end,
                    max_candidates=5
                )
                for s in suggestions:
                    meeting_suggestions.append({
                        "start": s.start.isoformat(),
                        "end": s.end.isoformat(),
                        "confidence": s.confidence,
                    })
            except Exception as e:
                logger.warning(f"findMeetingTimes failed: {e}")

        # Format free slots for response
        formatted_slots = []
        for slot in free_slots[:10]:  # Limit to 10 suggestions
            formatted_slots.append({
                "start": slot.start.isoformat(),
                "end": slot.end.isoformat(),
                "duration_minutes": slot.duration_minutes,
                "formatted": self._format_slot_human(slot, tz)
            })

        enhanced["actual_free_slots"] = formatted_slots
        enhanced["proposed_slot_analysis"] = slot_analysis
        enhanced["meeting_time_suggestions"] = meeting_suggestions
        enhanced["calendar_checked_at"] = datetime.now(dt_timezone.utc).isoformat()
        enhanced["timezone"] = timezone_str

        # Generate recommendation
        enhanced["recommendation"] = self._generate_recommendation(
            formatted_slots, slot_analysis, meeting_suggestions, constraints
        )

        return enhanced

    def _parse_time_window(
        self,
        time_window: Optional[str],
        now: datetime,
        tz: ZoneInfo
    ) -> tuple[datetime, datetime]:
        """Parse natural language time window into datetime range."""
        if not time_window:
            # Default: next 7 days
            return now, now + timedelta(days=7)

        time_window_lower = time_window.lower()

        if "tomorrow" in time_window_lower:
            tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            return tomorrow, tomorrow + timedelta(days=1)
        elif "today" in time_window_lower:
            end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=0)
            return now, end_of_day
        elif "this week" in time_window_lower:
            # Rest of this week
            days_until_sunday = 6 - now.weekday()
            week_end = now + timedelta(days=days_until_sunday)
            week_end = week_end.replace(hour=23, minute=59, second=59)
            return now, week_end
        elif "next week" in time_window_lower:
            # Next Monday to Sunday
            days_until_monday = 7 - now.weekday()
            next_monday = now + timedelta(days=days_until_monday)
            next_monday = next_monday.replace(hour=0, minute=0, second=0, microsecond=0)
            next_sunday = next_monday + timedelta(days=6, hours=23, minutes=59, seconds=59)
            return next_monday, next_sunday
        elif "next few days" in time_window_lower:
            return now, now + timedelta(days=3)
        else:
            # Try to extract date patterns or default to 7 days
            # Could add more sophisticated parsing here
            return now, now + timedelta(days=7)

    def _extract_free_slots(
        self,
        schedule_results: List,
        start: datetime,
        end: datetime,
        min_duration_minutes: int,
        tz: ZoneInfo
    ) -> List[TimeSlot]:
        """Extract free time slots from getSchedule results."""
        if not schedule_results:
            return []

        # Get working hours
        try:
            working_hours = self.calendar.get_working_hours()
            work_start_parts = working_hours.start_time.split(":")
            work_end_parts = working_hours.end_time.split(":")
            work_start_hour = int(work_start_parts[0])
            work_end_hour = int(work_end_parts[0])
            work_days = set(working_hours.days_of_week)
        except Exception:
            work_start_hour = 9
            work_end_hour = 17
            work_days = {"monday", "tuesday", "wednesday", "thursday", "friday"}

        # Collect all busy periods
        busy_periods = []
        for result in schedule_results:
            for item in result.schedule_items:
                if item.status in ("busy", "oof", "tentative"):
                    busy_periods.append(TimeSlot(start=item.start, end=item.end))

        # Sort and merge busy periods
        busy_periods = self._merge_time_slots(busy_periods)

        # Find free slots within working hours
        free_slots = []
        current_date = start.date()
        end_date = end.date()

        while current_date <= end_date:
            # Check if this is a working day
            day_name = current_date.strftime("%A").lower()
            if day_name not in work_days:
                current_date += timedelta(days=1)
                continue

            # Calculate working hours for this day
            day_start = datetime(
                current_date.year, current_date.month, current_date.day,
                work_start_hour, 0, 0, tzinfo=tz
            )
            day_end = datetime(
                current_date.year, current_date.month, current_date.day,
                work_end_hour, 0, 0, tzinfo=tz
            )

            # Clip to requested range
            window_start = max(day_start, start.astimezone(tz) if start.tzinfo else start.replace(tzinfo=tz))
            window_end = min(day_end, end.astimezone(tz) if end.tzinfo else end.replace(tzinfo=tz))

            if window_start >= window_end:
                current_date += timedelta(days=1)
                continue

            # Find free periods in this window
            current_time = window_start

            for busy in busy_periods:
                busy_start = busy.start.astimezone(tz) if busy.start.tzinfo else busy.start.replace(tzinfo=tz)
                busy_end = busy.end.astimezone(tz) if busy.end.tzinfo else busy.end.replace(tzinfo=tz)

                if busy_end <= current_time or busy_start >= window_end:
                    continue

                if current_time < busy_start:
                    gap_end = min(busy_start, window_end)
                    gap_duration = int((gap_end - current_time).total_seconds() / 60)
                    if gap_duration >= min_duration_minutes:
                        free_slots.append(TimeSlot(start=current_time, end=gap_end))

                current_time = max(current_time, busy_end)

            # Check remaining time after last busy period
            if current_time < window_end:
                gap_duration = int((window_end - current_time).total_seconds() / 60)
                if gap_duration >= min_duration_minutes:
                    free_slots.append(TimeSlot(start=current_time, end=window_end))

            current_date += timedelta(days=1)

        return free_slots

    def _merge_time_slots(self, slots: List[TimeSlot]) -> List[TimeSlot]:
        """Merge overlapping time slots."""
        if not slots:
            return []

        sorted_slots = sorted(slots, key=lambda s: s.start)
        merged = [sorted_slots[0]]

        for slot in sorted_slots[1:]:
            last = merged[-1]
            if slot.start <= last.end:
                # Overlapping or adjacent, merge
                new_end = max(last.end, slot.end)
                merged[-1] = TimeSlot(start=last.start, end=new_end)
            else:
                merged.append(slot)

        return merged

    def _apply_constraints(
        self,
        slots: List[TimeSlot],
        constraints: str,
        tz: ZoneInfo
    ) -> List[TimeSlot]:
        """Apply natural language constraints to filter slots."""
        constraints_lower = constraints.lower()
        filtered = []

        for slot in slots:
            slot_local = slot.start.astimezone(tz) if slot.start.tzinfo else slot.start.replace(tzinfo=tz)
            weekday = slot_local.weekday()
            hour = slot_local.hour
            day_name = slot_local.strftime("%A").lower()

            include = True

            # Day constraints
            if f"no {day_name}" in constraints_lower:
                include = False
            if "no friday" in constraints_lower and weekday == 4:
                include = False
            if "no monday" in constraints_lower and weekday == 0:
                include = False
            if "weekdays only" in constraints_lower and weekday >= 5:
                include = False

            # Time constraints
            if "avoid mornings" in constraints_lower and hour < 12:
                include = False
            if "afternoon only" in constraints_lower and (hour < 12 or hour >= 17):
                include = False
            if "mornings only" in constraints_lower and hour >= 12:
                include = False
            if "after lunch" in constraints_lower and hour < 13:
                include = False

            if include:
                filtered.append(slot)

        return filtered

    def _format_slot_human(self, slot: TimeSlot, tz: ZoneInfo) -> str:
        """Format a time slot for human-readable display."""
        start_local = slot.start.astimezone(tz) if slot.start.tzinfo else slot.start.replace(tzinfo=tz)
        end_local = slot.end.astimezone(tz) if slot.end.tzinfo else slot.end.replace(tzinfo=tz)

        day = start_local.strftime("%A, %B %d")
        start_time = start_local.strftime("%I:%M %p").lstrip("0")
        end_time = end_local.strftime("%I:%M %p").lstrip("0")

        return f"{day} from {start_time} to {end_time}"

    def _generate_recommendation(
        self,
        free_slots: List[Dict],
        slot_analysis: List[Dict],
        meeting_suggestions: List[Dict],
        constraints: Optional[str]
    ) -> str:
        """Generate a recommendation message."""
        # Check if any proposed slots work
        available_proposed = [s for s in slot_analysis if s.get("available")]

        if available_proposed:
            return f"{len(available_proposed)} of the proposed time(s) work. Can confirm any of those."

        if meeting_suggestions:
            return f"Found {len(meeting_suggestions)} times that work for both parties via findMeetingTimes."

        if free_slots:
            return f"None of the proposed times work. {len(free_slots)} alternative slots available."

        constraint_note = f" with constraints '{constraints}'" if constraints else ""
        return f"No available times found{constraint_note}. Consider expanding the search range."


def emit_enhanced_availability_trigger(
    user_email: str,
    email_data: Dict[str, Any],
    availability_payload: Dict[str, Any]
) -> Optional[str]:
    """
    Emit an enhanced availability_requested trigger with calendar data.
    Called from organizer.py when availability_requested is detected.

    Returns the trigger path if successful, None otherwise.
    """
    try:
        intelligence = CalendarIntelligence()
        enhanced_payload = intelligence.enhance_availability_trigger(availability_payload)

        # Add email context
        enhanced_payload["message_id"] = email_data.get("id")
        enhanced_payload["subject"] = email_data.get("subject")
        enhanced_payload["requester"] = email_data.get("sender")

        return write_trigger(
            user_email,
            "availability_requested_enhanced",
            enhanced_payload,
            dedupe_key=make_dedupe_key("availability_requested_enhanced", user_email, email_data.get("id", "")),
            routing={"channel": "teams"},
        )
    except Exception as e:
        logger.error(f"Failed to emit enhanced availability trigger: {e}")
        # Fall back to basic trigger
        try:
            return write_trigger(
                user_email,
                "availability_requested",
                availability_payload,
                dedupe_key=make_dedupe_key("availability_requested", user_email, email_data.get("id", "")),
                routing={"channel": "teams"},
            )
        except Exception as e2:
            logger.error(f"Failed to emit basic availability trigger: {e2}")
            return None
