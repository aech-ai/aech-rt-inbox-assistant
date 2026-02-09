"""
Calendar Intelligence module for Executive Assistant.

Enhances availability triggers from email detection with real calendar data.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from pydantic_ai import Agent
from zoneinfo import ZoneInfo

from .calendar import CalendarClient, TimeSlot
from .model_utils import parse_model_string, get_model_settings
from .triggers import make_dedupe_key, write_trigger

logger = logging.getLogger(__name__)


class AvailabilityWindowDecision(BaseModel):
    """Typed scheduling window inferred from availability request."""

    search_start_iso: str = Field(description="Inclusive ISO8601 datetime with timezone")
    search_end_iso: str = Field(description="Exclusive ISO8601 datetime with timezone")
    duration_minutes: int = Field(default=30, ge=5, le=480)
    timezone: str = Field(description="IANA timezone")
    interpretation_note: str = Field(default="", description="Short reasoning note")


class AvailabilityRecommendation(BaseModel):
    """Typed recommendation for how to respond to a scheduling request."""

    recommendation: str = Field(description="What the user should do next")
    suggested_reply: str | None = Field(default=None, description="Draft one-paragraph reply")


def _build_window_agent() -> Agent[None, AvailabilityWindowDecision]:
    model_string = os.getenv(
        "CALENDAR_INTELLIGENCE_MODEL",
        os.getenv("MODEL_NAME", "openai-responses:gpt-5-mini"),
    )
    model_name, _ = parse_model_string(model_string)
    model_settings = get_model_settings(model_string)

    instructions = """
You are a scheduling planner.
Input is JSON describing an availability request.
Return AvailabilityWindowDecision.

Rules:
- Determine the best search window from time_window text and current time.
- If time_window is missing, use now through now + 7 days.
- Preserve or infer an IANA timezone (prefer provided timezone).
- Keep duration_minutes aligned with user request when provided.
- search_end_iso must be after search_start_iso.
- Return ISO datetimes with timezone offsets.
"""

    return Agent(
        model_name,
        output_type=AvailabilityWindowDecision,
        instructions=instructions,
        model_settings=model_settings,
    )


def _build_recommendation_agent() -> Agent[None, AvailabilityRecommendation]:
    model_string = os.getenv(
        "CALENDAR_INTELLIGENCE_MODEL",
        os.getenv("MODEL_NAME", "openai-responses:gpt-5-mini"),
    )
    model_name, _ = parse_model_string(model_string)
    model_settings = get_model_settings(model_string)

    instructions = """
You are an executive scheduling assistant.
Input is JSON containing request details, actual free slots, proposed slot conflict checks,
and optional findMeetingTimes suggestions.

Return AvailabilityRecommendation:
- recommendation: direct next step guidance
- suggested_reply: optional concise reply draft in professional tone

Be concrete and role-aware: this assistant works for the principal mailbox owner.
"""

    return Agent(
        model_name,
        output_type=AvailabilityRecommendation,
        instructions=instructions,
        model_settings=model_settings,
    )


class CalendarIntelligence:
    """
    Integrates calendar data with email availability requests.
    Enhances availability triggers with actual free slots from Graph API.
    """

    def __init__(self, calendar_client: Optional[CalendarClient] = None):
        self.calendar = calendar_client or CalendarClient()
        self.user_email = self.calendar.user_email
        self._window_agent: Agent[None, AvailabilityWindowDecision] | None = None
        self._recommendation_agent: Agent[None, AvailabilityRecommendation] | None = None

    def _get_window_agent(self) -> Agent[None, AvailabilityWindowDecision]:
        if self._window_agent is None:
            self._window_agent = _build_window_agent()
        return self._window_agent

    def _get_recommendation_agent(self) -> Agent[None, AvailabilityRecommendation]:
        if self._recommendation_agent is None:
            self._recommendation_agent = _build_recommendation_agent()
        return self._recommendation_agent

    @staticmethod
    def _parse_iso_datetime(value: str, field_name: str) -> datetime:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception as exc:
            raise ValueError(f"Invalid datetime in {field_name}: {value}") from exc

        if dt.tzinfo is None:
            raise ValueError(f"{field_name} must include timezone: {value}")
        return dt

    async def enhance_availability_trigger(
        self,
        original_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Enhance an availability_requested payload with actual calendar availability.

        Adds:
        - actual_free_slots: List of available time windows from Graph API
        - proposed_slot_analysis: Conflict check on proposed times
        - recommendation: LLM-generated next-step guidance
        """
        enhanced = original_payload.copy()

        raw_duration = original_payload.get("duration_minutes")
        if raw_duration is None:
            requested_duration = 30
        else:
            requested_duration = int(raw_duration)
            if requested_duration <= 0:
                raise ValueError(f"duration_minutes must be > 0, got {raw_duration}")

        proposed_slots = original_payload.get("proposed_slots") or []
        if not isinstance(proposed_slots, list):
            raise ValueError("proposed_slots must be a list of ISO datetime strings")

        requested_timezone = original_payload.get("timezone") or self.calendar.get_user_timezone()
        time_window = original_payload.get("time_window")
        constraints = original_payload.get("constraints")
        requester_email = original_payload.get("requester")

        window_context = {
            "time_window": time_window,
            "duration_minutes": requested_duration,
            "timezone": requested_timezone,
            "constraints": constraints,
            "requester": requester_email,
            "now_utc": datetime.now(dt_timezone.utc).isoformat(),
            "principal_user_email": self.user_email,
        }

        window_result = await self._get_window_agent().run(
            json.dumps(window_context, default=str)
        )
        window = window_result.output

        try:
            tz = ZoneInfo(window.timezone)
        except Exception as exc:
            raise ValueError(f"Invalid timezone from availability planner: {window.timezone}") from exc

        search_start = self._parse_iso_datetime(window.search_start_iso, "search_start_iso").astimezone(tz)
        search_end = self._parse_iso_datetime(window.search_end_iso, "search_end_iso").astimezone(tz)

        if search_end <= search_start:
            raise ValueError(
                f"Availability search window is invalid: start={search_start.isoformat()} end={search_end.isoformat()}"
            )

        duration_minutes = int(window.duration_minutes)
        if duration_minutes <= 0:
            raise ValueError(f"duration_minutes must be > 0, got {duration_minutes}")

        schedule_results = self.calendar.get_schedule(search_start, search_end)
        free_slots = self._extract_free_slots(
            schedule_results,
            search_start,
            search_end,
            duration_minutes,
            tz,
        )

        slot_analysis: list[dict[str, Any]] = []
        for slot_str in proposed_slots:
            if not isinstance(slot_str, str):
                raise ValueError(f"proposed slot must be string, got: {slot_str!r}")
            slot_start = self._parse_iso_datetime(slot_str, "proposed_slot")
            slot_end = slot_start + timedelta(minutes=duration_minutes)
            is_available = self.calendar.check_availability(slot_start, slot_end)
            slot_analysis.append(
                {
                    "proposed": slot_str,
                    "available": is_available,
                    "message": "Available" if is_available else "Conflicts with existing event",
                }
            )

        meeting_suggestions: list[dict[str, Any]] = []
        if requester_email:
            suggestions = self.calendar.find_meeting_times(
                attendees=[requester_email],
                duration_minutes=duration_minutes,
                start=search_start,
                end=search_end,
                max_candidates=5,
            )
            for suggestion in suggestions:
                meeting_suggestions.append(
                    {
                        "start": suggestion.start.isoformat(),
                        "end": suggestion.end.isoformat(),
                        "confidence": suggestion.confidence,
                    }
                )

        formatted_slots = [
            {
                "start": slot.start.isoformat(),
                "end": slot.end.isoformat(),
                "duration_minutes": slot.duration_minutes,
                "formatted": self._format_slot_human(slot, tz),
            }
            for slot in free_slots[:10]
        ]

        recommendation_context = {
            "request": {
                "time_window": time_window,
                "constraints": constraints,
                "requester": requester_email,
                "requested_timezone": requested_timezone,
            },
            "window": window.model_dump(),
            "actual_free_slots": formatted_slots,
            "proposed_slot_analysis": slot_analysis,
            "meeting_time_suggestions": meeting_suggestions,
        }
        recommendation_result = await self._get_recommendation_agent().run(
            json.dumps(recommendation_context, default=str)
        )
        recommendation = recommendation_result.output

        enhanced["actual_free_slots"] = formatted_slots
        enhanced["proposed_slot_analysis"] = slot_analysis
        enhanced["meeting_time_suggestions"] = meeting_suggestions
        enhanced["calendar_checked_at"] = datetime.now(dt_timezone.utc).isoformat()
        enhanced["timezone"] = window.timezone
        enhanced["duration_minutes"] = duration_minutes
        enhanced["calendar_search_start"] = search_start.isoformat()
        enhanced["calendar_search_end"] = search_end.isoformat()
        enhanced["calendar_window_note"] = window.interpretation_note
        enhanced["recommendation"] = recommendation.recommendation
        enhanced["suggested_reply"] = recommendation.suggested_reply

        return enhanced

    def _extract_free_slots(
        self,
        schedule_results: List,
        start: datetime,
        end: datetime,
        min_duration_minutes: int,
        tz: ZoneInfo,
    ) -> List[TimeSlot]:
        """Extract free time slots from getSchedule results."""
        if not schedule_results:
            return []

        working_hours = self.calendar.get_working_hours()
        work_start_parts = working_hours.start_time.split(":")
        work_end_parts = working_hours.end_time.split(":")
        if len(work_start_parts) < 2 or len(work_end_parts) < 2:
            raise ValueError(
                "Invalid working hours format from Graph mailbox settings: "
                f"start={working_hours.start_time} end={working_hours.end_time}"
            )

        work_start_hour = int(work_start_parts[0])
        work_end_hour = int(work_end_parts[0])
        work_days = {str(day).lower() for day in working_hours.days_of_week if str(day).strip()}
        if not work_days:
            raise ValueError("Working hours returned empty days_of_week")

        busy_periods: list[TimeSlot] = []
        for result in schedule_results:
            for item in result.schedule_items:
                if item.status in ("busy", "oof", "tentative"):
                    busy_periods.append(TimeSlot(start=item.start, end=item.end))

        busy_periods = self._merge_time_slots(busy_periods)

        free_slots: list[TimeSlot] = []
        current_date = start.date()
        end_date = end.date()

        while current_date <= end_date:
            day_name = current_date.strftime("%A").lower()
            if day_name not in work_days:
                current_date += timedelta(days=1)
                continue

            day_start = datetime(
                current_date.year,
                current_date.month,
                current_date.day,
                work_start_hour,
                0,
                0,
                tzinfo=tz,
            )
            day_end = datetime(
                current_date.year,
                current_date.month,
                current_date.day,
                work_end_hour,
                0,
                0,
                tzinfo=tz,
            )

            window_start = max(day_start, start.astimezone(tz) if start.tzinfo else start.replace(tzinfo=tz))
            window_end = min(day_end, end.astimezone(tz) if end.tzinfo else end.replace(tzinfo=tz))

            if window_start >= window_end:
                current_date += timedelta(days=1)
                continue

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

        sorted_slots = sorted(slots, key=lambda slot: slot.start)
        merged = [sorted_slots[0]]

        for slot in sorted_slots[1:]:
            last = merged[-1]
            if slot.start <= last.end:
                new_end = max(last.end, slot.end)
                merged[-1] = TimeSlot(start=last.start, end=new_end)
            else:
                merged.append(slot)

        return merged

    def _format_slot_human(self, slot: TimeSlot, tz: ZoneInfo) -> str:
        """Format a time slot for human-readable display."""
        start_local = slot.start.astimezone(tz) if slot.start.tzinfo else slot.start.replace(tzinfo=tz)
        end_local = slot.end.astimezone(tz) if slot.end.tzinfo else slot.end.replace(tzinfo=tz)

        day = start_local.strftime("%A, %B %d")
        start_time = start_local.strftime("%I:%M %p").lstrip("0")
        end_time = end_local.strftime("%I:%M %p").lstrip("0")

        return f"{day} from {start_time} to {end_time}"


async def emit_enhanced_availability_trigger(
    user_email: str,
    email_data: Dict[str, Any],
    availability_payload: Dict[str, Any],
) -> str:
    """
    Emit an enhanced availability_requested trigger with calendar data.
    Called from organizer.py when availability_requested is detected.

    Returns the trigger path if successful.
    """
    intelligence = CalendarIntelligence()
    enhanced_payload = await intelligence.enhance_availability_trigger(availability_payload)

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
