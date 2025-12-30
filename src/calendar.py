"""
Calendar module for Executive Assistant.

Direct Graph API wrappers for calendar operations - no local database sync.
The EA interacts with the user's actual calendar in real-time.
"""

import logging
import os
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import List, Dict, Any, Optional

import requests
from pydantic import BaseModel, Field
from zoneinfo import ZoneInfo

from aech_cli_msgraph.graph import GraphClient

logger = logging.getLogger(__name__)


# =============================================================================
# Pydantic Models
# =============================================================================

class Attendee(BaseModel):
    """Attendee information."""
    email: str
    name: Optional[str] = None
    response: str = "none"  # none, accepted, tentative, declined
    type: str = "required"  # required, optional, resource


class CalendarEvent(BaseModel):
    """Calendar event representation."""
    event_id: str
    subject: str = ""
    start: datetime
    end: datetime
    is_all_day: bool = False
    location: Optional[str] = None
    organizer_email: Optional[str] = None
    organizer_name: Optional[str] = None
    attendees: List[Attendee] = Field(default_factory=list)
    response_status: str = "none"
    show_as: str = "busy"  # free, tentative, busy, oof, workingElsewhere
    is_online_meeting: bool = False
    online_meeting_url: Optional[str] = None
    body_preview: Optional[str] = None
    sensitivity: str = "normal"
    importance: str = "normal"


class TimeSlot(BaseModel):
    """A time slot (free or busy)."""
    start: datetime
    end: datetime

    @property
    def duration_minutes(self) -> int:
        return int((self.end - self.start).total_seconds() / 60)


class ScheduleItem(BaseModel):
    """A busy period from getSchedule."""
    status: str  # free, tentative, busy, oof, workingElsewhere
    start: datetime
    end: datetime
    subject: Optional[str] = None
    location: Optional[str] = None


class AvailabilityResult(BaseModel):
    """Free/busy result for a user."""
    email: str
    availability_view: Optional[str] = None  # String of 0/1/2/3/4 for time slots
    schedule_items: List[ScheduleItem] = Field(default_factory=list)
    working_hours: Optional[Dict[str, Any]] = None


class MeetingTimeSuggestion(BaseModel):
    """A suggested meeting time from findMeetingTimes."""
    start: datetime
    end: datetime
    confidence: float = 0.0
    organizer_availability: str = "unknown"
    attendee_availability: List[Dict[str, str]] = Field(default_factory=list)


class WorkingHours(BaseModel):
    """User's working hours configuration."""
    timezone: str
    days_of_week: List[str] = Field(default_factory=list)  # monday, tuesday, etc.
    start_time: str = "09:00:00"  # HH:MM:SS
    end_time: str = "17:00:00"


# =============================================================================
# CalendarClient
# =============================================================================

class CalendarClient:
    """
    Calendar operations via Microsoft Graph API.

    Uses direct API calls - no local database sync.
    """

    def __init__(self):
        self.user_email = os.getenv("DELEGATED_USER")
        if not self.user_email:
            raise ValueError("DELEGATED_USER environment variable must be set")

        self._graph = GraphClient()
        self._default_timezone = os.getenv("DEFAULT_TIMEZONE", "UTC")

    def _get_headers(self) -> Dict[str, str]:
        """Get auth headers from Graph client."""
        return self._graph._get_headers()

    def _get_base_path(self) -> str:
        """Get base path for calendar API calls."""
        return f"https://graph.microsoft.com/v1.0/users/{self.user_email}"

    # =========================================================================
    # Working Hours
    # =========================================================================

    def get_working_hours(self) -> WorkingHours:
        """
        Get user's working hours from mailbox settings.
        Falls back to defaults if not configured.
        """
        try:
            headers = self._get_headers()
            url = f"{self._get_base_path()}/mailboxSettings/workingHours"
            resp = requests.get(url, headers=headers)

            if resp.ok:
                data = resp.json()
                return WorkingHours(
                    timezone=data.get("timeZone", {}).get("name", self._default_timezone),
                    days_of_week=[d.lower() for d in data.get("daysOfWeek", ["monday", "tuesday", "wednesday", "thursday", "friday"])],
                    start_time=data.get("startTime", "09:00:00.0000000")[:8],
                    end_time=data.get("endTime", "17:00:00.0000000")[:8],
                )
            else:
                logger.warning(f"Could not fetch working hours: {resp.status_code}")
        except Exception as e:
            logger.warning(f"Error fetching working hours: {e}")

        # Return defaults
        return WorkingHours(
            timezone=self._default_timezone,
            days_of_week=["monday", "tuesday", "wednesday", "thursday", "friday"],
            start_time="09:00:00",
            end_time="17:00:00",
        )

    def get_user_timezone(self) -> str:
        """Get user's timezone."""
        return self.get_working_hours().timezone

    # =========================================================================
    # Calendar View (Events)
    # =========================================================================

    def get_calendar_view(
        self,
        start: datetime,
        end: datetime,
        max_results: int = 100
    ) -> List[CalendarEvent]:
        """
        Get calendar events in a date range.
        Uses calendarView which expands recurring events.
        """
        headers = self._get_headers()
        base_path = self._get_base_path()

        # Ensure timezone info
        if start.tzinfo is None:
            start = start.replace(tzinfo=dt_timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=dt_timezone.utc)

        # Use Z suffix for UTC to avoid URL encoding issues with +00:00
        start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ") if start.tzinfo == dt_timezone.utc else start.isoformat()
        end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ") if end.tzinfo == dt_timezone.utc else end.isoformat()

        url = (
            f"{base_path}/calendarView"
            f"?startDateTime={start_str}"
            f"&endDateTime={end_str}"
            f"&$top={max_results}"
            f"&$orderby=start/dateTime"
            f"&$select=id,subject,start,end,isAllDay,location,organizer,attendees,"
            f"responseStatus,showAs,isOnlineMeeting,onlineMeeting,bodyPreview,sensitivity,importance"
        )

        events = []
        while url:
            resp = requests.get(url, headers=headers)
            if not resp.ok:
                logger.error(f"calendarView failed: {resp.status_code} - {resp.text}")
                break

            data = resp.json()
            for event in data.get("value", []):
                events.append(self._parse_event(event))

            url = data.get("@odata.nextLink")
            if len(events) >= max_results:
                break

        return events[:max_results]

    def _parse_event(self, event: Dict[str, Any]) -> CalendarEvent:
        """Parse a Graph API event into CalendarEvent model."""
        # Parse attendees
        attendees = []
        for att in event.get("attendees") or []:
            attendees.append(Attendee(
                email=att.get("emailAddress", {}).get("address", ""),
                name=att.get("emailAddress", {}).get("name"),
                response=att.get("status", {}).get("response", "none"),
                type=att.get("type", "required"),
            ))

        # Parse start/end times
        start_data = event.get("start", {})
        end_data = event.get("end", {})

        start_dt = datetime.fromisoformat(start_data.get("dateTime", "").replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_data.get("dateTime", "").replace("Z", "+00:00"))

        # Parse location
        location = event.get("location", {})
        location_str = location.get("displayName") if location else None

        # Parse organizer
        organizer = event.get("organizer", {}).get("emailAddress", {})

        # Parse online meeting
        online_meeting = event.get("onlineMeeting") or {}

        return CalendarEvent(
            event_id=event.get("id", ""),
            subject=event.get("subject", ""),
            start=start_dt,
            end=end_dt,
            is_all_day=event.get("isAllDay", False),
            location=location_str,
            organizer_email=organizer.get("address"),
            organizer_name=organizer.get("name"),
            attendees=attendees,
            response_status=event.get("responseStatus", {}).get("response", "none"),
            show_as=event.get("showAs", "busy"),
            is_online_meeting=event.get("isOnlineMeeting", False),
            online_meeting_url=online_meeting.get("joinUrl"),
            body_preview=event.get("bodyPreview"),
            sensitivity=event.get("sensitivity", "normal"),
            importance=event.get("importance", "normal"),
        )

    def get_todays_agenda(self, timezone: Optional[str] = None) -> List[CalendarEvent]:
        """Get today's events in user's timezone."""
        tz_str = timezone or self.get_user_timezone()
        tz = ZoneInfo(tz_str)

        now = datetime.now(tz)
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1)

        return self.get_calendar_view(start_of_day, end_of_day)

    def get_upcoming_events(self, hours: int = 24, limit: int = 20) -> List[CalendarEvent]:
        """Get events in the next N hours."""
        now = datetime.now(dt_timezone.utc)
        end = now + timedelta(hours=hours)

        events = self.get_calendar_view(now, end, max_results=limit)
        return events

    # =========================================================================
    # Free/Busy (getSchedule)
    # =========================================================================

    def get_schedule(
        self,
        start: datetime,
        end: datetime,
        emails: Optional[List[str]] = None
    ) -> List[AvailabilityResult]:
        """
        Get free/busy information for one or more users.
        Uses POST /calendar/getSchedule.
        """
        headers = self._get_headers()
        base_path = self._get_base_path()

        # Default to self if no emails provided
        schedule_emails = emails or [self.user_email]

        # Ensure timezone info
        if start.tzinfo is None:
            start = start.replace(tzinfo=dt_timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=dt_timezone.utc)

        data = {
            "schedules": schedule_emails,
            "startTime": {
                "dateTime": start.isoformat(),
                "timeZone": "UTC"
            },
            "endTime": {
                "dateTime": end.isoformat(),
                "timeZone": "UTC"
            },
            "availabilityViewInterval": 30  # 30-minute slots
        }

        url = f"{base_path}/calendar/getSchedule"
        resp = requests.post(url, json=data, headers=headers)

        if not resp.ok:
            logger.error(f"getSchedule failed: {resp.status_code} - {resp.text}")
            return []

        results = []
        for schedule in resp.json().get("value", []):
            items = []
            for item in schedule.get("scheduleItems", []):
                item_start = item.get("start", {})
                item_end = item.get("end", {})
                items.append(ScheduleItem(
                    status=item.get("status", "busy"),
                    start=datetime.fromisoformat(item_start.get("dateTime", "").replace("Z", "+00:00")),
                    end=datetime.fromisoformat(item_end.get("dateTime", "").replace("Z", "+00:00")),
                    subject=item.get("subject"),
                    location=item.get("location"),
                ))

            results.append(AvailabilityResult(
                email=schedule.get("scheduleId", ""),
                availability_view=schedule.get("availabilityView"),
                schedule_items=items,
                working_hours=schedule.get("workingHours"),
            ))

        return results

    def check_availability(self, start: datetime, end: datetime) -> bool:
        """Check if the user is free during a time slot."""
        results = self.get_schedule(start, end)
        if not results:
            return True  # Assume free if we can't check

        # Check if any busy items overlap
        for result in results:
            for item in result.schedule_items:
                if item.status in ("busy", "oof", "tentative"):
                    # Check overlap
                    if item.start < end and item.end > start:
                        return False

        return True

    # =========================================================================
    # Find Meeting Times
    # =========================================================================

    def find_meeting_times(
        self,
        attendees: List[str],
        duration_minutes: int = 30,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        max_candidates: int = 10
    ) -> List[MeetingTimeSuggestion]:
        """
        Find available meeting times for all attendees.
        Uses POST /findMeetingTimes.
        """
        headers = self._get_headers()
        base_path = self._get_base_path()

        # Default time window: next 7 days
        if start is None:
            start = datetime.now(dt_timezone.utc)
        if end is None:
            end = start + timedelta(days=7)

        # Ensure timezone info
        if start.tzinfo is None:
            start = start.replace(tzinfo=dt_timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=dt_timezone.utc)

        attendee_list = [
            {"emailAddress": {"address": email}, "type": "required"}
            for email in attendees
        ]

        data = {
            "attendees": attendee_list,
            "meetingDuration": f"PT{duration_minutes}M",
            "maxCandidates": max_candidates,
            "isOrganizerOptional": False,
            "timeConstraint": {
                "activityDomain": "work",
                "timeSlots": [{
                    "start": {"dateTime": start.isoformat(), "timeZone": "UTC"},
                    "end": {"dateTime": end.isoformat(), "timeZone": "UTC"},
                }]
            },
            "returnSuggestionReasons": True,
        }

        url = f"{base_path}/findMeetingTimes"
        resp = requests.post(url, json=data, headers=headers)

        if not resp.ok:
            logger.error(f"findMeetingTimes failed: {resp.status_code} - {resp.text}")
            return []

        result = resp.json()
        suggestions = []

        for suggestion in result.get("meetingTimeSuggestions", []):
            meeting_slot = suggestion.get("meetingTimeSlot", {})
            start_data = meeting_slot.get("start", {})
            end_data = meeting_slot.get("end", {})

            attendee_avail = []
            for att in suggestion.get("attendeeAvailability", []):
                attendee_avail.append({
                    "email": att.get("attendee", {}).get("emailAddress", {}).get("address", ""),
                    "availability": att.get("availability", "unknown"),
                })

            suggestions.append(MeetingTimeSuggestion(
                start=datetime.fromisoformat(start_data.get("dateTime", "").replace("Z", "+00:00")),
                end=datetime.fromisoformat(end_data.get("dateTime", "").replace("Z", "+00:00")),
                confidence=suggestion.get("confidence", 0.0),
                organizer_availability=suggestion.get("organizerAvailability", "unknown"),
                attendee_availability=attendee_avail,
            ))

        return suggestions

    # =========================================================================
    # Create Event
    # =========================================================================

    def create_event(
        self,
        subject: str,
        start: datetime,
        end: Optional[datetime] = None,
        duration_minutes: int = 30,
        attendees: Optional[List[str]] = None,
        location: Optional[str] = None,
        body: Optional[str] = None,
        is_online_meeting: bool = False,
        send_invitations: bool = False,
    ) -> CalendarEvent:
        """
        Create a calendar event.

        By default, does NOT send invitations (send_invitations=False).
        The event is created as a placeholder that the user can review.
        """
        headers = self._get_headers()
        base_path = self._get_base_path()

        # Ensure timezone info
        if start.tzinfo is None:
            start = start.replace(tzinfo=dt_timezone.utc)

        # Calculate end time
        if end is None:
            end = start + timedelta(minutes=duration_minutes)
        elif end.tzinfo is None:
            end = end.replace(tzinfo=dt_timezone.utc)

        tz_str = self.get_user_timezone()

        data: Dict[str, Any] = {
            "subject": subject,
            "start": {
                "dateTime": start.strftime("%Y-%m-%dT%H:%M:%S"),
                "timeZone": tz_str,
            },
            "end": {
                "dateTime": end.strftime("%Y-%m-%dT%H:%M:%S"),
                "timeZone": tz_str,
            },
        }

        if attendees:
            data["attendees"] = [
                {"emailAddress": {"address": email}, "type": "required"}
                for email in attendees
            ]

        if location:
            data["location"] = {"displayName": location}

        if body:
            data["body"] = {"contentType": "text", "content": body}

        if is_online_meeting:
            data["isOnlineMeeting"] = True
            data["onlineMeetingProvider"] = "teamsForBusiness"

        # Control whether invitations are sent
        url = f"{base_path}/events"
        if not send_invitations:
            # Adding this header prevents sending invites
            headers["Prefer"] = 'outlook.timezone="UTC"'

        resp = requests.post(url, json=data, headers=headers)

        if not resp.ok:
            logger.error(f"create event failed: {resp.status_code} - {resp.text}")
            raise Exception(f"Failed to create event: {resp.text}")

        return self._parse_event(resp.json())

    # =========================================================================
    # Update/Delete Event
    # =========================================================================

    def update_event(
        self,
        event_id: str,
        subject: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        location: Optional[str] = None,
    ) -> CalendarEvent:
        """Update an existing calendar event."""
        headers = self._get_headers()
        base_path = self._get_base_path()

        data: Dict[str, Any] = {}

        if subject is not None:
            data["subject"] = subject

        tz_str = self.get_user_timezone()

        if start is not None:
            if start.tzinfo is None:
                start = start.replace(tzinfo=dt_timezone.utc)
            data["start"] = {
                "dateTime": start.strftime("%Y-%m-%dT%H:%M:%S"),
                "timeZone": tz_str,
            }

        if end is not None:
            if end.tzinfo is None:
                end = end.replace(tzinfo=dt_timezone.utc)
            data["end"] = {
                "dateTime": end.strftime("%Y-%m-%dT%H:%M:%S"),
                "timeZone": tz_str,
            }

        if location is not None:
            data["location"] = {"displayName": location}

        url = f"{base_path}/events/{event_id}"
        resp = requests.patch(url, json=data, headers=headers)

        if not resp.ok:
            logger.error(f"update event failed: {resp.status_code} - {resp.text}")
            raise Exception(f"Failed to update event: {resp.text}")

        return self._parse_event(resp.json())

    def delete_event(self, event_id: str) -> bool:
        """Delete a calendar event."""
        headers = self._get_headers()
        base_path = self._get_base_path()

        url = f"{base_path}/events/{event_id}"
        resp = requests.delete(url, headers=headers)

        if not resp.ok:
            logger.error(f"delete event failed: {resp.status_code} - {resp.text}")
            return False

        return True
