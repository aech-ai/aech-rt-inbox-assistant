"""Pydantic models for EA Working Memory state."""

from datetime import datetime
from enum import Enum
from typing import Any
import uuid

from pydantic import BaseModel, Field


class UrgencyLevel(str, Enum):
    """Urgency classification for items requiring attention."""

    IMMEDIATE = "immediate"  # Needs attention now
    TODAY = "today"  # Should handle today
    THIS_WEEK = "this_week"  # Can wait a few days
    SOMEDAY = "someday"  # Low priority/FYI


class ThreadStatus(str, Enum):
    """Status of an email thread."""

    ACTIVE = "active"  # Ongoing conversation
    AWAITING_REPLY = "awaiting_reply"  # User sent last message, waiting on others
    AWAITING_ACTION = "awaiting_action"  # Others sent, user needs to respond
    STALE = "stale"  # No activity for N days
    RESOLVED = "resolved"  # Thread concluded


class ContactRelationship(str, Enum):
    """Classification of contact relationship."""

    VIP = "vip"  # Important contact
    COLLEAGUE = "colleague"  # Internal team member
    CLIENT = "client"  # External client
    VENDOR = "vendor"  # Service provider
    RECRUITER = "recruiter"  # Recruiting contact
    UNKNOWN = "unknown"  # Not yet classified


class ObservationType(str, Enum):
    """Types of passive observations from email analysis."""

    PROJECT_MENTION = "project_mention"
    DECISION_MADE = "decision_made"
    DEADLINE_MENTIONED = "deadline_mentioned"
    PERSON_INTRODUCED = "person_introduced"
    STATUS_UPDATE = "status_update"
    MEETING_SCHEDULED = "meeting_scheduled"
    COMMITMENT_MADE = "commitment_made"
    CONTEXT_LEARNED = "context_learned"


# === Core Memory Entities ===


class ActiveThread(BaseModel):
    """An active email thread requiring awareness."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    conversation_id: str
    subject: str
    participants: list[str] = Field(default_factory=list)
    status: ThreadStatus = ThreadStatus.ACTIVE
    urgency: UrgencyLevel = UrgencyLevel.THIS_WEEK

    # Timeline
    started_at: datetime
    last_activity_at: datetime
    user_last_action_at: datetime | None = None

    # Context
    summary: str = ""  # AI-generated thread summary
    key_points: list[str] = Field(default_factory=list)
    pending_questions: list[str] = Field(default_factory=list)

    # Tracking
    message_count: int = 0
    user_is_cc: bool = False  # True if user is CC'd, not direct recipient
    needs_reply: bool = False
    reply_deadline: datetime | None = None

    # Metadata
    labels: list[str] = Field(default_factory=list)
    project_refs: list[str] = Field(default_factory=list)  # Project IDs
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Contact(BaseModel):
    """A person the user interacts with."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    email: str
    name: str | None = None
    organization: str | None = None
    relationship: ContactRelationship = ContactRelationship.UNKNOWN

    # Interaction history
    first_seen_at: datetime
    last_interaction_at: datetime
    total_interactions: int = 0
    user_initiated_count: int = 0  # Emails user sent TO them
    they_initiated_count: int = 0  # Emails they sent TO user
    cc_count: int = 0  # Times seen in CC

    # Context
    topics: list[str] = Field(default_factory=list)  # Common topics
    notes: str = ""  # AI-generated notes about this person

    # Flags
    is_vip: bool = False
    is_internal: bool = False

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Project(BaseModel):
    """An inferred project or initiative from email patterns."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: str = ""

    # Relationships
    related_threads: list[str] = Field(default_factory=list)  # Thread IDs
    participants: list[str] = Field(default_factory=list)  # Contact emails

    # Status
    status: str = "active"  # active, on_hold, completed
    confidence: float = 0.5  # How confident we are this is a real project

    # Timeline
    first_mentioned_at: datetime
    last_activity_at: datetime

    # Context
    key_decisions: list[str] = Field(default_factory=list)
    deadlines: list[dict[str, Any]] = Field(default_factory=list)  # {date, description}

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Observation(BaseModel):
    """A passive learning from email (especially CC'd emails)."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: ObservationType
    content: str

    # Source
    source_email_id: str
    source_thread_id: str | None = None

    # Relationships
    related_contacts: list[str] = Field(default_factory=list)
    related_projects: list[str] = Field(default_factory=list)

    # Context
    importance: float = 0.5  # 0-1 scale
    confidence: float = 0.5  # How confident in this observation

    # Timing
    observed_at: datetime = Field(default_factory=datetime.utcnow)
    relevant_until: datetime | None = None  # When this becomes stale


class PendingDecision(BaseModel):
    """A decision the user needs to make."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    # What needs deciding
    question: str
    context: str = ""
    options: list[str] = Field(default_factory=list)

    # Source
    source_email_id: str
    source_thread_id: str | None = None
    requester: str  # Email of person asking

    # Urgency
    urgency: UrgencyLevel = UrgencyLevel.THIS_WEEK
    deadline: datetime | None = None

    # Status
    is_resolved: bool = False
    resolution: str | None = None
    resolved_at: datetime | None = None

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Commitment(BaseModel):
    """Something the user committed to do."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    description: str
    to_whom: str  # Email address

    # Source
    source_email_id: str

    # Timing
    committed_at: datetime
    due_by: datetime | None = None

    # Status
    is_completed: bool = False
    completed_at: datetime | None = None

    created_at: datetime = Field(default_factory=datetime.utcnow)


# === AI Analysis Output ===


class EmailAnalysis(BaseModel):
    """AI-extracted intelligence from an email for working memory updates."""

    # Thread analysis
    thread_summary_update: str | None = None
    key_points: list[str] = Field(default_factory=list)
    pending_questions: list[str] = Field(default_factory=list)

    # Decisions requested
    decisions_requested: list[dict[str, Any]] = Field(
        default_factory=list
    )  # {question, context, options, deadline}

    # Commitments made
    commitments_made: list[dict[str, Any]] = Field(
        default_factory=list
    )  # {description, to_whom, due_by}

    # Observations (especially for CC emails)
    observations: list[dict[str, Any]] = Field(
        default_factory=list
    )  # {type, content, importance}

    # Project references
    project_mentions: list[str] = Field(default_factory=list)  # Project names

    # Contact insights
    contact_updates: list[dict[str, Any]] = Field(
        default_factory=list
    )  # {email, relationship_hint, topics}

    # Urgency assessment
    suggested_urgency: UrgencyLevel = UrgencyLevel.THIS_WEEK
    needs_reply: bool = False
    reply_deadline: str | None = None


# === Aggregate Views ===


class WorkingMemorySnapshot(BaseModel):
    """Complete snapshot of current working memory state."""

    # Active items
    active_threads: list[ActiveThread] = Field(default_factory=list)
    pending_decisions: list[PendingDecision] = Field(default_factory=list)
    open_commitments: list[Commitment] = Field(default_factory=list)

    # Knowledge base
    contacts: list[Contact] = Field(default_factory=list)
    projects: list[Project] = Field(default_factory=list)
    recent_observations: list[Observation] = Field(default_factory=list)

    # Summary stats
    threads_needing_reply: int = 0
    urgent_items_count: int = 0
    overdue_commitments_count: int = 0

    # Metadata
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    user_email: str = ""
