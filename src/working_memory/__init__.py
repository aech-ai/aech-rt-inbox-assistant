"""Working Memory module for EA cognitive state management."""

from .models import (
    UrgencyLevel,
    ThreadStatus,
    ContactRelationship,
    ObservationType,
    ActiveThread,
    Contact,
    Project,
    Observation,
    PendingDecision,
    Commitment,
    WorkingMemorySnapshot,
    EmailAnalysis,
    outlook_web_link,
    outlook_web_url,
)
from .updater import WorkingMemoryUpdater
from .engine import WorkingMemoryEngine, run_memory_engine_cycle

__all__ = [
    # Models
    "UrgencyLevel",
    "ThreadStatus",
    "ContactRelationship",
    "ObservationType",
    "ActiveThread",
    "Contact",
    "Project",
    "Observation",
    "PendingDecision",
    "Commitment",
    "WorkingMemorySnapshot",
    "EmailAnalysis",
    # URL utilities
    "outlook_web_link",
    "outlook_web_url",
    # Updater
    "WorkingMemoryUpdater",
    # Engine
    "WorkingMemoryEngine",
    "run_memory_engine_cycle",
]
