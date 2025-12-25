"""Working Memory Engine - periodic processing for state maintenance and nudges."""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from ..database import get_connection
from ..triggers import make_dedupe_key, write_trigger
from .models import ThreadStatus, UrgencyLevel

logger = logging.getLogger(__name__)


class WorkingMemoryEngine:
    """
    Periodic engine for working memory maintenance.

    Runs alongside the email polling loop to:
    - Re-evaluate urgency as time passes
    - Mark stale threads
    - Generate proactive nudges
    - Prune old data
    """

    def __init__(self, user_email: str):
        self.user_email = user_email
        self._last_run: datetime | None = None

    async def run_cycle(self) -> dict[str, int]:
        """
        Run one cycle of working memory maintenance.

        Returns stats about what was updated.
        """
        stats = {
            "threads_marked_stale": 0,
            "urgency_escalated": 0,
            "observations_pruned": 0,
            "nudges_emitted": 0,
        }

        now = datetime.now(timezone.utc)
        self._last_run = now

        conn = get_connection()
        try:
            # 1. Mark stale threads
            stats["threads_marked_stale"] = self._mark_stale_threads(conn, now)

            # 2. Escalate urgency for overdue items
            stats["urgency_escalated"] = self._escalate_urgency(conn, now)

            # 3. Prune old observations
            stats["observations_pruned"] = self._prune_observations(conn, now)

            conn.commit()

            # 4. Generate and emit nudges (after commit so we see current state)
            stats["nudges_emitted"] = self._emit_nudges(now)

        except Exception as e:
            logger.error(f"Working memory engine cycle failed: {e}")
            conn.rollback()
        finally:
            conn.close()

        if any(stats.values()):
            logger.info(f"Working memory engine cycle: {stats}")

        return stats

    def _mark_stale_threads(self, conn, now: datetime) -> int:
        """Mark threads as stale after N days of no activity."""
        stale_days = int(os.getenv("WM_STALE_THRESHOLD_DAYS", "3"))
        stale_threshold = (now - timedelta(days=stale_days)).isoformat()

        result = conn.execute(
            """
            UPDATE wm_threads
            SET status = ?, updated_at = ?
            WHERE status = ?
            AND last_activity_at < ?
            """,
            (
                ThreadStatus.STALE.value,
                now.isoformat(),
                ThreadStatus.ACTIVE.value,
                stale_threshold,
            ),
        )
        return result.rowcount

    def _escalate_urgency(self, conn, now: datetime) -> int:
        """Escalate urgency for threads awaiting reply too long."""
        escalation_days = int(os.getenv("WM_URGENCY_ESCALATION_DAYS", "2"))
        escalate_threshold = (now - timedelta(days=escalation_days)).isoformat()

        # Escalate threads needing reply
        result = conn.execute(
            """
            UPDATE wm_threads
            SET urgency = ?, updated_at = ?
            WHERE needs_reply = 1
            AND status NOT IN (?, ?)
            AND urgency IN (?, ?)
            AND last_activity_at < ?
            """,
            (
                UrgencyLevel.TODAY.value,
                now.isoformat(),
                ThreadStatus.RESOLVED.value,
                ThreadStatus.STALE.value,
                UrgencyLevel.THIS_WEEK.value,
                UrgencyLevel.SOMEDAY.value,
                escalate_threshold,
            ),
        )
        count = result.rowcount

        # Escalate pending decisions
        result = conn.execute(
            """
            UPDATE wm_decisions
            SET urgency = ?, updated_at = ?
            WHERE is_resolved = 0
            AND urgency IN (?, ?)
            AND created_at < ?
            """,
            (
                UrgencyLevel.TODAY.value,
                now.isoformat(),
                UrgencyLevel.THIS_WEEK.value,
                UrgencyLevel.SOMEDAY.value,
                escalate_threshold,
            ),
        )
        count += result.rowcount

        return count

    def _prune_observations(self, conn, now: datetime) -> int:
        """Prune observations older than retention period."""
        retention_days = int(os.getenv("WM_OBSERVATION_RETENTION_DAYS", "30"))
        prune_threshold = (now - timedelta(days=retention_days)).isoformat()

        result = conn.execute(
            "DELETE FROM wm_observations WHERE observed_at < ?",
            (prune_threshold,),
        )
        return result.rowcount

    def _emit_nudges(self, now: datetime) -> int:
        """Generate and emit proactive nudges."""
        nudges = self._generate_nudges(now)
        emitted = 0

        for nudge in nudges:
            nudge_type = nudge.get("type", "working_memory_nudge")
            nudge_id = (
                nudge.get("thread_id")
                or nudge.get("commitment_id")
                or nudge.get("decision_id")
                or str(hash(nudge.get("message", "")))
            )

            dedupe_key = make_dedupe_key(
                f"wm_nudge_{nudge_type}",
                self.user_email,
                nudge_id,
            )

            write_trigger(
                self.user_email,
                "working_memory_nudge",
                nudge,
                dedupe_key=dedupe_key,
                routing={"channel": "teams"},
            )
            emitted += 1

        return emitted

    def _generate_nudges(self, now: datetime) -> list[dict[str, Any]]:
        """Analyze working memory and generate appropriate nudges."""
        nudges: list[dict[str, Any]] = []
        conn = get_connection()

        try:
            # Check for overdue replies
            nudges.extend(self._check_overdue_replies(conn, now))

            # Check for overdue commitments
            nudges.extend(self._check_overdue_commitments(conn, now))

            # Check for stale urgent threads
            nudges.extend(self._check_stale_urgent_threads(conn, now))

            # Check for unanswered decisions
            nudges.extend(self._check_pending_decisions(conn, now))

        finally:
            conn.close()

        return nudges

    def _check_overdue_replies(
        self,
        conn,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Find threads awaiting reply for too long."""
        nudges: list[dict[str, Any]] = []
        reply_days = int(os.getenv("WM_REPLY_NUDGE_DAYS", "2"))
        threshold = (now - timedelta(days=reply_days)).isoformat()

        threads = conn.execute(
            """
            SELECT * FROM wm_threads
            WHERE needs_reply = 1
            AND status NOT IN (?, ?)
            AND last_activity_at < ?
            ORDER BY last_activity_at ASC
            LIMIT 5
            """,
            (ThreadStatus.RESOLVED.value, ThreadStatus.STALE.value, threshold),
        ).fetchall()

        for t in threads:
            try:
                last_activity = datetime.fromisoformat(
                    str(t["last_activity_at"]).replace("Z", "+00:00")
                )
                if last_activity.tzinfo is None:
                    last_activity = last_activity.replace(tzinfo=timezone.utc)
                days_waiting = (now - last_activity).days
            except Exception:
                days_waiting = reply_days

            nudges.append({
                "type": "reply_overdue",
                "urgency": UrgencyLevel.TODAY.value,
                "subject": t["subject"],
                "thread_id": t["id"],
                "conversation_id": t["conversation_id"],
                "days_waiting": days_waiting,
                "summary": t.get("summary") or "",
                "message": f"No reply sent for {days_waiting} days: {t['subject'][:50]}",
            })

        return nudges

    def _check_overdue_commitments(
        self,
        conn,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Find commitments past their due date."""
        nudges: list[dict[str, Any]] = []

        commitments = conn.execute(
            """
            SELECT * FROM wm_commitments
            WHERE is_completed = 0
            AND due_by IS NOT NULL
            AND due_by < ?
            LIMIT 5
            """,
            (now.isoformat(),),
        ).fetchall()

        for c in commitments:
            nudges.append({
                "type": "commitment_overdue",
                "urgency": UrgencyLevel.IMMEDIATE.value,
                "commitment_id": c["id"],
                "description": c["description"],
                "to_whom": c["to_whom"],
                "due_by": c["due_by"],
                "message": f"Overdue commitment to {c['to_whom']}: {c['description'][:50]}",
            })

        return nudges

    def _check_stale_urgent_threads(
        self,
        conn,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Find urgent threads going stale (no activity for 24h)."""
        nudges: list[dict[str, Any]] = []
        threshold = (now - timedelta(hours=24)).isoformat()

        threads = conn.execute(
            """
            SELECT * FROM wm_threads
            WHERE urgency IN (?, ?)
            AND status = ?
            AND last_activity_at < ?
            LIMIT 3
            """,
            (
                UrgencyLevel.IMMEDIATE.value,
                UrgencyLevel.TODAY.value,
                ThreadStatus.ACTIVE.value,
                threshold,
            ),
        ).fetchall()

        for t in threads:
            nudges.append({
                "type": "urgent_thread_stale",
                "urgency": t["urgency"],
                "thread_id": t["id"],
                "subject": t["subject"],
                "message": f"Urgent thread has no activity for 24h: {t['subject'][:50]}",
            })

        return nudges

    def _check_pending_decisions(
        self,
        conn,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Find decisions waiting too long."""
        nudges: list[dict[str, Any]] = []
        decision_days = int(os.getenv("WM_DECISION_NUDGE_DAYS", "3"))
        threshold = (now - timedelta(days=decision_days)).isoformat()

        decisions = conn.execute(
            """
            SELECT * FROM wm_decisions
            WHERE is_resolved = 0
            AND created_at < ?
            LIMIT 3
            """,
            (threshold,),
        ).fetchall()

        for d in decisions:
            nudges.append({
                "type": "decision_pending",
                "urgency": UrgencyLevel.TODAY.value,
                "decision_id": d["id"],
                "question": d["question"],
                "requester": d["requester"],
                "message": f"Decision pending from {d['requester']}: {d['question'][:50]}",
            })

        return nudges


async def run_memory_engine_cycle(user_email: str) -> dict[str, int]:
    """
    Convenience function to run a single engine cycle.

    Can be called from the main service loop.
    """
    engine = WorkingMemoryEngine(user_email)
    return await engine.run_cycle()
