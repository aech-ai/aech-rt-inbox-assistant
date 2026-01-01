"""Working Memory Engine - periodic processing for state maintenance and nudges.

Uses derived views (active_threads, contacts) and unified facts table for state.
Thread and contact state is computed on-demand - no mutable state to maintain.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from ..database import get_connection
from ..triggers import make_dedupe_key, write_trigger
from .models import UrgencyLevel

logger = logging.getLogger(__name__)


class WorkingMemoryEngine:
    """
    Periodic engine for working memory maintenance.

    Uses the new architecture:
    - active_threads view: computed thread state from emails
    - contacts view: computed contact stats from emails
    - facts table: unified storage for decisions, commitments, observations

    Runs alongside the email polling loop to:
    - Prune expired facts
    - Generate proactive nudges
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
            "facts_pruned": 0,
            "nudges_emitted": 0,
        }

        now = datetime.now(timezone.utc)
        self._last_run = now

        conn = get_connection()
        try:
            # 1. Prune expired facts (observations with limited relevance)
            stats["facts_pruned"] = self._prune_expired_facts(conn, now)

            conn.commit()

            # 2. Generate and emit nudges (after commit so we see current state)
            stats["nudges_emitted"] = self._emit_nudges(now)

        except Exception as e:
            logger.error(f"Working memory engine cycle failed: {e}")
            conn.rollback()
        finally:
            conn.close()

        if any(stats.values()):
            logger.info(f"Working memory engine cycle: {stats}")

        return stats

    def _prune_expired_facts(self, conn, now: datetime) -> int:
        """Mark observation-type facts as expired after retention period."""
        retention_days = int(os.getenv("WM_OBSERVATION_RETENTION_DAYS", "30"))
        prune_threshold = (now - timedelta(days=retention_days)).isoformat()

        # Mark old preference/pattern observations as expired
        result = conn.execute(
            """
            UPDATE facts
            SET status = 'expired'
            WHERE status = 'active'
            AND fact_type IN ('preference', 'relationship', 'pattern')
            AND extracted_at < ?
            """,
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

            # Evaluate user-defined alert rules for WM events
            self._evaluate_alert_rules_for_nudge(nudge, nudge_type, nudge_id)

        return emitted

    def _evaluate_alert_rules_for_nudge(
        self,
        nudge: dict[str, Any],
        nudge_type: str,
        nudge_id: str,
    ) -> None:
        """Evaluate user alert rules against a working memory nudge."""
        try:
            from ..alerts import AlertRulesEngine
            import asyncio

            # Map nudge type to event type
            event_type_map = {
                "reply_overdue": "wm_thread",
                "urgent_thread_stale": "wm_thread",
                "commitment_overdue": "wm_commitment",
                "decision_pending": "wm_decision",
            }
            event_type = event_type_map.get(nudge_type, "wm_thread")

            alert_engine = AlertRulesEngine(self.user_email)

            # Run async evaluation
            loop = asyncio.new_event_loop()
            try:
                triggered = loop.run_until_complete(
                    alert_engine.evaluate_wm_rules(nudge, event_type)
                )
            finally:
                loop.close()

            for t in triggered:
                alert_engine.emit_alert_trigger(
                    t["rule"],
                    event_type,
                    nudge_id,
                    nudge,
                    t["match_reason"],
                )
        except Exception as e:
            logger.warning(f"Alert rule evaluation failed for WM nudge {nudge_id}: {e}")

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
        """Find threads awaiting reply for too long using active_threads view."""
        nudges: list[dict[str, Any]] = []
        reply_days = int(os.getenv("WM_REPLY_NUDGE_DAYS", "2"))
        threshold = (now - timedelta(days=reply_days)).isoformat()

        # Use active_threads view - needs_reply is computed as last_sender != user_email
        threads = conn.execute(
            """
            SELECT * FROM active_threads
            WHERE last_sender != ?
            AND last_activity < ?
            ORDER BY last_activity ASC
            LIMIT 5
            """,
            (self.user_email, threshold),
        ).fetchall()

        for t in threads:
            try:
                last_activity = datetime.fromisoformat(
                    str(t["last_activity"]).replace("Z", "+00:00")
                )
                if last_activity.tzinfo is None:
                    last_activity = last_activity.replace(tzinfo=timezone.utc)
                days_waiting = (now - last_activity).days
            except Exception:
                days_waiting = reply_days

            nudges.append({
                "type": "reply_overdue",
                "urgency": UrgencyLevel.TODAY.value,
                "subject": t["subject"] or "(no subject)",
                "thread_id": t["conversation_id"],
                "conversation_id": t["conversation_id"],
                "days_waiting": days_waiting,
                "message": f"No reply sent for {days_waiting} days: {(t['subject'] or '(no subject)')[:50]}",
            })

        return nudges

    def _check_overdue_commitments(
        self,
        conn,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Find commitments past their due date using facts table."""
        nudges: list[dict[str, Any]] = []

        # Query facts table for overdue commitments
        commitments = conn.execute(
            """
            SELECT f.*, e.sender
            FROM facts f
            LEFT JOIN emails e ON f.source_id = e.id
            WHERE f.fact_type = 'commitment'
            AND f.status = 'active'
            AND f.due_date IS NOT NULL
            AND f.due_date < ?
            LIMIT 5
            """,
            (now.isoformat(),),
        ).fetchall()

        for c in commitments:
            nudges.append({
                "type": "commitment_overdue",
                "urgency": UrgencyLevel.IMMEDIATE.value,
                "commitment_id": c["id"],
                "description": c["fact_value"],
                "to_whom": c["sender"] or "unknown",
                "due_by": c["due_date"],
                "message": f"Overdue commitment: {c['fact_value'][:50]}",
            })

        return nudges

    def _check_stale_urgent_threads(
        self,
        conn,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Find urgent threads going stale (no activity for 24h) using active_threads view."""
        nudges: list[dict[str, Any]] = []
        threshold = (now - timedelta(hours=24)).isoformat()

        # Use active_threads view
        threads = conn.execute(
            """
            SELECT * FROM active_threads
            WHERE urgency IN (?, ?)
            AND last_activity < ?
            LIMIT 3
            """,
            (
                UrgencyLevel.IMMEDIATE.value,
                UrgencyLevel.TODAY.value,
                threshold,
            ),
        ).fetchall()

        for t in threads:
            nudges.append({
                "type": "urgent_thread_stale",
                "urgency": t["urgency"],
                "thread_id": t["conversation_id"],
                "subject": t["subject"] or "(no subject)",
                "message": f"Urgent thread has no activity for 24h: {(t['subject'] or '(no subject)')[:50]}",
            })

        return nudges

    def _check_pending_decisions(
        self,
        conn,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Find decisions waiting too long using facts table."""
        nudges: list[dict[str, Any]] = []
        decision_days = int(os.getenv("WM_DECISION_NUDGE_DAYS", "3"))
        threshold = (now - timedelta(days=decision_days)).isoformat()

        # Query facts table for pending decisions
        decisions = conn.execute(
            """
            SELECT f.*, e.sender
            FROM facts f
            LEFT JOIN emails e ON f.source_id = e.id
            WHERE f.fact_type = 'decision'
            AND f.status = 'active'
            AND f.extracted_at < ?
            LIMIT 3
            """,
            (threshold,),
        ).fetchall()

        for d in decisions:
            nudges.append({
                "type": "decision_pending",
                "urgency": UrgencyLevel.TODAY.value,
                "decision_id": d["id"],
                "question": d["fact_value"],
                "requester": d["sender"] or "unknown",
                "message": f"Decision pending from {d['sender'] or 'unknown'}: {d['fact_value'][:50]}",
            })

        return nudges


async def run_memory_engine_cycle(user_email: str) -> dict[str, int]:
    """
    Convenience function to run a single engine cycle.

    Can be called from the main service loop.
    """
    engine = WorkingMemoryEngine(user_email)
    return await engine.run_cycle()
