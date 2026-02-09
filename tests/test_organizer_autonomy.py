import asyncio
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.modules.setdefault(
    "aech_llm_observability",
    SimpleNamespace(
        build_llm_log_path=lambda capability: Path(tempfile.gettempdir()) / f"{capability}-llm.jsonl",
        init_instrumentation=lambda service_name: None,
        set_llm_log_path=lambda path: None,
    ),
)
sys.modules.setdefault("aech_cli_msgraph", MagicMock())
sys.modules.setdefault("aech_cli_msgraph.graph", MagicMock())
sys.modules["aech_cli_msgraph.graph"].GraphClient = MagicMock()

from src.database import get_connection, init_db
from src.organizer import (
    EmailClassification,
    NotificationDecision,
    Organizer,
    PeriodicInboxSummary,
)


class _FakeSummaryAgent:
    def __init__(self, output: PeriodicInboxSummary):
        self._output = output

    def run_sync(self, _prompt: str):
        return SimpleNamespace(
            output=self._output,
            usage=lambda: SimpleNamespace(request_tokens=10, response_tokens=20),
        )


class OrganizerAutonomyTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "assistant.sqlite")
        os.environ["INBOX_DB_PATH"] = self.db_path
        init_db()

        self.poller = MagicMock()
        self.poller.user_email = "principal@example.com"
        self.organizer = Organizer(self.poller, backfill=False)

    def tearDown(self):
        os.environ.pop("INBOX_DB_PATH", None)
        self._tmp.cleanup()

    def test_reply_trigger_emitted_with_draft(self):
        decision = EmailClassification(
            outlook_categories=["Action Required"],
            urgency="today",
            reason="Requires principal decision",
            labels=["work"],
            requires_reply=True,
            reply_reason="asks_for_decision",
            availability_requested=False,
        )
        notification = NotificationDecision(
            importance="important",
            notify_now=True,
            notification_channel="teams",
            notification_reason="Draft prepared for quick response",
            create_reply_draft=True,
            reply_draft="Hi Kim, thanks for the update. I can do Thursday at 3pm UTC.",
            include_in_periodic_summary=True,
            summary_note="Decision needed from principal.",
        )
        email = {
            "id": "msg-1",
            "subject": "Need your decision",
            "sender": "kim@example.com",
            "received_at": datetime.now(timezone.utc).isoformat(),
        }
        prefs = {"inbox_assistant": {"alert_only_when_draft_ready": True}}

        with patch("src.organizer.write_trigger") as mock_write_trigger:
            asyncio.run(
                self.organizer._emit_triggers_for_email(
                    email=email,
                    decision=decision,
                    prefs=prefs,
                    notification_decision=notification,
                )
            )

        self.assertEqual(mock_write_trigger.call_count, 1)
        _, trigger_type, payload = mock_write_trigger.call_args.args[:3]
        self.assertEqual(trigger_type, "reply_needed")
        self.assertTrue(payload["draft_ready"])
        self.assertIn("suggested_reply", payload)
        self.assertEqual(payload["suggested_reply"], notification.reply_draft)

    def test_reply_trigger_suppressed_without_draft_when_policy_requires_draft(self):
        decision = EmailClassification(
            outlook_categories=["Action Required"],
            urgency="today",
            reason="Follow-up needed",
            labels=["work"],
            requires_reply=True,
            availability_requested=False,
        )
        notification = NotificationDecision(
            importance="important",
            notify_now=True,
            notification_channel="teams",
            notification_reason="Potentially important, but no draft",
            create_reply_draft=False,
            reply_draft=None,
            include_in_periodic_summary=True,
            summary_note="Include in digest.",
        )
        email = {
            "id": "msg-2",
            "subject": "Status update",
            "sender": "ops@example.com",
            "received_at": datetime.now(timezone.utc).isoformat(),
        }
        prefs = {"inbox_assistant": {"alert_only_when_draft_ready": True}}

        with patch("src.organizer.write_trigger") as mock_write_trigger:
            asyncio.run(
                self.organizer._emit_triggers_for_email(
                    email=email,
                    decision=decision,
                    prefs=prefs,
                    notification_decision=notification,
                )
            )

        mock_write_trigger.assert_not_called()

    def test_periodic_summary_trigger_emitted(self):
        conn = get_connection()
        try:
            conn.execute(
                """
                INSERT INTO emails (
                    id, conversation_id, subject, sender, to_emails, cc_emails,
                    received_at, body_preview, outlook_categories, urgency,
                    suggested_action, processed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    "msg-3",
                    "conv-3",
                    "Quarterly budget review",
                    "finance@example.com",
                    json.dumps(["principal@example.com"]),
                    json.dumps([]),
                    datetime.now(timezone.utc).isoformat(),
                    "Please confirm your preferred review window.",
                    json.dumps(["Action Required"]),
                    "today",
                    "keep",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        fake_summary = PeriodicInboxSummary(
            summary="One high-priority thread needs response.",
            priority_items=["Quarterly budget review from finance@example.com"],
            draft_ready_items=["Quarterly budget review"],
            newsletter_insights=["No major newsletter insight changes this window."],
            recommended_actions=["Reply to finance with availability today."],
        )
        prefs = {
            "inbox_assistant": {
                "periodic_summary_enabled": True,
                "periodic_summary_interval_minutes": 1,
            }
        }

        with patch.object(self.organizer, "_get_periodic_summary_agent", return_value=_FakeSummaryAgent(fake_summary)):
            with patch("src.organizer.write_trigger") as mock_write_trigger:
                self.organizer._emit_periodic_summary_trigger(prefs)

        self.assertEqual(mock_write_trigger.call_count, 1)
        _, trigger_type, payload = mock_write_trigger.call_args.args[:3]
        self.assertEqual(trigger_type, "inbox_activity_summary_ready")
        self.assertEqual(payload["summary"], fake_summary.summary)
        self.assertEqual(payload["priority_items"], fake_summary.priority_items)

        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT value FROM user_preferences WHERE key = ?",
                ("_internal.last_periodic_inbox_summary_at",),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertTrue(str(row[0]).strip())
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
