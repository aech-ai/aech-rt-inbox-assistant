import unittest
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

# Add src to path
import sys
sys.path.append(".")

# Mock pydantic_ai before importing src.organizer
sys.modules["pydantic_ai"] = MagicMock()
sys.modules["pydantic_ai"].Agent = MagicMock()

# Mock pydantic
sys.modules["pydantic"] = MagicMock()
sys.modules["pydantic"].BaseModel = MagicMock
sys.modules["pydantic"].Field = MagicMock()

# Mock requests
sys.modules["requests"] = MagicMock()

# Mock aech_cli_msgraph (not required for unit tests)
sys.modules["aech_cli_msgraph"] = MagicMock()
sys.modules["aech_cli_msgraph.graph"] = MagicMock()
sys.modules["aech_cli_msgraph.graph"].GraphClient = MagicMock()

from aech_cli_inbox_assistant.database import init_db, get_connection
from aech_cli_inbox_assistant.organizer import Organizer
from aech_cli_inbox_assistant.poller import GraphPoller

class TestInboxAssistant(unittest.TestCase):
    def setUp(self):
        self.db_path = Path("test_inbox.sqlite")
        if self.db_path.exists():
            self.db_path.unlink()
        # Ensure the code under test uses this DB
        import os

        os.environ["INBOX_DB_PATH"] = str(self.db_path)
        init_db(self.db_path)
        self.conn = get_connection(self.db_path)

    def tearDown(self):
        self.conn.close()
        if self.db_path.exists():
            self.db_path.unlink()
        import os

        os.environ.pop("INBOX_DB_PATH", None)

    def test_organizer_flow(self):
        # 1. Insert a test email
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO emails (id, subject, sender, received_at, body_preview, is_read, folder_id, processed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
        """, ("msg1", "Important Work", '{"name": "Boss"}', "2025-01-01T00:00:00Z", "Do this now", False, "inbox"))
        self.conn.commit()

        # 2. Mock Poller
        poller = MagicMock(spec=GraphPoller)
        poller.move_email = MagicMock()
        poller.get_user_folders = MagicMock(return_value=[])
        poller.user_email = "test@example.com"
        poller.folder_prefix = "aa_"

        decision = SimpleNamespace(
            category="Work",
            reason="Sender is Boss",
            action="move",
            destination_folder="Work",
            labels=["action_required"],
            confidence=0.9,
            requires_reply=False,
            reply_reason=None,
            availability_requested=False,
            availability=None,
        )
        mock_agent = MagicMock()
        mock_agent.run = AsyncMock(return_value=SimpleNamespace(output=decision))

        with patch("src.organizer.read_preferences", return_value={}):
            with patch.object(Organizer, "_get_agent", return_value=mock_agent):
                organizer = Organizer(poller)
                asyncio.run(organizer.organize_emails())

        # Check DB
        cursor.execute("SELECT * FROM emails WHERE id = 'msg1'")
        row = cursor.fetchone()
        self.assertEqual(row["category"], "Work")
        self.assertIsNotNone(row["processed_at"])

        # Check Triage Log
        cursor.execute("SELECT * FROM triage_log WHERE email_id = 'msg1'")
        log = cursor.fetchone()
        self.assertEqual(log["action"], "move")
        self.assertEqual(log["destination_folder"], "Work")

        # Check Labels persisted
        cursor.execute("SELECT label FROM labels WHERE message_id = 'msg1'")
        labels = {r[0] for r in cursor.fetchall()}
        self.assertIn("action_required", labels)

        # Check Poller called
        poller.move_email.assert_called_with("msg1", "Work")

if __name__ == "__main__":
    unittest.main()
