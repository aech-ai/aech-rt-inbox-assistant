import unittest
import sqlite3
import json
import asyncio
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime

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

from src.database import init_db, get_connection
from src.organizer import Organizer, EmailCategory
from src.poller import GraphPoller

class TestInboxAssistant(unittest.TestCase):
    def setUp(self):
        self.db_path = Path("test_inbox.sqlite")
        if self.db_path.exists():
            self.db_path.unlink()
        init_db(self.db_path)
        self.conn = get_connection(self.db_path)
        self.user_email = "test@example.com"

    def tearDown(self):
        self.conn.close()
        if self.db_path.exists():
            self.db_path.unlink()

    def test_organizer_flow(self):
        # 1. Insert a test email
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO emails (id, subject, sender, received_at, body_preview, is_read, folder_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, ("msg1", "Important Work", '{"name": "Boss"}', datetime.now(), "Do this now", False, "inbox"))
        self.conn.commit()

        # 2. Mock Poller
        poller = MagicMock(spec=GraphPoller)
        poller.move_email = MagicMock()

        # 3. Mock Agent
        with patch("src.organizer.agent") as mock_agent:
            # Mock the async run method
            future = asyncio.Future()
            result = MagicMock()
            result.data = EmailCategory(
                category="Work",
                reason="Sender is Boss",
                action="move",
                destination_folder="Work"
            )
            future.set_result(result)
            mock_agent.run.return_value = future

            # 4. Run Organizer
            organizer = Organizer(str(self.db_path), poller, self.user_email)
            
            asyncio.run(organizer.organize_emails())

            # 5. Verify
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

            # Check Poller called
            poller.move_email.assert_called_with("msg1", "Work")

if __name__ == "__main__":
    unittest.main()
