import logging
import json
import subprocess
from typing import List, Dict, Any

import requests
from aech_cli_msgraph.graph import GraphClient
from .database import get_connection
from .folders_config import STANDARD_FOLDERS

logger = logging.getLogger(__name__)

class GraphPoller:
    """
    Lightweight wrapper around aech-cli-msgraph. All Microsoft Graph operations
    are delegated to that CLI; this class only handles persistence.
    """

    def __init__(self, db_path: str, user_email: str):
        self.db_path = db_path
        self.user_email = user_email
        self._graph_client = GraphClient()

    def _run_cli(self, args: List[str]) -> str:
        """Run aech-cli-msgraph with the delegated user and return stdout."""
        cmd = ["aech-cli-msgraph"] + args + ["--user", self.user_email]
        result = subprocess.run(cmd, capture_output=True, text=True)
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()

        if result.returncode != 0:
            logger.error(
                "CLI command failed",
                extra={
                    "cmd": cmd,
                    "returncode": result.returncode,
                    "stdout": stdout,
                    "stderr": stderr,
                },
            )
            raise RuntimeError(f"Command failed (code {result.returncode}): {stderr or stdout or 'unknown error'}")

        # Trace successes at debug to reduce noise
        logger.debug(
            "CLI command succeeded: %s", " ".join(cmd), extra={"stdout": stdout, "stderr": stderr}
        )
        return stdout

    def poll_inbox(self) -> List[Dict[str, Any]]:
        """Poll the delegated inbox for unread messages via aech-cli-msgraph."""
        logger.info(f"Polling inbox for {self.user_email}")
        try:
            stdout = self._run_cli(["poll-inbox", "--json", "--count", "50", "--all-senders", "--include-read"])
            messages = json.loads(stdout or "[]")
            if not isinstance(messages, list):
                logger.error("Unexpected poll-inbox output (not a list)")
                return []

            conn = get_connection(self.db_path)
            for msg in messages:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO emails (id, subject, sender, received_at, body_preview, is_read, folder_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        msg.get("id"),
                        msg.get("subject", ""),
                        msg.get("from", {}).get("emailAddress", {}).get("address", ""),
                        msg.get("receivedDateTime"),
                        msg.get("bodyPreview", ""),
                        msg.get("isRead", False),
                        msg.get("parentFolderId"),
                    ),
                )
            conn.commit()
            conn.close()
            logger.debug(f"Poll-inbox returned {len(messages)} messages")
            return messages
        except Exception as e:
            logger.error(f"Error polling inbox: {e}")
            return []

    def move_email(self, message_id: str, destination_folder: str):
        """Move an email to a folder by name using the CLI."""
        try:
            stdout = self._run_cli(["move-email", "--json", message_id, destination_folder])
            if not stdout:
                logger.warning(f"Move-email returned empty output for {message_id}")
                return

            try:
                data = json.loads(stdout)
            except json.JSONDecodeError:
                logger.warning(f"Move-email returned non-JSON output for {message_id}: {stdout}")
                return

            if isinstance(data, dict) and data.get("error"):
                logger.error(f"Move-email Graph error for {message_id}: {data}")
                return

            logger.info(f"Moved email {message_id} to folder '{destination_folder}'", extra={"cli_stdout": data})
        except Exception as e:
            logger.error(f"Error moving email {message_id}: {e}")

    def delete_email(self, message_id: str):
        """Delete an email (move to Deleted Items) in the delegated mailbox."""
        try:
            stdout = self._run_cli(["delete-email", "--json", message_id])
            if stdout:
                try:
                    data = json.loads(stdout)
                except json.JSONDecodeError:
                    data = stdout
            else:
                data = None
            logger.info(f"Deleted email {message_id}", extra={"cli_stdout": data})
        except Exception as e:
            logger.error(f"Error deleting email {message_id}: {e}")

    def ensure_standard_folders(self):
        """
        Ensure all standard folders exist in the delegated mailbox.
        Uses aech-cli-msgraph's GraphClient for folder operations.
        """
        try:
            existing = self._graph_client.get_mail_folders(user_id=self.user_email).get("value", [])
            existing_lower = {f.get("displayName", "").lower() for f in existing}
            base_path = self._graph_client._get_base_path(self.user_email)
            headers = self._graph_client._get_headers()

            created = []
            for name in STANDARD_FOLDERS:
                if name.lower() in existing_lower:
                    continue
                resp = requests.post(
                    f"{base_path}/mailFolders",
                    json={"displayName": name},
                    headers=headers,
                )
                if resp.ok:
                    created.append(name)
                else:
                    logger.warning(
                        "Failed to create folder",
                        extra={"folder": name, "status": resp.status_code, "body": resp.text},
                    )
            if created:
                logger.info(f"Created missing folders: {created}")
        except Exception as e:
            logger.warning(f"Could not ensure standard folders: {e}")
