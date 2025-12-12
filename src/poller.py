import logging
import json
import subprocess
import os
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

    def __init__(self):
        self.user_email = os.getenv("DELEGATED_USER")
        if not self.user_email:
            raise ValueError("DELEGATED_USER environment variable must be set") 
        
        self._graph_client = GraphClient()
        self.folder_prefix = os.getenv("FOLDER_PREFIX", "aa_")

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

            conn = get_connection()
            for msg in messages:
                sender = msg.get("from", {}).get("emailAddress", {}).get("address", "")
                to_emails = [
                    r.get("emailAddress", {}).get("address", "")
                    for r in (msg.get("toRecipients") or [])
                    if r.get("emailAddress", {}).get("address")
                ]
                cc_emails = [
                    r.get("emailAddress", {}).get("address", "")
                    for r in (msg.get("ccRecipients") or [])
                    if r.get("emailAddress", {}).get("address")
                ]
                conversation_id = msg.get("conversationId") or msg.get("conversation_id")
                internet_message_id = msg.get("internetMessageId") or msg.get("internet_message_id")
                etag = msg.get("@odata.etag") or msg.get("etag")
                conn.execute(
                    """
                    INSERT INTO emails (
                        id,
                        conversation_id,
                        internet_message_id,
                        subject,
                        sender,
                        to_emails,
                        cc_emails,
                        received_at,
                        body_preview,
                        has_attachments,
                        is_read,
                        folder_id,
                        etag
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        conversation_id=excluded.conversation_id,
                        internet_message_id=excluded.internet_message_id,
                        subject=excluded.subject,
                        sender=excluded.sender,
                        to_emails=excluded.to_emails,
                        cc_emails=excluded.cc_emails,
                        received_at=excluded.received_at,
                        body_preview=excluded.body_preview,
                        has_attachments=excluded.has_attachments,
                        is_read=excluded.is_read,
                        folder_id=excluded.folder_id,
                        etag=excluded.etag
                    """,
                    (
                        msg.get("id"),
                        conversation_id,
                        internet_message_id,
                        msg.get("subject", ""),
                        sender,
                        json.dumps(to_emails),
                        json.dumps(cc_emails),
                        msg.get("receivedDateTime"),
                        msg.get("bodyPreview", ""),
                        bool(msg.get("hasAttachments")) if msg.get("hasAttachments") is not None else None,
                        msg.get("isRead", False),
                        msg.get("parentFolderId"),
                        etag,
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
        # Prepend prefix to destination folder
        prefixed_folder = f"{self.folder_prefix}{destination_folder}"
        
        try:
            stdout = self._run_cli(["move-email", "--json", message_id, prefixed_folder])
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

            logger.info(f"Moved email {message_id} to folder '{prefixed_folder}'", extra={"cli_stdout": data})
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
                # Prepend prefix for creation/checking
                prefixed_name = f"{self.folder_prefix}{name}"
                
                if prefixed_name.lower() in existing_lower:
                    continue
                resp = requests.post(
                    f"{base_path}/mailFolders",
                    json={"displayName": prefixed_name},
                    headers=headers,
                )
                if resp.ok:
                    created.append(prefixed_name)
                else:
                    logger.warning(
                        "Failed to create folder",
                        extra={"folder": prefixed_name, "status": resp.status_code, "body": resp.text},
                    )
            if created:
                logger.info(f"Created missing folders: {created}")
        except Exception as e:
            logger.warning(f"Could not ensure standard folders: {e}")

    def get_user_folders(self) -> List[str]:
        """
        Fetch the current list of folders from the mailbox using the CLI.
        """
        try:
            stdout = self._run_cli(["list-folders", "--json"])
            if not stdout:
                return []
                
            folders_data = json.loads(stdout)
            if not isinstance(folders_data, list):
                logger.warning(f"list-folders returned non-list: {type(folders_data)}")
                return []

            # System folders to exclude
            system_folders = {
                "inbox", "sent items", "drafts", "deleted items", 
                "junk email", "outbox", "archive", "conversation history",
                "sync issues", "conflicts", "local failures", "server failures"
            }
            
            user_folders = []
            for f in folders_data:
                name = f.get("displayName", "")
                if name and name.lower() not in system_folders:
                    user_folders.append(name)
            
            return user_folders
        except Exception as e:
            logger.error(f"Error fetching user folders: {e}")
            return []

    def reprocess_all_folders(self):
        """
        Scan all folders (Inbox + user folders), fetch messages, and reset their status in DB.
        """
        logger.info("Reprocessing: Fetching all folders...")
        try:
            # 1. Get all folders
            folders = self.get_user_folders()
            # Add Inbox explicitly as it's excluded from get_user_folders
            folders.append("Inbox")
            
            logger.info(f"Found {len(folders)} folders to scan: {folders}")

            conn = get_connection()
            cursor = conn.cursor()
            
            # 2. Scan each folder
            for folder_name in folders:
                logger.info(f"Scanning folder: {folder_name}")
                try:
                    # Fetch messages (limit to 100 for now as CLI pagination might be tricky)
                    stdout = self._run_cli(["list-messages", "--folder", folder_name, "--json", "--count", "100"])
                    if not stdout:
                        continue
                        
                    messages = json.loads(stdout)
                    if not isinstance(messages, list):
                        logger.warning(f"list-messages returned non-list for {folder_name}")
                        continue
                        
                    for msg in messages:
                        # Upsert and reset processed_at
                        sender = msg.get("from", {}).get("emailAddress", {}).get("address", "unknown")
                        cursor.execute("""
                            INSERT INTO emails (id, subject, sender, body_preview, received_at, processed_at)
                            VALUES (?, ?, ?, ?, ?, NULL)
                            ON CONFLICT(id) DO UPDATE SET
                                subject=excluded.subject,
                                sender=excluded.sender,
                                body_preview=excluded.body_preview,
                                received_at=excluded.received_at,
                                processed_at=NULL
                        """, (
                            msg["id"],
                            msg.get("subject", ""),
                            sender,
                            msg.get("bodyPreview", ""),
                            msg.get("receivedDateTime", "")
                        ))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error scanning folder {folder_name}: {e}")
            
            conn.close()
            logger.info("Reprocessing complete. All emails reset.")

        except Exception as e:
            logger.error(f"Error during reprocessing: {e}")
