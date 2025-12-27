import logging
import json
import subprocess
import os
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import requests
from aech_cli_msgraph.graph import GraphClient
from .database import get_connection
from .folders_config import STANDARD_FOLDERS

logger = logging.getLogger(__name__)

# Graph API base URL
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

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
        logger.debug(f"Polling inbox for {self.user_email}")
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

    # =========================================================================
    # Full Mailbox Sync Methods (for Email Corpus Intelligence)
    # =========================================================================

    def get_all_folders(self) -> List[Dict[str, Any]]:
        """Get all mail folders with their IDs for sync operations."""
        try:
            assert self.user_email is not None
            folders_data = self._graph_client.get_mail_folders(user_id=self.user_email)
            return folders_data.get("value", [])
        except Exception as e:
            logger.error(f"Error fetching folders: {e}")
            return []

    def get_sync_state(self, folder_id: str) -> Optional[Tuple[str, str]]:
        """Get the delta link and sync type for a folder."""
        conn = get_connection()
        cursor = conn.cursor()
        row = cursor.execute(
            "SELECT delta_link, sync_type FROM sync_state WHERE folder_id = ?",
            (folder_id,)
        ).fetchone()
        conn.close()
        if row:
            return (row["delta_link"], row["sync_type"])
        return None

    def save_sync_state(self, folder_id: str, delta_link: str, sync_type: str, messages_synced: int) -> None:
        """Save the sync state for a folder."""
        conn = get_connection()
        conn.execute(
            """
            INSERT INTO sync_state (folder_id, delta_link, last_sync_at, sync_type, messages_synced)
            VALUES (?, ?, datetime('now'), ?, ?)
            ON CONFLICT(folder_id) DO UPDATE SET
                delta_link = excluded.delta_link,
                last_sync_at = excluded.last_sync_at,
                sync_type = excluded.sync_type,
                messages_synced = sync_state.messages_synced + excluded.messages_synced
            """,
            (folder_id, delta_link, sync_type, messages_synced)
        )
        conn.commit()
        conn.close()

    def _get_message_body(self, message_id: str) -> Tuple[Optional[str], Optional[str]]:
        """Fetch the full body of a message (text and HTML)."""
        try:
            assert self.user_email is not None
            headers = self._graph_client._get_headers()
            base_path = self._graph_client._get_base_path(self.user_email)
            url = f"{base_path}/messages/{message_id}?$select=body"
            resp = requests.get(url, headers=headers)
            if resp.ok:
                data = resp.json()
                body = data.get("body", {})
                content = body.get("content", "")
                content_type = body.get("contentType", "text")

                if content_type == "html":
                    # Store HTML and extract plain text
                    from html import unescape
                    import re
                    text = re.sub(r'<[^>]+>', ' ', content)
                    text = unescape(text)
                    text = re.sub(r'\s+', ' ', text).strip()
                    return (text, content)
                else:
                    return (content, None)
            else:
                logger.warning(f"Failed to fetch body for {message_id}: {resp.status_code}")
                return (None, None)
        except Exception as e:
            logger.error(f"Error fetching message body: {e}")
            return (None, None)

    def _extract_message_data(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and normalize message data from Graph API response."""
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

        return {
            "id": msg.get("id"),
            "conversation_id": msg.get("conversationId"),
            "internet_message_id": msg.get("internetMessageId"),
            "subject": msg.get("subject", ""),
            "sender": sender,
            "to_emails": json.dumps(to_emails),
            "cc_emails": json.dumps(cc_emails),
            "received_at": msg.get("receivedDateTime"),
            "body_preview": msg.get("bodyPreview", ""),
            "has_attachments": bool(msg.get("hasAttachments")),
            "is_read": msg.get("isRead", False),
            "folder_id": msg.get("parentFolderId"),
            "etag": msg.get("@odata.etag"),
            "web_link": msg.get("webLink"),  # Folder-agnostic deep link to email
        }

    def _upsert_message(self, conn, msg_data: Dict[str, Any], body_text: Optional[str] = None, body_html: Optional[str] = None) -> None:
        """Upsert a message into the database."""
        body_hash = None
        if body_text:
            body_hash = hashlib.sha256(body_text.encode()).hexdigest()[:16]

        conn.execute(
            """
            INSERT INTO emails (
                id, conversation_id, internet_message_id, subject, sender,
                to_emails, cc_emails, received_at, body_preview, has_attachments,
                is_read, folder_id, etag, body_text, body_html, body_hash, web_link
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                etag=excluded.etag,
                body_text=COALESCE(excluded.body_text, emails.body_text),
                body_html=COALESCE(excluded.body_html, emails.body_html),
                body_hash=COALESCE(excluded.body_hash, emails.body_hash),
                web_link=excluded.web_link
            """,
            (
                msg_data["id"],
                msg_data["conversation_id"],
                msg_data["internet_message_id"],
                msg_data["subject"],
                msg_data["sender"],
                msg_data["to_emails"],
                msg_data["cc_emails"],
                msg_data["received_at"],
                msg_data["body_preview"],
                msg_data["has_attachments"],
                msg_data["is_read"],
                msg_data["folder_id"],
                msg_data["etag"],
                body_text,
                body_html,
                body_hash,
                msg_data.get("web_link"),
            ),
        )

    def _upsert_attachments_metadata(self, conn, email_id: str, attachments: List[Dict[str, Any]]) -> None:
        """Store attachment metadata for later extraction."""
        for att in attachments:
            conn.execute(
                """
                INSERT INTO attachments (id, email_id, filename, content_type, size_bytes, extraction_status)
                VALUES (?, ?, ?, ?, ?, 'pending')
                ON CONFLICT(id) DO UPDATE SET
                    filename=excluded.filename,
                    content_type=excluded.content_type,
                    size_bytes=excluded.size_bytes
                """,
                (
                    att.get("id"),
                    email_id,
                    att.get("name"),
                    att.get("contentType"),
                    att.get("size"),
                )
            )

    def full_sync_folder(self, folder_id: str, folder_name: str, fetch_body: bool = True, page_size: int = 50) -> int:
        """
        Perform a full sync of a folder using pagination.
        Returns the number of messages synced.
        """
        logger.info(f"Starting full sync for folder: {folder_name} ({folder_id})")

        assert self.user_email is not None
        headers = self._graph_client._get_headers()
        base_path = self._graph_client._get_base_path(self.user_email)

        # Request messages with metadata and attachments list
        select_fields = "id,conversationId,internetMessageId,subject,from,toRecipients,ccRecipients,receivedDateTime,bodyPreview,hasAttachments,isRead,parentFolderId,webLink"
        url = f"{base_path}/mailFolders/{folder_id}/messages?$select={select_fields}&$top={page_size}&$expand=attachments($select=id,name,contentType,size)"

        messages_synced = 0
        conn = get_connection()

        try:
            while url:
                resp = requests.get(url, headers=headers)
                if not resp.ok:
                    logger.error(f"Failed to fetch messages: {resp.status_code} - {resp.text}")
                    break

                data = resp.json()
                messages = data.get("value", [])

                for msg in messages:
                    msg_data = self._extract_message_data(msg)

                    # Fetch full body if requested
                    body_text, body_html = None, None
                    if fetch_body:
                        body_text, body_html = self._get_message_body(msg["id"])

                    self._upsert_message(conn, msg_data, body_text, body_html)

                    # Store attachment metadata
                    if msg.get("attachments"):
                        self._upsert_attachments_metadata(conn, msg["id"], msg["attachments"])

                    messages_synced += 1

                conn.commit()
                logger.debug(f"Synced {messages_synced} messages so far from {folder_name}")

                # Get next page
                url = data.get("@odata.nextLink")

            # Get delta link for future incremental syncs
            delta_url = f"{base_path}/mailFolders/{folder_id}/messages/delta?$select={select_fields}"
            delta_resp = requests.get(delta_url, headers=headers)
            if delta_resp.ok:
                delta_data = delta_resp.json()
                delta_link = delta_data.get("@odata.deltaLink")
                if delta_link:
                    self.save_sync_state(folder_id, delta_link, "initial", messages_synced)

            logger.info(f"Full sync complete for {folder_name}: {messages_synced} messages")

        except Exception as e:
            logger.error(f"Error during full sync of {folder_name}: {e}")
        finally:
            conn.close()

        return messages_synced

    def delta_sync_folder(self, folder_id: str, folder_name: str, fetch_body: bool = True) -> Tuple[int, int]:
        """
        Perform an incremental delta sync of a folder.
        Returns (messages_updated, messages_deleted).
        """
        sync_state = self.get_sync_state(folder_id)
        if not sync_state or not sync_state[0]:
            logger.info(f"No delta link for {folder_name}, falling back to full sync")
            count = self.full_sync_folder(folder_id, folder_name, fetch_body)
            return (count, 0)

        delta_link, _ = sync_state
        logger.info(f"Starting delta sync for folder: {folder_name}")

        headers = self._graph_client._get_headers()
        url = delta_link

        messages_updated = 0
        messages_deleted = 0
        conn = get_connection()

        try:
            while url:
                resp = requests.get(url, headers=headers)
                if not resp.ok:
                    if resp.status_code == 410:  # Delta token expired
                        logger.warning(f"Delta token expired for {folder_name}, doing full sync")
                        conn.close()
                        count = self.full_sync_folder(folder_id, folder_name, fetch_body)
                        return (count, 0)
                    logger.error(f"Delta sync failed: {resp.status_code} - {resp.text}")
                    break

                data = resp.json()
                messages = data.get("value", [])

                for msg in messages:
                    # Check if this is a deletion
                    if msg.get("@removed"):
                        conn.execute("DELETE FROM emails WHERE id = ?", (msg["id"],))
                        messages_deleted += 1
                    else:
                        msg_data = self._extract_message_data(msg)

                        body_text, body_html = None, None
                        if fetch_body:
                            body_text, body_html = self._get_message_body(msg["id"])

                        self._upsert_message(conn, msg_data, body_text, body_html)
                        messages_updated += 1

                conn.commit()

                # Check for next page or final delta link
                if "@odata.nextLink" in data:
                    url = data["@odata.nextLink"]
                elif "@odata.deltaLink" in data:
                    self.save_sync_state(folder_id, data["@odata.deltaLink"], "delta", messages_updated)
                    url = None
                else:
                    url = None

            logger.info(f"Delta sync complete for {folder_name}: {messages_updated} updated, {messages_deleted} deleted")

        except Exception as e:
            logger.error(f"Error during delta sync of {folder_name}: {e}")
        finally:
            conn.close()

        return (messages_updated, messages_deleted)

    def sync_all_folders(self, fetch_body: bool = True) -> Dict[str, Any]:
        """
        Sync all folders in the mailbox.
        Uses delta sync if available, otherwise full sync.
        Returns a summary of the sync operation.
        """
        folders = self.get_all_folders()
        logger.info(f"Starting sync for {len(folders)} folders")

        results = {
            "folders_synced": 0,
            "total_messages": 0,
            "total_deleted": 0,
            "folder_details": []
        }

        for folder in folders:
            folder_id = folder.get("id")
            folder_name = folder.get("displayName", "Unknown")

            if not folder_id:
                continue

            sync_state = self.get_sync_state(folder_id)

            if sync_state and sync_state[0]:
                # Delta sync
                updated, deleted = self.delta_sync_folder(folder_id, folder_name, fetch_body)
                results["folder_details"].append({
                    "name": folder_name,
                    "sync_type": "delta",
                    "messages": updated,
                    "deleted": deleted
                })
                results["total_messages"] += updated
                results["total_deleted"] += deleted
            else:
                # Full sync
                count = self.full_sync_folder(folder_id, folder_name, fetch_body)
                results["folder_details"].append({
                    "name": folder_name,
                    "sync_type": "full",
                    "messages": count,
                    "deleted": 0
                })
                results["total_messages"] += count

            results["folders_synced"] += 1

        logger.info(f"Sync complete: {results['total_messages']} messages across {results['folders_synced']} folders")
        return results

    def get_sync_status(self) -> List[Dict[str, Any]]:
        """Get the sync status for all folders."""
        conn = get_connection()
        cursor = conn.cursor()

        # Get all folders
        folders = self.get_all_folders()
        folder_map = {f["id"]: f["displayName"] for f in folders}

        # Get sync state
        rows = cursor.execute(
            "SELECT folder_id, delta_link, last_sync_at, sync_type, messages_synced FROM sync_state"
        ).fetchall()
        conn.close()

        status = []
        for row in rows:
            status.append({
                "folder_id": row["folder_id"],
                "folder_name": folder_map.get(row["folder_id"], "Unknown"),
                "last_sync_at": row["last_sync_at"],
                "sync_type": row["sync_type"],
                "messages_synced": row["messages_synced"],
                "has_delta_link": bool(row["delta_link"])
            })

        return status
