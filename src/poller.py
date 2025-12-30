import logging
import json
import subprocess
import os
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple, Callable

import requests
from aech_cli_msgraph.graph import GraphClient
from .database import get_connection
from .body_parser import parse_email_body

logger = logging.getLogger(__name__)

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

        logger.debug(
            "CLI command succeeded: %s", " ".join(cmd), extra={"stdout": stdout, "stderr": stderr}
        )
        return stdout

    def poll_inbox(self) -> List[Dict[str, Any]]:
        """Poll the delegated inbox for messages via aech-cli-msgraph."""
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
                categories = msg.get("categories") or []
                categories_json = json.dumps(categories) if categories else None
                processed_at = datetime.now(timezone.utc).isoformat() if categories else None
                conn.execute(
                    """
                    INSERT INTO emails (
                        id, conversation_id, internet_message_id, subject, sender,
                        to_emails, cc_emails, received_at, body_preview, has_attachments,
                        is_read, etag, web_link, outlook_categories, processed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        etag=excluded.etag,
                        web_link=excluded.web_link,
                        outlook_categories=COALESCE(excluded.outlook_categories, emails.outlook_categories),
                        processed_at=COALESCE(emails.processed_at, excluded.processed_at)
                    """,
                    (
                        msg.get("id"),
                        conversation_id,
                        internet_message_id,
                        msg.get("subject", ""),
                        sender,
                        json.dumps(to_emails),
                        json.dumps(cc_emails),
                        msg.get("receivedDateTime") or datetime.now(timezone.utc).isoformat(),
                        msg.get("bodyPreview", ""),
                        bool(msg.get("hasAttachments")) if msg.get("hasAttachments") is not None else None,
                        msg.get("isRead", False),
                        etag,
                        msg.get("webLink"),
                        categories_json or json.dumps([]),
                        processed_at,
                    ),
                )
            conn.commit()
            conn.close()
            logger.debug(f"Poll-inbox returned {len(messages)} messages")
            return messages
        except Exception as e:
            logger.error(f"Error polling inbox: {e}")
            return []

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

    def _get_message_body(self, message_id: str, max_retries: int = 3) -> Optional[str]:
        """Fetch the full HTML body of a message with retry on rate limit."""
        import time

        assert self.user_email is not None
        headers = self._graph_client._get_headers()
        base_path = self._graph_client._get_base_path(self.user_email)
        url = f"{base_path}/messages/{message_id}?$select=body"

        for attempt in range(max_retries):
            try:
                resp = requests.get(url, headers=headers)

                if resp.ok:
                    data = resp.json()
                    body = data.get("body", {})
                    content = body.get("content", "")
                    content_type = body.get("contentType", "text")

                    if content_type == "html":
                        return content
                    else:
                        # Plain text - wrap in simple HTML for consistent processing
                        return f"<pre>{content}</pre>"

                elif resp.status_code == 429:
                    # Rate limited - respect Retry-After header
                    retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
                    logger.debug(f"Rate limited, waiting {retry_after}s before retry {attempt + 1}")
                    time.sleep(retry_after)
                    continue

                else:
                    logger.warning(f"Failed to fetch body for {message_id}: {resp.status_code}")
                    return None

            except Exception as e:
                logger.error(f"Error fetching message body: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None

        logger.warning(f"Max retries exceeded for {message_id}")
        return None

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

        categories = msg.get("categories") or []
        categories_json = json.dumps(categories) if categories else None

        # If this message already has categories in Outlook, assume it was triaged before.
        processed_at = datetime.now(timezone.utc).isoformat() if categories else None

        return {
            "id": msg.get("id"),
            "conversation_id": msg.get("conversationId"),
            "internet_message_id": msg.get("internetMessageId"),
            "subject": msg.get("subject", ""),
            "sender": sender,
            "to_emails": json.dumps(to_emails),
            "cc_emails": json.dumps(cc_emails),
            "received_at": msg.get("receivedDateTime") or datetime.now(timezone.utc).isoformat(),
            "body_preview": msg.get("bodyPreview", ""),
            "has_attachments": bool(msg.get("hasAttachments")),
            "is_read": msg.get("isRead", False),
            "etag": msg.get("@odata.etag"),
            "web_link": msg.get("webLink"),
            "outlook_categories": categories_json,
            "processed_at": processed_at,
        }

    def _upsert_message(self, conn, msg_data: Dict[str, Any], body_html: Optional[str] = None) -> None:
        """Upsert a message into the database."""
        # Parse HTML body into structured markdown
        body_markdown = None
        signature_block = None
        body_hash = None

        if body_html:
            parsed = parse_email_body(body_html)
            body_markdown = parsed.main_content
            signature_block = parsed.signature_block
            body_hash = hashlib.sha256(body_html.encode()).hexdigest()[:16]

        categories_value = msg_data.get("outlook_categories")
        processed_at_value = msg_data.get("processed_at")

        conn.execute(
            """
            INSERT INTO emails (
                id, conversation_id, internet_message_id, subject, sender,
                to_emails, cc_emails, received_at, body_preview, has_attachments,
                is_read, etag, body_html, body_markdown, signature_block, body_hash, web_link,
                outlook_categories, processed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                etag=excluded.etag,
                body_html=COALESCE(excluded.body_html, emails.body_html),
                body_markdown=COALESCE(excluded.body_markdown, emails.body_markdown),
                signature_block=COALESCE(excluded.signature_block, emails.signature_block),
                body_hash=COALESCE(excluded.body_hash, emails.body_hash),
                web_link=excluded.web_link,
                outlook_categories=COALESCE(excluded.outlook_categories, emails.outlook_categories),
                processed_at=COALESCE(emails.processed_at, excluded.processed_at)
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
                msg_data["etag"],
                body_html,
                body_markdown,
                signature_block,
                body_hash,
                msg_data.get("web_link"),
                categories_value or json.dumps([]),
                processed_at_value,
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

    def full_sync_folder(
        self,
        folder_id: str,
        folder_name: str,
        fetch_body: bool = True,
        page_size: int = 50,
        message_callback: Optional[Callable[[int, str], None]] = None,
        body_concurrency: int = 5,
        since_date: Optional[datetime] = None,
    ) -> int:
        """
        Perform a full sync of a folder using pagination.
        Returns the number of messages synced.

        Args:
            message_callback: Optional callback(count, subject) for per-message progress
            body_concurrency: Number of concurrent body fetches (default 5, conservative for Graph API limits)
            since_date: Optional date to filter emails (only sync emails received on or after this date)
        """
        if since_date:
            logger.info(f"Starting full sync for folder: {folder_name} (since {since_date.date()})")
        else:
            logger.info(f"Starting full sync for folder: {folder_name} ({folder_id})")

        assert self.user_email is not None
        headers = self._graph_client._get_headers()
        base_path = self._graph_client._get_base_path(self.user_email)

        select_fields = "id,conversationId,internetMessageId,subject,from,toRecipients,ccRecipients,receivedDateTime,bodyPreview,hasAttachments,isRead,webLink,categories"
        url = f"{base_path}/mailFolders/{folder_id}/messages?$select={select_fields}&$top={page_size}&$expand=attachments($select=id,name,contentType,size)"

        # Add date filter if specified
        if since_date:
            iso_date = since_date.strftime("%Y-%m-%dT00:00:00Z")
            url += f"&$filter=receivedDateTime ge {iso_date}"

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

                # Extract message data first
                page_messages = []
                for msg in messages:
                    msg_data = self._extract_message_data(msg)
                    page_messages.append((msg, msg_data))

                # Fetch bodies concurrently if enabled
                bodies = {}
                if fetch_body and page_messages:
                    with ThreadPoolExecutor(max_workers=body_concurrency) as executor:
                        future_to_id = {
                            executor.submit(self._get_message_body, msg["id"]): msg["id"]
                            for msg, _ in page_messages
                        }
                        for future in as_completed(future_to_id):
                            msg_id = future_to_id[future]
                            try:
                                bodies[msg_id] = future.result()
                            except Exception as e:
                                logger.warning(f"Failed to fetch body for {msg_id}: {e}")
                                bodies[msg_id] = None

                # Now upsert with bodies
                for msg, msg_data in page_messages:
                    body_html = bodies.get(msg["id"]) if fetch_body else None
                    self._upsert_message(conn, msg_data, body_html)

                    if msg.get("attachments"):
                        self._upsert_attachments_metadata(conn, msg["id"], msg["attachments"])

                    messages_synced += 1

                    if message_callback:
                        subject = msg_data.get("subject", "")[:40]
                        message_callback(messages_synced, subject)

                conn.commit()
                logger.debug(f"Synced {messages_synced} messages so far from {folder_name}")

                url = data.get("@odata.nextLink")

            # Establish delta link by following all pages until we get @odata.deltaLink
            # The first call to /delta returns all existing messages as pages, not the deltaLink
            delta_url: Optional[str] = f"{base_path}/mailFolders/{folder_id}/messages/delta?$select={select_fields}"
            while delta_url:
                delta_resp = requests.get(delta_url, headers=headers)
                if not delta_resp.ok:
                    logger.warning(f"Failed to establish delta link for {folder_name}: {delta_resp.status_code}")
                    break
                delta_data = delta_resp.json()
                if "@odata.deltaLink" in delta_data:
                    self.save_sync_state(folder_id, delta_data["@odata.deltaLink"], "initial", messages_synced)
                    logger.debug(f"Delta link established for {folder_name}")
                    break
                delta_url = delta_data.get("@odata.nextLink")

            logger.info(f"Full sync complete for {folder_name}: {messages_synced} messages")

        except Exception as e:
            logger.error(f"Error during full sync of {folder_name}: {e}")
        finally:
            conn.close()

        return messages_synced

    def delta_sync_folder(
        self,
        folder_id: str,
        folder_name: str,
        fetch_body: bool = True,
        message_callback: Optional[Callable[[int, str], None]] = None,
        body_concurrency: int = 5,
        since_date: Optional[datetime] = None,
    ) -> Tuple[int, int]:
        """
        Perform an incremental delta sync of a folder.
        Returns (messages_updated, messages_deleted).

        Args:
            message_callback: Optional callback(count, subject) for per-message progress
            body_concurrency: Number of concurrent body fetches (default 5, conservative for Graph API limits)
            since_date: Optional date filter (only used if falling back to full sync)
        """
        sync_state = self.get_sync_state(folder_id)
        if not sync_state or not sync_state[0]:
            logger.info(f"No delta link for {folder_name}, falling back to full sync")
            count = self.full_sync_folder(folder_id, folder_name, fetch_body, message_callback=message_callback, since_date=since_date)
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
                    if resp.status_code == 410:
                        logger.warning(f"Delta token expired for {folder_name}, doing full sync")
                        conn.close()
                        count = self.full_sync_folder(folder_id, folder_name, fetch_body, message_callback=message_callback)
                        return (count, 0)
                    logger.error(f"Delta sync failed: {resp.status_code} - {resp.text}")
                    break

                data = resp.json()
                messages = data.get("value", [])

                # Separate deletions from updates
                to_delete = []
                to_update = []
                for msg in messages:
                    if msg.get("@removed"):
                        to_delete.append(msg["id"])
                    else:
                        msg_data = self._extract_message_data(msg)
                        to_update.append((msg, msg_data))

                # Handle deletions
                for msg_id in to_delete:
                    conn.execute("DELETE FROM emails WHERE id = ?", (msg_id,))
                    messages_deleted += 1

                # Fetch bodies concurrently for updates
                bodies = {}
                if fetch_body and to_update:
                    with ThreadPoolExecutor(max_workers=body_concurrency) as executor:
                        future_to_id = {
                            executor.submit(self._get_message_body, msg["id"]): msg["id"]
                            for msg, _ in to_update
                        }
                        for future in as_completed(future_to_id):
                            msg_id = future_to_id[future]
                            try:
                                bodies[msg_id] = future.result()
                            except Exception as e:
                                logger.warning(f"Failed to fetch body for {msg_id}: {e}")
                                bodies[msg_id] = None

                # Upsert updates with bodies
                for msg, msg_data in to_update:
                    body_html = bodies.get(msg["id"]) if fetch_body else None
                    self._upsert_message(conn, msg_data, body_html)
                    messages_updated += 1

                    if message_callback:
                        subject = msg_data.get("subject", "")[:40]
                        message_callback(messages_updated, subject)

                conn.commit()

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

    def sync_all_folders(
        self,
        fetch_body: bool = True,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        message_callback: Optional[Callable[[int, str], None]] = None,
        since_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Sync all folders in the mailbox.
        Uses delta sync if available, otherwise full sync.
        Returns a summary of the sync operation.

        Args:
            fetch_body: Whether to fetch full email bodies
            progress_callback: Optional callback(current, total, folder_name) for folder progress
            message_callback: Optional callback(count, subject) for per-message progress
            since_date: Optional date to filter emails (only sync emails received on or after this date)
        """
        folders = self.get_all_folders()
        total_folders = len(folders)
        logger.info(f"Starting sync for {total_folders} folders")

        results = {
            "folders_synced": 0,
            "total_messages": 0,
            "total_deleted": 0,
            "folder_details": []
        }

        for i, folder in enumerate(folders):
            folder_id = folder.get("id")
            folder_name = folder.get("displayName", "Unknown")

            if not folder_id:
                continue

            if progress_callback:
                progress_callback(i + 1, total_folders, folder_name)

            sync_state = self.get_sync_state(folder_id)

            if sync_state and sync_state[0]:
                updated, deleted = self.delta_sync_folder(
                    folder_id, folder_name, fetch_body, message_callback=message_callback,
                    since_date=since_date
                )
                results["folder_details"].append({
                    "name": folder_name,
                    "sync_type": "delta",
                    "messages": updated,
                    "deleted": deleted
                })
                results["total_messages"] += updated
                results["total_deleted"] += deleted
            else:
                count = self.full_sync_folder(
                    folder_id, folder_name, fetch_body, message_callback=message_callback,
                    since_date=since_date
                )
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

        folders = self.get_all_folders()
        folder_map = {f["id"]: f["displayName"] for f in folders}

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
