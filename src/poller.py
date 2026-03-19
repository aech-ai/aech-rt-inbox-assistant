import logging
import json
import subprocess
import os
import hashlib
import html
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Callable

import requests
from .database import get_connection
from .body_parser import parse_email_body

logger = logging.getLogger(__name__)

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
MESSAGE_SELECT_FIELDS = (
    "id,conversationId,internetMessageId,subject,from,toRecipients,ccRecipients,bccRecipients,"
    "receivedDateTime,createdDateTime,lastModifiedDateTime,bodyPreview,hasAttachments,isRead,"
    "webLink,categories,isDraft,parentFolderId"
)
ATTACHMENT_EXPAND_FIELDS = "attachments($select=id,name,contentType,size)"
SIMPLE_ATTACHMENT_MAX_BYTES = 3 * 1024 * 1024
UPLOAD_CHUNK_SIZE = 327680 * 16


def _load_graph_client():
    try:
        from aech_cli_msgraph.graph import GraphClient
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "aech-cli-msgraph is required for inbox-assistant Graph operations"
        ) from exc
    return GraphClient


class GraphPoller:
    """
    Lightweight wrapper around aech-cli-msgraph. All Microsoft Graph operations
    are delegated to that CLI; this class only handles persistence.
    """

    def __init__(self):
        self.user_email = os.getenv("DELEGATED_USER")
        if not self.user_email:
            raise ValueError("DELEGATED_USER environment variable must be set")

        self._graph_client = _load_graph_client()()
        self._ignored_senders = self._load_ignored_senders()

    @staticmethod
    def _normalize_email(address: Optional[str]) -> str:
        return (address or "").strip().lower()

    def _load_ignored_senders(self) -> set[str]:
        configured = os.getenv("INBOX_IGNORED_SENDERS", "")
        return {
            self._normalize_email(address)
            for address in configured.split(",")
            if self._normalize_email(address)
        }

    def _is_ignored_sender(self, sender: Optional[str]) -> bool:
        return self._normalize_email(sender) in self._ignored_senders

    @staticmethod
    def _normalize_recipient_list(addresses: Optional[List[str]]) -> List[str]:
        recipients: List[str] = []
        for address in addresses or []:
            cleaned = str(address or "").strip()
            if cleaned:
                recipients.append(cleaned)
        return recipients

    @staticmethod
    def _format_recipients(addresses: Optional[List[str]]) -> List[Dict[str, Dict[str, str]]]:
        return [
            {"emailAddress": {"address": address}}
            for address in GraphPoller._normalize_recipient_list(addresses)
        ]

    @staticmethod
    def _resolve_body_content_type(body_content_type: str) -> str:
        normalized = (body_content_type or "text").strip().lower()
        if normalized == "text":
            return "Text"
        if normalized == "html":
            return "HTML"
        raise ValueError(f"Unsupported body content type: {body_content_type}")

    @staticmethod
    def _message_body_to_html(body: Optional[Dict[str, Any]]) -> Optional[str]:
        if not body:
            return None
        content = str(body.get("content") or "")
        content_type = str(body.get("contentType") or "text").strip().lower()
        if not content:
            return ""
        if content_type == "html":
            return content
        return f"<pre>{html.escape(content)}</pre>"

    @staticmethod
    def _render_body_html(body: str, body_content_type: str) -> str:
        if not body:
            return ""
        if GraphPoller._resolve_body_content_type(body_content_type) == "HTML":
            return body
        escaped = html.escape(body)
        return f"<div>{escaped.replace(chr(10), '<br>')}</div>"

    def _probe_default_folder_statuses(self) -> dict[str, int]:
        headers = self._graph_client._get_headers()
        base_path = self._graph_client._get_base_path(self.user_email)
        statuses: dict[str, int] = {}
        for folder_name in ("inbox", "drafts", "sentitems"):
            try:
                response = requests.get(
                    f"{base_path}/mailFolders/{folder_name}",
                    headers=headers,
                    timeout=30,
                )
            except Exception:
                continue
            statuses[folder_name] = response.status_code
        return statuses

    def _raise_mailbox_write_error_if_diagnosable(
        self,
        action: str,
        response: requests.Response,
    ) -> None:
        if response.status_code != 404:
            return

        statuses = self._probe_default_folder_statuses()
        inbox_ok = statuses.get("inbox") == 200
        missing_default_folders = [
            label
            for key, label in (("drafts", "Drafts"), ("sentitems", "Sent Items"))
            if statuses.get(key) == 404
        ]
        if not inbox_ok or not missing_default_folders:
            return

        missing_text = ", ".join(missing_default_folders)
        raise RuntimeError(
            f"{action} failed because the authenticated Graph principal can read "
            f"{self.user_email} Inbox but cannot access the mailbox default folder(s) {missing_text}. "
            f"Share those default folders with the acting account, or grant equivalent mailbox delegation "
            f"such as Full Access on {self.user_email}. If this workflow will later send mail, also grant "
            f"Send As or Send on Behalf. "
            f"Raw Graph error: {response.status_code} {response.text}"
        )

    @classmethod
    def _prepend_reply_body(cls, existing_html: str, body: str, body_content_type: str) -> str:
        rendered = cls._render_body_html(body, body_content_type)
        if not existing_html:
            return rendered
        if not rendered:
            return existing_html
        return f"{rendered}<br><br>{existing_html}"

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
        """Poll the delegated inbox for recent messages via a lightweight Graph query."""
        logger.debug(f"Polling inbox for {self.user_email}")
        try:
            headers = self._graph_client._get_headers()
            base_path = self._graph_client._get_base_path(self.user_email)
            resp = requests.get(
                f"{base_path}/mailFolders/inbox/messages",
                headers=headers,
                params={
                    "$top": 50,
                    "$orderby": "receivedDateTime desc",
                    "$select": MESSAGE_SELECT_FIELDS,
                    "$expand": ATTACHMENT_EXPAND_FIELDS,
                },
                timeout=30,
            )
            if not resp.ok:
                raise RuntimeError(
                    f"Recent inbox fetch failed: {resp.status_code} {resp.text}"
                )

            payload = resp.json()
            messages = payload.get("value", []) if isinstance(payload, dict) else []
            if not isinstance(messages, list):
                logger.error("Unexpected inbox response payload (value is not a list)")
                return []

            conn = get_connection()
            for msg in messages:
                msg_data = self._extract_message_data(msg, folder_name="Inbox")
                if self._is_ignored_sender(msg_data["sender"]):
                    logger.debug("Skipping ignored sender during poll-inbox: %s", msg_data["sender"])
                    continue
                self._upsert_message(conn, msg_data)
                if msg.get("attachments"):
                    self._upsert_attachments_metadata(conn, msg["id"], msg["attachments"])
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
            raise

    def archive_email(self, message_id: str) -> None:
        """Archive an email by moving it to the Archive folder."""
        if not self.user_email:
            raise ValueError("DELEGATED_USER is required to archive email")

        headers = self._graph_client._get_headers()
        base_path = self._graph_client._get_base_path(self.user_email)
        move_url = f"{base_path}/messages/{message_id}/move"
        payload = {"destinationId": "archive"}

        resp = requests.post(move_url, headers=headers, json=payload, timeout=30)
        if not resp.ok:
            raise RuntimeError(
                f"Archive move failed for {message_id}: {resp.status_code} {resp.text}"
            )

        logger.info(f"Archived email {message_id}")

    def _get_message_detail(
        self,
        message_id: str,
        *,
        include_body: bool = False,
        include_attachments: bool = False,
    ) -> Dict[str, Any]:
        if not self.user_email:
            raise ValueError("DELEGATED_USER is required to fetch message details")

        headers = self._graph_client._get_headers()
        base_path = self._graph_client._get_base_path(self.user_email)
        select_fields = MESSAGE_SELECT_FIELDS
        if include_body:
            select_fields += ",body"

        url = f"{base_path}/messages/{message_id}?$select={select_fields}"
        if include_attachments:
            url += f"&$expand={ATTACHMENT_EXPAND_FIELDS}"

        resp = requests.get(url, headers=headers, timeout=30)
        if not resp.ok:
            raise RuntimeError(
                f"Message fetch failed for {message_id}: {resp.status_code} {resp.text}"
            )
        return resp.json()

    def _persist_message_record(
        self,
        message: Dict[str, Any],
        *,
        folder_id: str | None = None,
        folder_name: str | None = None,
    ) -> None:
        msg_data = self._extract_message_data(message, folder_id=folder_id, folder_name=folder_name)
        if self._is_ignored_sender(msg_data["sender"]):
            conn = get_connection()
            try:
                self._delete_message_if_present(conn, msg_data["id"])
                conn.commit()
            finally:
                conn.close()
            return

        body_html = self._message_body_to_html(message.get("body"))
        conn = get_connection()
        try:
            self._upsert_message(conn, msg_data, body_html)
            if message.get("attachments"):
                self._upsert_attachments_metadata(conn, msg_data["id"], message["attachments"])
            conn.commit()
        finally:
            conn.close()

    def sync_message_to_db(
        self,
        message_id: str,
        *,
        folder_name: str | None = None,
        fetch_body: bool = True,
    ) -> Dict[str, Any]:
        message = self._get_message_detail(
            message_id,
            include_body=fetch_body,
            include_attachments=True,
        )
        self._persist_message_record(message, folder_name=folder_name)
        return message

    def update_draft(
        self,
        message_id: str,
        *,
        subject: str | None = None,
        body: str | None = None,
        body_content_type: str = "text",
        prepend_to_existing: bool = False,
    ) -> Dict[str, Any]:
        if not self.user_email:
            raise ValueError("DELEGATED_USER is required to update a draft")

        payload: Dict[str, Any] = {}
        if subject is not None:
            payload["subject"] = subject

        if body is not None:
            body_value = body
            content_type = self._resolve_body_content_type(body_content_type)
            if prepend_to_existing:
                current_message = self._get_message_detail(message_id, include_body=True)
                existing_html = self._message_body_to_html(current_message.get("body")) or ""
                body_value = self._prepend_reply_body(existing_html, body, body_content_type)
                content_type = "HTML"
            payload["body"] = {
                "contentType": content_type,
                "content": body_value,
            }

        if not payload:
            return self._get_message_detail(message_id, include_body=True, include_attachments=True)

        headers = self._graph_client._get_headers()
        base_path = self._graph_client._get_base_path(self.user_email)
        resp = requests.patch(
            f"{base_path}/messages/{message_id}",
            headers=headers,
            json=payload,
            timeout=30,
        )
        if not resp.ok:
            raise RuntimeError(
                f"Draft update failed for {message_id}: {resp.status_code} {resp.text}"
            )
        return resp.json()

    def add_message_attachment(self, message_id: str, file_path: str) -> None:
        if not self.user_email:
            raise ValueError("DELEGATED_USER is required to add draft attachments")

        path = Path(file_path).expanduser().resolve()
        if not path.is_file():
            raise ValueError(f"Attachment file not found: {file_path}")

        content = path.read_bytes()
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        headers = self._graph_client._get_headers()
        base_path = self._graph_client._get_base_path(self.user_email)

        if len(content) <= SIMPLE_ATTACHMENT_MAX_BYTES:
            import base64

            payload = {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": path.name,
                "contentType": content_type,
                "contentBytes": base64.b64encode(content).decode("utf-8"),
            }
            resp = requests.post(
                f"{base_path}/messages/{message_id}/attachments",
                headers=headers,
                json=payload,
                timeout=30,
            )
            if not resp.ok:
                raise RuntimeError(
                    f"Attachment upload failed for {path.name}: {resp.status_code} {resp.text}"
                )
            return

        session_payload = {
            "AttachmentItem": {
                "attachmentType": "file",
                "name": path.name,
                "size": len(content),
                "contentType": content_type,
            }
        }
        session_resp = requests.post(
            f"{base_path}/messages/{message_id}/attachments/createUploadSession",
            headers=headers,
            json=session_payload,
            timeout=30,
        )
        if not session_resp.ok:
            raise RuntimeError(
                f"Attachment upload session failed for {path.name}: "
                f"{session_resp.status_code} {session_resp.text}"
            )

        upload_url = (session_resp.json() or {}).get("uploadUrl")
        if not upload_url:
            raise RuntimeError(f"Attachment upload session returned no uploadUrl for {path.name}")

        for offset in range(0, len(content), UPLOAD_CHUNK_SIZE):
            chunk = content[offset: offset + UPLOAD_CHUNK_SIZE]
            end_byte = offset + len(chunk) - 1
            chunk_headers = {
                "Content-Length": str(len(chunk)),
                "Content-Range": f"bytes {offset}-{end_byte}/{len(content)}",
            }
            upload_resp = requests.put(
                upload_url,
                headers=chunk_headers,
                data=chunk,
                timeout=60,
            )
            if upload_resp.status_code not in (200, 201, 202):
                raise RuntimeError(
                    f"Attachment chunk upload failed for {path.name}: "
                    f"{upload_resp.status_code} {upload_resp.text}"
                )

    def create_draft(
        self,
        *,
        subject: str = "",
        body: str = "",
        body_content_type: str = "text",
        to_recipients: Optional[List[str]] = None,
        cc_recipients: Optional[List[str]] = None,
        bcc_recipients: Optional[List[str]] = None,
        attachments: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Create a new draft message in the delegated mailbox."""
        if not self.user_email:
            raise ValueError("DELEGATED_USER is required to create a draft")

        headers = self._graph_client._get_headers()
        base_path = self._graph_client._get_base_path(self.user_email)
        payload: Dict[str, Any] = {
            "subject": subject or "",
            "body": {
                "contentType": self._resolve_body_content_type(body_content_type),
                "content": body or "",
            },
        }

        to_payload = self._format_recipients(to_recipients)
        cc_payload = self._format_recipients(cc_recipients)
        bcc_payload = self._format_recipients(bcc_recipients)
        if to_payload:
            payload["toRecipients"] = to_payload
        if cc_payload:
            payload["ccRecipients"] = cc_payload
        if bcc_payload:
            payload["bccRecipients"] = bcc_payload

        resp = requests.post(f"{base_path}/messages", headers=headers, json=payload, timeout=30)
        if not resp.ok:
            self._raise_mailbox_write_error_if_diagnosable("Draft create", resp)
            raise RuntimeError(
                f"Draft create failed: {resp.status_code} {resp.text}"
            )
        draft = resp.json()
        draft_id = draft.get("id")
        if not draft_id:
            raise RuntimeError("Draft create succeeded but returned no message id")

        for attachment_path in attachments or []:
            self.add_message_attachment(draft_id, attachment_path)

        return self.sync_message_to_db(draft_id, folder_name="Drafts", fetch_body=True)

    def create_reply_draft(
        self,
        message_id: str,
        *,
        subject: str | None = None,
        body: str = "",
        body_content_type: str = "text",
        attachments: Optional[List[str]] = None,
        reply_all: bool = False,
    ) -> Dict[str, Any]:
        """Create a reply or reply-all draft in the delegated mailbox."""
        if not self.user_email:
            raise ValueError("DELEGATED_USER is required to create a reply draft")

        headers = self._graph_client._get_headers()
        base_path = self._graph_client._get_base_path(self.user_email)
        action = "createReplyAll" if reply_all else "createReply"

        resp = requests.post(
            f"{base_path}/messages/{message_id}/{action}",
            headers=headers,
            json=None,
            timeout=30,
        )
        if not resp.ok:
            self._raise_mailbox_write_error_if_diagnosable("Reply draft create", resp)
            raise RuntimeError(
                f"Reply draft create failed for {message_id}: {resp.status_code} {resp.text}"
            )
        draft = resp.json()
        draft_id = draft.get("id")
        if not draft_id:
            raise RuntimeError(f"Reply draft create succeeded but returned no draft id for {message_id}")

        if subject is not None or body:
            self.update_draft(
                draft_id,
                subject=subject,
                body=body if body else None,
                body_content_type=body_content_type,
                prepend_to_existing=bool(body),
            )

        for attachment_path in attachments or []:
            self.add_message_attachment(draft_id, attachment_path)

        return self.sync_message_to_db(draft_id, folder_name="Drafts", fetch_body=True)

    # =========================================================================
    # Full Mailbox Sync Methods (for Email Corpus Intelligence)
    # =========================================================================

    def get_all_folders(self) -> List[Dict[str, Any]]:
        """Get all mail folders with their IDs for sync operations.

        Falls back to well-known folder names if folder enumeration fails
        (some mailboxes have issues with GET /mailFolders but work with
        GET /mailFolders/{well-known-name}).
        """
        try:
            assert self.user_email is not None
            folders_data = self._graph_client.get_mail_folders(user_id=self.user_email)
            return folders_data.get("value", [])
        except Exception as e:
            logger.warning(f"Folder enumeration failed, trying well-known folders: {e}")
            # Fall back to well-known folder names
            # See: https://learn.microsoft.com/en-us/graph/api/resources/mailfolder
            well_known_folders = ["inbox", "sentitems", "drafts", "deleteditems", "junkemail", "archive"]
            folders = []
            headers = self._graph_client._get_headers()
            base_path = self._graph_client._get_base_path(self.user_email)
            for folder_name in well_known_folders:
                try:
                    url = f"{base_path}/mailFolders/{folder_name}"
                    resp = requests.get(url, headers=headers)
                    if resp.ok:
                        folder_data = resp.json()
                        folders.append(folder_data)
                        logger.debug(f"Found well-known folder: {folder_name} -> {folder_data.get('displayName')}")
                except Exception as folder_err:
                    logger.debug(f"Well-known folder {folder_name} not accessible: {folder_err}")
            if folders:
                logger.info(f"Recovered {len(folders)} folders via well-known names")
            else:
                logger.error("Could not access any mail folders")
            return folders

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

    def _extract_message_data(
        self,
        msg: Dict[str, Any],
        *,
        folder_id: str | None = None,
        folder_name: str | None = None,
    ) -> Dict[str, Any]:
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
        bcc_emails = [
            r.get("emailAddress", {}).get("address", "")
            for r in (msg.get("bccRecipients") or [])
            if r.get("emailAddress", {}).get("address")
        ]

        categories = msg.get("categories") or []
        categories_json = json.dumps(categories) if categories else None

        # If this message already has categories in Outlook, assume it was triaged before.
        processed_at = datetime.now(timezone.utc).isoformat() if categories else None
        effective_folder_name = folder_name or None
        effective_folder_id = folder_id or msg.get("parentFolderId")
        is_draft = bool(msg.get("isDraft"))
        if not is_draft and effective_folder_name:
            is_draft = effective_folder_name.strip().lower() == "drafts"

        return {
            "id": msg.get("id"),
            "conversation_id": msg.get("conversationId"),
            "internet_message_id": msg.get("internetMessageId"),
            "subject": msg.get("subject", ""),
            "sender": sender,
            "to_emails": json.dumps(to_emails),
            "cc_emails": json.dumps(cc_emails),
            "bcc_emails": json.dumps(bcc_emails),
            "received_at": (
                msg.get("receivedDateTime")
                or msg.get("createdDateTime")
                or msg.get("lastModifiedDateTime")
                or datetime.now(timezone.utc).isoformat()
            ),
            "body_preview": msg.get("bodyPreview", ""),
            "has_attachments": bool(msg.get("hasAttachments")),
            "is_draft": is_draft,
            "is_read": msg.get("isRead", False),
            "etag": msg.get("@odata.etag"),
            "web_link": msg.get("webLink"),
            "mail_folder_id": effective_folder_id,
            "mail_folder_name": effective_folder_name,
            "outlook_categories": categories_json,
            "processed_at": processed_at,
        }

    def _delete_message_if_present(self, conn, message_id: Optional[str]) -> None:
        if not message_id:
            return
        conn.execute("DELETE FROM emails WHERE id = ?", (message_id,))

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
                to_emails, cc_emails, bcc_emails, received_at, body_preview, has_attachments,
                is_draft, is_read, etag, body_html, body_markdown, signature_block, body_hash, web_link,
                mail_folder_id, mail_folder_name,
                outlook_categories, processed_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                conversation_id=excluded.conversation_id,
                internet_message_id=excluded.internet_message_id,
                subject=excluded.subject,
                sender=excluded.sender,
                to_emails=excluded.to_emails,
                cc_emails=excluded.cc_emails,
                bcc_emails=excluded.bcc_emails,
                received_at=excluded.received_at,
                body_preview=excluded.body_preview,
                has_attachments=excluded.has_attachments,
                is_draft=excluded.is_draft,
                is_read=excluded.is_read,
                etag=excluded.etag,
                body_html=COALESCE(excluded.body_html, emails.body_html),
                body_markdown=COALESCE(excluded.body_markdown, emails.body_markdown),
                signature_block=COALESCE(excluded.signature_block, emails.signature_block),
                body_hash=COALESCE(excluded.body_hash, emails.body_hash),
                web_link=excluded.web_link,
                mail_folder_id=COALESCE(excluded.mail_folder_id, emails.mail_folder_id),
                mail_folder_name=COALESCE(excluded.mail_folder_name, emails.mail_folder_name),
                outlook_categories=COALESCE(excluded.outlook_categories, emails.outlook_categories),
                processed_at=COALESCE(emails.processed_at, excluded.processed_at),
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                msg_data["id"],
                msg_data["conversation_id"],
                msg_data["internet_message_id"],
                msg_data["subject"],
                msg_data["sender"],
                msg_data["to_emails"],
                msg_data["cc_emails"],
                msg_data["bcc_emails"],
                msg_data["received_at"],
                msg_data["body_preview"],
                msg_data["has_attachments"],
                msg_data["is_draft"],
                msg_data["is_read"],
                msg_data["etag"],
                body_html,
                body_markdown,
                signature_block,
                body_hash,
                msg_data.get("web_link"),
                msg_data.get("mail_folder_id"),
                msg_data.get("mail_folder_name"),
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
                    size_bytes=excluded.size_bytes,
                    updated_at=CURRENT_TIMESTAMP
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
        establish_delta_link: bool = True,
    ) -> int:
        """
        Perform a full sync of a folder using pagination.
        Returns the number of messages synced.

        Args:
            message_callback: Optional callback(count, subject) for per-message progress
            body_concurrency: Number of concurrent body fetches (default 5, conservative for Graph API limits)
            since_date: Optional date to filter emails (only sync emails received on or after this date)
            establish_delta_link: Whether to walk the folder delta feed and persist a delta token.
        """
        if since_date:
            logger.info(f"Starting full sync for folder: {folder_name} (since {since_date.date()})")
        else:
            logger.info(f"Starting full sync for folder: {folder_name} ({folder_id})")

        assert self.user_email is not None
        headers = self._graph_client._get_headers()
        base_path = self._graph_client._get_base_path(self.user_email)

        select_fields = MESSAGE_SELECT_FIELDS
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
                    msg_data = self._extract_message_data(msg, folder_id=folder_id, folder_name=folder_name)
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
                    if self._is_ignored_sender(msg_data["sender"]):
                        self._delete_message_if_present(conn, msg_data["id"])
                        logger.debug(
                            "Skipping ignored sender during full sync: %s", msg_data["sender"]
                        )
                        continue
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

            if establish_delta_link:
                # Establish delta link by following all pages until we get @odata.deltaLink.
                # The first call to /delta returns all existing messages as pages, not the deltaLink.
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
            else:
                logger.info("Skipping delta link establishment for scoped backfill of %s", folder_name)

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

                # Fetch message details when body and/or attachments are required.
                details: dict[str, Dict[str, Any]] = {}
                if to_update:
                    with ThreadPoolExecutor(max_workers=body_concurrency) as executor:
                        future_to_id = {}
                        for msg, _ in to_update:
                            if fetch_body or bool(msg.get("hasAttachments")):
                                future = executor.submit(
                                    self._get_message_detail,
                                    msg["id"],
                                    include_body=fetch_body,
                                    include_attachments=bool(msg.get("hasAttachments")),
                                )
                                future_to_id[future] = msg["id"]
                        for future in as_completed(future_to_id):
                            msg_id = future_to_id[future]
                            try:
                                details[msg_id] = future.result()
                            except Exception as e:
                                logger.warning(f"Failed to fetch details for {msg_id}: {e}")

                # Upsert updates with bodies
                for msg, msg_data in to_update:
                    source_msg = details.get(msg["id"]) or msg
                    msg_data = self._extract_message_data(
                        source_msg,
                        folder_id=folder_id,
                        folder_name=folder_name,
                    )
                    if self._is_ignored_sender(msg_data["sender"]):
                        self._delete_message_if_present(conn, msg_data["id"])
                        logger.debug(
                            "Skipping ignored sender during delta sync: %s", msg_data["sender"]
                        )
                        continue
                    body_html = (
                        self._message_body_to_html(source_msg.get("body"))
                        if fetch_body else None
                    )
                    self._upsert_message(conn, msg_data, body_html)
                    if source_msg.get("attachments"):
                        self._upsert_attachments_metadata(conn, msg["id"], source_msg["attachments"])
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
