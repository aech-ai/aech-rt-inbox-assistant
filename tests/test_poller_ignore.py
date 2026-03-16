import json
from pathlib import Path

from src.database import get_connection, init_db
from src.poller import GraphPoller


class _FakeGraphClient:
    def _get_headers(self):
        return {}

    def _get_base_path(self, user_email: str) -> str:
        return f"https://graph.microsoft.test/users/{user_email}"


class _FakeResponse:
    def __init__(self, payload=None, *, ok=True, status_code=200, text=None, headers=None):
        self._payload = payload
        self.ok = ok
        self.status_code = status_code
        self.text = text if text is not None else json.dumps(payload)
        self.headers = headers or {}

    def json(self):
        return self._payload


def _make_poller(monkeypatch, tmp_path: Path, ignored_senders: str = "agent@aech.ai") -> GraphPoller:
    state_dir = tmp_path / ".inbox-assistant"
    db_path = state_dir / "assistant.sqlite"
    monkeypatch.setenv("DELEGATED_USER", "steven@aech.ai")
    monkeypatch.setenv("INBOX_STATE_DIR", str(state_dir))
    monkeypatch.setenv("INBOX_DB_PATH", str(db_path))
    monkeypatch.setenv("INBOX_IGNORED_SENDERS", ignored_senders)
    init_db(db_path)

    poller = GraphPoller.__new__(GraphPoller)
    poller.user_email = "steven@aech.ai"
    poller._graph_client = _FakeGraphClient()
    poller._ignored_senders = poller._load_ignored_senders()
    return poller


def _email_row_ids() -> list[str]:
    conn = get_connection()
    rows = conn.execute("SELECT id FROM emails ORDER BY id").fetchall()
    conn.close()
    return [row["id"] for row in rows]


def _email_row(message_id: str):
    conn = get_connection()
    row = conn.execute("SELECT * FROM emails WHERE id = ?", (message_id,)).fetchone()
    conn.close()
    return row


def _attachment_rows(email_id: str):
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM attachments WHERE email_id = ? ORDER BY id",
        (email_id,),
    ).fetchall()
    conn.close()
    return rows


def test_poll_inbox_ignores_configured_senders(monkeypatch, tmp_path: Path):
    poller = _make_poller(monkeypatch, tmp_path)

    payload = [
        {
            "id": "ignored-email",
            "conversationId": "thread-1",
            "internetMessageId": "<ignored@example.com>",
            "subject": "Ignore this",
            "from": {"emailAddress": {"address": "agent@aech.ai"}},
            "toRecipients": [{"emailAddress": {"address": "steven@aech.ai"}}],
            "ccRecipients": [],
            "receivedDateTime": "2026-03-09T06:00:00Z",
            "bodyPreview": "preview",
            "hasAttachments": False,
            "isRead": False,
        },
        {
            "id": "kept-email",
            "conversationId": "thread-2",
            "internetMessageId": "<kept@example.com>",
            "subject": "Keep this",
            "from": {"emailAddress": {"address": "ceo@example.com"}},
            "toRecipients": [{"emailAddress": {"address": "steven@aech.ai"}}],
            "ccRecipients": [],
            "receivedDateTime": "2026-03-09T06:01:00Z",
            "bodyPreview": "preview",
            "hasAttachments": False,
            "isRead": False,
        },
    ]
    monkeypatch.setattr(poller, "_run_cli", lambda args: json.dumps(payload))

    poller.poll_inbox()

    assert _email_row_ids() == ["kept-email"]


def test_full_sync_folder_skips_ignored_senders(monkeypatch, tmp_path: Path):
    poller = _make_poller(monkeypatch, tmp_path)

    payload = {
        "value": [
            {
                "id": "ignored-email",
                "conversationId": "thread-1",
                "internetMessageId": "<ignored@example.com>",
                "subject": "Ignore this",
                "from": {"emailAddress": {"address": "agent@aech.ai"}},
                "toRecipients": [{"emailAddress": {"address": "steven@aech.ai"}}],
                "ccRecipients": [],
                "receivedDateTime": "2026-03-09T06:00:00Z",
                "bodyPreview": "preview",
                "hasAttachments": False,
                "isRead": False,
                "attachments": [],
            },
            {
                "id": "kept-email",
                "conversationId": "thread-2",
                "internetMessageId": "<kept@example.com>",
                "subject": "Keep this",
                "from": {"emailAddress": {"address": "ceo@example.com"}},
                "toRecipients": [{"emailAddress": {"address": "steven@aech.ai"}}],
                "ccRecipients": [],
                "receivedDateTime": "2026-03-09T06:01:00Z",
                "bodyPreview": "preview",
                "hasAttachments": False,
                "isRead": False,
                "attachments": [],
            },
        ]
    }
    monkeypatch.setattr("src.poller.requests.get", lambda url, headers: _FakeResponse(payload))

    count = poller.full_sync_folder(
        "inbox-id",
        "Inbox",
        fetch_body=False,
        establish_delta_link=False,
    )

    assert count == 1
    assert _email_row_ids() == ["kept-email"]


def test_delta_sync_folder_removes_ignored_sender_rows(monkeypatch, tmp_path: Path):
    poller = _make_poller(monkeypatch, tmp_path)

    conn = get_connection()
    conn.execute(
        """
        INSERT INTO emails (
            id, conversation_id, subject, sender, to_emails, cc_emails,
            received_at, body_preview, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (
            "ignored-email",
            "thread-1",
            "Old ignored email",
            "agent@aech.ai",
            '["steven@aech.ai"]',
            "[]",
            "2026-03-08T00:00:00Z",
            "preview",
        ),
    )
    conn.execute(
        """
        INSERT INTO sync_state (folder_id, delta_link, last_sync_at, sync_type, messages_synced)
        VALUES (?, ?, datetime('now'), 'delta', 1)
        """,
        ("inbox-id", "https://graph.microsoft.test/delta"),
    )
    conn.commit()
    conn.close()

    payload = {
        "value": [
            {
                "id": "ignored-email",
                "conversationId": "thread-1",
                "internetMessageId": "<ignored@example.com>",
                "subject": "Still ignore this",
                "from": {"emailAddress": {"address": "agent@aech.ai"}},
                "toRecipients": [{"emailAddress": {"address": "steven@aech.ai"}}],
                "ccRecipients": [],
                "receivedDateTime": "2026-03-09T06:00:00Z",
                "bodyPreview": "preview",
                "hasAttachments": False,
                "isRead": False,
            },
            {
                "id": "kept-email",
                "conversationId": "thread-2",
                "internetMessageId": "<kept@example.com>",
                "subject": "Keep this",
                "from": {"emailAddress": {"address": "ceo@example.com"}},
                "toRecipients": [{"emailAddress": {"address": "steven@aech.ai"}}],
                "ccRecipients": [],
                "receivedDateTime": "2026-03-09T06:01:00Z",
                "bodyPreview": "preview",
                "hasAttachments": False,
                "isRead": False,
            },
        ],
        "@odata.deltaLink": "https://graph.microsoft.test/delta-2",
    }
    monkeypatch.setattr("src.poller.requests.get", lambda url, headers: _FakeResponse(payload))

    updated, deleted = poller.delta_sync_folder("inbox-id", "Inbox", fetch_body=False)

    assert updated == 1
    assert deleted == 0
    assert _email_row_ids() == ["kept-email"]


def test_create_draft_persists_to_db_and_uploads_attachments(monkeypatch, tmp_path: Path):
    poller = _make_poller(monkeypatch, tmp_path)
    attachment_path = tmp_path / "note.txt"
    attachment_path.write_text("draft attachment", encoding="utf-8")
    post_calls: list[tuple[str, object]] = []
    get_calls: list[str] = []

    final_message = {
        "id": "draft-1",
        "conversationId": "thread-1",
        "internetMessageId": "<draft-1@example.com>",
        "subject": "Draft subject",
        "from": {"emailAddress": {"address": "steven@aech.ai"}},
        "toRecipients": [{"emailAddress": {"address": "alice@example.com"}}],
        "ccRecipients": [{"emailAddress": {"address": "finance@example.com"}}],
        "bccRecipients": [{"emailAddress": {"address": "legal@example.com"}}],
        "receivedDateTime": "2026-03-09T06:02:00Z",
        "createdDateTime": "2026-03-09T06:02:00Z",
        "lastModifiedDateTime": "2026-03-09T06:03:00Z",
        "bodyPreview": "Draft body",
        "body": {"contentType": "html", "content": "<div>Draft body</div>"},
        "hasAttachments": True,
        "isRead": False,
        "webLink": "https://example.com/drafts/1",
        "categories": [],
        "isDraft": True,
        "parentFolderId": "drafts-folder",
        "attachments": [
            {
                "id": "att-1",
                "name": "note.txt",
                "contentType": "text/plain",
                "size": 16,
            }
        ],
    }

    def fake_post(url, headers=None, json=None, timeout=None):
        post_calls.append((url, json))
        if url.endswith("/users/steven@aech.ai/messages"):
            return _FakeResponse({"id": "draft-1"})
        if url.endswith("/users/steven@aech.ai/messages/draft-1/attachments"):
            return _FakeResponse(
                {
                    "id": "att-1",
                    "name": "note.txt",
                    "contentType": "text/plain",
                    "size": 16,
                }
            )
        raise AssertionError(f"Unexpected POST: {url}")

    def fake_get(url, headers=None, timeout=None):
        get_calls.append(url)
        if "/users/steven@aech.ai/messages/draft-1?" in url:
            return _FakeResponse(final_message)
        raise AssertionError(f"Unexpected GET: {url}")

    monkeypatch.setattr("src.poller.requests.post", fake_post)
    monkeypatch.setattr("src.poller.requests.get", fake_get)

    draft = poller.create_draft(
        subject="Draft subject",
        body="Draft body",
        body_content_type="html",
        to_recipients=["alice@example.com"],
        cc_recipients=["finance@example.com"],
        bcc_recipients=["legal@example.com"],
        attachments=[str(attachment_path)],
    )

    assert draft["id"] == "draft-1"
    assert post_calls[0][0] == "https://graph.microsoft.test/users/steven@aech.ai/messages"
    assert post_calls[0][1] == {
        "subject": "Draft subject",
        "body": {
            "contentType": "HTML",
            "content": "Draft body",
        },
        "toRecipients": [{"emailAddress": {"address": "alice@example.com"}}],
        "ccRecipients": [{"emailAddress": {"address": "finance@example.com"}}],
        "bccRecipients": [{"emailAddress": {"address": "legal@example.com"}}],
    }
    assert post_calls[1][0] == "https://graph.microsoft.test/users/steven@aech.ai/messages/draft-1/attachments"
    assert get_calls
    email_row = _email_row("draft-1")
    assert email_row is not None
    assert json.loads(email_row["bcc_emails"]) == ["legal@example.com"]
    assert email_row["is_draft"] == 1
    assert email_row["mail_folder_name"] == "Drafts"
    attachments = _attachment_rows("draft-1")
    assert [row["id"] for row in attachments] == ["att-1"]


def test_create_reply_draft_prepends_body_and_persists_to_db(monkeypatch, tmp_path: Path):
    poller = _make_poller(monkeypatch, tmp_path)
    attachment_path = tmp_path / "reply-note.txt"
    attachment_path.write_text("reply attachment", encoding="utf-8")
    post_calls: list[tuple[str, object]] = []
    patch_calls: list[tuple[str, object]] = []
    get_count = 0

    def fake_post(url, headers=None, json=None, timeout=None):
        post_calls.append((url, json))
        if url.endswith("/users/steven@aech.ai/messages/email-1/createReplyAll"):
            return _FakeResponse({"id": "draft-reply-1"})
        if url.endswith("/users/steven@aech.ai/messages/draft-reply-1/attachments"):
            return _FakeResponse(
                {
                    "id": "att-reply-1",
                    "name": "reply-note.txt",
                    "contentType": "text/plain",
                    "size": 16,
                }
            )
        raise AssertionError(f"Unexpected POST: {url}")

    def fake_patch(url, headers=None, json=None, timeout=None):
        patch_calls.append((url, json))
        return _FakeResponse({"id": "draft-reply-1"})

    def fake_get(url, headers=None, timeout=None):
        nonlocal get_count
        get_count += 1
        if get_count == 1:
            return _FakeResponse(
                {
                    "id": "draft-reply-1",
                    "conversationId": "thread-1",
                    "subject": "Re: Draft subject",
                    "from": {"emailAddress": {"address": "steven@aech.ai"}},
                    "toRecipients": [{"emailAddress": {"address": "alice@example.com"}}],
                    "ccRecipients": [{"emailAddress": {"address": "finance@example.com"}}],
                    "bccRecipients": [],
                    "receivedDateTime": "2026-03-09T06:02:00Z",
                    "createdDateTime": "2026-03-09T06:02:00Z",
                    "lastModifiedDateTime": "2026-03-09T06:03:00Z",
                    "bodyPreview": "quoted original",
                    "body": {
                        "contentType": "html",
                        "content": "<div>quoted original</div>",
                    },
                    "hasAttachments": False,
                    "isRead": False,
                    "webLink": "https://example.com/drafts/reply-1",
                    "categories": [],
                    "isDraft": True,
                    "parentFolderId": "drafts-folder",
                    "attachments": [],
                }
            )
        return _FakeResponse(
            {
                "id": "draft-reply-1",
                "conversationId": "thread-1",
                "internetMessageId": "<draft-reply-1@example.com>",
                "subject": "Custom subject",
                "from": {"emailAddress": {"address": "steven@aech.ai"}},
                "toRecipients": [{"emailAddress": {"address": "alice@example.com"}}],
                "ccRecipients": [{"emailAddress": {"address": "finance@example.com"}}],
                "bccRecipients": [],
                "receivedDateTime": "2026-03-09T06:02:00Z",
                "createdDateTime": "2026-03-09T06:02:00Z",
                "lastModifiedDateTime": "2026-03-09T06:04:00Z",
                "bodyPreview": "Thanks",
                "body": {
                    "contentType": "html",
                    "content": "<div>Thanks,<br>Will review.</div><br><br><div>quoted original</div>",
                },
                "hasAttachments": True,
                "isRead": False,
                "webLink": "https://example.com/drafts/reply-1",
                "categories": [],
                "isDraft": True,
                "parentFolderId": "drafts-folder",
                "attachments": [
                    {
                        "id": "att-reply-1",
                        "name": "reply-note.txt",
                        "contentType": "text/plain",
                        "size": 16,
                    }
                ],
            }
        )

    monkeypatch.setattr("src.poller.requests.post", fake_post)
    monkeypatch.setattr("src.poller.requests.patch", fake_patch)
    monkeypatch.setattr("src.poller.requests.get", fake_get)

    draft = poller.create_reply_draft(
        "email-1",
        subject="Custom subject",
        body="Thanks,\nWill review.",
        body_content_type="text",
        attachments=[str(attachment_path)],
        reply_all=True,
    )

    assert draft["id"] == "draft-reply-1"
    assert post_calls[0] == (
        "https://graph.microsoft.test/users/steven@aech.ai/messages/email-1/createReplyAll",
        None,
    )
    assert patch_calls[0][0] == "https://graph.microsoft.test/users/steven@aech.ai/messages/draft-reply-1"
    assert patch_calls[0][1]["subject"] == "Custom subject"
    assert patch_calls[0][1]["body"]["contentType"] == "HTML"
    assert "Thanks,<br>Will review." in patch_calls[0][1]["body"]["content"]
    assert "quoted original" in patch_calls[0][1]["body"]["content"]
    attachments = _attachment_rows("draft-reply-1")
    assert [row["id"] for row in attachments] == ["att-reply-1"]
    email_row = _email_row("draft-reply-1")
    assert email_row is not None
    assert email_row["is_draft"] == 1
    assert email_row["mail_folder_name"] == "Drafts"


def test_add_message_attachment_uses_upload_session_for_large_files(monkeypatch, tmp_path: Path):
    poller = _make_poller(monkeypatch, tmp_path)
    large_file = tmp_path / "large.bin"
    large_file.write_bytes(b"x" * (3 * 1024 * 1024 + 1))
    post_calls: list[tuple[str, object]] = []
    put_calls: list[tuple[str, dict[str, str], int]] = []

    def fake_post(url, headers=None, json=None, timeout=None):
        post_calls.append((url, json))
        if url.endswith("/users/steven@aech.ai/messages/draft-1/attachments/createUploadSession"):
            return _FakeResponse({"uploadUrl": "https://upload.microsoft.test/session"})
        raise AssertionError(f"Unexpected POST: {url}")

    def fake_put(url, headers=None, data=None, timeout=None):
        put_calls.append((url, headers or {}, len(data or b"")))
        return _FakeResponse({}, status_code=201)

    monkeypatch.setattr("src.poller.requests.post", fake_post)
    monkeypatch.setattr("src.poller.requests.put", fake_put)

    poller.add_message_attachment("draft-1", str(large_file))

    assert post_calls[0][0] == (
        "https://graph.microsoft.test/users/steven@aech.ai/messages/draft-1/attachments/createUploadSession"
    )
    assert post_calls[0][1]["AttachmentItem"]["size"] == large_file.stat().st_size
    assert put_calls
    assert put_calls[0][0] == "https://upload.microsoft.test/session"
    assert put_calls[0][1]["Content-Range"].startswith("bytes 0-")
