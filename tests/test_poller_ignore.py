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
    def __init__(self, payload):
        self._payload = payload
        self.ok = True
        self.status_code = 200
        self.text = json.dumps(payload)

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
