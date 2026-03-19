from __future__ import annotations

import json

from src import main as service_main


def test_service_loop_syncs_drafts_folder(monkeypatch):
    calls: list[tuple] = []

    class FakePoller:
        def poll_inbox(self):
            calls.append(("poll_inbox",))

        def get_all_folders(self):
            return [
                {"id": "inbox-id", "displayName": "Inbox"},
                {"id": "sent-id", "displayName": "Sent Items"},
                {"id": "drafts-id", "displayName": "Drafts"},
            ]

        def delta_sync_folder(self, folder_id: str, folder_name: str, fetch_body: bool = True):
            calls.append(("delta_sync_folder", folder_id, folder_name, fetch_body))
            return (0, 0)

    async def fake_process_pending_content(concurrency: int = 5):
        calls.append(("process_pending_content", concurrency))

    monkeypatch.setenv("DELTA_SYNC_INTERVAL", "0")
    monkeypatch.setenv("SENT_SYNC_INTERVAL", "0")
    monkeypatch.setenv("DRAFT_SYNC_INTERVAL", "0")
    monkeypatch.setattr(service_main, "init_db", lambda: calls.append(("init_db",)))
    monkeypatch.setattr(service_main, "GraphPoller", FakePoller)
    monkeypatch.setattr(service_main, "process_cli_requests", lambda _poller: {"processed": 0, "failed": 0})
    monkeypatch.setattr(service_main, "process_pending_content", fake_process_pending_content)

    service_main.service_loop(
        "steven@aech.ai",
        poll_interval=1,
        run_once=True,
        concurrency=7,
        sync_sent_items=True,
    )

    assert ("init_db",) in calls
    assert ("poll_inbox",) in calls
    assert ("process_pending_content", 7) in calls
    assert ("delta_sync_folder", "inbox-id", "Inbox", True) in calls
    assert ("delta_sync_folder", "sent-id", "Sent Items", False) in calls
    assert ("delta_sync_folder", "drafts-id", "Drafts", True) in calls


def test_process_cli_requests_creates_done_result(monkeypatch, tmp_path):
    queue_root = tmp_path / "queue"
    monkeypatch.setenv("INBOX_ASSISTANT_CLI_QUEUE_ROOT", str(queue_root))

    request_path = queue_root / "outbox" / "req-1.json"
    request_path.parent.mkdir(parents=True, exist_ok=True)
    request_path.write_text(
        json.dumps(
            {
                "id": "req-1",
                "action": "draft.create",
                "payload": {
                    "subject": "Quarterly update",
                    "body": "Please review.",
                    "body_content_type": "text",
                    "to_recipients": ["alice@example.com"],
                    "cc_recipients": [],
                    "bcc_recipients": [],
                    "attachments": [],
                },
            }
        ),
        encoding="utf-8",
    )

    calls: list[tuple] = []

    class FakePoller:
        def create_draft(self, **kwargs):
            calls.append(("create_draft", kwargs))
            return {"id": "draft-1", "subject": kwargs["subject"], "isDraft": True}

    result = service_main.process_cli_requests(FakePoller())

    assert result == {"processed": 1, "failed": 0}
    done_path = queue_root / "done" / "req-1.json"
    assert done_path.exists()
    payload = json.loads(done_path.read_text(encoding="utf-8"))
    assert payload["ok"] is True
    assert payload["result"]["created_via"] == "new"
    assert payload["result"]["draft"]["id"] == "draft-1"
    assert calls == [
        (
            "create_draft",
            {
                "subject": "Quarterly update",
                "body": "Please review.",
                "body_content_type": "text",
                "to_recipients": ["alice@example.com"],
                "cc_recipients": [],
                "bcc_recipients": [],
                "attachments": [],
            },
        )
    ]
    assert not (queue_root / "outbox" / "req-1.json").exists()
    assert not (queue_root / "processing" / "req-1.json").exists()


def test_process_cli_requests_writes_failed_result(monkeypatch, tmp_path):
    queue_root = tmp_path / "queue"
    monkeypatch.setenv("INBOX_ASSISTANT_CLI_QUEUE_ROOT", str(queue_root))

    request_path = queue_root / "outbox" / "req-2.json"
    request_path.parent.mkdir(parents=True, exist_ok=True)
    request_path.write_text(
        json.dumps({"id": "req-2", "action": "draft.nope", "payload": {}}),
        encoding="utf-8",
    )

    class FakePoller:
        pass

    result = service_main.process_cli_requests(FakePoller())

    assert result == {"processed": 0, "failed": 1}
    failed_path = queue_root / "failed" / "req-2.json"
    assert failed_path.exists()
    payload = json.loads(failed_path.read_text(encoding="utf-8"))
    assert payload["ok"] is False
    assert "Unsupported action" in payload["error"]["message"]
    assert not (queue_root / "outbox" / "req-2.json").exists()
    assert not (queue_root / "processing" / "req-2.json").exists()


def test_process_cli_requests_resolves_staged_reply_attachments(monkeypatch, tmp_path):
    queue_root = tmp_path / "queue"
    monkeypatch.setenv("INBOX_ASSISTANT_CLI_QUEUE_ROOT", str(queue_root))

    staged_attachment = queue_root / "uploads" / "req-3" / "0000" / "reply.txt"
    staged_attachment.parent.mkdir(parents=True, exist_ok=True)
    staged_attachment.write_text("reply attachment", encoding="utf-8")

    request_path = queue_root / "outbox" / "req-3.json"
    request_path.parent.mkdir(parents=True, exist_ok=True)
    request_path.write_text(
        json.dumps(
            {
                "id": "req-3",
                "action": "draft.reply",
                "payload": {
                    "message_id": "msg-1",
                    "subject": "Re: Budget",
                    "body": "Please see attached.",
                    "body_content_type": "text",
                    "reply_all": True,
                    "attachments": ["uploads/req-3/0000/reply.txt"],
                },
            }
        ),
        encoding="utf-8",
    )

    calls: list[tuple] = []

    class FakePoller:
        def create_reply_draft(self, message_id: str, **kwargs):
            calls.append(("create_reply_draft", message_id, kwargs))
            return {"id": "draft-reply-1", "subject": kwargs["subject"], "isDraft": True}

    result = service_main.process_cli_requests(FakePoller())

    assert result == {"processed": 1, "failed": 0}
    assert calls == [
        (
            "create_reply_draft",
            "msg-1",
            {
                "subject": "Re: Budget",
                "body": "Please see attached.",
                "body_content_type": "text",
                "attachments": [str(staged_attachment)],
                "reply_all": True,
            },
        )
    ]
    assert not staged_attachment.exists()
