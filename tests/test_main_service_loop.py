from __future__ import annotations

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
