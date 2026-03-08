import json
from pathlib import Path

from src.attachments import AttachmentProcessor
from src.database import get_connection, get_state_dir, init_db


def _insert_email(conn, email_id: str) -> None:
    conn.execute(
        """
        INSERT INTO emails (
            id, conversation_id, subject, sender, to_emails, cc_emails,
            received_at, body_preview, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?, datetime('now'))
        """,
        (
            email_id,
            "thread-1",
            f"Subject for {email_id}",
            "sender@example.com",
            json.dumps(["steven@aech.ai"]),
            json.dumps([]),
            "preview",
        ),
    )


def _insert_attachment(conn, attachment_id: str, email_id: str, filename: str) -> None:
    conn.execute(
        """
        INSERT INTO attachments (
            id, email_id, filename, content_type, size_bytes, extraction_status, updated_at
        )
        VALUES (?, ?, ?, 'text/plain', 5, 'pending', datetime('now'))
        """,
        (attachment_id, email_id, filename),
    )


def test_process_attachment_stores_blob_and_reuses_duplicate(monkeypatch, tmp_path: Path):
    state_dir = tmp_path / ".inbox-assistant"
    monkeypatch.setenv("DELEGATED_USER", "steven@aech.ai")
    monkeypatch.setenv("INBOX_STATE_DIR", str(state_dir))
    monkeypatch.setenv("INBOX_DB_PATH", str(state_dir / "assistant.sqlite"))

    init_db()

    conn = get_connection()
    _insert_email(conn, "email-1")
    _insert_email(conn, "email-2")
    _insert_attachment(conn, "att-1", "email-1", "one.txt")
    _insert_attachment(conn, "att-2", "email-2", "two.txt")
    conn.commit()
    conn.close()

    processor = AttachmentProcessor.__new__(AttachmentProcessor)
    processor.user_email = "steven@aech.ai"
    processor._graph_client = object()

    monkeypatch.setattr(
        AttachmentProcessor,
        "_download_attachment",
        lambda self, email_id, attachment_id: b"hello",
    )
    monkeypatch.setattr(AttachmentProcessor, "_index_attachment", lambda self, attachment_id, filename: None)

    assert processor.process_attachment({"id": "att-1", "email_id": "email-1", "filename": "one.txt", "content_type": "text/plain"})
    assert processor.process_attachment({"id": "att-2", "email_id": "email-2", "filename": "two.txt", "content_type": "text/plain"})

    conn = get_connection()
    first = dict(conn.execute("SELECT * FROM attachments WHERE id = 'att-1'").fetchone())
    second = dict(conn.execute("SELECT * FROM attachments WHERE id = 'att-2'").fetchone())
    conn.close()

    assert first["storage_path"]
    assert first["extracted_text"] == "hello"
    assert second["storage_path"] == first["storage_path"]
    assert second["extracted_text"] == "hello"

    stored_path = get_state_dir() / first["storage_path"]
    assert stored_path.exists()
    assert stored_path.read_text() == "hello"
