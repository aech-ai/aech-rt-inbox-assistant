import json
import sys
from pathlib import Path

from click.testing import CliRunner

from src.database import init_db

PACKAGE_SRC = Path(__file__).resolve().parents[1] / "packages" / "aech-cli-inbox-assistant" / "src"
if str(PACKAGE_SRC) not in sys.path:
    sys.path.insert(0, str(PACKAGE_SRC))

from aech_cli_inbox_assistant.main import app  # noqa: E402


def _seed_db(db_path: Path, state_dir: Path) -> None:
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO emails (
            id, conversation_id, subject, sender, to_emails, cc_emails,
            received_at, body_preview, body_markdown, has_attachments, is_read,
            outlook_categories, created_at, updated_at
        )
        VALUES (
            'email-1', 'thread-1', 'Quarterly update', 'ceo@example.com',
            '["steven@aech.ai"]', '[]',
            '2026-03-08T10:00:00', 'preview', 'full body', 1, 0,
            '["Work"]', datetime('now', '-1 hour'), datetime('now')
        )
        """
    )
    conn.execute(
        """
        INSERT INTO attachments (
            id, email_id, filename, content_type, size_bytes, extraction_status,
            extracted_text, storage_path, downloaded_at, stored_at, extracted_at,
            created_at, updated_at
        )
        VALUES (
            'att-1', 'email-1', 'budget.txt', 'text/plain', 11, 'completed',
            'budget text', 'attachments/att-1/budget.txt',
            datetime('now'), datetime('now'), datetime('now'),
            datetime('now'), datetime('now')
        )
        """
    )
    conn.commit()
    conn.close()

    attachment_path = state_dir / "attachments" / "att-1" / "budget.txt"
    attachment_path.parent.mkdir(parents=True, exist_ok=True)
    attachment_path.write_text("budget text")


def test_cli_email_get_and_attachment_fetch(monkeypatch, tmp_path: Path):
    state_dir = tmp_path / ".inbox-assistant"
    db_path = state_dir / "assistant.sqlite"
    monkeypatch.setenv("INBOX_STATE_DIR", str(state_dir))
    monkeypatch.setenv("INBOX_DB_PATH", str(db_path))
    monkeypatch.setenv("DELEGATED_USER", "steven@aech.ai")

    init_db(db_path)
    _seed_db(db_path, state_dir)

    runner = CliRunner()

    result = runner.invoke(app, ["email", "get", "email-1"])
    assert result.exit_code == 0
    email = json.loads(result.output)
    assert email["id"] == "email-1"
    assert email["attachments"][0]["id"] == "att-1"
    assert email["attachments"][0]["stored_path"].endswith("budget.txt")

    result = runner.invoke(app, ["email", "thread", "thread-1"])
    assert result.exit_code == 0
    thread = json.loads(result.output)
    assert thread["conversation_id"] == "thread-1"
    assert thread["message_count"] == 1

    result = runner.invoke(app, ["attachment", "text", "att-1"])
    assert result.exit_code == 0
    attachment_text = json.loads(result.output)
    assert attachment_text["extracted_text"] == "budget text"

    output_path = tmp_path / "copied-budget.txt"
    result = runner.invoke(app, ["attachment", "fetch", "att-1", "--output", str(output_path)])
    assert result.exit_code == 0
    fetched = json.loads(result.output)
    assert fetched["output_path"] == str(output_path)
    assert output_path.read_text() == "budget text"


def test_cli_email_changes_uses_updated_at(monkeypatch, tmp_path: Path):
    state_dir = tmp_path / ".inbox-assistant"
    db_path = state_dir / "assistant.sqlite"
    monkeypatch.setenv("INBOX_STATE_DIR", str(state_dir))
    monkeypatch.setenv("INBOX_DB_PATH", str(db_path))

    init_db(db_path)

    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO emails (
            id, conversation_id, subject, sender, to_emails, cc_emails,
            received_at, body_preview, created_at, updated_at, is_read
        )
        VALUES (
            'old-email', 'thread-old', 'Old', 'sender@example.com', '[]', '[]',
            '2026-03-01T00:00:00', 'old preview',
            '2026-03-01T00:00:00', '2026-03-01T00:00:00', 0
        ),
        (
            'new-email', 'thread-new', 'New', 'sender@example.com', '[]', '[]',
            '2026-03-08T00:00:00', 'new preview',
            '2026-03-08T00:00:00', '2026-03-08T12:00:00', 0
        )
        """
    )
    conn.commit()
    conn.close()

    runner = CliRunner()
    result = runner.invoke(app, ["email", "changes", "--since", "2026-03-05T00:00:00"])
    assert result.exit_code == 0
    rows = json.loads(result.output)
    assert [row["id"] for row in rows] == ["new-email"]


def test_cli_email_project_batch_returns_email_thread_and_attachment_manifests(monkeypatch, tmp_path: Path):
    state_dir = tmp_path / ".inbox-assistant"
    db_path = state_dir / "assistant.sqlite"
    monkeypatch.setenv("INBOX_STATE_DIR", str(state_dir))
    monkeypatch.setenv("INBOX_DB_PATH", str(db_path))

    init_db(db_path)
    _seed_db(db_path, state_dir)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "email",
            "project-batch",
            "--received-after",
            "2026-03-01T00:00:00",
            "--limit",
            "10",
            "--include-read",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["count"] == 1
    assert payload["items"][0]["message_key"]
    assert payload["items"][0]["email"]["id"] == "email-1"
    assert payload["items"][0]["email"]["attachments"][0]["id"] == "att-1"
    assert payload["items"][0]["thread"]["conversation_id"] == "thread-1"
    assert payload["items"][0]["thread"]["message_count"] == 1


def test_cli_categories_workflow(monkeypatch, tmp_path: Path):
    state_dir = tmp_path / ".inbox-assistant"
    db_path = state_dir / "assistant.sqlite"
    prefs_path = tmp_path / "preferences.json"
    monkeypatch.setenv("INBOX_STATE_DIR", str(state_dir))
    monkeypatch.setenv("INBOX_DB_PATH", str(db_path))
    monkeypatch.setenv("AECH_PREFERENCES_PATH", str(prefs_path))

    init_db(db_path)

    runner = CliRunner()

    result = runner.invoke(app, ["categories", "show"])
    assert result.exit_code == 0
    shown = json.loads(result.output)
    assert shown["configured"] is False
    assert any(category["name"] == "Action Required" for category in shown["categories"])

    result = runner.invoke(app, ["categories", "init-defaults"])
    assert result.exit_code == 0
    initialized = json.loads(result.output)
    assert initialized["initialized"] is True
    assert prefs_path.exists()

    result = runner.invoke(
        app,
        [
            "categories",
            "add",
            "Finance",
            "--color",
            "purple",
            "--description",
            "Finance-related mail",
            "--flag-urgency",
            "this_week",
        ],
    )
    assert result.exit_code == 0
    added = json.loads(result.output)
    assert added["category"]["name"] == "Finance"
    assert added["category"]["preset"] == "preset8"

    result = runner.invoke(
        app,
        [
            "categories",
            "update",
            "Finance",
            "--new-name",
            "Finance Ops",
            "--color",
            "teal",
            "--clear-flag-urgency",
        ],
    )
    assert result.exit_code == 0
    updated = json.loads(result.output)
    assert updated["category"]["name"] == "Finance Ops"
    assert updated["category"]["color"] == "teal"
    assert updated["category"]["flag_urgency"] is None

    result = runner.invoke(app, ["categories", "remove", "Finance Ops"])
    assert result.exit_code == 0
    removed = json.loads(result.output)
    assert removed["removed"]["name"] == "Finance Ops"

    result = runner.invoke(app, ["categories", "colors"])
    assert result.exit_code == 0
    colors = json.loads(result.output)
    assert any(item["name"] == "purple" for item in colors["colors"])
    assert "today" in colors["valid_flag_urgencies"]
