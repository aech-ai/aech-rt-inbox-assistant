import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

from click.testing import CliRunner

PACKAGE_SRC = Path(__file__).resolve().parents[1] / "packages" / "aech-cli-inbox-assistant" / "src"
if str(PACKAGE_SRC) not in sys.path:
    sys.path.insert(0, str(PACKAGE_SRC))

from aech_cli_inbox_assistant import main as cli_main  # noqa: E402
from aech_cli_inbox_assistant.main import app  # noqa: E402


def test_src_import_uses_shared_observability_path_builder(monkeypatch, tmp_path: Path):
    state_dir = tmp_path / ".inbox-assistant"
    monkeypatch.setenv("INBOX_STATE_DIR", str(state_dir))
    monkeypatch.delenv("LLM_LOG_DIR", raising=False)
    monkeypatch.delenv("LLM_LOG_PATH", raising=False)

    observability = importlib.import_module("aech_llm_observability")
    previous_path = observability.get_llm_log_path()
    try:
        src_module = importlib.import_module("src")
        importlib.reload(src_module)
        log_path = observability.get_llm_log_path()
        assert log_path is not None
        assert log_path.parent == state_dir / "llm_logs"
        assert log_path.name.endswith("-llm.jsonl")
    finally:
        observability.set_llm_log_path(previous_path)


def test_cli_search_imports_runtime_module_from_src_package(monkeypatch):
    imported: list[str] = []

    def fake_import_module(name: str):
        imported.append(name)
        if name != "src.search":
            raise AssertionError(f"Unexpected import: {name}")
        return SimpleNamespace(
            unified_search=lambda **kwargs: [
                SimpleNamespace(
                    id="chunk-1",
                    result_type="email",
                    source_id="email-1",
                    content_preview="preview",
                    score=0.9,
                    email_subject="Budget",
                    email_sender="ceo@example.com",
                    email_date="2026-03-10T12:00:00",
                    conversation_id="thread-1",
                    filename=None,
                    fact_type=None,
                    fact_value=None,
                    web_link="https://example.com/mail/1",
                )
            ]
        )

    monkeypatch.setattr(cli_main.importlib, "import_module", fake_import_module)

    runner = CliRunner()
    result = runner.invoke(app, ["search", "budget"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert imported == ["src.search"]
    assert payload[0]["id"] == "chunk-1"
    assert payload[0]["web_link"] == "https://example.com/mail/1"


def test_cli_ask_imports_runtime_module_from_src_package(monkeypatch):
    imported: list[str] = []

    async def fake_run_query_agent(**kwargs):
        return {
            "answer": "Found it.",
            "matched_emails": [],
            "suggested_next_steps": [],
            "clarification_question": None,
        }

    def fake_import_module(name: str):
        imported.append(name)
        if name != "src.query_agent":
            raise AssertionError(f"Unexpected import: {name}")
        return SimpleNamespace(run_query_agent=fake_run_query_agent)

    monkeypatch.setenv("DELEGATED_USER", "steven@aech.ai")
    monkeypatch.setattr(cli_main.importlib, "import_module", fake_import_module)

    runner = CliRunner()
    result = runner.invoke(app, ["ask", "show me budget email"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert imported == ["src.query_agent"]
    assert payload["answer"] == "Found it."


def test_cli_draft_create_imports_runtime_module_from_src_package(monkeypatch, tmp_path: Path):
    imported: list[str] = []
    captured: dict[str, object] = {}
    attachment_one = tmp_path / "one.txt"
    attachment_two = tmp_path / "two.txt"
    attachment_one.write_text("one", encoding="utf-8")
    attachment_two.write_text("two", encoding="utf-8")

    class FakeGraphPoller:
        def create_draft(
            self,
            *,
            subject: str,
            body: str,
            body_content_type: str,
            to_recipients: list[str],
            cc_recipients: list[str],
            bcc_recipients: list[str],
            attachments: list[str],
        ):
            captured["subject"] = subject
            captured["body"] = body
            captured["body_content_type"] = body_content_type
            captured["to_recipients"] = to_recipients
            captured["cc_recipients"] = cc_recipients
            captured["bcc_recipients"] = bcc_recipients
            captured["attachments"] = attachments
            return {
                "id": "draft-1",
                "subject": subject,
                "conversationId": "thread-1",
                "webLink": "https://example.com/drafts/1",
                "isDraft": True,
                "bodyPreview": body[:255],
                "toRecipients": [
                    {"emailAddress": {"address": address}} for address in to_recipients
                ],
                "ccRecipients": [
                    {"emailAddress": {"address": address}} for address in cc_recipients
                ],
                "bccRecipients": [
                    {"emailAddress": {"address": address}} for address in bcc_recipients
                ],
                "attachments": [
                    {
                        "id": "att-1",
                        "name": "one.txt",
                        "contentType": "text/plain",
                        "size": 3,
                    },
                    {
                        "id": "att-2",
                        "name": "two.txt",
                        "contentType": "text/plain",
                        "size": 3,
                    },
                ],
            }

    def fake_import_module(name: str):
        imported.append(name)
        if name != "src.poller":
            raise AssertionError(f"Unexpected import: {name}")
        return SimpleNamespace(GraphPoller=FakeGraphPoller)

    monkeypatch.setattr(cli_main.importlib, "import_module", fake_import_module)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "draft",
            "create",
            "--to",
            "alice@example.com,bob@example.com",
            "--cc",
            "finance@example.com",
            "--bcc",
            "legal@example.com",
            "--subject",
            "Draft subject",
            "--body",
            "Draft body",
                "--content-type",
                "html",
                "--attachment",
                str(attachment_one),
                "--attachment",
                str(attachment_two),
            ],
        )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert imported == ["src.poller"]
    assert captured == {
        "subject": "Draft subject",
        "body": "Draft body",
        "body_content_type": "html",
        "to_recipients": ["alice@example.com", "bob@example.com"],
        "cc_recipients": ["finance@example.com"],
        "bcc_recipients": ["legal@example.com"],
        "attachments": [str(attachment_one), str(attachment_two)],
    }
    assert payload["created_via"] == "new"
    assert payload["draft"]["id"] == "draft-1"
    assert payload["draft"]["to_recipients"] == ["alice@example.com", "bob@example.com"]
    assert payload["draft"]["attachments"][0]["id"] == "att-1"


def test_cli_draft_reply_imports_runtime_module_from_src_package(monkeypatch, tmp_path: Path):
    imported: list[str] = []
    captured: dict[str, object] = {}
    attachment_path = tmp_path / "reply.txt"
    attachment_path.write_text("reply attachment", encoding="utf-8")

    class FakeGraphPoller:
        def create_reply_draft(
            self,
            message_id: str,
            *,
            subject: str | None,
            body: str,
            body_content_type: str,
            attachments: list[str],
            reply_all: bool,
        ):
            captured["message_id"] = message_id
            captured["subject"] = subject
            captured["body"] = body
            captured["body_content_type"] = body_content_type
            captured["attachments"] = attachments
            captured["reply_all"] = reply_all
            return {
                "id": "draft-reply-1",
                "subject": "Re: Draft subject",
                "conversationId": "thread-1",
                "webLink": "https://example.com/drafts/reply-1",
                "isDraft": True,
                "bodyPreview": body[:255],
                "toRecipients": [{"emailAddress": {"address": "alice@example.com"}}],
                "ccRecipients": [{"emailAddress": {"address": "finance@example.com"}}],
                "bccRecipients": [],
                "attachments": [
                    {
                        "id": "att-reply-1",
                        "name": "reply.txt",
                        "contentType": "text/plain",
                        "size": 16,
                    }
                ],
            }

    def fake_import_module(name: str):
        imported.append(name)
        if name != "src.poller":
            raise AssertionError(f"Unexpected import: {name}")
        return SimpleNamespace(GraphPoller=FakeGraphPoller)

    monkeypatch.setattr(cli_main.importlib, "import_module", fake_import_module)

    runner = CliRunner()
    result = runner.invoke(
        app,
            [
                "draft",
                "reply",
                "email-1",
                "--subject",
                "Re: Quarterly update",
                "--body",
                "<p>Thanks, will review.</p>",
                "--content-type",
                "html",
                "--attachment",
                str(attachment_path),
                "--reply-all",
            ],
        )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert imported == ["src.poller"]
    assert captured == {
        "message_id": "email-1",
        "subject": "Re: Quarterly update",
        "body": "<p>Thanks, will review.</p>",
        "body_content_type": "html",
        "attachments": [str(attachment_path)],
        "reply_all": True,
    }
    assert payload["created_via"] == "reply_all"
    assert payload["draft"]["id"] == "draft-reply-1"
    assert payload["draft"]["cc_recipients"] == ["finance@example.com"]
    assert payload["draft"]["attachments"][0]["id"] == "att-reply-1"


def test_cli_draft_create_rejects_body_and_body_file(monkeypatch, tmp_path: Path):
    imported: list[str] = []
    body_path = tmp_path / "draft.txt"
    body_path.write_text("Draft body", encoding="utf-8")

    class FakeGraphPoller:
        def __init__(self):
            raise AssertionError("GraphPoller should not be instantiated when input validation fails")

    def fake_import_module(name: str):
        imported.append(name)
        if name != "src.poller":
            raise AssertionError(f"Unexpected import: {name}")
        return SimpleNamespace(GraphPoller=FakeGraphPoller)

    monkeypatch.setattr(cli_main.importlib, "import_module", fake_import_module)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "draft",
            "create",
            "--body",
            "",
            "--body-file",
            str(body_path),
        ],
    )
    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert imported == ["src.poller"]
    assert payload["error"] == "invalid_input"
