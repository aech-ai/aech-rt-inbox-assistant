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
