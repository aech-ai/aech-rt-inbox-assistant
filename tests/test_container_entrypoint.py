from __future__ import annotations

from pathlib import Path

from scripts.container_entrypoint import build_owned_paths


def test_build_owned_paths_tracks_runtime_state_and_fallback_home() -> None:
    ensure_dirs, repair_paths = build_owned_paths(
        {
            "INBOX_STATE_DIR": "/app/state/steven@aech.ai",
            "INBOX_DB_PATH": "/app/state/steven@aech.ai/assistant.sqlite",
            "LLM_LOG_DIR": "/app/state/llm_logs",
        }
    )

    assert Path("/home/agentaech/.inbox-assistant") in ensure_dirs
    assert Path("/app/state/steven@aech.ai") in ensure_dirs
    assert Path("/app/state/steven@aech.ai") in repair_paths
    assert Path("/app/state/llm_logs") in repair_paths
