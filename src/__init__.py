# Package marker for inbox assistant runtime.

# Initialize LLM observability ONCE for all code paths
# This runs when any src.* module is imported
from datetime import datetime, timezone
import os
from pathlib import Path

try:
    from aech_llm_observability import build_llm_log_path, init_instrumentation, set_llm_log_path
except ImportError:
    from aech_llm_observability import init_instrumentation, set_llm_log_path

    def build_llm_log_path(capability: str) -> Path:
        configured = os.environ.get("LLM_LOG_PATH")
        if configured:
            return Path(configured).expanduser().resolve()

        state_dir = os.environ.get("INBOX_STATE_DIR")
        if state_dir:
            log_dir = Path(state_dir).expanduser().resolve() / "llm_logs"
        else:
            log_dir = (Path.cwd() / "data" / "app_context" / capability / "llm_logs").resolve()

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return log_dir / f"{timestamp}-llm.jsonl"

init_instrumentation(service_name="inbox-assistant")

# Set log path: environment overrides, otherwise app_context/<capability>/llm_logs/<timestamp>-llm.jsonl
_log_path = build_llm_log_path(capability="inbox-assistant")
_log_path.parent.mkdir(parents=True, exist_ok=True)
set_llm_log_path(_log_path)
