# Package marker for inbox assistant runtime.

# Initialize LLM observability ONCE for all code paths
# This runs when any src.* module is imported
from aech_llm_observability import init_instrumentation, set_llm_log_path
from .database import get_state_dir

init_instrumentation(service_name="inbox-assistant")

# Set log path (uses same state dir as database)
_log_path = get_state_dir() / "llm.jsonl"
_log_path.parent.mkdir(parents=True, exist_ok=True)
set_llm_log_path(_log_path)
