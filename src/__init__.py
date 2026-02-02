# Package marker for inbox assistant runtime.

# Initialize LLM observability ONCE for all code paths
# This runs when any src.* module is imported
from aech_llm_observability import build_llm_log_path, init_instrumentation, set_llm_log_path

init_instrumentation(service_name="inbox-assistant")

# Set log path: environment overrides, otherwise app_context/<capability>/llm_logs/<timestamp>-llm.jsonl
_log_path = build_llm_log_path(capability="inbox-assistant")
_log_path.parent.mkdir(parents=True, exist_ok=True)
set_llm_log_path(_log_path)
