# Package marker for inbox assistant runtime.

from aech_llm_observability import (
    build_llm_log_path,
    init_instrumentation,
    set_llm_log_path,
)

init_instrumentation(service_name="inbox-assistant")

# Use inbox-assistant's mailbox-scoped state dir contract when constructing
# the default log path. The shared helper owns the policy; this package only
# passes the capability-specific env naming.
_log_path = build_llm_log_path(
    capability="inbox-assistant",
    state_dir_env="INBOX_STATE_DIR",
)
_log_path.parent.mkdir(parents=True, exist_ok=True)
set_llm_log_path(_log_path)
