import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

CAPABILITY_NAME = "inbox-assistant"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    os.replace(tmp_path, path)


def write_trigger(
    user_email: str,
    trigger_type: str,
    payload: Dict[str, Any],
    *,
    routing: Optional[Dict[str, Any]] = None,
    trigger_id: Optional[str] = None,
    created_at: Optional[str] = None,
    outbox_dir: Optional[Path] = None,
    write_legacy: bool | None = None,
) -> Dict[str, Any]:
    """
    Emit a v1 RT trigger.

    Preferred format: one JSON file per trigger in the capability outbox directory.
      - write <uuid>.json.tmp then rename to <uuid>.json (atomic claim friendly)

    Legacy format (optional): append into a shared rt-triggers.json object.
    """
    trigger_uuid = trigger_id or str(uuid.uuid4())
    trigger: Dict[str, Any] = {
        "id": trigger_uuid,
        "user": user_email,
        "type": trigger_type,
        "created_at": created_at or _now_utc_iso(),
        "payload": payload,
    }
    if routing:
        trigger["routing"] = routing

    outbox = outbox_dir or Path(os.environ.get("RT_OUTBOX_DIR", "/triggers/outbox"))
    _atomic_write_json(outbox / f"{trigger_uuid}.json", trigger)

    if write_legacy is None:
        write_legacy = os.environ.get("RT_WRITE_LEGACY", "").strip().lower() in {"1", "true", "yes"}
    if write_legacy:
        legacy_path = Path(os.environ.get("RT_LEGACY_FILE", "/triggers/rt-triggers.json"))
        _write_legacy_trigger(legacy_path, trigger)

    return trigger


def _write_legacy_trigger(path: Path, trigger: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        try:
            data = json.loads(path.read_text() or "{}")
        except json.JSONDecodeError:
            data = {}
    else:
        data = {}

    existing = data.get(CAPABILITY_NAME)
    if not isinstance(existing, list):
        existing = []
        data[CAPABILITY_NAME] = existing

    existing.append(trigger)
    _atomic_write_json(path, data)
