import json
import os
import time
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


def make_dedupe_key(trigger_type: str, user_email: str, primary_id: str) -> str:
    return f"{CAPABILITY_NAME}:{trigger_type}:{user_email}:{primary_id}"


def _sanitize_dedupe_key(key: str) -> str:
    sanitized = key.replace(os.sep, "_")
    if os.altsep:
        sanitized = sanitized.replace(os.altsep, "_")
    return sanitized


def _dedupe_ttl_seconds(ttl_days: Optional[int]) -> int:
    if ttl_days is None:
        raw = os.environ.get("RT_DEDUPE_TTL_DAYS", "7")
        try:
            ttl_days = int(raw)
        except ValueError:
            ttl_days = 7
    return max(0, ttl_days) * 24 * 60 * 60


def _dedupe_marker_path(dedupe_dir: Path, dedupe_key: str) -> Path:
    return dedupe_dir / _sanitize_dedupe_key(dedupe_key)


def _is_marker_fresh(marker: Path, ttl_seconds: int) -> bool:
    if ttl_seconds <= 0 or not marker.exists():
        return False
    try:
        age = time.time() - marker.stat().st_mtime
    except OSError:
        return False
    return age < ttl_seconds


def _claim_dedupe_marker(dedupe_dir: Path, dedupe_key: str, ttl_seconds: int, trigger_id: str) -> bool:
    if ttl_seconds <= 0:
        return True

    marker = _dedupe_marker_path(dedupe_dir, dedupe_key)
    if _is_marker_fresh(marker, ttl_seconds):
        return False

    if marker.exists():
        try:
            marker.unlink()
        except OSError:
            return False

    marker.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False

    with os.fdopen(fd, "w") as handle:
        handle.write(
            json.dumps({"dedupe_key": dedupe_key, "trigger_id": trigger_id, "created_at": _now_utc_iso()})
            + "\n"
        )
    return True


def write_trigger(
    user_email: str,
    trigger_type: str,
    payload: Dict[str, Any],
    *,
    dedupe_key: str,
    routing: Optional[Dict[str, Any]] = None,
    trigger_id: Optional[str] = None,
    created_at: Optional[str] = None,
    outbox_dir: Optional[Path] = None,
    dedupe_dir: Optional[Path] = None,
    dedupe_ttl_days: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Emit a v1 RT trigger.

    Preferred format: one JSON file per trigger in the capability outbox directory.
      - write <uuid>.json.tmp then rename to <uuid>.json (atomic claim friendly)
    Creates a dedupe marker to avoid emitting duplicate triggers.
    """
    if not dedupe_key:
        raise ValueError("dedupe_key is required")

    trigger_uuid = trigger_id or str(uuid.uuid4())
    trigger: Dict[str, Any] = {
        "id": trigger_uuid,
        "capability": CAPABILITY_NAME,
        "user": user_email,
        "type": trigger_type,
        "created_at": created_at or _now_utc_iso(),
        "dedupe_key": dedupe_key,
        "payload": payload,
    }
    if routing:
        trigger["routing"] = routing

    outbox = outbox_dir or Path(os.environ.get("RT_OUTBOX_DIR", "/triggers/outbox"))
    dedupe_root = dedupe_dir or Path(os.environ.get("RT_DEDUPE_DIR", str(outbox.parent / "dedupe" / "emitted")))
    ttl_seconds = _dedupe_ttl_seconds(dedupe_ttl_days)
    if not _claim_dedupe_marker(dedupe_root, dedupe_key, ttl_seconds, trigger_uuid):
        return trigger

    _atomic_write_json(outbox / f"{trigger_uuid}.json", trigger)

    return trigger
