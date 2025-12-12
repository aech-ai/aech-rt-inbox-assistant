import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

CAPABILITY_NAME = "inbox-assistant"


def get_user_root() -> Path:
    configured = os.environ.get("AECH_USER_DIR")
    if configured:
        return Path(configured).expanduser().resolve()

    container_root = Path("/home/agentaech")
    if container_root.exists():
        return container_root

    return Path.home().resolve()


def get_state_dir() -> Path:
    configured = os.environ.get("INBOX_STATE_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return get_user_root() / "capabilities" / CAPABILITY_NAME


def get_db_path() -> Path:
    configured = os.environ.get("INBOX_DB_PATH")
    if configured:
        return Path(configured).expanduser().resolve()
    return get_state_dir() / "state.sqlite"


def get_preferences_path() -> Path:
    configured = os.environ.get("AECH_PREFERENCES_PATH")
    if configured:
        return Path(configured).expanduser().resolve()
    return get_user_root() / "preferences.json"


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def read_preferences() -> Dict[str, Any]:
    path = get_preferences_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text() or "{}")
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def write_preferences(prefs: Dict[str, Any]) -> Path:
    path = get_preferences_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(prefs, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)
    return path


def set_preference(key: str, value: Any) -> Path:
    prefs = read_preferences()
    prefs[key] = value
    return write_preferences(prefs)


def _parse_value(raw: str) -> Any:
    raw = raw.strip()
    if not raw:
        return raw
    if raw.lower() in {"true", "false"}:
        return raw.lower() == "true"
    try:
        if "." in raw:
            return float(raw)
        return int(raw)
    except ValueError:
        pass
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def set_preference_from_string(key: str, raw_value: str) -> Path:
    return set_preference(key, _parse_value(raw_value))
