import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

CAPABILITY_NAME = "inbox-assistant"

# Valid top-level preference keys that the agent can set.
# Keep this list intentionally small until a real preference contract exists.
VALID_PREFERENCE_KEYS = {
    "categories",
}


class InvalidPreferenceKeyError(ValueError):
    """Raised when an unknown preference key is used."""
    pass


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
    return get_user_root() / f".{CAPABILITY_NAME}"


def get_db_path() -> Path:
    configured = os.environ.get("INBOX_DB_PATH")
    if configured:
        return Path(configured).expanduser().resolve()
    return get_state_dir() / "assistant.sqlite"


def get_preferences_path() -> Path:
    configured = os.environ.get("AECH_PREFERENCES_PATH")
    if configured:
        return Path(configured).expanduser().resolve()
    return get_user_root() / "preferences.json"


def connect_db(*, read_only: bool = False) -> sqlite3.Connection:
    db_path = get_db_path()
    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found at {db_path}. "
            "The inbox has not been synced yet."
        )
    if read_only:
        # Subagents mount the shared inbox read-only. When the database uses WAL,
        # SQLite needs immutable mode to read table data without creating shm locks.
        conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    else:
        conn = sqlite3.connect(db_path)
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


def validate_preference_key(key: str) -> None:
    """Validate that a preference key is known.

    Raises:
        InvalidPreferenceKeyError: If the key is not in VALID_PREFERENCE_KEYS
    """
    if key not in VALID_PREFERENCE_KEYS:
        valid_keys = ", ".join(sorted(VALID_PREFERENCE_KEYS))
        raise InvalidPreferenceKeyError(
            f"Unknown preference key: '{key}'. "
            f"Valid keys are: {valid_keys}"
        )


def set_preference_from_string(key: str, raw_value: str) -> Path:
    """Set a preference from a string value, with validation.

    Raises:
        InvalidPreferenceKeyError: If the key is not a valid preference key
    """
    validate_preference_key(key)
    return set_preference(key, _parse_value(raw_value))


def get_capability_prefs(namespace: str) -> Dict[str, Any]:
    """Get all preferences for a specific namespace."""
    prefs = read_preferences()
    return prefs.get(namespace, {})


def set_capability_prefs(namespace: str, capability_prefs: Dict[str, Any]) -> Path:
    """Set all preferences for a specific namespace."""
    prefs = read_preferences()
    prefs[namespace] = capability_prefs
    return write_preferences(prefs)


def get_capability_pref(namespace: str, key: str, default: Any = None) -> Any:
    """Get a specific preference from a namespace."""
    capability_prefs = get_capability_prefs(namespace)
    return capability_prefs.get(key, default)


def set_capability_pref(namespace: str, key: str, value: Any) -> Path:
    """Set a specific preference in a namespace."""
    prefs = read_preferences()
    if namespace not in prefs:
        prefs[namespace] = {}
    prefs[namespace][key] = value
    return write_preferences(prefs)
