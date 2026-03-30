import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict

CAPABILITY_NAME = "inbox-assistant"
STATE_DB_NAME = "assistant.sqlite"
STATE_HINT_DIRS = ("attachments", "queries")
SHARED_INBOX_ROOT_ENV = "AECH_SHARED_INBOX_ROOT"
DEFAULT_SHARED_INBOX_ROOT = Path("/shared-inbox-root")

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


def _configured_db_path() -> Path | None:
    configured = os.environ.get("INBOX_DB_PATH")
    if configured:
        return Path(configured).expanduser().resolve()
    return None


def _configured_state_root() -> Path:
    configured = os.environ.get("INBOX_STATE_DIR")
    if configured:
        return Path(configured).expanduser().resolve()

    configured_db_path = _configured_db_path()
    if configured_db_path is not None:
        return configured_db_path.parent

    shared_root = os.environ.get(SHARED_INBOX_ROOT_ENV)
    if shared_root:
        return Path(shared_root).expanduser().resolve()

    if DEFAULT_SHARED_INBOX_ROOT.exists():
        return DEFAULT_SHARED_INBOX_ROOT.resolve()

    return get_user_root() / f".{CAPABILITY_NAME}"


def _delegated_user() -> str | None:
    delegated = os.environ.get("DELEGATED_USER", "").strip().lower()
    return delegated or None


def _mailbox_selector() -> str | None:
    for name in ("DELEGATED_USER", "DELEGATED_INBOX_USER", "AECH_SHARED_INBOX_MAILBOX"):
        value = os.environ.get(name, "").strip().lower()
        if value:
            return value
    return None


def _looks_like_mailbox_state_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    if (path / STATE_DB_NAME).exists():
        return True
    return any((path / name).is_dir() for name in STATE_HINT_DIRS)


def _available_mailbox_state_dirs(root: Path) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []

    mailbox_dirs: list[Path] = []
    for child in sorted(root.iterdir(), key=lambda item: item.name.lower()):
        if child.is_dir() and _looks_like_mailbox_state_dir(child):
            mailbox_dirs.append(child.resolve())
    return mailbox_dirs


def _infer_mailbox_from_state_dir(
    state_dir: Path,
    *,
    root_state_dir: Path,
    delegated_user: str | None,
) -> str | None:
    if state_dir.parent == root_state_dir:
        return state_dir.name
    if "@" in state_dir.name:
        return state_dir.name.lower()
    return delegated_user


def _resolve_state_binding() -> dict[str, Any]:
    delegated_user = _delegated_user()
    mailbox_selector = _mailbox_selector()
    explicit_state_dir = os.environ.get("INBOX_STATE_DIR")
    explicit_db_path = _configured_db_path()
    root_state_dir = _configured_state_root()
    available_mailbox_dirs = _available_mailbox_state_dirs(root_state_dir)
    available_mailboxes = [path.name for path in available_mailbox_dirs]

    if _looks_like_mailbox_state_dir(root_state_dir):
        state_dir = root_state_dir
        if explicit_state_dir:
            binding_mode = "explicit_state_dir"
        elif explicit_db_path is not None:
            binding_mode = "explicit_db_path"
        else:
            binding_mode = "root_default"
    elif mailbox_selector:
        candidate = (root_state_dir / mailbox_selector).resolve()
        if _looks_like_mailbox_state_dir(candidate):
            state_dir = candidate
            binding_mode = "shared_root_env_selected"
        elif available_mailboxes:
            raise RuntimeError(
                f"Configured mailbox selector '{mailbox_selector}' does not match a mailbox state under "
                f"{root_state_dir}. Available mailboxes: {', '.join(available_mailboxes)}"
            )
        else:
            state_dir = root_state_dir
            binding_mode = "explicit_state_dir" if explicit_state_dir else "root_default"
    elif len(available_mailbox_dirs) == 1:
        state_dir = available_mailbox_dirs[0]
        binding_mode = "single_mailbox_subdir"
    elif len(available_mailbox_dirs) > 1:
        raise RuntimeError(
            f"Ambiguous inbox state root at {root_state_dir}. "
            "Set INBOX_STATE_DIR to a mailbox-scoped directory, set INBOX_DB_PATH directly, "
            f"or set DELEGATED_USER to select one of: {', '.join(available_mailboxes)}"
        )
    else:
        state_dir = root_state_dir
        binding_mode = "explicit_state_dir" if explicit_state_dir else "root_default"

    db_path = explicit_db_path or (state_dir / STATE_DB_NAME)
    mailbox = _infer_mailbox_from_state_dir(
        state_dir,
        root_state_dir=root_state_dir,
        delegated_user=mailbox_selector or delegated_user,
    )
    return {
        "binding_mode": binding_mode,
        "root_state_dir": root_state_dir,
        "state_dir": state_dir,
        "db_path": db_path,
        "mailbox": mailbox,
        "delegated_user": delegated_user,
        "mailbox_selector": mailbox_selector,
        "available_mailboxes": available_mailboxes,
    }


def get_state_dir() -> Path:
    return _resolve_state_binding()["state_dir"]


def get_db_path() -> Path:
    return _resolve_state_binding()["db_path"]


def describe_state_context() -> Dict[str, Any]:
    root_state_dir = _configured_state_root()
    delegated_user = _delegated_user()
    mailbox_selector = _mailbox_selector()
    available_mailboxes = [path.name for path in _available_mailbox_state_dirs(root_state_dir)]

    payload: Dict[str, Any] = {
        "user_root": str(get_user_root()),
        "root_state_dir": str(root_state_dir),
        "state_dir": None,
        "db_path": str((_configured_db_path() or (root_state_dir / STATE_DB_NAME)).resolve()),
        "db_exists": False,
        "mailbox": None,
        "delegated_user": delegated_user,
        "mailbox_selector": mailbox_selector,
        "available_mailboxes": available_mailboxes,
        "binding_mode": "unknown",
        "error": None,
    }

    try:
        binding = _resolve_state_binding()
    except Exception as exc:
        payload["error"] = str(exc)
        return payload

    payload.update(
        {
            "root_state_dir": str(binding["root_state_dir"]),
            "state_dir": str(binding["state_dir"]),
            "db_path": str(binding["db_path"]),
            "db_exists": bool(binding["db_path"].exists()),
            "mailbox": binding["mailbox"],
            "mailbox_selector": binding["mailbox_selector"],
            "available_mailboxes": binding["available_mailboxes"],
            "binding_mode": binding["binding_mode"],
        }
    )
    return payload


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
