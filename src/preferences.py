import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from .database import get_user_root


def get_preferences_path(user_root: Optional[Path] = None) -> Path:
    configured = os.environ.get("AECH_PREFERENCES_PATH")
    if configured:
        return Path(configured).expanduser().resolve()
    return (user_root or get_user_root()) / "preferences.json"


def read_preferences(user_root: Optional[Path] = None) -> Dict[str, Any]:
    path = get_preferences_path(user_root)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text() or "{}")
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def write_preferences(prefs: Dict[str, Any], user_root: Optional[Path] = None) -> Path:
    path = get_preferences_path(user_root)
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(prefs, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)
    return path


def set_preference(key: str, value: Any, user_root: Optional[Path] = None) -> Path:
    prefs = read_preferences(user_root)
    prefs[key] = value
    return write_preferences(prefs, user_root)
