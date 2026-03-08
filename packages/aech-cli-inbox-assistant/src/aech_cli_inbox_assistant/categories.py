from __future__ import annotations

from typing import Any


DEFAULT_CATEGORIES: list[dict[str, Any]] = [
    {
        "name": "Action Required",
        "color": "red",
        "preset": "preset0",
        "flag_urgency": "today",
        "description": "Needs your response, decision, or action.",
    },
    {
        "name": "Follow Up",
        "color": "orange",
        "preset": "preset1",
        "flag_urgency": "this_week",
        "description": "Track this and circle back later.",
    },
    {
        "name": "Work",
        "color": "blue",
        "preset": "preset7",
        "flag_urgency": None,
        "description": "General work correspondence.",
    },
    {
        "name": "Personal",
        "color": "green",
        "preset": "preset4",
        "flag_urgency": None,
        "description": "Non-work personal correspondence.",
    },
]


COLOR_PRESETS: dict[str, str] = {
    "red": "preset0",
    "orange": "preset1",
    "brown": "preset2",
    "yellow": "preset3",
    "green": "preset4",
    "teal": "preset5",
    "olive": "preset6",
    "blue": "preset7",
    "purple": "preset8",
    "cranberry": "preset9",
    "steel": "preset10",
    "darksteel": "preset11",
    "gray": "preset12",
    "darkgray": "preset13",
    "black": "preset14",
}

VALID_FLAG_URGENCIES = {"immediate", "today", "this_week", "someday"}


def _normalize_color(color: str) -> str:
    value = color.strip().lower()
    if value not in COLOR_PRESETS:
        valid = ", ".join(sorted(COLOR_PRESETS))
        raise ValueError(f"Unknown color '{color}'. Valid colors: {valid}")
    return value


def _normalize_flag_urgency(flag_urgency: str | None) -> str | None:
    if flag_urgency in (None, ""):
        return None
    value = flag_urgency.strip().lower()
    if value not in VALID_FLAG_URGENCIES:
        valid = ", ".join(sorted(VALID_FLAG_URGENCIES))
        raise ValueError(f"Unknown flag urgency '{flag_urgency}'. Valid values: {valid}")
    return value


def _categories_copy(categories: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [dict(category) for category in categories]


def get_categories(prefs: dict[str, Any] | None) -> tuple[list[dict[str, Any]], bool]:
    if not prefs:
        return _categories_copy(DEFAULT_CATEGORIES), False

    categories = prefs.get("categories")
    if not isinstance(categories, list):
        return _categories_copy(DEFAULT_CATEGORIES), False
    return _categories_copy(categories), True


def ensure_categories_initialized(prefs: dict[str, Any]) -> tuple[list[dict[str, Any]], bool]:
    categories, configured = get_categories(prefs)
    if configured:
        return categories, False
    prefs["categories"] = categories
    return categories, True


def add_category(
    prefs: dict[str, Any],
    name: str,
    color: str = "blue",
    description: str = "",
    flag_urgency: str | None = None,
) -> dict[str, Any]:
    categories, _ = ensure_categories_initialized(prefs)
    lowered = name.strip().lower()
    if any(category["name"].strip().lower() == lowered for category in categories):
        raise ValueError(f"Category '{name}' already exists")

    normalized_color = _normalize_color(color)
    normalized_flag = _normalize_flag_urgency(flag_urgency)
    category = {
        "name": name.strip(),
        "color": normalized_color,
        "preset": COLOR_PRESETS[normalized_color],
        "flag_urgency": normalized_flag,
        "description": description.strip(),
    }
    categories.append(category)
    prefs["categories"] = categories
    return category


def remove_category(prefs: dict[str, Any], name: str) -> dict[str, Any]:
    categories, _ = ensure_categories_initialized(prefs)
    lowered = name.strip().lower()
    for index, category in enumerate(categories):
        if category["name"].strip().lower() == lowered:
            removed = categories.pop(index)
            prefs["categories"] = categories
            return removed
    raise ValueError(f"Category '{name}' not found")


def update_category(
    prefs: dict[str, Any],
    name: str,
    *,
    new_name: str | None = None,
    color: str | None = None,
    description: str | None = None,
    flag_urgency: str | None = None,
    clear_flag_urgency: bool = False,
) -> dict[str, Any]:
    categories, _ = ensure_categories_initialized(prefs)
    lowered = name.strip().lower()
    for category in categories:
        if category["name"].strip().lower() != lowered:
            continue

        if new_name:
            new_lowered = new_name.strip().lower()
            if new_lowered != lowered and any(
                existing["name"].strip().lower() == new_lowered for existing in categories
            ):
                raise ValueError(f"Category '{new_name}' already exists")
            category["name"] = new_name.strip()
        if color is not None:
            normalized_color = _normalize_color(color)
            category["color"] = normalized_color
            category["preset"] = COLOR_PRESETS[normalized_color]
        if description is not None:
            category["description"] = description.strip()
        if clear_flag_urgency:
            category["flag_urgency"] = None
        elif flag_urgency is not None:
            category["flag_urgency"] = _normalize_flag_urgency(flag_urgency)

        prefs["categories"] = categories
        return dict(category)

    raise ValueError(f"Category '{name}' not found")


def reset_categories(prefs: dict[str, Any]) -> list[dict[str, Any]]:
    categories = _categories_copy(DEFAULT_CATEGORIES)
    prefs["categories"] = categories
    return categories
