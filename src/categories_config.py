"""
Outlook Categories configuration for email organization.

This module provides default categories and utility functions for the
categories-based email organization system. Categories are applied as
Outlook labels (staying in Inbox) rather than moving to folders.

Preferences are stored under the "inbox_assistant" namespace:
{
    "inbox_assistant": {
        "categories": [...]
    }
}
"""

from typing import Any

# Namespace for inbox assistant preferences
NAMESPACE = "inbox_assistant"
CATEGORIES_KEY = "categories"

# Default categories with colors and flag behavior
DEFAULT_CATEGORIES: list[dict[str, Any]] = [
    {
        "name": "Action Required",
        "color": "red",
        "preset": "preset0",
        "flag_urgency": "today",
        "description": "Needs your response, decision, or action",
    },
    {
        "name": "Follow Up",
        "color": "orange",
        "preset": "preset1",
        "flag_urgency": "this_week",
        "description": "Track this, circle back later",
    },
    {
        "name": "Work",
        "color": "blue",
        "preset": "preset7",
        "flag_urgency": None,
        "description": "Work-related, read when available",
    },
    {
        "name": "FYI",
        "color": "gray",
        "preset": "preset14",
        "flag_urgency": None,
        "description": "Newsletters, notifications, updates - no action needed",
    },
    {
        "name": "Personal",
        "color": "green",
        "preset": "preset4",
        "flag_urgency": None,
        "description": "Non-work personal correspondence",
    },
]

# Color name to preset mapping
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

# Urgency to flag mapping
URGENCY_TO_FLAG: dict[str, dict[str, str]] = {
    "immediate": {"flag_status": "flagged", "flag_due": "today"},
    "today": {"flag_status": "flagged", "flag_due": "today"},
    "this_week": {"flag_status": "flagged", "flag_due": "this-week"},
    "someday": {},  # No flag
}


def _get_namespace_prefs(prefs: dict[str, Any] | None) -> dict[str, Any]:
    """Get the inbox_assistant namespace from preferences."""
    if not prefs:
        return {}
    return prefs.get(NAMESPACE, {})


def get_categories(prefs: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Get categories from preferences or return defaults.

    Args:
        prefs: User preferences dict (from read_preferences())

    Returns:
        List of category configurations
    """
    if not prefs:
        return DEFAULT_CATEGORIES

    namespace_prefs = _get_namespace_prefs(prefs)
    if CATEGORIES_KEY in namespace_prefs:
        return namespace_prefs[CATEGORIES_KEY]

    return DEFAULT_CATEGORIES


def get_category_names(prefs: dict[str, Any] | None = None) -> list[str]:
    """Get just the category names for prompt building."""
    return [cat["name"] for cat in get_categories(prefs)]


def get_category_config(name: str, prefs: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Get a specific category's configuration by name.

    Args:
        name: Category name (case-insensitive match)
        prefs: User preferences dict

    Returns:
        Category config dict or None if not found
    """
    categories = get_categories(prefs)
    name_lower = name.lower()
    for cat in categories:
        if cat["name"].lower() == name_lower:
            return cat
    return None


def get_flag_settings(urgency: str) -> dict[str, str]:
    """Get flag settings for a given urgency level.

    Args:
        urgency: One of 'immediate', 'today', 'this_week', 'someday'

    Returns:
        Dict with 'flag_status' and 'flag_due' keys if applicable
    """
    return URGENCY_TO_FLAG.get(urgency, {})


def format_categories_for_prompt(prefs: dict[str, Any] | None = None) -> str:
    """Format categories as a string for LLM prompts.

    Returns a formatted list like:
    - Action Required: Needs your response, decision, or action
    - Follow Up: Track this, circle back later
    ...
    """
    categories = get_categories(prefs)
    lines = []
    for cat in categories:
        desc = cat.get("description", "")
        lines.append(f"- {cat['name']}: {desc}")
    return "\n".join(lines)


def ensure_categories_initialized(prefs: dict[str, Any]) -> tuple[list[dict[str, Any]], bool]:
    """Ensure categories are initialized in preferences under the namespace.

    Args:
        prefs: User preferences dict (will be modified in place)

    Returns:
        Tuple of (categories list, was_initialized bool)
    """
    # Ensure namespace exists
    if NAMESPACE not in prefs:
        prefs[NAMESPACE] = {}

    namespace_prefs = prefs[NAMESPACE]

    # Already initialized
    if CATEGORIES_KEY in namespace_prefs:
        return namespace_prefs[CATEGORIES_KEY], False

    # Initialize with defaults
    namespace_prefs[CATEGORIES_KEY] = [cat.copy() for cat in DEFAULT_CATEGORIES]
    return namespace_prefs[CATEGORIES_KEY], True


def add_category(
    prefs: dict[str, Any],
    name: str,
    color: str = "blue",
    description: str = "",
    flag_urgency: str | None = None,
) -> dict[str, Any]:
    """Add a new category to preferences.

    Args:
        prefs: User preferences dict (will be modified in place)
        name: Category name
        color: Color name (red, orange, yellow, green, blue, purple, gray, etc.)
        description: Description of when to use this category
        flag_urgency: Optional urgency level (immediate, today, this_week, someday)

    Returns:
        The new category dict

    Raises:
        ValueError: If category with same name already exists
    """
    categories, _ = ensure_categories_initialized(prefs)

    # Check for duplicate
    for cat in categories:
        if cat["name"].lower() == name.lower():
            raise ValueError(f"Category '{name}' already exists")

    preset = COLOR_PRESETS.get(color.lower(), "preset7")
    new_cat = {
        "name": name,
        "color": color.lower(),
        "preset": preset,
        "flag_urgency": flag_urgency,
        "description": description,
    }
    categories.append(new_cat)
    return new_cat


def remove_category(prefs: dict[str, Any], name: str) -> dict[str, Any]:
    """Remove a category from preferences.

    Args:
        prefs: User preferences dict (will be modified in place)
        name: Category name to remove

    Returns:
        The removed category dict

    Raises:
        ValueError: If category not found
    """
    categories, _ = ensure_categories_initialized(prefs)

    for i, cat in enumerate(categories):
        if cat["name"].lower() == name.lower():
            return categories.pop(i)

    raise ValueError(f"Category '{name}' not found")


def edit_category(
    prefs: dict[str, Any],
    name: str,
    new_name: str | None = None,
    color: str | None = None,
    description: str | None = None,
    flag_urgency: str | None = None,
) -> dict[str, Any]:
    """Edit an existing category.

    Args:
        prefs: User preferences dict (will be modified in place)
        name: Current category name
        new_name: New name (optional)
        color: New color (optional)
        description: New description (optional)
        flag_urgency: New urgency level (optional, use "none" to clear)

    Returns:
        The updated category dict

    Raises:
        ValueError: If category not found or new name conflicts
    """
    categories, _ = ensure_categories_initialized(prefs)

    # Find the category
    cat = None
    for c in categories:
        if c["name"].lower() == name.lower():
            cat = c
            break

    if cat is None:
        raise ValueError(f"Category '{name}' not found")

    # Check for name conflict if renaming
    if new_name and new_name.lower() != name.lower():
        for c in categories:
            if c["name"].lower() == new_name.lower():
                raise ValueError(f"Category '{new_name}' already exists")
        cat["name"] = new_name

    if color:
        cat["color"] = color.lower()
        cat["preset"] = COLOR_PRESETS.get(color.lower(), "preset7")

    if description is not None:
        cat["description"] = description

    if flag_urgency is not None:
        cat["flag_urgency"] = None if flag_urgency.lower() == "none" else flag_urgency

    return cat


def get_available_colors() -> list[str]:
    """Get list of available color names."""
    return list(COLOR_PRESETS.keys())


def get_inbox_assistant_pref(prefs: dict[str, Any] | None, key: str, default: Any = None) -> Any:
    """Get a preference from the inbox_assistant namespace.

    Args:
        prefs: User preferences dict
        key: Preference key within inbox_assistant namespace
        default: Default value if not set

    Returns:
        The preference value or default
    """
    if not prefs:
        return default
    namespace_prefs = prefs.get(NAMESPACE, {})
    return namespace_prefs.get(key, default)


def set_inbox_assistant_pref(prefs: dict[str, Any], key: str, value: Any) -> None:
    """Set a preference in the inbox_assistant namespace.

    Args:
        prefs: User preferences dict (will be modified in place)
        key: Preference key within inbox_assistant namespace
        value: Value to set
    """
    if NAMESPACE not in prefs:
        prefs[NAMESPACE] = {}
    prefs[NAMESPACE][key] = value


