"""
Outlook Categories configuration for email organization.

This module provides default categories and utility functions for the
categories-based email organization system. Categories are applied as
Outlook labels (staying in Inbox) rather than moving to folders.
"""

from typing import Any

# Default categories with colors and flag behavior
# Users can customize via preferences.json "outlook_categories" key
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


def get_categories(prefs: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Get categories from preferences or return defaults.

    Args:
        prefs: User preferences dict (from read_preferences())

    Returns:
        List of category configurations
    """
    if prefs and "outlook_categories" in prefs:
        return prefs["outlook_categories"]
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
