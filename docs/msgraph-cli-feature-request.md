# Feature Request: Message Categories & Flags Support

## Summary

Add support for updating message properties (categories, flags, importance) and managing Outlook master categories via Microsoft Graph API.

## Background

The inbox-assistant currently moves emails to folders for categorization, but this breaks the chronological inbox view. We want to switch to using **Outlook Categories** (color-coded labels) and **Flags** (follow-up dates) instead, which keeps emails in the Inbox while still organizing them.

## Required New Commands

### 1. `update-message` - Update message properties

**Usage**:
```bash
aech-cli-msgraph update-message <message_id> [options]
```

**Options**:
| Option | Type | Description |
|--------|------|-------------|
| `--categories` | string (comma-separated) | Set categories: `"Work,Action Required"` |
| `--add-category` | string | Add a category without replacing existing |
| `--remove-category` | string | Remove a specific category |
| `--clear-categories` | flag | Remove all categories |
| `--flag` | string | Set flag status: `flagged`, `complete`, `notFlagged` |
| `--flag-due` | string | Due date (ISO-8601 or relative: `today`, `tomorrow`, `this-week`, `next-week`) |
| `--importance` | string | Set importance: `low`, `normal`, `high` |
| `--is-read` | bool | Mark as read/unread |
| `--json` | flag | Output JSON response |

**Examples**:
```bash
# Set categories and flag for today
aech-cli-msgraph update-message AAMkAG... --categories "Work,Action Required" --flag flagged --flag-due today

# Add a category without replacing
aech-cli-msgraph update-message AAMkAG... --add-category "VIP"

# Clear flag
aech-cli-msgraph update-message AAMkAG... --flag notFlagged

# Mark as high importance
aech-cli-msgraph update-message AAMkAG... --importance high
```

**Graph API**:
```http
PATCH /users/{userId}/messages/{messageId}
Content-Type: application/json

{
  "categories": ["Work", "Action Required"],
  "flag": {
    "flagStatus": "flagged",
    "startDateTime": {
      "dateTime": "2025-01-02T08:00:00",
      "timeZone": "America/Vancouver"
    },
    "dueDateTime": {
      "dateTime": "2025-01-02T17:00:00",
      "timeZone": "America/Vancouver"
    }
  },
  "importance": "high",
  "isRead": true
}
```

**Permissions**: `Mail.ReadWrite`

---

### 2. `list-categories` - List master categories

**Usage**:
```bash
aech-cli-msgraph list-categories [--json]
```

**Output** (human-readable):
```
Master Categories:
  - Work (Blue)
  - Personal (Green)
  - Action Required (Red)
  - Finance (Orange)
  ...
```

**Output** (JSON):
```json
{
  "value": [
    {"id": "abc123", "displayName": "Work", "color": "preset7"},
    {"id": "def456", "displayName": "Personal", "color": "preset4"}
  ]
}
```

**Graph API**:
```http
GET /users/{userId}/outlook/masterCategories
```

**Permissions**: `MailboxSettings.Read`

---

### 3. `create-category` - Create a master category

**Usage**:
```bash
aech-cli-msgraph create-category <name> [--color <preset>] [--json]
```

**Options**:
| Option | Type | Description |
|--------|------|-------------|
| `--color` | string | Color preset (preset0-preset24) or name (red, blue, green, etc.) |

**Examples**:
```bash
# Create with specific color
aech-cli-msgraph create-category "Action Required" --color red

# Create with preset number
aech-cli-msgraph create-category "VIP" --color preset9
```

**Color name mapping** (convenience):
| Name | Preset |
|------|--------|
| red | preset0 |
| orange | preset1 |
| yellow | preset3 |
| green | preset4 |
| blue | preset7 |
| purple | preset8 |
| gray | preset14 |

**Graph API**:
```http
POST /users/{userId}/outlook/masterCategories
Content-Type: application/json

{
  "displayName": "Action Required",
  "color": "preset0"
}
```

**Permissions**: `MailboxSettings.ReadWrite`

---

### 4. `delete-category` - Delete a master category

**Usage**:
```bash
aech-cli-msgraph delete-category <category_id_or_name> [--json]
```

**Examples**:
```bash
# Delete by name
aech-cli-msgraph delete-category "Old Category"

# Delete by ID
aech-cli-msgraph delete-category abc123-def456
```

**Graph API**:
```http
DELETE /users/{userId}/outlook/masterCategories/{id}
```

**Permissions**: `MailboxSettings.ReadWrite`

---

## GraphClient API Methods

Also expose these as Python methods in `GraphClient`:

```python
class GraphClient:
    # Message updates
    def update_message(
        self,
        message_id: str,
        categories: list[str] | None = None,
        flag_status: str | None = None,  # flagged, complete, notFlagged
        flag_due: datetime | None = None,
        importance: str | None = None,  # low, normal, high
        is_read: bool | None = None,
        user_id: str | None = None,
    ) -> dict:
        """Update message properties via PATCH."""
        ...

    # Master categories
    def get_master_categories(self, user_id: str | None = None) -> dict:
        """Get all master categories."""
        ...

    def create_master_category(
        self,
        display_name: str,
        color: str = "preset7",  # blue default
        user_id: str | None = None,
    ) -> dict:
        """Create a new master category."""
        ...

    def delete_master_category(
        self,
        category_id: str,
        user_id: str | None = None,
    ) -> bool:
        """Delete a master category."""
        ...
```

---

## Permissions Summary

| Feature | Permission Required |
|---------|---------------------|
| Update message (categories, flags) | `Mail.ReadWrite` |
| Read master categories | `MailboxSettings.Read` |
| Create/delete master categories | `MailboxSettings.ReadWrite` |

**Note**: For shared/delegated mailboxes, use `Mail.ReadWrite.Shared` and ensure the delegate has appropriate permissions in Exchange.

---

## Implementation Notes

### 1. Flag date handling
When setting `dueDateTime`, you MUST also include `startDateTime` or Graph API returns 400 Bad Request.

Suggested approach for relative dates:
```python
def resolve_flag_due(due: str, timezone: str) -> dict:
    """Convert relative date to Graph API flag object."""
    today = datetime.now(ZoneInfo(timezone))

    if due == "today":
        start = today.replace(hour=8, minute=0)
        end = today.replace(hour=17, minute=0)
    elif due == "tomorrow":
        tomorrow = today + timedelta(days=1)
        start = tomorrow.replace(hour=8, minute=0)
        end = tomorrow.replace(hour=17, minute=0)
    elif due == "this-week":
        # Friday of current week
        days_until_friday = (4 - today.weekday()) % 7
        friday = today + timedelta(days=days_until_friday)
        start = today.replace(hour=8, minute=0)
        end = friday.replace(hour=17, minute=0)
    elif due == "next-week":
        # Friday of next week
        days_until_friday = (4 - today.weekday()) % 7 + 7
        friday = today + timedelta(days=days_until_friday)
        start = today.replace(hour=8, minute=0)
        end = friday.replace(hour=17, minute=0)
    else:
        # Assume ISO-8601
        end = datetime.fromisoformat(due)
        start = end.replace(hour=8, minute=0)

    return {
        "flagStatus": "flagged",
        "startDateTime": {
            "dateTime": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "timeZone": timezone
        },
        "dueDateTime": {
            "dateTime": end.strftime("%Y-%m-%dT%H:%M:%S"),
            "timeZone": timezone
        }
    }
```

### 2. Category case sensitivity
Category names are case-sensitive in Graph API. "Work" ≠ "work".

### 3. Category existence
Setting a category on a message that doesn't exist in masterCategories will create it with default color (grey). For consistent colors, create master categories first.

### 4. Shared mailbox context
Always use `/users/{sharedMailboxId}/...` not `/me/...` for delegated access. This is consistent with existing move-email and delete-email behavior.

### 5. Error handling
Return structured JSON errors for common cases:
- 404: Message not found
- 403: Permission denied (need admin consent for masterCategories on shared mailbox)
- 400: Invalid flag configuration (missing startDateTime)

### 6. Add-category behavior
For `--add-category`, the command should:
1. GET the current message to read existing categories
2. Append the new category to the list
3. PATCH with the updated list

This avoids replacing existing categories.

---

## Color Preset Reference

Microsoft Graph uses `preset0` through `preset24`:

| Preset | Color |
|--------|-------|
| preset0 | Red |
| preset1 | Orange |
| preset2 | Brown |
| preset3 | Yellow |
| preset4 | Green |
| preset5 | Teal |
| preset6 | Olive |
| preset7 | Blue |
| preset8 | Purple |
| preset9 | Cranberry |
| preset10 | Steel |
| preset11 | DarkSteel |
| preset12 | Gray |
| preset13 | DarkGray |
| preset14 | Black |
| preset15 | DarkRed |
| preset16 | DarkOrange |
| preset17 | DarkBrown |
| preset18 | DarkYellow |
| preset19 | DarkGreen |
| preset20 | DarkTeal |
| preset21 | DarkOlive |
| preset22 | DarkBlue |
| preset23 | DarkPurple |
| preset24 | DarkCranberry |

---

## References

- [Update message - Microsoft Graph](https://learn.microsoft.com/en-us/graph/api/message-update)
- [followupFlag resource](https://learn.microsoft.com/en-us/graph/api/resources/followupflag)
- [outlookCategory resource](https://learn.microsoft.com/en-us/graph/api/resources/outlookcategory)
- [Create masterCategories](https://learn.microsoft.com/en-us/graph/api/outlookuser-post-mastercategories)
- [List masterCategories](https://learn.microsoft.com/en-us/graph/api/outlookuser-list-mastercategories)
- [Organize messages in Outlook](https://github.com/microsoftgraph/microsoft-graph-docs-contrib/blob/main/concepts/outlook-organize-messages.md)
