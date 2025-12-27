# Outlook Categories Setup

The inbox assistant uses **Outlook categories** to organize emails instead of moving them to folders. This keeps all emails in your Inbox while applying color-coded labels for easy filtering.

## How It Works

1. **Categories are name-based**: When the assistant applies a category like "Action Required" to an email, Outlook looks up the master category by name to determine its color.

2. **Master categories must exist**: For colors to display, the category must exist in your Outlook master category list with the correct color assigned.

3. **One-time setup required**: Due to Microsoft Graph API limitations, the assistant cannot create master categories in your mailbox via delegated permissions. You must create them manually once.

## Required Categories

Create these categories in your Outlook with the specified colors:

| Category | Color | Description |
|----------|-------|-------------|
| **Action Required** | Red | Needs your response, decision, or action |
| **Follow Up** | Orange | Track this, circle back later |
| **Work** | Blue | Work-related, read when available |
| **FYI** | Gray | Newsletters, notifications, updates - no action needed |
| **Personal** | Green | Non-work personal correspondence |

## Setup Instructions

### Outlook Web (outlook.office.com)

1. Click the **Settings** gear icon (top right)
2. Click **View all Outlook settings**
3. Go to **General** → **Categories**
4. For each category above:
   - Click **+ Create category**
   - Enter the exact name (e.g., "Action Required")
   - Select the matching color
   - Click **Save**

### Outlook Desktop (Windows)

1. Go to **Home** tab → **Categorize** → **All Categories**
2. Click **New** for each category
3. Enter the exact name and select the color
4. Click **OK**

### Outlook Desktop (Mac)

1. Go to **Home** tab → **Categorize** → **Edit Categories**
2. Click **+** to add each category
3. Enter the exact name and select the color

## Important Notes

- **Names must match exactly**: The category name applied by the assistant must match your master category name exactly (case-sensitive).

- **Colors are visual only**: If a category doesn't exist in your master list, it will still be applied but appear gray/colorless.

- **Multiple categories**: Emails can have multiple categories applied (e.g., "Work" + "FYI" for a work newsletter).

- **Flags for urgency**: The assistant also sets Outlook flags with due dates based on urgency:
  - `immediate` / `today` → Flag due today
  - `this_week` → Flag due this week
  - `someday` → No flag

## Managing Categories via CLI

Use the `categories` subcommand to manage your category profile:

```bash
# List your categories (auto-populates defaults on first use)
aech-cli-inbox categories list --human

# Add a new category
aech-cli-inbox categories add "Urgent" --color red --flag today -d "Time-sensitive items"

# Edit an existing category
aech-cli-inbox categories edit "Work" --color teal --flag this_week

# Remove a category
aech-cli-inbox categories remove "Old Category"

# Reset to defaults
aech-cli-inbox categories reset --yes

# List available colors
aech-cli-inbox categories colors --human
```

Categories are stored in your user profile (`preferences.json`) and auto-populated with defaults on first access.

## Customizing Categories (Manual)

You can also customize categories by editing your `preferences.json` directly. Categories are stored under the `inbox_assistant` namespace:

```json
{
  "inbox_assistant": {
    "categories": [
      {
        "name": "Action Required",
        "color": "red",
        "preset": "preset0",
        "flag_urgency": "today",
        "description": "Needs your response, decision, or action"
      },
      ...
    ],
    "use_categories_mode": true
  }
}
```

Set `use_categories_mode` to `false` to use the legacy folder-based organization instead.

## Why Manual Setup?

Microsoft Graph API does not provide a delegated permission for writing to mailbox settings (master categories) of another user. The `MailboxSettings.ReadWrite` permission only works for `/me/` endpoint, not delegated access. This is a platform limitation, not an application limitation.
