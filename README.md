# Aech RT Inbox Assistant

A capability for Agent Aech to manage delegated email inboxes. This CLI wraps `aech-cli-msgraph` to provide a simplified interface for operating on mailboxes where the agent has **Exchange Delegate Access**.

## What this CLI does (for Agent Aech)

- **Delegated Access**: All commands require the `--user <email>` flag to specify which user's mailbox to act upon.
- **Operations**:
  - `list-emails`: Poll for emails (supports filtering).
  - `move-email`: Organize emails into folders.
  - `archive-email`: Quickly move emails to the "Archive" folder.
  - `delete-email`: Move emails to "Deleted Items".

## Prerequisites

1. **aech-cli-msgraph**: This CLI must be installed and available in the PATH (version 0.1.19+ required).
2. **Delegate Access**: The Agent's M365 account must have been granted Delegate Access to the target user's mailbox.

## Usage

The CLI is designed to be used by the agent, but can be run manually for testing.

```bash
# List all emails in Steven's inbox (including read ones)
aech-rt-inbox-assistant list-emails --user steven@aech.ai --all-senders --include-read

# Archive a specific email
aech-rt-inbox-assistant archive-email <message_id> --user steven@aech.ai

# Move an email to a project folder
aech-rt-inbox-assistant move-email <message_id> "Project Alpha" --user steven@aech.ai

# Delete an email
aech-rt-inbox-assistant delete-email <message_id> --user steven@aech.ai
```

## Manifest-Based Help

This CLI implements the Aech capability pattern where `aech-rt-inbox-assistant --help` emits a JSON manifest for the agent runtime. To see human-readable help, use the subcommand help:

```bash
aech-rt-inbox-assistant list-emails --help
```
