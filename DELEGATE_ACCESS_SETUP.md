# Mailbox Sharing Setup Guide

The inbox assistant runs as `agent@aech.ai` and manages mailboxes that are shared with it.

## Architecture

- **Service Account**: `agent@aech.ai` (authenticates via `aech-cli-msgraph`)
- **Managed Mailbox**: Any user who shares their mailbox with Agent Aech (e.g., `steven@aech.ai`)
- **Communication**: Service uses `/users/{managed-user}/mailFolders` endpoints

## Current Status

✅ **Reading emails works** - Steven's mailbox is shared for read access
❌ **Creating folders fails (404)** - Need additional permissions

## Required Permissions

Steven needs to grant `agent@aech.ai` **Editor** or **Full Access** permissions to their mailbox.

### Grant Mailbox Permissions

**Option A: Via Outlook Web**
1. Log in to Outlook as `steven@aech.ai`
2. Go to Settings → Mail → Accounts → **Delegate access** or **Mailbox permissions**
3. Add `agent@aech.ai`
4. Grant **Editor** permissions (allows creating folders)

**Option B: Via Microsoft 365 Admin Center**
1. Log in to Microsoft 365 Admin Center as admin
2. Go to **Users** → **Active users**
3. Select `steven@aech.ai`
4. Go to **Mail** tab → **Manage mailbox permissions**
5. Under **Full Access**, add `agent@aech.ai`
6. Under **Send As** (optional), add `agent@aech.ai` if you want the agent to send emails
7. **Save** changes

**Option C: Via PowerShell (Exchange Online)**
```powershell
# Connect to Exchange Online
Connect-ExchangeOnline

# Grant Full Access permission
Add-MailboxPermission -Identity "steven@aech.ai" -User "agent@aech.ai" -AccessRights FullAccess -InheritanceType All

# Optional: Grant Send As permission
Add-RecipientPermission -Identity "steven@aech.ai" -Trustee "agent@aech.ai" -AccessRights SendAs
```

### Verify Permissions

After granting permissions, wait 5-10 minutes for propagation, then test:

```bash
# Test folder listing (should return 200, not 404)
curl -H "Authorization: Bearer $TOKEN" \
  "https://graph.microsoft.com/v1.0/users/steven@aech.ai/mailFolders"
```

## Troubleshooting

| Error | Cause | Solution |
|-------|-------|----------|
| **404 Not Found** | Mailbox not shared or permissions not propagated | Grant Full Access/Editor permissions, wait 10 minutes |
| **403 Forbidden** | Token missing required scopes | Re-authenticate: `aech-cli-msgraph auth login` |
| **401 Unauthorized** | Token expired | Re-authenticate: `aech-cli-msgraph auth login` |

## Required API Scopes

The `agent@aech.ai` token already has these scopes (configured in `aech-cli-msgraph`):
- ✅ `Mail.Read.Shared` - Read messages in shared mailboxes
- ✅ `Mail.ReadWrite.Shared` - Create/manage folders in shared mailboxes
- ✅ `Mail.Send.Shared` - Send emails from shared mailboxes

