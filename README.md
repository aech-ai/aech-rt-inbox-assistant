# aech-rt-inbox-assistant

`aech-rt-inbox-assistant` is the shared mailbox substrate for Agent Aech.

It does five things:
- ingests delegated mailbox email into SQLite
- downloads and stores attachments canonically
- extracts/indexes text for search
- exposes a JSON CLI for email, thread, attachment, and search retrieval
- creates delegated mailbox drafts on demand through a deterministic runtime queue

It does **not** decide what any role should do with an email. COO, CFO, Sales, HR, or any other subagent should consume this repo as infrastructure.

## Runtime Shape

The service loop is intentionally dumb:

1. drain queued draft/reply-draft requests
2. poll inbox
3. persist/update email rows
4. fetch missing bodies
5. download and store attachments
6. extract text
7. build chunks and embeddings
8. delta-sync Inbox and optionally Sent Items

That loop lives in [src/main.py](/Users/steven/work/github/agent@aech.ai/aech-rt-inbox-assistant/src/main.py).

## Storage Layout

All state belongs under `INBOX_STATE_DIR` when set. Otherwise it falls back to `~/.inbox-assistant`.

In multi-instance deployments, `INBOX_STATE_DIR` should be mailbox-scoped, for example:
- `$AECH_HOST_DATA/app_context/inbox-assistant/steven@aech.ai`
- `$AECH_HOST_DATA/app_context/inbox-assistant/agent@customer.com`

Important paths inside each mailbox-scoped state root:
- SQLite DB: `assistant.sqlite`
- canonical attachment store: `attachments/<attachment-id>/<filename>`
- logs/other capability state: same state root

Attachments are stored once by inbox-assistant and referenced by manifest in CLI output. Downstream role managers should project email manifests into their own homes and let workers fetch attachment text/binaries on demand through the CLI.

## CLI Surface

The packaged CLI is JSON-only:
- `aech-cli-inbox-assistant categories show|init-defaults|add|update|remove|reset|colors`
- `aech-cli-inbox-assistant draft create [--attachment <path> ...]`
- `aech-cli-inbox-assistant draft reply <message-id> [--reply-all] [--subject <text>] [--attachment <path> ...]`
- `aech-cli-inbox-assistant email list`
- `aech-cli-inbox-assistant email changes --since <iso>`
- `aech-cli-inbox-assistant email get <message-id>`
- `aech-cli-inbox-assistant email thread <conversation-id>`
- `aech-cli-inbox-assistant attachment list`
- `aech-cli-inbox-assistant attachment meta <attachment-id>`
- `aech-cli-inbox-assistant attachment text <attachment-id>`
- `aech-cli-inbox-assistant attachment fetch <attachment-id> --output <path>`
- `aech-cli-inbox-assistant search "<query>"`
- `aech-cli-inbox-assistant ask "<query>"`
- `aech-cli-inbox-assistant sync-status`
- `aech-cli-inbox-assistant stats`
- `aech-cli-inbox-assistant prefs show|set|unset|keys`

Categories are still supported, but only as explicit agent-editable configuration. This repo no longer auto-classifies mail or applies policy-driven category actions on its own.

Draft creation writes only draft messages. This repo does not send outbound mail.
Drafts are also synced into the local corpus and marked with `is_draft` plus folder metadata in the `emails` table.
Subagents do not import Graph directly for this. The CLI stages a deterministic request under the shared trigger mount, and the inbox-assistant runtime performs the Graph operation.

The CLI entrypoint lives in [packages/aech-cli-inbox-assistant/src/aech_cli_inbox_assistant/main.py](/Users/steven/work/github/agent@aech.ai/aech-rt-inbox-assistant/packages/aech-cli-inbox-assistant/src/aech_cli_inbox_assistant/main.py).

## Environment

Required:
- `DELEGATED_USER`: mailbox being ingested

Optional:
- `INBOX_STATE_DIR`: root state directory
- `INBOX_DB_PATH`: override SQLite path
- `POLL_INTERVAL`: loop sleep between cycles
- `DELTA_SYNC_INTERVAL`: inbox delta sync cadence
- `SENT_SYNC_INTERVAL`: sent items delta sync cadence
- `DRAFT_SYNC_INTERVAL`: drafts delta sync cadence

## Development

Install deps and run tests with `uv`:

```bash
uv lock
uv run pytest tests -q
uv run python -m src.main --help
```

CLI help:

```bash
cd packages/aech-cli-inbox-assistant
uv run aech-cli-inbox-assistant --help
```

## What This Repo Is Not

This repo is not:
- an executive assistant
- a role-specific policy engine
- a calendar/action workflow runtime
- a triggering system for Teams nudges

Those behaviors belong elsewhere. If a role needs inbox context, it should read from inbox-assistant, not push role judgment down into this service.
