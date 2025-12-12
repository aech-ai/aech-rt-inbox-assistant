# Capability Specification

This document defines the specification for Agent Aech capabilities.

## Capability Types

| Prefix | Type | Description |
|--------|------|-------------|
| `aech-cli-*` | CLI-only | Stateless CLI tool, no background process |
| `aech-rt-*` | Real-time | Background service + CLI for agent interaction |

## Naming Convention

```
aech-{type}-{name}

Examples:
  aech-cli-documents      # Document conversion CLI
  aech-cli-translator     # Translation CLI
  aech-rt-inbox-assistant # Real-time inbox management
  aech-rt-log-monitor     # Real-time log monitoring
```

---

## CLI-Only Capabilities (`aech-cli-*`)

Stateless tools that perform a task and exit. No background process.

### Structure

```
aech-cli-{name}/
├── pyproject.toml
├── src/
│   └── aech_cli_{name}/
│       ├── __init__.py
│       ├── main.py          # Entry point
│       └── cli.py           # Click commands
└── README.md
```

### pyproject.toml

```toml
[project]
name = "aech-cli-{name}"
version = "0.1.0"
description = "Description of the capability"
requires-python = ">=3.11"
dependencies = [
    "click>=8.0",
]

[project.scripts]
aech-cli-{name} = "aech_cli_{name}.main:cli"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### CLI JSON Description

The CLI must output a JSON description when invoked with `--help` (or provide it via a separate mechanism). This is used by the installer to generate the manifest.

```json
{
  "name": "{name}",
  "type": "cli",
  "command": "aech-cli-{name}",
  "description": "What this capability does",
  "required_scopes": ["Scope.Read", "Scope.Write"],
  "actions": [
    {
      "name": "action-name",
      "description": "What this action does",
      "parameters": ["--param1", "--param2", "arg1"],
      "required_scopes": ["Scope.Read"]
    }
  ],
  "documentation": {
    "usage": "How to use this capability",
    "examples": [
      "aech-cli-{name} action-name --param1 value"
    ]
  }
}
```

### Required Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Capability name (without prefix) |
| `type` | Yes | Always `"cli"` |
| `command` | Yes | Full command name |
| `description` | Yes | Brief description |
| `actions` | Yes | List of available actions |
| `required_scopes` | No | Capability-level OAuth scopes |
| `documentation` | No | Extended documentation |

### Action Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Action/subcommand name |
| `description` | Yes | What the action does |
| `parameters` | Yes | List of parameters (flags and args) |
| `required_scopes` | No | Action-specific OAuth scopes |

---

## Real-Time Capabilities (`aech-rt-*`)

Background services that run continuously alongside Agent Aech. Include both:
1. A background service (Docker container) - structure is up to the developer
2. A CLI for agent interaction - **must produce a `.whl` installable in Agent Aech**

### CLI Requirement

The RT-capability must produce a pip-installable wheel (`.whl`) for its CLI. This is the **only** way Agent Aech can use the capability. The wheel is placed in `aech-main/capabilities/clis/` and discovered by the installer.

**Why?** Agent Aech controls capability discovery via:
1. `installer.py` generates manifest from installed CLIs
2. `manager.py` filters manifest by permissions/scopes
3. Filtered manifest written to session folder
4. Worker only sees capabilities it's allowed to use

### CLI Package Structure (Required)

The CLI portion must follow standard pip package structure:

```
aech_cli_{name}/
├── aech_cli_{name}/
│   ├── __init__.py
│   └── main.py              # Entry point with cli() function
├── manifest.json            # CLI JSON description (optional, can use --help)
├── pyproject.toml
├── requirements.txt
└── README.md
```

### RT-Capability Structure (Flexible)

The service itself can be structured however the developer needs. Example:

```
aech-rt-{name}/
├── src/                 # Service code (any structure)
│   └── ...
├── cli/                 # CLI package (must follow structure above)
│   ├─aech_cli_{name}/
│   │   ├─__init__.py
│   │   └─main.py          # Entry point with cli() function
│   ├─manifest.json        # CLI JSON description (optional, can use --help)
│   ├─pyproject.toml
│   ├─requirements.txt
│   └─README.md
├── docker-compose.yml
├── Dockerfile
├── .env.example
└── README.md
```

The key point: **the CLI is a separate pip package** that gets built into a `.whl` and installed in Agent Aech independently of the service.

### Communication Patterns

#### Pattern A: File-Based (Recommended for per-user capabilities)

```
RT-Service                   Shared Filesystem              CLI (in Agent Worker)
    │                              │                              │
    ├──writes──▶  data/users/{email}/capabilities/{name}/  ◀──reads────────────┤
    │                   └── state.sqlite                                     │
    │                                                                        │
    └──trigger──▶  data/rt_triggers/{name}/*.json  ◀──poll── Agent Manager───┘
```

**When to use:**
- Per-user data isolation required
- Simple state (SQLite, JSON files)
- Privacy-first design

#### Pattern B: API-Based (For complex shared services)

```
CLI (in Agent Worker)              aech-network              RT-Service
    │                                   │                        │
    └────HTTP────▶  {service}:8080  ────┼────────────────────────┘
                                        │
                    (Docker DNS resolves container name)
```

**When to use:**
- Multi-tenant/shared data
- Complex database (Postgres, etc.)
- High-performance requirements

> Note: these patterns are not exclusive - you can mix and match as needed. 

---

## File-Based RT-Capability Specification

### docker-compose.yml

```yaml
services:
  {name}:
    build: .
    image: aech-rt-{name}
    env_file:
      - .env
    volumes:
      # Mount the delegated user's directory (capability state lives under /home/agentaech/capabilities/{name}/)
      - ../data/users/${DELEGATED_USER}:/home/agentaech:rw
      # Preferred trigger outbox (one file per trigger)
      - ../data/rt_triggers/{name}:/triggers/outbox:rw
    restart: unless-stopped

# No network needed - communicates via filesystem
```

### Environment Variables

```bash
# .env
DELEGATED_USER=user@example.com    # User this instance manages
POLL_INTERVAL=60                   # Seconds between polls
```

### Data Directory Structure

```
data/users/{email}/
├── capabilities/{name}/
│   ├── state.sqlite          # Primary data store
│   └── ...                  # capability-owned user state
├── sessions/<session_id>/    # Aech sessions (created by Manager)
└── preferences.json          # Aech routing prefs (optional)
```

### Trigger Format

Preferred: RT-capabilities write one-file-per-trigger JSON documents to:

`data/rt_triggers/{name}/*.json`

```json
{
  "id": "uuid-v4",
  "user": "user@example.com",
  "type": "event_type",
  "created_at": "2025-01-15T10:30:00Z",
  "payload": {
    "key": "value"
  },
  "routing": {
    "channel": "teams",
    "target": "chat:<composite_id>"
  }
}
```

Atomic write pattern:
1. write `.../<uuid>.json.tmp`
2. rename to `.../<uuid>.json`

Legacy (supported): `data/rt-triggers.json` with `{ "{name}": [ ... ] }` (avoid for production).

**Required trigger fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique identifier (UUID v4) |
| `user` | Yes | User email this trigger is for |
| `type` | Yes | Event type (capability-defined) |
| `created_at` | Yes | ISO 8601 timestamp |
| `payload` | No | Event-specific data |

### Trigger Lifecycle

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  RT-Service writes trigger                                      │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────┐                                                │
│  │   pending   │  (in rt_triggers/{name}/<id>.json)             │
│  └─────────────┘                                                │
│       │                                                         │
│       │  Manager polls, claims by rename/move (atomic)          │
│       ▼                                                         │
│  ┌─────────────┐                                                │
│  │ processing  │  (file claimed; session spawned)               │
│  └─────────────┘                                                │
│       │                                                         │
│       ├──success──▶  Manager moves to _done/ (or deletes)        │
│       │                                                         │
│       └──failure──▶  Manager moves back for retry               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### CLI for File-Based RT-Capability

```python
# cli/aech_cli_{name}/cli.py
import sqlite3
import click
import json
from pathlib import Path

# Data location: user's home directory in worker container
# Maps to: data/users/{email}/{name}/ on host
DATA_DIR = Path.home() / "{name}"

@click.group()
def cli():
    """{Name} capability CLI"""
    pass

@cli.command()
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def status(as_json):
    """Get current status"""
    db = sqlite3.connect(DATA_DIR / "state.sqlite")
    # ... query and return status

@cli.command()
@click.argument("item_id")
def approve(item_id):
    """Approve an item"""
    db = sqlite3.connect(DATA_DIR / "state.sqlite")
    db.execute("UPDATE items SET status = 'approved' WHERE id = ?", (item_id,))
    db.commit()
    click.echo(json.dumps({"status": "approved", "id": item_id}))

if __name__ == "__main__":
    cli()
```

---

## API-Based RT-Capability Specification

### docker-compose.yml

```yaml
services:
  {name}:
    build: .
    image: aech-rt-{name}
    env_file:
      - .env
    volumes:
      - ../data/app_context/{name}:/data
    networks:
      - aech-network
    restart: unless-stopped

networks:
  aech-network:
    external: true
```

### Service Requirements

1. **HTTP API on port 8080** (or configured port)
2. **Health endpoint**: `GET /health` returning `200 OK`
3. **JSON responses** for all endpoints

### CLI JSON Description (API-Based)

```json
{
  "name": "{name}",
  "type": "cli",
  "command": "aech-cli-{name}",
  "description": "What this capability does",
  "required_scopes": ["Scope.Read"],
  "service": {
    "container": "aech-rt-{name}",
    "port": 8080,
    "health_endpoint": "/health"
  },
  "actions": [
    {
      "name": "query",
      "description": "Query the service",
      "parameters": ["--filter", "--json"]
    }
  ]
}
```

### CLI for API-Based RT-Capability

```python
# cli/aech_cli_{name}/cli.py
import requests
import click
import json

SERVICE_URL = "http://aech-rt-{name}:8080"

@click.group()
def cli():
    """{Name} capability CLI"""
    pass

@cli.command()
@click.option("--filter", help="Filter results")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def query(filter, as_json):
    """Query the service"""
    response = requests.get(
        f"{SERVICE_URL}/query",
        params={"filter": filter} if filter else {},
        timeout=30
    )
    response.raise_for_status()
    if as_json:
        click.echo(json.dumps(response.json()))
    else:
        # Human-readable output
        for item in response.json():
            click.echo(f"{item['id']}: {item['name']}")

if __name__ == "__main__":
    cli()
```

---

## Installation

### CLI-Only Capabilities

1. Build wheel: `python -m build --wheel`
2. Place in `aech-main/capabilities/clis/`
3. Rebuild Agent Aech Docker images

### RT-Capabilities

1. Clone RT-capability repo alongside `aech-main/`
2. Build CLI wheel and place in `aech-main/capabilities/clis/`
3. Configure `.env` with required settings
4. Start with `docker-compose up -d`

### Directory Layout (Customer Deployment)

```
/opt/aech/  (or wherever deployed)
├── data/
│   ├── sessions/
│   ├── users/
│   │   └── user@example.com/
│   │       ├── capabilities/
│   │       │   ├── inbox-assistant/    # RT-capability data
│   │       └── ...
│   ├── app_context/
│   │   └── helpdesk/               # Shared service data
│   └── rt_triggers/
│       ├── inbox-assistant/
│       └── helpdesk/
├── aech-main/
│   └── capabilities/
│       └── clis/
│           ├── aech_cli_inbox-*.whl
│           └── aech_cli_helpdesk-*.whl
├── aech-rt-inbox-assistant/
│   ├── docker-compose.yml
│   └── .env
└── aech-rt-helpdesk/
    ├── docker-compose.yml
    └── .env
```

---

## Scope Filtering

Capabilities can specify required Microsoft Graph API scopes at two levels:

### Capability-Level Scopes

If the capability requires certain scopes to function at all:

```json
{
  "name": "inbox-assistant",
  "required_scopes": ["Mail.Read", "Mail.Send"],
  ...
}
```

If the agent doesn't have ALL required scopes, the entire capability is excluded.

### Action-Level Scopes

If specific actions require additional scopes:

```json
{
  "actions": [
    {
      "name": "read-drafts",
      "required_scopes": ["Mail.Read"]
    },
    {
      "name": "send-draft",
      "required_scopes": ["Mail.Send"]
    }
  ]
}
```

Actions are individually filtered based on granted scopes.

---

## Best Practices

### General

1. **JSON output**: Always support `--json` flag for machine-readable output
2. **Timeouts**: Use reasonable timeouts (30s for queries, 120s for file operations)
3. **Error handling**: Return non-zero exit codes on failure with error in stderr
4. **Idempotency**: Actions should be safe to retry

### File-Based RT-Capabilities

1. **SQLite for state**: Use SQLite with WAL mode for concurrent access
2. **Atomic writes**: Use temp file + rename for config updates
3. **Unique trigger IDs**: Always use UUID v4 for trigger IDs
4. **Clean up**: Remove processed data periodically

### API-Based RT-Capabilities

1. **Health checks**: Implement `/health` endpoint
2. **Graceful shutdown**: Handle SIGTERM properly
3. **Connection pooling**: Reuse database connections
4. **Rate limiting**: Protect against excessive requests

---

## Examples

### CLI-Only: Document Converter

```
aech-cli-documents convert input.pdf --output-dir ./outputs --format png
aech-cli-documents extract input.docx --output-dir ./outputs --format markdown
```

### File-Based RT: Inbox Assistant

```
# RT-service monitors inbox, creates drafts, writes to SQLite
# Agent uses CLI to interact:

aech-cli-inbox list-drafts --json
aech-cli-inbox approve draft-123
aech-cli-inbox search "project update" --json
```

### API-Based RT: Helpdesk

```
# RT-service runs Postgres, ingests tickets from ConnectWise
# Agent uses CLI to query:

aech-cli-helpdesk search "network issue" --json
aech-cli-helpdesk get-ticket T-12345 --json
aech-cli-helpdesk add-note T-12345 "Escalating to network team"
```
