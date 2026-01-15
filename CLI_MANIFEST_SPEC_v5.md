# CLI Manifest Specification

> For LLM-based coding agents building Agent Aech CLI capabilities

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 5.0 | 2026-01-13 | Added `bundled_skills` for packaging skills with CLIs |
| 4.0 | 2026-01-09 | Grouped subcommand support for domain CLIs |
| 3.0 | 2025-12-15 | Added `available_in_sandbox` field |
| 2.0 | 2025-11-01 | Structured `documentation` section with outputs and notes |
| 1.0 | 2025-10-01 | Initial spec with actions and parameters |

**Current Version:** 5.0

---

## Purpose

This manifest enables another LLM agent to use your CLI correctly. Write it as if explaining your CLI to a capable assistant who has never seen it before. Every field should answer: **What can I do with this? How do I do it? When should I use it?**

## Manifest Schema

```json
{
  "name": "cli-name",
  "type": "cli",
  "command": "aech-cli-name",
  "description": "What this CLI does, what inputs it accepts, what outputs it produces.",
  "actions": [
    {
      "name": "action-name",
      "description": "What this action does. Input: what it takes. Output: what it produces. Use when: the scenario.",
      "parameters": [
        {
          "name": "param-name",
          "type": "argument|option",
          "required": true|false,
          "description": "What this is, format/valid values, when to use it."
        }
      ]
    }
  ],
  "available_in_sandbox": true|false,
  "bundled_skills": [
    {
      "name": "skill-name",
      "description": "What this skill does and when to use it."
    }
  ],
  "documentation": {
    "outputs": { ... },
    "notes": [ ... ]
  }
}
```

## Writing Effective Descriptions

Descriptions are the most important part of the manifest. They must enable action.

### Action Descriptions

An action description answers three questions:

1. **What does it do?** - The core function
2. **What are the inputs/outputs?** - File types, formats, locations
3. **When should I use it?** - The scenario that calls for this action

**Example - Document Translation:**
```json
{
  "name": "translate",
  "description": "Translate a Markdown document to another language. Input: Markdown file path. Output: translated file at <output-dir>/<stem>_<lang>.md and QA report at <output-dir>/<stem>_translation_report.md. Use when user needs a document in another language."
}
```

**Example - Email Update:**
```json
{
  "name": "update-message",
  "description": "Update email message properties: categories, flags, importance, read status. Input: message ID from list-messages. Output: JSON confirmation. Use when user wants to organize, flag, or mark emails."
}
```

**Example - Document Conversion:**
```json
{
  "name": "convert",
  "description": "Render document pages to PNG images. Input: PDF, DOCX, PPTX, or image file. Output: page_001.png, page_002.png, etc. in output directory. Use when user needs to view or process document pages as images."
}
```

### Parameter Descriptions

A parameter description must include:

1. **What it is** - The purpose of this parameter
2. **Format/Valid values** - Exact formats, allowed values, examples
3. **When to use it** (for optional params) - The scenario

**Example - Language Code:**
```json
{
  "name": "target-lang",
  "type": "argument",
  "required": true,
  "description": "Target language as ISO 639-1 code. Examples: fr (French), de (German), es (Spanish), ja (Japanese), zh (Chinese)."
}
```

**Example - Output Directory:**
```json
{
  "name": "output-dir",
  "type": "option",
  "required": true,
  "description": "Directory path where output files will be written. Must exist or will be created."
}
```

**Example - Flag Status:**
```json
{
  "name": "flag",
  "type": "option",
  "required": false,
  "description": "Set email flag status. Values: flagged (mark for follow-up), complete (mark as done), notFlagged (remove flag). Use to help user track action items."
}
```

**Example - Due Date:**
```json
{
  "name": "flag-due",
  "type": "option",
  "required": false,
  "description": "Due date for flagged email. Formats: ISO-8601 date (2024-12-31) or relative (today, tomorrow, this-week, next-week). Use with --flag flagged."
}
```

**Example - Repeatable Option:**
```json
{
  "name": "format",
  "type": "option",
  "required": false,
  "description": "Output format. Values: docx, pdf, pptx. Can be specified multiple times for multiple outputs. Defaults to docx and pdf if not specified."
}
```

## Bundled Skills

CLIs can bundle skills that implement higher-level workflows using the CLI's capabilities. These skills are automatically extracted and installed to the agent's skill tree during CLI installation.

### Directory Structure

```
aech-cli-name/
├── aech_cli_name/
│   ├── __init__.py
│   ├── main.py
│   ├── manifest.json
│   └── skills/
│       ├── skill-one/
│       │   ├── SKILL.md
│       │   └── scripts/
│       │       └── helper.py
│       └── skill-two/
│           ├── SKILL.md
│           └── scripts/
│               └── process.py
└── pyproject.toml
```

### pyproject.toml Configuration

Include skills in package data so they're bundled in the wheel:

```toml
[tool.setuptools.package-data]
aech_cli_name = [
    "manifest.json",
    "skills/*/SKILL.md",
    "skills/*/scripts/*.py",
]
```

### Manifest Declaration

Declare bundled skills in the manifest so the installer knows to extract them:

```json
{
  "command": "aech-cli-legal",
  "bundled_skills": [
    {
      "name": "precedent-finder",
      "description": "Find precedent clauses from past deals using clause search"
    },
    {
      "name": "email-edit-extractor",
      "description": "Extract edits from client emails and apply to documents"
    }
  ]
}
```

### SKILL.md Format

Each skill follows the standard skill specification:

```markdown
---
name: skill-name
description: What this skill does. Use when [trigger scenario].
allowed-tools: Read, Bash, Write, Grep, Glob
---

# Skill Name

Description and workflow documentation.

## Available Scripts

### scripts/helper.py

```bash
python scripts/helper.py --input file.txt --output result.json
```

## CLI Dependencies

- `aech-cli-legal documents redline` - Generate Track Changes
- `aech-cli-msgraph send-message` - Deliver via email

### Installation Behavior

When `capabilities/installer.py` processes a wheel with bundled skills:

1. Reads `bundled_skills` from manifest
2. Extracts `skills/<skill-name>/` directories from wheel
3. Installs to `data/skills/<skill-name>/`
4. Reports installed skills in summary

Skills inherit the CLI's capabilities - they can call any of the CLI's subcommands.

---

## Auto-Generating the Manifest

To keep the manifest in sync with CLI code, use an LLM-powered script that introspects the source and generates accurate documentation. Copy these files verbatim to your CLI's `scripts/` directory.

### scripts/update_manifest.py

```python
#!/usr/bin/env python3
"""
Auto-generate manifest.json from CLI source code using LLM.

Usage:
    uv run python scripts/update_manifest.py              # Update manifest.json
    uv run python scripts/update_manifest.py --dry-run    # Print without writing

Requires AECH_LLM_WORKER_MODEL env var (e.g., "openai:gpt-4o")
"""

import argparse
import json
import os
import sys
import tomllib
from pathlib import Path

from pydantic import BaseModel
from pydantic_ai import Agent


class Parameter(BaseModel):
    name: str
    type: str  # "argument" or "option"
    required: bool
    description: str


class Action(BaseModel):
    name: str
    description: str
    parameters: list[Parameter]


class BundledSkill(BaseModel):
    name: str
    description: str


class Manifest(BaseModel):
    name: str
    type: str
    command: str
    spec_version: int
    description: str
    available_in_sandbox: bool
    actions: list[Action]
    documentation: dict
    bundled_skills: list[BundledSkill]


SYSTEM_PROMPT = """You are a CLI documentation expert. Generate a manifest.json
that accurately documents a CLI tool based on its source code.

Rules:

1. ACTION NAMES:
   - Root-level commands: use command name directly (e.g., "classify")
   - Subcommand groups: prefix with group name (e.g., "documents convert")
   - Match exact command name from @app.command() or function name

2. PARAMETER NAMES:
   - Must match CLI flags exactly (use hyphens, not underscores)
   - typer.Argument -> type: "argument"
   - typer.Option -> type: "option"
   - ... means required, None/default means optional

3. DESCRIPTIONS:
   - Action format: "What it does. Input: X. Output: Y. Use when: scenario."
   - Parameter: explain format, valid values, when to use (for optional)
   - Never mention implementation details (libraries, internal functions)

4. DOCUMENTATION NOTES:
   - Include notes about JSON output
   - Note environment variables required (e.g., AECH_LLM_WORKER_MODEL for LLM commands)

5. BUNDLED SKILLS:
   - Extract name and description from SKILL.md frontmatter

Do NOT include hidden commands (hidden=True in typer).
Do NOT invent commands that don't exist in the code.
"""


def find_package_dir(cli_dir: Path) -> Path:
    """Find the Python package directory (contains __init__.py)."""
    for child in cli_dir.iterdir():
        if child.is_dir() and (child / "__init__.py").exists():
            if not child.name.startswith(".") and child.name not in ("scripts", "tests", "dist", "build"):
                return child
    raise FileNotFoundError("No Python package directory found")


def read_pyproject(cli_dir: Path) -> dict:
    """Read pyproject.toml to get CLI metadata."""
    pyproject_path = cli_dir / "pyproject.toml"
    if not pyproject_path.exists():
        return {}
    with open(pyproject_path, "rb") as f:
        return tomllib.load(f)


def get_cli_metadata(cli_dir: Path) -> tuple[str, str, str]:
    """Extract CLI name, command, and description from pyproject.toml."""
    pyproject = read_pyproject(cli_dir)
    project = pyproject.get("project", {})

    # Get package name and derive CLI name (e.g., "aech-cli-legal" -> "legal")
    package_name = project.get("name", "unknown")
    cli_name = package_name.replace("aech-cli-", "")

    # Get command from scripts entry
    scripts = project.get("scripts", {})
    command = list(scripts.keys())[0] if scripts else package_name

    # Get description
    description = project.get("description", "")

    return cli_name, command, description


def collect_source_files(cli_dir: Path) -> dict[str, str]:
    """Collect all Python source files from the CLI package."""
    sources = {}
    package_dir = find_package_dir(cli_dir)

    for py_file in package_dir.glob("*.py"):
        if py_file.name.startswith("_") and py_file.name != "__init__.py":
            continue
        sources[py_file.name] = py_file.read_text()

    return sources


def collect_skills(cli_dir: Path) -> list[dict]:
    """Collect bundled skills from skills/ directory."""
    skills = []
    package_dir = find_package_dir(cli_dir)
    skills_dir = package_dir / "skills"

    if not skills_dir.exists():
        return skills

    for skill_dir in skills_dir.iterdir():
        if not skill_dir.is_dir():
            continue

        skill_md = skill_dir / "SKILL.md"
        if skill_md.exists():
            skills.append({
                "name": skill_dir.name,
                "content": skill_md.read_text()[:2000]
            })

    return skills


def generate_manifest(cli_dir: Path) -> Manifest:
    """Generate manifest from source code using LLM."""
    cli_name, command, description = get_cli_metadata(cli_dir)
    sources = collect_source_files(cli_dir)
    skills = collect_skills(cli_dir)

    source_context = "\n\n".join([
        f"### {filename}\n```python\n{content}\n```"
        for filename, content in sources.items()
    ])

    skills_context = "\n\n".join([
        f"### skills/{s['name']}/SKILL.md\n```markdown\n{s['content']}\n```"
        for s in skills
    ]) or "No bundled skills."

    prompt = f"""Generate manifest.json for this CLI based on the source code.

## CLI Metadata (from pyproject.toml)
- name: "{cli_name}"
- command: "{command}"
- description: "{description}"
- spec_version: 5
- available_in_sandbox: true

## Source Files

{source_context}

## Bundled Skills

{skills_context}
"""

    model = os.environ.get("AECH_LLM_WORKER_MODEL", "openai:gpt-4o")
    agent = Agent(model, result_type=Manifest, system_prompt=SYSTEM_PROMPT)
    return agent.run_sync(prompt).data


def main():
    parser = argparse.ArgumentParser(description="Auto-generate manifest.json from CLI source")
    parser.add_argument("--dry-run", action="store_true", help="Print without writing")
    args = parser.parse_args()

    cli_dir = Path(__file__).parent.parent
    package_dir = find_package_dir(cli_dir)
    manifest_path = package_dir / "manifest.json"

    print("Analyzing CLI source code...", file=sys.stderr)
    manifest = generate_manifest(cli_dir)
    manifest_json = json.dumps(manifest.model_dump(), indent=2)

    if args.dry_run:
        print(manifest_json)
    else:
        manifest_path.write_text(manifest_json + "\n")
        print(f"Updated {manifest_path}", file=sys.stderr)
        print(f"  Actions: {len(manifest.actions)}", file=sys.stderr)
        print(f"  Skills: {len(manifest.bundled_skills)}", file=sys.stderr)


if __name__ == "__main__":
    main()
```

### scripts/build.sh

```bash
#!/bin/bash
# Build script that updates manifest before building
# Requires AECH_LLM_WORKER_MODEL env var (e.g., "openai:gpt-4o")
set -e
cd "$(dirname "$0")/.."

echo "==> Updating manifest.json from source code..."
uv run python scripts/update_manifest.py

echo "==> Building wheel..."
uv build

echo "==> Done!"
```

The script auto-detects the package directory and reads CLI metadata from `pyproject.toml`. No hardcoded paths needed.

---

## Key Requirements

### 1. Complete Parameter Coverage

Every parameter your CLI accepts must be in the manifest. The agent only knows what's in `actions[].parameters`.

Check your Typer function signature:
```python
@app.command("convert-markdown")
def convert_markdown(
    input_path: str,
    output_dir: str = typer.Option(..., "--output-dir"),
    format: list[str] = typer.Option(None, "--format"),
    reference_doc: str = typer.Option(None, "--reference-doc"),
):
```

All four parameters must appear in the manifest.

### 2. Exact Parameter Names

The manifest name must match the CLI flag exactly:
- `--output-dir` → `"name": "output-dir"`
- `--reference-doc` → `"name": "reference-doc"`

### 3. Descriptions Are Mandatory

Every action and every parameter needs a description. No exceptions. A manifest without descriptions is unusable.

### 4. Behavior Over Implementation

Describe what the CLI does for the user, not how it works internally. Don't mention libraries, engines, or technical internals.

### 5. JSON Output Only

All CLI commands output JSON. No format flags. The agent parses JSON directly.

### 6. No Hidden Commands

Commands marked `hidden=True` in Typer are for internal use and must not appear in the manifest.

## Complete Example

```json
{
  "name": "documents",
  "type": "cli",
  "command": "aech-cli-documents",
  "description": "Convert documents between formats. Accepts PDFs, Office files, and images. Produces PNG page images, Markdown text, or Office/PDF outputs.",
  "actions": [
    {
      "name": "convert",
      "description": "Render document pages to PNG images. Input: PDF, DOCX, PPTX, or image file. Output: page_001.png, page_002.png, etc. Returns JSON with list of image paths. Use when user needs to view or analyze document pages.",
      "parameters": [
        {
          "name": "input_path",
          "type": "argument",
          "required": true,
          "description": "Path to the document file to convert. Accepts PDF, DOCX, PPTX, XLS, or image files."
        },
        {
          "name": "output-dir",
          "type": "option",
          "required": true,
          "description": "Directory where PNG images will be written. Will be created if it doesn't exist."
        }
      ]
    },
    {
      "name": "convert-to-markdown",
      "description": "Extract document text as Markdown. Input: PDF or Office file. Output: <stem>.md file with extracted text. Returns JSON with output path. Use when user needs document content as editable text.",
      "parameters": [
        {
          "name": "input_path",
          "type": "argument",
          "required": true,
          "description": "Path to the document file. Accepts PDF, DOCX, PPTX, XLS, DOC, PPT, XLS."
        },
        {
          "name": "output-dir",
          "type": "option",
          "required": true,
          "description": "Directory where the Markdown file will be written."
        }
      ]
    },
    {
      "name": "convert-markdown",
      "description": "Render Markdown to Office or PDF formats. Input: Markdown file. Output: DOCX and PDF by default, or specified formats. Returns JSON with output paths. Use when user needs a polished document from Markdown source.",
      "parameters": [
        {
          "name": "input_path",
          "type": "argument",
          "required": true,
          "description": "Path to the Markdown (.md) file to convert."
        },
        {
          "name": "output-dir",
          "type": "option",
          "required": true,
          "description": "Directory where output files will be written."
        },
        {
          "name": "format",
          "type": "option",
          "required": false,
          "description": "Output format. Values: docx, pdf, pptx. Can be repeated (--format docx --format pdf). Defaults to docx and pdf."
        },
        {
          "name": "reference-doc",
          "type": "option",
          "required": false,
          "description": "Template file for styling. Use a .docx template for Word output or .pptx for PowerPoint. Applies fonts, colors, and layout from the template."
        }
      ]
    }
  ],
  "documentation": {
    "outputs": {
      "page_images": {
        "path": "<output-dir>/page_###.png",
        "description": "Numbered PNG pages from convert command"
      },
      "markdown_file": {
        "path": "<output-dir>/<stem>.md",
        "description": "Markdown file from convert-to-markdown command"
      }
    },
    "notes": [
      "All commands return JSON to stdout with output file paths",
      "Check exit code for success (0) or failure (non-zero)"
    ]
  },
  "available_in_sandbox": true
}
```

## Self-Test

Before finalizing your manifest, ask:

> **Could an agent with no other context correctly use this CLI?**

Read each action and parameter. If the description doesn't tell you exactly what to pass and when, improve it.

## Validation Checklist

- [ ] Every non-hidden Typer command has a matching action
- [ ] Every function parameter is in the manifest
- [ ] Parameter names match CLI flags exactly (hyphens, not underscores)
- [ ] Every action has a description with what/input/output/when
- [ ] Every parameter has a description with format and valid values
- [ ] No library names or implementation details in descriptions
- [ ] Test: `aech-cli-name action --help` matches manifest
- [ ] Bundled skills declared in `bundled_skills` match `skills/` directories
- [ ] Each skill has SKILL.md with valid frontmatter (name, description, allowed-tools)
- [ ] Skills in pyproject.toml `package-data` include `skills/*/SKILL.md` and `skills/*/scripts/*.py`

## How the Agent Uses This

At runtime, the agent receives:

```
DOCUMENTS: Convert documents between formats.
  Command: `aech-cli-documents`
  Actions:
  - convert: Render document pages to PNG images. Input: PDF, DOCX...
    Usage: aech-cli-documents convert <input_path> --output-dir <value>
```

This formatted view is everything the agent knows. Clear descriptions mean correct usage. Vague descriptions mean guessing and errors.
