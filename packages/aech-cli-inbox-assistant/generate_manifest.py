#!/usr/bin/env python3
"""
Pure Introspection Manifest Generator for CLI

Introspects the Click CLI application and generates manifest.json
from docstrings and parameter metadata. No LLM required.

Usage:
    python generate_manifest.py

Run before building the package to update manifest.json.
"""

import json
import sys
from pathlib import Path
from typing import Any

import click


# Paths relative to this script
SCRIPT_DIR = Path(__file__).parent
SRC_DIR = SCRIPT_DIR / "src" / "aech_cli_inbox_assistant"
MANIFEST_PATH = SRC_DIR / "manifest.json"


def introspect_parameter(param: click.Parameter) -> dict[str, Any] | None:
    """Extract parameter metadata from a Click parameter."""
    if isinstance(param, click.Argument):
        return {
            "name": param.name,
            "type": "argument",
            "required": param.required,
            "description": "",
        }

    elif isinstance(param, click.Option):
        # Skip internal Click options like --help
        if param.name in ("help",):
            return None

        is_flag = param.is_flag
        description = param.help or ""

        result = {
            "name": param.name,
            "type": "flag" if is_flag else "option",
            "required": param.required,
            "description": description,
        }

        # Include default if meaningful
        default_val = param.default
        if default_val is not None and default_val is not False and default_val != ():
            try:
                json.dumps(default_val)  # Check if serializable
                result["default"] = default_val
            except (TypeError, ValueError):
                pass

        return result

    return None


def introspect_command(cmd: click.Command, prefix: str = "") -> dict[str, Any]:
    """Introspect a single Click command."""
    name = f"{prefix} {cmd.name}".strip() if prefix else cmd.name
    description = cmd.help or ""
    # First line only
    description = description.split("\n")[0] if description else ""

    parameters = []
    for param in cmd.params:
        param_info = introspect_parameter(param)
        if param_info:
            parameters.append(param_info)

    return {
        "name": name,
        "description": description,
        "parameters": parameters,
    }


def introspect_click_app(group: click.Group, name: str, command: str) -> dict[str, Any]:
    """Introspect a Click Group and extract all command metadata."""
    description = group.help or ""

    actions = []

    def process_group(grp: click.Group, prefix: str = ""):
        """Recursively process groups and commands."""
        for cmd_name, cmd in grp.commands.items():
            if isinstance(cmd, click.Group):
                # It's a subgroup, recurse
                process_group(cmd, f"{prefix} {cmd_name}".strip() if prefix else cmd_name)
            else:
                # It's a command
                action = introspect_command(cmd, prefix)
                actions.append(action)

    process_group(group)

    return {
        "name": name,
        "type": "cli",
        "description": description,
        "command": command,
        "actions": actions,
        "documentation": {
            "usage": f"{command} <command> [options]",
            "examples": [
                f"{command} list --limit 10",
                f"{command} search 'query' --limit 10",
                f"{command} reply-needed",
            ],
        },
    }


def main():
    """Generate manifest.json from CLI introspection."""
    # Add src to path so we can import the module
    sys.path.insert(0, str(SRC_DIR.parent))

    try:
        from aech_cli_inbox_assistant.main import app
    except ImportError as e:
        print(f"Error: Could not import CLI app: {e}", file=sys.stderr)
        print("Make sure dependencies are installed: pip install -e .", file=sys.stderr)
        sys.exit(1)

    print("Introspecting CLI app...")

    manifest = introspect_click_app(
        app,
        name="inbox-assistant",
        command="aech-cli-inbox-assistant",
    )

    print(f"Found {len(manifest['actions'])} commands")
    print(f"Writing {MANIFEST_PATH}...")

    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n")

    print("Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
