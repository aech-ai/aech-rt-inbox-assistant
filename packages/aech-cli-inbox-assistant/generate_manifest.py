#!/usr/bin/env python3
"""
Pure Introspection Manifest Generator for CLI

Introspects the Typer CLI application and generates manifest.json
from docstrings and parameter metadata. No LLM required.

Usage:
    python generate_manifest.py

Run before building the package to update manifest.json.
"""

import inspect
import json
import sys
from pathlib import Path
from typing import Any

import typer


# Paths relative to this script
SCRIPT_DIR = Path(__file__).parent
SRC_DIR = SCRIPT_DIR / "src" / "aech_cli_inbox_assistant"
MANIFEST_PATH = SRC_DIR / "manifest.json"


def introspect_parameter(param_name: str, param: inspect.Parameter) -> dict[str, Any] | None:
    """Extract parameter metadata from a Typer parameter."""
    # Skip 'human' parameter - it's for terminal output, not agent use
    if param_name == "human":
        return None

    if param.default is inspect.Parameter.empty:
        return None

    default_value = param.default
    if not hasattr(default_value, "__class__"):
        return None

    class_name = default_value.__class__.__name__
    description = getattr(default_value, "help", "") or ""

    if "Argument" in class_name:
        is_required = getattr(default_value, "default", ...) is ...
        return {
            "name": param_name,
            "type": "argument",
            "required": is_required,
            "description": description,
        }

    elif "Option" in class_name:
        default_val = getattr(default_value, "default", ...)
        is_required = default_val is ...

        # Get explicit CLI option name from param_decls if available
        cli_name = param_name
        param_decls = getattr(default_value, "param_decls", None)
        if param_decls:
            for decl in param_decls:
                if decl.startswith("--"):
                    cli_name = decl[2:]
                    break

        result = {
            "name": cli_name,
            "type": "flag" if isinstance(default_val, bool) else "option",
            "required": is_required,
            "description": description,
        }

        # Include default if meaningful
        if default_val is not ... and default_val is not None and default_val is not False:
            result["default"] = default_val

        return result

    return None


def introspect_typer_app(app: typer.Typer, name: str, command: str) -> dict[str, Any]:
    """Introspect a Typer app and extract all command metadata."""
    description = ""
    if app.info and app.info.help:
        description = app.info.help

    actions = []

    # Process registered commands
    for cmd_info in app.registered_commands:
        callback = cmd_info.callback
        if callback is None:
            continue

        cmd_name = cmd_info.name or callback.__name__.replace("_", "-")
        docstring = inspect.getdoc(callback) or ""
        # First line of docstring is the description
        action_description = docstring.split("\n")[0] if docstring else ""

        sig = inspect.signature(callback)
        parameters = []
        for param_name, param in sig.parameters.items():
            param_info = introspect_parameter(param_name, param)
            if param_info:
                parameters.append(param_info)

        actions.append({
            "name": cmd_name,
            "description": action_description,
            "parameters": parameters,
        })

    # Process sub-apps (like prefs_app)
    for group_info in app.registered_groups:
        sub_app = group_info.typer_instance
        group_name = group_info.name or ""

        if sub_app is None:
            continue

        for cmd_info in sub_app.registered_commands:
            callback = cmd_info.callback
            if callback is None:
                continue

            cmd_name = cmd_info.name or callback.__name__.replace("_", "-")
            full_name = f"{group_name} {cmd_name}" if group_name else cmd_name
            docstring = inspect.getdoc(callback) or ""
            action_description = docstring.split("\n")[0] if docstring else ""

            sig = inspect.signature(callback)
            parameters = []
            for param_name, param in sig.parameters.items():
                param_info = introspect_parameter(param_name, param)
                if param_info:
                    parameters.append(param_info)

            actions.append({
                "name": full_name,
                "description": action_description,
                "parameters": parameters,
            })

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

    print(f"Introspecting CLI app...")

    manifest = introspect_typer_app(
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
