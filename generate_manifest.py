#!/usr/bin/env python3
"""
AI-Assisted Manifest Generator for Agent Aech CLIs

Introspects a Typer CLI application and uses an LLM to infer outputs.
Generates a compact, LLM-friendly manifest.json that is embedded in the
wheel and read by installer.py.

Usage:
    python generate_manifest.py aech_cli_yourname
    python generate_manifest.py aech_cli_yourname --model anthropic:claude-sonnet-4
"""
import argparse
import importlib
import inspect
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer


def introspect_typer_parameter(param_name: str, param: inspect.Parameter) -> Optional[Dict[str, Any]]:
    """
    Extract parameter metadata from a Typer parameter.

    Returns a dict with essential fields, or None to skip.
    Uses explicit CLI names from param_decls when available.
    """
    if param.default is inspect.Parameter.empty:
        return None

    default_value = param.default
    if not hasattr(default_value, '__class__'):
        return None

    class_name = default_value.__class__.__name__
    description = getattr(default_value, 'help', '') or ''

    if 'Argument' in class_name:
        is_required = getattr(default_value, 'default', ...) is ...
        return {
            "name": param_name,
            "type": "argument",
            "required": is_required,
            "description": description,
        }

    elif 'Option' in class_name:
        default_val = getattr(default_value, 'default', ...)
        is_required = default_val is ...

        # Get the explicit CLI option name from param_decls if available
        # param_decls is a tuple like ('--output-dir', '-o')
        cli_name = param_name
        param_decls = getattr(default_value, 'param_decls', None)
        if param_decls:
            # Find the long option (starts with --)
            for decl in param_decls:
                if decl.startswith('--'):
                    cli_name = decl[2:]  # Remove leading --
                    break

        result = {
            "name": cli_name,
            "type": "option",
            "required": is_required,
            "description": description,
        }

        # Include default if meaningful
        if default_val is not ... and default_val is not None and default_val is not False:
            result["default"] = default_val

        return result

    return None


def introspect_cli_app(module_path: str) -> Dict[str, Any]:
    """
    Introspect a Typer CLI application to extract structure.

    Derives name/command from module path:
      aech_cli_msgraph.main -> name=msgraph, command=aech-cli-msgraph

    Gets description from app.info.help (set via typer.Typer(help="..."))
    """
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        print(f"Error: Could not import module '{module_path}': {e}", file=sys.stderr)
        print("Make sure the module is installed: pip install -e .", file=sys.stderr)
        sys.exit(1)

    app = getattr(module, 'app', None)
    if not app or not isinstance(app, typer.Typer):
        print(f"Error: No Typer app found in module '{module_path}'", file=sys.stderr)
        print("Expected: app = typer.Typer()", file=sys.stderr)
        sys.exit(1)

    # Derive name and command from module path
    # aech_cli_msgraph.main -> aech_cli_msgraph -> name=msgraph, command=aech-cli-msgraph
    package_name = module_path.split('.')[0]  # aech_cli_msgraph
    command = package_name.replace('_', '-')  # aech-cli-msgraph
    name = package_name.replace('aech_cli_', '')  # msgraph

    # Get description from Typer app
    description = ""
    if app.info and app.info.help:
        description = app.info.help

    actions = []
    for cmd_info in app.registered_commands:
        callback = cmd_info.callback
        if callback is None:
            continue

        cmd_name = cmd_info.name or callback.__name__.replace('_', '-')
        docstring = inspect.getdoc(callback) or ""
        action_description = docstring.split('\n')[0] if docstring else ""

        sig = inspect.signature(callback)
        parameters = []
        for param_name, param in sig.parameters.items():
            # Skip common utility params
            if param_name in {'verbose', 'debug'}:
                continue

            param_info = introspect_typer_parameter(param_name, param)
            if param_info:
                parameters.append(param_info)

        # Get source code for LLM output inference
        try:
            source_code = inspect.getsource(callback)
        except OSError:
            source_code = ""

        actions.append({
            "name": cmd_name,
            "description": action_description,
            "parameters": parameters,
            "docstring": docstring,
            "source_code": source_code,
        })

    return {
        "name": name,
        "command": command,
        "description": description,
        "actions": actions
    }


def infer_outputs_with_llm(actions: List[Dict[str, Any]], model: str) -> Dict[str, str]:
    """
    Use LLM to infer output paths for each action.

    Returns a dict mapping action_name -> output path string.
    Only includes actions that produce file outputs (not stdout).
    """
    from pydantic_ai import Agent
    from pydantic_ai.models import infer_model

    # Prepare minimal context for LLM
    actions_for_llm = []
    for action in actions:
        actions_for_llm.append({
            "name": action["name"],
            "description": action["description"],
            "parameters": action["parameters"],
            "docstring": action["docstring"][:500]
        })

    prompt = f"""You are inferring file outputs for CLI actions.

For each action below, determine if it produces a file output (not stdout).

Actions:
{json.dumps(actions_for_llm, indent=2)}

Return a JSON object mapping action names to their output path pattern.
Only include actions that write files. Omit actions that output to stdout.

Output format:
{{
  "action-name": "<output_dir>/filename_<YYYYMMDD>.xlsx"
}}

Guidelines:
- Use <param_name> for dynamic parts from parameters
- Use YYYYMMDD for dates in filenames
- Common patterns: "<output_dir>/report.xlsx", "<output_path>"
- Omit search/query/list commands (they typically output to stdout)
- Only include actions that create files

Return ONLY the JSON object, no markdown formatting.
"""

    llm_model = infer_model(model)
    agent = Agent(llm_model)

    print(f"🤖 Calling {model} to infer outputs...", file=sys.stderr)

    result = agent.run_sync(prompt)
    response_text = result.output.strip()

    try:
        usage = result.usage() if hasattr(result, 'usage') else {}
        print(f"   LLM usage: {usage}", file=sys.stderr)
    except Exception:
        pass

    # Strip markdown if present
    if response_text.startswith('```'):
        lines = response_text.split('\n')
        response_text = '\n'.join(lines[1:-1])
        if response_text.startswith('json'):
            response_text = '\n'.join(response_text.split('\n')[1:])

    try:
        return json.loads(response_text)
    except json.JSONDecodeError as e:
        print(f"Error: LLM did not return valid JSON: {e}", file=sys.stderr)
        print("Response:", response_text, file=sys.stderr)
        return {}


def compact_parameters(parameters: List[Dict[str, Any]]) -> tuple[List[str], List[str]]:
    """
    Convert parameters to compact args/opts format.

    Returns (args, opts) where:
    - args: ["name: description [value]"] for positional arguments
    - opts: ["--name: description [value]"] for options
    """
    args = []
    opts = []

    for param in parameters:
        name = param.get("name", "")
        desc = param.get("description", "")
        default = param.get("default")
        is_required = param.get("required", False)
        param_type = param.get("type", "option")

        if param_type == "argument":
            # Positional argument - format: "name: description [value]"
            if desc and default is not None:
                arg_str = f"{name}: {desc} [{default}]"
            elif desc:
                arg_str = f"{name}: {desc}"
            elif default is not None:
                arg_str = f"{name} [{default}]"
            else:
                arg_str = name
            if not is_required:
                arg_str = f"[{arg_str}]"
            args.append(arg_str)
        else:
            # Option flag - format: "--name: description [value]"
            if desc and default is not None:
                opt_str = f"--{name}: {desc} [{default}]"
            elif desc:
                opt_str = f"--{name}: {desc}"
            elif default is not None:
                opt_str = f"--{name} [{default}]"
            else:
                opt_str = f"--{name}"
            opts.append(opt_str)

    return args, opts


def build_compact_manifest(introspected: Dict[str, Any], outputs_map: Dict[str, str]) -> Dict[str, Any]:
    """
    Build a compact, LLM-friendly manifest from introspected data.

    Compact format:
    - name: short name (e.g., "msgraph")
    - command: CLI command (e.g., "aech-cli-msgraph")
    - desc: CLI description
    - actions: {
        "action-name": {
          "desc": "description",
          "args": ["required_arg", "[optional_arg]"],
          "opts": ["--option: [default] description"],
          "out": "output/path/pattern.xlsx"  (only if produces file output)
        }
      }
    """
    command = introspected["command"]
    manifest = {
        "name": introspected["name"],
        "command": command,
        "example": f"{command} ACTION ARG --OPTION VALUE",
        "desc": introspected["description"],
    }

    # Compact actions
    actions = {}
    for action in introspected["actions"]:
        action_name = action["name"]
        action_compact = {}

        if action.get("description"):
            action_compact["desc"] = action["description"]

        args, opts = compact_parameters(action.get("parameters", []))

        if args:
            action_compact["args"] = args
        if opts:
            action_compact["opts"] = opts

        # Add output path if this action produces file output
        if action_name in outputs_map:
            action_compact["out"] = outputs_map[action_name]

        actions[action_name] = action_compact

    if actions:
        manifest["actions"] = actions

    return manifest


def main():
    parser = argparse.ArgumentParser(
        description="Generate compact manifest.json for Agent Aech CLI using AI assistance"
    )
    parser.add_argument(
        "package_path",
        help="Path to the CLI package folder (e.g., aech_cli_pbwc)"
    )
    parser.add_argument(
        "--model",
        default="openai:gpt-4.1",
        help="Model to use for output inference (default: openai:gpt-4.1)"
    )

    args = parser.parse_args()

    # Derive module path from package folder
    # aech_cli_pbwc -> aech_cli_pbwc.main
    package_path = Path(args.package_path)
    package_name = package_path.name
    module_path = f"{package_name}.main"

    print(f"🔍 Introspecting {module_path}...", file=sys.stderr)
    introspected = introspect_cli_app(module_path)
    print(f"   Found {len(introspected['actions'])} actions", file=sys.stderr)
    print(f"   Name: {introspected['name']}, Command: {introspected['command']}", file=sys.stderr)

    outputs_map = infer_outputs_with_llm(
        introspected["actions"],
        args.model
    )

    # Build compact manifest
    manifest = build_compact_manifest(introspected, outputs_map)

    # Write to manifest.json in the package folder
    output_path = package_path / "manifest.json"
    output_path.write_text(json.dumps(manifest, indent=2) + '\n')

    print(f"✅ Manifest written to {output_path}", file=sys.stderr)
    print(f"📝 Review and edit as needed, then rebuild wheel", file=sys.stderr)


if __name__ == "__main__":
    main()
