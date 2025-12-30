#!/usr/bin/env python3
"""
Build script for aech-cli-inbox-assistant.

Regenerates manifest.json from CLI introspection, then builds the wheel.

Usage:
    python build_cli.py
    # or
    ./build_cli.py
"""

import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent


def main():
    print("=" * 60)
    print("Building aech-cli-inbox-assistant")
    print("=" * 60)

    # Step 1: Generate manifest
    print("\n[1/2] Regenerating manifest.json...")
    result = subprocess.run(
        ["uv", "run", "python", "generate_manifest.py"],
        cwd=SCRIPT_DIR,
    )
    if result.returncode != 0:
        print("Error: Manifest generation failed")
        return 1

    # Step 2: Build with uv
    print("\n[2/2] Building wheel with uv...")
    result = subprocess.run(
        ["uv", "build"],
        cwd=SCRIPT_DIR,
    )
    if result.returncode != 0:
        print("Error: Build failed")
        return 1

    print("\n" + "=" * 60)
    print("Build complete!")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
