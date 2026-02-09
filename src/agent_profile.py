"""Runtime loader for AGENT_PROFILE steering instructions."""

from __future__ import annotations

import os
from pathlib import Path
import re

PROFILE_FILENAME = "AGENT_PROFILE.md"
CAPABILITIES_DIRNAME = "capabilities"
_CAPABILITY_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def _repo_root() -> Path:
    """Resolve repository root for AGENT_PROFILE lookup.

    AECH_AGENT_PROFILE_ROOT may override this for tests and custom deployments.
    """
    override = os.getenv("AECH_AGENT_PROFILE_ROOT", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parents[1]


def _read_profile(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception as exc:
        raise RuntimeError(f"Failed to read AGENT_PROFILE file at {path}: {exc}") from exc


def _validate_capability_name(capability_name: str) -> None:
    if not _CAPABILITY_NAME_RE.match(capability_name):
        raise ValueError(
            "capability_name contains invalid characters; expected letters, numbers, '.', '_' or '-'."
        )


def load_agent_profile(capability_name: str | None = None) -> str:
    """Load root AGENT_PROFILE and optional capability addendum."""
    repo_root = _repo_root()
    sections: list[str] = []

    root_profile_path = repo_root / PROFILE_FILENAME
    if root_profile_path.exists():
        root_profile = _read_profile(root_profile_path)
        if root_profile:
            sections.append("## Base Profile\n" + root_profile)

    if capability_name:
        _validate_capability_name(capability_name)
        capability_profile_path = (
            repo_root / CAPABILITIES_DIRNAME / capability_name / PROFILE_FILENAME
        )
        if capability_profile_path.exists():
            capability_profile = _read_profile(capability_profile_path)
            if capability_profile:
                sections.append(
                    f"## Capability Addendum ({capability_name})\n{capability_profile}"
                )

    return "\n\n".join(sections)


def append_agent_profile(base_instructions: str, capability_name: str | None = None) -> str:
    """Append runtime AGENT_PROFILE content to static instructions if available."""
    profile = load_agent_profile(capability_name=capability_name)
    if not profile:
        return base_instructions
    return f"{base_instructions.rstrip()}\n\n## Runtime AGENT_PROFILE\n{profile}\n"
