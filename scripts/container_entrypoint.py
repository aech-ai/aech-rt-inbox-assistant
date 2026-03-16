#!/usr/bin/env python3
"""Repair mounted runtime state ownership, then drop to agentaech."""

from __future__ import annotations

import os
import pwd
import sys
from pathlib import Path
from typing import Iterable, Mapping


def build_owned_paths(environ: Mapping[str, str]) -> tuple[set[Path], set[Path]]:
    home = Path("/home/agentaech")
    fallback_state_dir = home / ".inbox-assistant"
    state_root = Path("/app/state")
    state_dir = Path(environ.get("INBOX_STATE_DIR", str(fallback_state_dir))).expanduser()

    ensure_dirs = {home, fallback_state_dir, state_root, state_dir}
    repair_paths = {fallback_state_dir, state_dir}

    for key in ("INBOX_DB_PATH", "LLM_LOG_DIR"):
        raw = environ.get(key)
        if not raw:
            continue
        path = Path(raw).expanduser()
        ensure_dirs.add(path.parent if path.suffix else path)
        repair_paths.add(path.parent if path.suffix else path)

    return ensure_dirs, repair_paths


def _walk_paths(root: Path) -> Iterable[Path]:
    yield root
    if root.is_dir() and not root.is_symlink():
        for child in root.iterdir():
            yield from _walk_paths(child)


def _chown_tree(path: Path, uid: int, gid: int) -> None:
    if not path.exists():
        return
    for item in _walk_paths(path):
        os.chown(item, uid, gid)


def _prepare_runtime_permissions(uid: int, gid: int) -> None:
    ensure_dirs, repair_paths = build_owned_paths(os.environ)
    for directory in sorted(ensure_dirs):
        directory.mkdir(parents=True, exist_ok=True)
        os.chown(directory, uid, gid)
    for path in sorted(repair_paths):
        _chown_tree(path, uid, gid)


def _drop_to_agentaech() -> None:
    account = pwd.getpwnam("agentaech")
    os.initgroups(account.pw_name, account.pw_gid)
    os.setgid(account.pw_gid)
    os.setuid(account.pw_uid)
    os.environ["HOME"] = account.pw_dir
    os.environ["USER"] = account.pw_name
    os.environ["LOGNAME"] = account.pw_name


def main() -> None:
    command = sys.argv[1:] or ["python", "-m", "src.main"]
    os.umask(0o002)

    if os.geteuid() == 0:
        account = pwd.getpwnam("agentaech")
        _prepare_runtime_permissions(account.pw_uid, account.pw_gid)
        _drop_to_agentaech()

    os.execvp(command[0], command)


if __name__ == "__main__":
    main()
