from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parents[1]
        / "packages"
        / "aech-cli-inbox-assistant"
        / "src"
    ),
)

from aech_cli_inbox_assistant.state import connect_db


def _seed_db(path: Path, *, wal_mode: bool = False) -> None:
    conn = sqlite3.connect(path)
    if wal_mode:
        conn.execute("PRAGMA journal_mode=WAL;").fetchone()
    conn.execute("CREATE TABLE emails (id TEXT PRIMARY KEY, subject TEXT)")
    conn.execute("INSERT INTO emails (id, subject) VALUES (?, ?)", ("email-1", "Hello"))
    conn.commit()
    conn.close()


def test_connect_db_read_only_supports_read_only_state_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    state_dir = tmp_path / ".inbox-assistant"
    state_dir.mkdir()
    db_path = state_dir / "assistant.sqlite"
    _seed_db(db_path)

    db_path.chmod(0o444)
    state_dir.chmod(0o555)
    monkeypatch.setenv("INBOX_DB_PATH", str(db_path))

    conn = connect_db(read_only=True)
    try:
        assert conn.execute("SELECT subject FROM emails").fetchone()[0] == "Hello"
    finally:
        conn.close()

    with pytest.raises(sqlite3.OperationalError):
        conn = connect_db()
        try:
            conn.execute("PRAGMA journal_mode=WAL;").fetchone()
        finally:
            conn.close()


def test_connect_db_read_only_supports_wal_mode_on_read_only_mount(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    state_dir = tmp_path / ".inbox-assistant"
    state_dir.mkdir()
    db_path = state_dir / "assistant.sqlite"
    _seed_db(db_path, wal_mode=True)

    db_path.chmod(0o444)
    state_dir.chmod(0o555)
    monkeypatch.setenv("INBOX_DB_PATH", str(db_path))

    with pytest.raises(sqlite3.OperationalError):
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            conn.execute("SELECT subject FROM emails").fetchone()
        finally:
            conn.close()

    conn = connect_db(read_only=True)
    try:
        assert conn.execute("SELECT subject FROM emails").fetchone()[0] == "Hello"
    finally:
        conn.close()
