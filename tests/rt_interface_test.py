#!/usr/bin/env python3
import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)


def _data_root(arg: Optional[str]) -> Path:
    return Path(arg or os.environ.get("AECH_DATA_DIR", "data")).expanduser().resolve()


def _user_dir(data_root: Path, user: str) -> Path:
    return data_root / "users" / user


def _outbox_dir(data_root: Path, capability: str) -> Path:
    return data_root / "rt_triggers" / capability


def prefs_show(args: argparse.Namespace) -> None:
    data_root = _data_root(args.data_dir)
    prefs_path = _user_dir(data_root, args.user) / "preferences.json"
    if not prefs_path.exists():
        print("{}")
        return
    print(prefs_path.read_text())


def prefs_set(args: argparse.Namespace) -> None:
    data_root = _data_root(args.data_dir)
    user_dir = _user_dir(data_root, args.user)
    prefs_path = user_dir / "preferences.json"

    prefs: Dict[str, Any] = {}
    if prefs_path.exists():
        try:
            prefs = json.loads(prefs_path.read_text() or "{}")
        except json.JSONDecodeError:
            prefs = {}
        if not isinstance(prefs, dict):
            prefs = {}

    if args.teams_default_target is not None:
        prefs["teams_default_target"] = args.teams_default_target

    if args.key is not None:
        try:
            prefs[args.key] = json.loads(args.value)
        except Exception:
            prefs[args.key] = args.value

    _atomic_write_json(prefs_path, prefs)
    print(str(prefs_path))


def trigger_emit(args: argparse.Namespace, *, legacy: bool) -> None:
    data_root = _data_root(args.data_dir)
    trigger_id = str(uuid.uuid4())
    payload = json.loads(args.payload) if args.payload else {}
    routing = json.loads(args.routing) if args.routing else None

    trigger: Dict[str, Any] = {
        "id": trigger_id,
        "user": args.user,
        "type": args.type,
        "created_at": _now_utc_iso(),
        "payload": payload,
    }
    if routing:
        trigger["routing"] = routing

    if legacy:
        legacy_path = data_root / "rt-triggers.json"
        if legacy_path.exists():
            try:
                data = json.loads(legacy_path.read_text() or "{}")
            except json.JSONDecodeError:
                data = {}
        else:
            data = {}
        data.setdefault(args.capability, [])
        data[args.capability].append(trigger)
        _atomic_write_json(legacy_path, data)
        print(trigger_id)
        return

    outbox = _outbox_dir(data_root, args.capability)
    _atomic_write_json(outbox / f"{trigger_id}.json", trigger)
    print(trigger_id)


def trigger_wait(args: argparse.Namespace) -> None:
    data_root = _data_root(args.data_dir)
    outbox = _outbox_dir(data_root, args.capability)

    outbox_file = outbox / f"{args.id}.json"
    done_candidates = [
        outbox / "_done" / f"{args.id}.json",
        data_root / "rt_triggers" / "_done" / args.capability / f"{args.id}.json",
        data_root / "rt_triggers" / "_done" / f"{args.id}.json",
        outbox.parent / "_done" / args.capability / f"{args.id}.json",
        outbox.parent / "_done" / f"{args.id}.json",
    ]

    deadline = time.time() + args.timeout
    while time.time() < deadline:
        if outbox_file.exists():
            time.sleep(args.poll)
            continue

        for cand in done_candidates:
            if cand.exists():
                print(str(cand))
                return

        # Claimed but not moved into a known done location.
        print("claimed")
        return

    raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Local dev helper for preferences + RT triggers.")
    parser.add_argument("--data-dir", default=None, help="Root data dir (default: ./data or AECH_DATA_DIR)")

    sub = parser.add_subparsers(dest="cmd", required=True)

    prefs = sub.add_parser("prefs", help="Manage preferences.json")
    prefs_sub = prefs.add_subparsers(dest="prefs_cmd", required=True)

    prefs_show_p = prefs_sub.add_parser("show", help="Show preferences.json")
    prefs_show_p.add_argument("--user", required=True)
    prefs_show_p.set_defaults(func=prefs_show)

    prefs_set_p = prefs_sub.add_parser("set", help="Set preferences.json values")
    prefs_set_p.add_argument("--user", required=True)
    prefs_set_p.add_argument("--teams-default-target", dest="teams_default_target")
    prefs_set_p.add_argument("--key", help="Generic key to set")
    prefs_set_p.add_argument("--value", help="Generic value (string or JSON)")
    prefs_set_p.set_defaults(func=prefs_set)

    trig = sub.add_parser("trigger", help="Emit/wait for RT triggers")
    trig_sub = trig.add_subparsers(dest="trigger_cmd", required=True)

    emit = trig_sub.add_parser("emit", help="Emit a trigger (directory outbox)")
    emit.add_argument("--capability", required=True)
    emit.add_argument("--user", required=True)
    emit.add_argument("--type", required=True)
    emit.add_argument("--payload", default="{}")
    emit.add_argument("--routing", default=None, help="JSON routing override")
    emit.set_defaults(func=lambda a: trigger_emit(a, legacy=False))

    emit_legacy = trig_sub.add_parser("emit-legacy", help="Emit a trigger (legacy rt-triggers.json)")
    emit_legacy.add_argument("--capability", required=True)
    emit_legacy.add_argument("--user", required=True)
    emit_legacy.add_argument("--type", required=True)
    emit_legacy.add_argument("--payload", default="{}")
    emit_legacy.add_argument("--routing", default=None, help="JSON routing override")
    emit_legacy.set_defaults(func=lambda a: trigger_emit(a, legacy=True))

    wait = trig_sub.add_parser("wait", help="Wait for a trigger to be claimed")
    wait.add_argument("--capability", required=True)
    wait.add_argument("--id", required=True)
    wait.add_argument("--timeout", type=int, default=60)
    wait.add_argument("--poll", type=float, default=0.5)
    wait.set_defaults(func=trigger_wait)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
