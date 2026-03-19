from __future__ import annotations

import json
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

CAPABILITY_NAME = "inbox-assistant"
QUEUE_ROOT_ENV = "INBOX_ASSISTANT_CLI_QUEUE_ROOT"
RT_ROOT_ENV = "AECH_RT_TRIGGERS_ROOT"


@dataclass(frozen=True)
class DraftQueuePaths:
    root: Path
    outbox: Path
    processing: Path
    done: Path
    failed: Path
    uploads: Path


def _resolve_queue_root() -> Path:
    configured = os.environ.get(QUEUE_ROOT_ENV)
    if configured:
        return Path(configured).expanduser().resolve()

    rt_root = Path(os.environ.get(RT_ROOT_ENV, "/triggers")).expanduser().resolve()
    namespaced_root = rt_root / CAPABILITY_NAME
    if namespaced_root.is_dir():
        return (namespaced_root / "cli").resolve()

    capability_local_markers = ("outbox", "processing", "done", "failed", "dedupe")
    if any((rt_root / name).exists() for name in capability_local_markers):
        return (rt_root / "cli").resolve()
    return (rt_root / CAPABILITY_NAME / "cli").resolve()


def get_draft_queue_paths() -> DraftQueuePaths:
    root = _resolve_queue_root()
    return DraftQueuePaths(
        root=root,
        outbox=root / "outbox",
        processing=root / "processing",
        done=root / "done",
        failed=root / "failed",
        uploads=root / "uploads",
    )


def ensure_draft_queue_dirs(paths: DraftQueuePaths | None = None) -> DraftQueuePaths:
    resolved = paths or get_draft_queue_paths()
    for directory in (
        resolved.root,
        resolved.outbox,
        resolved.processing,
        resolved.done,
        resolved.failed,
        resolved.uploads,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    return resolved


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def _request_filename(request_id: str) -> str:
    return f"{request_id}.json"


def _cleanup_uploads(paths: DraftQueuePaths, request_id: str) -> None:
    shutil.rmtree(paths.uploads / request_id, ignore_errors=True)


def _coerce_string_list(values: Any, field_name: str) -> list[str]:
    if values is None:
        return []
    if not isinstance(values, list):
        raise ValueError(f"{field_name} must be a JSON array of strings")

    output: list[str] = []
    for item in values:
        cleaned = str(item or "").strip()
        if cleaned:
            output.append(cleaned)
    return output


def _resolve_staged_attachment(paths: DraftQueuePaths, relative_path: str) -> str:
    candidate = (paths.root / relative_path).resolve()
    try:
        candidate.relative_to(paths.root.resolve())
    except ValueError as exc:
        raise ValueError(f"Attachment path escapes queue root: {relative_path}") from exc
    if not candidate.is_file():
        raise ValueError(f"Staged attachment not found: {relative_path}")
    return str(candidate)


def _stage_attachments(
    paths: DraftQueuePaths,
    request_id: str,
    attachments: Iterable[str],
) -> list[str]:
    staged_paths: list[str] = []
    for index, raw_path in enumerate(attachments):
        source = Path(raw_path).expanduser().resolve()
        if not source.is_file():
            raise ValueError(f"Attachment file not found: {raw_path}")
        target_dir = paths.uploads / request_id / f"{index:04d}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / source.name
        shutil.copy2(source, target_path)
        staged_paths.append(str(target_path.relative_to(paths.root).as_posix()))
    return staged_paths


class DraftRequestClient:
    def __init__(
        self,
        *,
        timeout_seconds: float = 60.0,
        poll_interval_seconds: float = 0.1,
    ) -> None:
        self.paths = ensure_draft_queue_dirs()
        self.timeout_seconds = timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds

    def create_draft(
        self,
        *,
        subject: str,
        body: str,
        body_content_type: str,
        to_recipients: list[str],
        cc_recipients: list[str],
        bcc_recipients: list[str],
        attachments: list[str],
    ) -> dict[str, Any]:
        return self._submit(
            "draft.create",
            payload={
                "subject": subject,
                "body": body,
                "body_content_type": body_content_type,
                "to_recipients": to_recipients,
                "cc_recipients": cc_recipients,
                "bcc_recipients": bcc_recipients,
            },
            attachments=attachments,
        )

    def reply_draft(
        self,
        message_id: str,
        *,
        subject: str | None,
        body: str,
        body_content_type: str,
        attachments: list[str],
        reply_all: bool,
    ) -> dict[str, Any]:
        return self._submit(
            "draft.reply",
            payload={
                "message_id": message_id,
                "subject": subject,
                "body": body,
                "body_content_type": body_content_type,
                "reply_all": reply_all,
            },
            attachments=attachments,
        )

    def _submit(
        self,
        action: str,
        *,
        payload: dict[str, Any],
        attachments: list[str],
    ) -> dict[str, Any]:
        request_id = uuid.uuid4().hex
        request_path = self.paths.outbox / _request_filename(request_id)
        try:
            staged_attachments = _stage_attachments(self.paths, request_id, attachments)
            request_payload = {
                "id": request_id,
                "action": action,
                "payload": {
                    **payload,
                    "attachments": staged_attachments,
                },
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            _write_json_atomic(request_path, request_payload)
        except Exception:
            _cleanup_uploads(self.paths, request_id)
            raise

        return self._wait_for_result(request_id)

    def _wait_for_result(self, request_id: str) -> dict[str, Any]:
        filename = _request_filename(request_id)
        done_path = self.paths.done / filename
        failed_path = self.paths.failed / filename
        deadline = time.monotonic() + self.timeout_seconds

        while time.monotonic() < deadline:
            if done_path.exists():
                payload = json.loads(done_path.read_text(encoding="utf-8"))
                done_path.unlink(missing_ok=True)
                result = payload.get("result")
                if not isinstance(result, dict):
                    raise RuntimeError("Draft request succeeded but returned no result payload")
                return result

            if failed_path.exists():
                payload = json.loads(failed_path.read_text(encoding="utf-8"))
                failed_path.unlink(missing_ok=True)
                error = payload.get("error") if isinstance(payload.get("error"), dict) else {}
                message = str(error.get("message") or "Draft request failed").strip()
                raise RuntimeError(message)

            time.sleep(self.poll_interval_seconds)

        raise TimeoutError(
            f"Timed out waiting {self.timeout_seconds:.1f}s for inbox-assistant draft request {request_id}"
        )


def process_pending_draft_requests(poller: Any) -> dict[str, int]:
    paths = ensure_draft_queue_dirs()
    processed = 0
    failed = 0

    for request_path in sorted(paths.outbox.glob("*.json")):
        processing_path = paths.processing / request_path.name
        try:
            request_path.rename(processing_path)
        except FileNotFoundError:
            continue
        except Exception:
            failed += 1
            continue

        request_id = processing_path.stem
        try:
            request = json.loads(processing_path.read_text(encoding="utf-8"))
            if not isinstance(request, dict):
                raise ValueError("request payload must be a JSON object")
            result = _handle_request(poller, request, paths)
            _write_json_atomic(
                paths.done / request_path.name,
                {"ok": True, "result": result},
            )
            processed += 1
        except Exception as exc:
            _write_json_atomic(
                paths.failed / request_path.name,
                {"ok": False, "error": {"message": str(exc)}},
            )
            failed += 1
        finally:
            processing_path.unlink(missing_ok=True)
            _cleanup_uploads(paths, request_id)

    return {"processed": processed, "failed": failed}


def _handle_request(
    poller: Any,
    request: dict[str, Any],
    paths: DraftQueuePaths,
) -> dict[str, Any]:
    action = str(request.get("action") or "").strip()
    payload = request.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("request payload must include an object 'payload'")

    staged_attachments = [
        _resolve_staged_attachment(paths, relative_path)
        for relative_path in _coerce_string_list(payload.get("attachments"), "attachments")
    ]

    if action == "draft.create":
        draft = poller.create_draft(
            subject=str(payload.get("subject") or ""),
            body=str(payload.get("body") or ""),
            body_content_type=str(payload.get("body_content_type") or "text"),
            to_recipients=_coerce_string_list(payload.get("to_recipients"), "to_recipients"),
            cc_recipients=_coerce_string_list(payload.get("cc_recipients"), "cc_recipients"),
            bcc_recipients=_coerce_string_list(payload.get("bcc_recipients"), "bcc_recipients"),
            attachments=staged_attachments,
        )
        return {"created_via": "new", "draft": draft}

    if action == "draft.reply":
        message_id = str(payload.get("message_id") or "").strip()
        if not message_id:
            raise ValueError("message_id is required for draft.reply")

        draft = poller.create_reply_draft(
            message_id,
            subject=payload.get("subject"),
            body=str(payload.get("body") or ""),
            body_content_type=str(payload.get("body_content_type") or "text"),
            attachments=staged_attachments,
            reply_all=bool(payload.get("reply_all", False)),
        )
        return {
            "created_via": "reply_all" if bool(payload.get("reply_all", False)) else "reply",
            "draft": draft,
        }

    raise ValueError(f"Unsupported action '{action}'")
