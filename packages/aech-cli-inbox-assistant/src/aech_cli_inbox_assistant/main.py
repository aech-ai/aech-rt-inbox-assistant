"""
JSON-only CLI for querying inbox-assistant state.

This CLI intentionally exposes deterministic retrieval and preference surfaces.
It does not implement role-specific reasoning.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

import click

from .categories import (
    COLOR_PRESETS,
    VALID_FLAG_URGENCIES,
    add_category,
    ensure_categories_initialized,
    get_categories,
    remove_category,
    reset_categories,
    update_category,
)
from .state import (
    InvalidPreferenceKeyError,
    VALID_PREFERENCE_KEYS,
    connect_db,
    get_db_path,
    get_state_dir,
    read_preferences,
    set_preference_from_string,
    write_preferences,
)


def output_json(data: Any) -> None:
    click.echo(json.dumps(data, indent=2, default=str))


def output_error(message: str, code: str = "error") -> None:
    click.echo(json.dumps({"error": code, "message": message}), err=True)


def get_param_info(param: click.Parameter) -> dict[str, Any]:
    param_info: dict[str, Any] = {
        "name": param.name,
        "type": param.type.name if hasattr(param.type, "name") else str(param.type),
        "required": getattr(param, "required", False),
    }
    if isinstance(param, click.Option):
        if param.help:
            param_info["help"] = param.help
        if param.is_flag:
            param_info["is_flag"] = True
    if isinstance(param, click.Argument):
        param_info["argument"] = True
    if param.default is not None and param.default != ():
        try:
            json.dumps(param.default)
            param_info["default"] = param.default
        except (TypeError, ValueError):
            pass
    return param_info


def get_command_help(cmd: click.Command) -> dict[str, Any]:
    return {
        "help": cmd.help or "",
        "options": [get_param_info(p) for p in cmd.params],
    }


class JSONGroup(click.Group):
    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        return None

    def get_help(self, ctx: click.Context) -> str:
        commands = {}
        for name, cmd in self.commands.items():
            cmd_info = get_command_help(cmd)
            if isinstance(cmd, click.Group):
                cmd_info["subcommands"] = list(cmd.commands.keys())
            commands[name] = cmd_info
        return json.dumps(
            {
                "name": ctx.info_name,
                "help": self.help or "",
                "options": [get_param_info(p) for p in self.params],
                "commands": commands,
            },
            indent=2,
        )


class JSONCommand(click.Command):
    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        return None

    def get_help(self, ctx: click.Context) -> str:
        return json.dumps(
            {
                "name": ctx.info_name,
                "help": self.help or "",
                "options": [get_param_info(p) for p in self.params],
            },
            indent=2,
        )


def _resolve_storage_path(path_str: str | None) -> str | None:
    if not path_str:
        return None
    path = Path(path_str).expanduser()
    if path.is_absolute():
        return str(path.resolve())
    return str((get_state_dir() / path).resolve())


def _load_runtime_attr(module_name: str, attr_name: str) -> Any:
    module_path = f"src.{module_name}"
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        if exc.name == "src" or "No module named 'src'" in str(exc):
            raise ImportError(
                "aech-rt-inbox-assistant runtime package is not installed in this environment"
            ) from exc
        raise ImportError(f"Failed to import {module_path}: {exc}") from exc

    try:
        return getattr(module, attr_name)
    except AttributeError as exc:
        raise ImportError(f"{module_path} does not export {attr_name}") from exc


def _row_to_email_dict(row) -> dict[str, Any]:
    item = dict(row)
    item["to_emails"] = json.loads(item.get("to_emails") or "[]")
    item["cc_emails"] = json.loads(item.get("cc_emails") or "[]")
    item["bcc_emails"] = json.loads(item.get("bcc_emails") or "[]")
    item["outlook_categories"] = json.loads(item.get("outlook_categories") or "[]")
    return item


def _row_to_attachment_dict(row) -> dict[str, Any]:
    item = dict(row)
    item["stored_path"] = _resolve_storage_path(item.get("storage_path"))
    return item


def _normalize_recipient_inputs(values: tuple[str, ...]) -> list[str]:
    recipients: list[str] = []
    for value in values:
        for part in str(value or "").split(","):
            cleaned = part.strip()
            if cleaned:
                recipients.append(cleaned)
    return recipients


def _read_body_input(body: str | None, body_file: str | None) -> str:
    if body is not None and body_file is not None:
        raise ValueError("Provide either --body or --body-file, not both.")
    if body_file:
        try:
            return Path(body_file).expanduser().read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(f"Failed to read body file: {exc}") from exc
    return body or ""


def _extract_recipient_addresses(recipients: Any) -> list[str]:
    output: list[str] = []
    for recipient in recipients or []:
        email_address = recipient.get("emailAddress") or {}
        address = str(email_address.get("address") or "").strip()
        if address:
            output.append(address)
    return output


def _project_graph_attachment(attachment: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": attachment.get("id"),
        "filename": attachment.get("name") or "",
        "content_type": attachment.get("contentType"),
        "size_bytes": attachment.get("size"),
    }


def _project_graph_message(message: dict[str, Any]) -> dict[str, Any]:
    sender = ((message.get("from") or {}).get("emailAddress") or {}).get("address")
    return {
        "id": message.get("id"),
        "internet_message_id": message.get("internetMessageId"),
        "conversation_id": message.get("conversationId"),
        "subject": message.get("subject") or "",
        "sender": sender or "",
        "web_link": message.get("webLink"),
        "body_preview": message.get("bodyPreview") or "",
        "is_draft": bool(message.get("isDraft", False)),
        "created_at": message.get("createdDateTime"),
        "updated_at": message.get("lastModifiedDateTime"),
        "to_recipients": _extract_recipient_addresses(message.get("toRecipients")),
        "cc_recipients": _extract_recipient_addresses(message.get("ccRecipients")),
        "bcc_recipients": _extract_recipient_addresses(message.get("bccRecipients")),
        "has_attachments": bool(message.get("hasAttachments") or message.get("attachments")),
        "attachments": [
            _project_graph_attachment(attachment)
            for attachment in (message.get("attachments") or [])
        ],
    }


def _load_attachments(conn, email_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, email_id, filename, content_type, size_bytes, extraction_status,
               extraction_error, content_hash, storage_path, downloaded_at, stored_at,
               extracted_at, created_at, updated_at
        FROM attachments
        WHERE email_id = ?
        ORDER BY filename COLLATE NOCASE, id
        """,
        (email_id,),
    ).fetchall()
    return [_row_to_attachment_dict(row) for row in rows]


def _stable_message_key(email: dict[str, Any]) -> str:
    import hashlib

    stable_id = (
        str(email.get("internet_message_id") or "").strip()
        or str(email.get("id") or "").strip()
        or "|".join(
            [
                str(email.get("conversation_id") or "").strip(),
                str(email.get("received_at") or "").strip(),
                str(email.get("sender") or "").strip(),
                str(email.get("subject") or "").strip(),
            ]
        )
    )
    return hashlib.sha256(stable_id.encode("utf-8")).hexdigest()[:24]


def _load_thread_messages(conn, conversation_id: str, *, limit: int = 200) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM emails
        WHERE conversation_id = ?
        ORDER BY received_at ASC
        LIMIT ?
        """,
        (conversation_id, limit),
    ).fetchall()
    messages: list[dict[str, Any]] = []
    for row in rows:
        email = _row_to_email_dict(row)
        email["attachments"] = _load_attachments(conn, email["id"])
        messages.append(email)
    return messages


def _project_batch_output(
    conn,
    *,
    since: str | None,
    received_after: str | None,
    received_before: str | None,
    limit: int,
    include_read: bool,
) -> dict[str, Any]:
    query = "SELECT * FROM emails WHERE 1=1"
    params: list[Any] = []
    if since:
        query += " AND COALESCE(updated_at, created_at) > ?"
        params.append(since)
    if received_after:
        query += " AND received_at >= ?"
        params.append(received_after)
    if received_before:
        query += " AND received_at < ?"
        params.append(received_before)
    if not include_read:
        query += " AND is_read = 0"
    query += " ORDER BY received_at DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    items: list[dict[str, Any]] = []
    latest_updated_at: str | None = None
    oldest_received_at: str | None = None
    for row in rows:
        email = _row_to_email_dict(row)
        email["attachments"] = _load_attachments(conn, email["id"])
        thread_messages = _load_thread_messages(conn, str(email.get("conversation_id") or ""))
        item = {
            "message_key": _stable_message_key(email),
            "email": email,
            "thread": {
                "conversation_id": email.get("conversation_id"),
                "message_count": len(thread_messages),
                "messages": thread_messages,
            },
        }
        items.append(item)
        updated_at = str(email.get("updated_at") or email.get("created_at") or "").strip() or None
        received_at = str(email.get("received_at") or "").strip() or None
        if updated_at and (latest_updated_at is None or updated_at > latest_updated_at):
            latest_updated_at = updated_at
        if received_at and (oldest_received_at is None or received_at < oldest_received_at):
            oldest_received_at = received_at

    return {
        "count": len(items),
        "latest_updated_at": latest_updated_at,
        "oldest_received_at": oldest_received_at,
        "items": items,
    }


@click.group(cls=JSONGroup, help="Query inbox-assistant state, content, and preferences.")
def app():
    pass


@app.group(cls=JSONGroup, name="prefs", help="Manage /home/agentaech/preferences.json.")
def prefs_app():
    pass


@app.group(cls=JSONGroup, name="categories", help="Manage configurable Outlook categories.")
def categories_app():
    pass


@app.group(cls=JSONGroup, name="email", help="Inspect ingested emails and threads.")
def email_app():
    pass


@app.group(cls=JSONGroup, name="attachment", help="Inspect and retrieve canonical attachments.")
def attachment_app():
    pass


@app.group(cls=JSONGroup, name="draft", help="Create delegated mailbox drafts without sending.")
def draft_app():
    pass


@categories_app.command(cls=JSONCommand, name="show")
def categories_show() -> None:
    """Show configured categories or defaults."""
    prefs = read_preferences()
    categories, configured = get_categories(prefs)
    output_json(
        {
            "configured": configured,
            "categories": categories,
        }
    )


@categories_app.command(cls=JSONCommand, name="init-defaults")
def categories_init_defaults() -> None:
    """Initialize default categories if none are configured."""
    prefs = read_preferences()
    categories, initialized = ensure_categories_initialized(prefs)
    path = write_preferences(prefs) if initialized else None
    output_json(
        {
            "initialized": initialized,
            "path": str(path) if path else None,
            "categories": categories,
        }
    )


@categories_app.command(cls=JSONCommand, name="add")
@click.argument("name")
@click.option("--color", default="blue", help="Category color name")
@click.option("--description", default="", help="Description for when to use this category")
@click.option("--flag-urgency", default=None, help="Optional default flag urgency")
def categories_add(name: str, color: str, description: str, flag_urgency: str | None) -> None:
    """Add a category to the category configuration."""
    prefs = read_preferences()
    try:
        category = add_category(
            prefs,
            name=name,
            color=color,
            description=description,
            flag_urgency=flag_urgency,
        )
    except ValueError as exc:
        output_error(str(exc), "invalid_category")
        raise SystemExit(1)
    path = write_preferences(prefs)
    output_json({"path": str(path), "category": category})


@categories_app.command(cls=JSONCommand, name="remove")
@click.argument("name")
def categories_remove(name: str) -> None:
    """Remove a configured category."""
    prefs = read_preferences()
    try:
        removed = remove_category(prefs, name)
    except ValueError as exc:
        output_error(str(exc), "invalid_category")
        raise SystemExit(1)
    path = write_preferences(prefs)
    output_json({"path": str(path), "removed": removed})


@categories_app.command(cls=JSONCommand, name="update")
@click.argument("name")
@click.option("--new-name", default=None, help="Rename the category")
@click.option("--color", default=None, help="New category color")
@click.option("--description", default=None, help="New description")
@click.option("--flag-urgency", default=None, help="New default flag urgency")
@click.option("--clear-flag-urgency", is_flag=True, help="Remove the default flag urgency")
def categories_update(
    name: str,
    new_name: str | None,
    color: str | None,
    description: str | None,
    flag_urgency: str | None,
    clear_flag_urgency: bool,
) -> None:
    """Update an existing category."""
    prefs = read_preferences()
    try:
        updated = update_category(
            prefs,
            name,
            new_name=new_name,
            color=color,
            description=description,
            flag_urgency=flag_urgency,
            clear_flag_urgency=clear_flag_urgency,
        )
    except ValueError as exc:
        output_error(str(exc), "invalid_category")
        raise SystemExit(1)
    path = write_preferences(prefs)
    output_json({"path": str(path), "category": updated})


@categories_app.command(cls=JSONCommand, name="reset")
def categories_reset() -> None:
    """Reset categories to the default set."""
    prefs = read_preferences()
    categories = reset_categories(prefs)
    path = write_preferences(prefs)
    output_json({"path": str(path), "categories": categories})


@categories_app.command(cls=JSONCommand, name="colors")
def categories_colors() -> None:
    """List valid Outlook category colors."""
    output_json(
        {
            "colors": [
                {"name": color, "preset": preset}
                for color, preset in sorted(COLOR_PRESETS.items())
            ],
            "valid_flag_urgencies": sorted(VALID_FLAG_URGENCIES),
        }
    )


@email_app.command(cls=JSONCommand, name="list")
@click.option("--limit", default=20, help="Number of emails to list")
@click.option("--include-read", is_flag=True, help="Include read emails")
def email_list(limit: int, include_read: bool) -> None:
    """List ingested emails."""
    conn = connect_db(read_only=True)
    query = "SELECT * FROM emails WHERE 1=1"
    params: list[Any] = []
    if not include_read:
        query += " AND is_read = 0"
    query += " ORDER BY received_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    output_json([_row_to_email_dict(row) for row in rows])


@email_app.command(cls=JSONCommand, name="changes")
@click.option("--since", default=None, help="Only emails updated after this ISO timestamp")
@click.option("--limit", default=100, help="Maximum changed emails to return")
@click.option("--include-read", is_flag=True, help="Include read emails")
def email_changes(since: str | None, limit: int, include_read: bool) -> None:
    """List emails changed since a checkpoint."""
    conn = connect_db(read_only=True)
    query = "SELECT * FROM emails WHERE 1=1"
    params: list[Any] = []
    if since:
        query += " AND COALESCE(updated_at, created_at) > ?"
        params.append(since)
    if not include_read:
        query += " AND is_read = 0"
    query += " ORDER BY COALESCE(updated_at, created_at) DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    output_json([_row_to_email_dict(row) for row in rows])


@email_app.command(cls=JSONCommand, name="get")
@click.argument("message_id")
def email_get(message_id: str) -> None:
    """Get a single email plus attachment manifests."""
    conn = connect_db(read_only=True)
    row = conn.execute("SELECT * FROM emails WHERE id = ?", (message_id,)).fetchone()
    if not row:
        conn.close()
        output_error(f"Email not found: {message_id}", "not_found")
        raise SystemExit(1)

    email = _row_to_email_dict(row)
    email["attachments"] = _load_attachments(conn, message_id)
    conn.close()
    output_json(email)


@email_app.command(cls=JSONCommand, name="thread")
@click.argument("conversation_id")
@click.option("--limit", default=200, help="Maximum messages to return")
def email_thread(conversation_id: str, limit: int) -> None:
    """Get all emails in a thread plus attachment manifests."""
    conn = connect_db(read_only=True)
    rows = conn.execute(
        """
        SELECT * FROM emails
        WHERE conversation_id = ?
        ORDER BY received_at ASC
        LIMIT ?
        """,
        (conversation_id, limit),
    ).fetchall()
    if not rows:
        conn.close()
        output_error(f"Thread not found: {conversation_id}", "not_found")
        raise SystemExit(1)

    messages = []
    for row in rows:
        email = _row_to_email_dict(row)
        email["attachments"] = _load_attachments(conn, email["id"])
        messages.append(email)
    conn.close()
    output_json(
        {
            "conversation_id": conversation_id,
            "message_count": len(messages),
            "messages": messages,
        }
    )


@email_app.command(cls=JSONCommand, name="project-batch")
@click.option("--since", default=None, help="Only emails updated after this ISO timestamp")
@click.option("--received-after", default=None, help="Only emails received at or after this ISO timestamp")
@click.option("--received-before", default=None, help="Only emails received before this ISO timestamp")
@click.option("--limit", default=100, help="Maximum email bundles to return")
@click.option("--include-read", is_flag=True, help="Include read emails")
def email_project_batch(
    since: str | None,
    received_after: str | None,
    received_before: str | None,
    limit: int,
    include_read: bool,
) -> None:
    """Return message bundles for manager-side inbox projection."""
    conn = connect_db(read_only=True)
    payload = _project_batch_output(
        conn,
        since=since,
        received_after=received_after,
        received_before=received_before,
        limit=limit,
        include_read=include_read,
    )
    conn.close()
    output_json(payload)


@draft_app.command(cls=JSONCommand, name="create")
@click.option(
    "--to",
    "to_recipients",
    multiple=True,
    help="To recipient email. Repeat or use comma-separated values.",
)
@click.option(
    "--cc",
    "cc_recipients",
    multiple=True,
    help="CC recipient email. Repeat or use comma-separated values.",
)
@click.option(
    "--bcc",
    "bcc_recipients",
    multiple=True,
    help="BCC recipient email. Repeat or use comma-separated values.",
)
@click.option("--subject", default="", help="Draft subject")
@click.option("--body", default=None, help="Draft body content")
@click.option("--body-file", default=None, help="Read draft body from a UTF-8 text file")
@click.option(
    "--content-type",
    type=click.Choice(["text", "html"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Draft body content type",
)
@click.option(
    "--attachment",
    "attachments",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False, path_type=str),
    help="Attach a local file. Repeat to add multiple attachments.",
)
def draft_create(
    to_recipients: tuple[str, ...],
    cc_recipients: tuple[str, ...],
    bcc_recipients: tuple[str, ...],
    subject: str,
    body: str | None,
    body_file: str | None,
    content_type: str,
    attachments: tuple[str, ...],
) -> None:
    """Create a new draft message in the delegated mailbox."""
    try:
        GraphPoller = _load_runtime_attr("poller", "GraphPoller")
    except (AttributeError, ImportError) as exc:
        output_error(f"Failed to import draft runtime: {exc}", "import_error")
        raise SystemExit(1)

    try:
        draft_body = _read_body_input(body, body_file)
        poller = GraphPoller()
        draft = poller.create_draft(
            subject=subject,
            body=draft_body,
            body_content_type=content_type.lower(),
            to_recipients=_normalize_recipient_inputs(to_recipients),
            cc_recipients=_normalize_recipient_inputs(cc_recipients),
            bcc_recipients=_normalize_recipient_inputs(bcc_recipients),
            attachments=list(attachments),
        )
    except ValueError as exc:
        output_error(str(exc), "invalid_input")
        raise SystemExit(1)
    except Exception as exc:
        output_error(f"Draft creation failed: {exc}", "draft_create_error")
        raise SystemExit(1)

    output_json({"created_via": "new", "draft": _project_graph_message(draft)})


@draft_app.command(cls=JSONCommand, name="reply")
@click.argument("message_id")
@click.option("--subject", default=None, help="Optional subject override for the reply draft")
@click.option("--body", default=None, help="Reply text to place above the quoted thread")
@click.option("--body-file", default=None, help="Read reply text from a UTF-8 text file")
@click.option(
    "--content-type",
    type=click.Choice(["text", "html"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Reply body content type",
)
@click.option(
    "--attachment",
    "attachments",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False, path_type=str),
    help="Attach a local file. Repeat to add multiple attachments.",
)
@click.option("--reply-all", is_flag=True, help="Create a reply-all draft instead of reply")
def draft_reply(
    message_id: str,
    subject: str | None,
    body: str | None,
    body_file: str | None,
    content_type: str,
    attachments: tuple[str, ...],
    reply_all: bool,
) -> None:
    """Create a reply or reply-all draft for an existing message."""
    try:
        GraphPoller = _load_runtime_attr("poller", "GraphPoller")
    except (AttributeError, ImportError) as exc:
        output_error(f"Failed to import draft runtime: {exc}", "import_error")
        raise SystemExit(1)

    try:
        reply_body = _read_body_input(body, body_file)
        poller = GraphPoller()
        draft = poller.create_reply_draft(
            message_id,
            subject=subject,
            body=reply_body,
            body_content_type=content_type.lower(),
            attachments=list(attachments),
            reply_all=reply_all,
        )
    except ValueError as exc:
        output_error(str(exc), "invalid_input")
        raise SystemExit(1)
    except Exception as exc:
        output_error(f"Reply draft creation failed: {exc}", "draft_reply_error")
        raise SystemExit(1)

    output_json(
        {
            "created_via": "reply_all" if reply_all else "reply",
            "draft": _project_graph_message(draft),
        }
    )


@attachment_app.command(cls=JSONCommand, name="list")
@click.option("--email-id", default=None, help="Filter to a specific email")
@click.option("--status", "status_filter", default=None, help="Filter by extraction status")
@click.option("--limit", default=50, help="Number of attachments to list")
def attachment_list(email_id: str | None, status_filter: str | None, limit: int) -> None:
    """List attachment manifests."""
    conn = connect_db(read_only=True)
    query = """
        SELECT id, email_id, filename, content_type, size_bytes, extraction_status,
               extraction_error, content_hash, storage_path, downloaded_at, stored_at,
               extracted_at, created_at, updated_at
        FROM attachments
        WHERE 1=1
    """
    params: list[Any] = []
    if email_id:
        query += " AND email_id = ?"
        params.append(email_id)
    if status_filter:
        query += " AND extraction_status = ?"
        params.append(status_filter)
    query += " ORDER BY COALESCE(updated_at, created_at) DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    output_json([_row_to_attachment_dict(row) for row in rows])


@attachment_app.command(cls=JSONCommand, name="meta")
@click.argument("attachment_id")
def attachment_meta(attachment_id: str) -> None:
    """Get attachment manifest metadata."""
    conn = connect_db(read_only=True)
    row = conn.execute(
        """
        SELECT id, email_id, filename, content_type, size_bytes, extraction_status,
               extraction_error, content_hash, storage_path, downloaded_at, stored_at,
               extracted_at, created_at, updated_at
        FROM attachments
        WHERE id = ?
        """,
        (attachment_id,),
    ).fetchone()
    conn.close()
    if not row:
        output_error(f"Attachment not found: {attachment_id}", "not_found")
        raise SystemExit(1)
    output_json(_row_to_attachment_dict(row))


@attachment_app.command(cls=JSONCommand, name="text")
@click.argument("attachment_id")
def attachment_text(attachment_id: str) -> None:
    """Get extracted text for an attachment."""
    conn = connect_db(read_only=True)
    row = conn.execute(
        """
        SELECT id, filename, content_type, extraction_status, extracted_text
        FROM attachments
        WHERE id = ?
        """,
        (attachment_id,),
    ).fetchone()
    conn.close()
    if not row:
        output_error(f"Attachment not found: {attachment_id}", "not_found")
        raise SystemExit(1)
    output_json(dict(row))


@attachment_app.command(cls=JSONCommand, name="fetch")
@click.argument("attachment_id")
@click.option("--output", default=None, help="Optional output path to copy the attachment to")
def attachment_fetch(attachment_id: str, output: str | None) -> None:
    """Resolve or copy a stored attachment blob."""
    conn = connect_db(read_only=True)
    row = conn.execute(
        """
        SELECT id, filename, content_type, size_bytes, extraction_status, storage_path
        FROM attachments
        WHERE id = ?
        """,
        (attachment_id,),
    ).fetchone()
    conn.close()
    if not row:
        output_error(f"Attachment not found: {attachment_id}", "not_found")
        raise SystemExit(1)

    item = _row_to_attachment_dict(row)
    stored_path = item.get("stored_path")
    if not stored_path:
        output_error(
            f"Attachment {attachment_id} has not been stored yet.",
            "not_stored",
        )
        raise SystemExit(1)

    if output:
        destination = Path(output).expanduser().resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(stored_path, destination)
        item["output_path"] = str(destination)

    output_json(item)


@app.command(cls=JSONCommand, name="search")
@click.argument("query")
@click.option("--limit", default=20, help="Number of results to return")
@click.option("--mode", default="hybrid", help="Search mode: hybrid, fts, or vector")
@click.option("--facts/--no-facts", default=True, help="Include facts in search")
def search(query: str, limit: int, mode: str, facts: bool) -> None:
    """Search emails, attachments, and facts using unified search."""
    try:
        unified_search = _load_runtime_attr("search", "unified_search")
    except (AttributeError, ImportError) as exc:
        output_error(f"Failed to import unified search: {exc}", "import_error")
        raise SystemExit(1)

    results = unified_search(
        query=query,
        limit=limit,
        mode=mode,
        include_facts=facts,
        recency_weight=True,
    )

    output = []
    for result in results:
        item = {
            "id": result.id,
            "result_type": result.result_type,
            "source_id": result.source_id,
            "content_preview": result.content_preview,
            "score": result.score,
        }
        if result.email_subject:
            item["email_subject"] = result.email_subject
        if result.email_sender:
            item["email_sender"] = result.email_sender
        if result.email_date:
            item["email_date"] = result.email_date
        if result.conversation_id:
            item["conversation_id"] = result.conversation_id
        if result.filename:
            item["filename"] = result.filename
        if result.fact_type:
            item["fact_type"] = result.fact_type
        if result.fact_value:
            item["fact_value"] = result.fact_value
        if result.web_link:
            item["web_link"] = result.web_link
        output.append(item)
    output_json(output)


@app.command(cls=JSONCommand, name="ask")
@click.argument("query")
@click.option("--max-results", default=5, help="Maximum matched emails to return")
def ask_query(query: str, max_results: int) -> None:
    """Ask inbox-assistant in natural language with grounded retrieval."""
    user_email = os.environ.get("DELEGATED_USER", "").strip().lower()
    if not user_email:
        output_error("DELEGATED_USER environment variable must be set", "missing_user")
        raise SystemExit(1)

    try:
        run_query_agent = _load_runtime_attr("query_agent", "run_query_agent")
    except (AttributeError, ImportError) as exc:
        output_error(f"Failed to import query agent: {exc}", "import_error")
        raise SystemExit(1)

    try:
        result = asyncio.run(
            run_query_agent(
                user_email=user_email,
                user_prompt=query,
                max_results=max_results,
            )
        )
    except Exception as exc:
        output_error(f"Query agent failed: {exc}", "query_agent_error")
        raise SystemExit(1)

    output_json(result)


@app.command(cls=JSONCommand)
def dbpath() -> None:
    """Get the absolute path to the inbox database."""
    output_json({"path": str(get_db_path())})


@app.command(cls=JSONCommand, name="sync-status")
def sync_status() -> None:
    """Show per-folder sync checkpoints."""
    conn = connect_db(read_only=True)
    rows = conn.execute(
        """
        SELECT folder_id, last_sync_at, sync_type, messages_synced,
               CASE WHEN delta_link IS NOT NULL THEN 1 ELSE 0 END AS has_delta_link
        FROM sync_state
        ORDER BY last_sync_at DESC
        """
    ).fetchall()
    conn.close()
    output_json([dict(row) for row in rows])


@app.command(cls=JSONCommand)
def stats() -> None:
    """Show mailbox corpus statistics."""
    conn = connect_db(read_only=True)
    cursor = conn.cursor()

    data: dict[str, Any] = {}
    cursor.execute("SELECT COUNT(*) FROM emails")
    data["total_emails"] = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM emails WHERE body_markdown IS NOT NULL")
    data["emails_with_body"] = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM emails WHERE has_attachments = 1")
    data["emails_with_attachments"] = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM attachments")
    data["total_attachments"] = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM attachments WHERE storage_path IS NOT NULL")
    data["attachments_stored"] = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM attachments WHERE extraction_status = 'completed'")
    data["attachments_extracted"] = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM chunks")
    data["total_chunks"] = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM sync_state")
    data["folders_synced"] = cursor.fetchone()[0]
    conn.close()
    output_json(data)


@prefs_app.command(cls=JSONCommand, name="show")
def prefs_show() -> None:
    """Show preferences.json."""
    output_json(read_preferences())


@prefs_app.command(cls=JSONCommand, name="set")
@click.argument("key")
@click.argument("value")
def prefs_set(key: str, value: str) -> None:
    """Set a top-level preference key."""
    try:
        path = set_preference_from_string(key, value)
    except InvalidPreferenceKeyError as exc:
        output_error(str(exc), "invalid_preference_key")
        raise SystemExit(1)
    output_json({"path": str(path), "key": key})


@prefs_app.command(cls=JSONCommand, name="keys")
def prefs_keys() -> None:
    """List valid top-level preference keys."""
    output_json({"keys": sorted(VALID_PREFERENCE_KEYS)})


@prefs_app.command(cls=JSONCommand, name="unset")
@click.argument("key")
def prefs_unset(key: str) -> None:
    """Remove a preference key."""
    prefs = read_preferences()
    prefs.pop(key, None)
    path = write_preferences(prefs)
    output_json({"path": str(path), "key": key, "removed": True})


def run() -> None:
    app()


if __name__ == "__main__":
    run()
