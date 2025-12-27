#!/usr/bin/env python3
"""
Working Memory Backfill Script

Processes existing emails through the working memory system to build
EA awareness from historical data. For dev/testing/evals only.

This script works on a COPY of the database to allow:
- Safe experimentation without affecting live data
- Checkpointing at any point
- Restoring to previous states
- A/B testing of different WM strategies

Usage:
    # Fresh sync: Create new DB and fetch ALL emails from M365 (recommended)
    DELEGATED_USER=steven@aech.ai python scripts/wm_backfill.py --fresh-sync

    # Metadata sync: Fast update of webLink, conversationId, folder (no body fetch)
    DELEGATED_USER=steven@aech.ai python scripts/wm_backfill.py --metadata-sync

    # Setup: Copy live DB to eval environment (legacy - missing conversation_id)
    DELEGATED_USER=steven@aech.ai python scripts/wm_backfill.py --setup

    # Dry run - see what would be processed
    DELEGATED_USER=steven@aech.ai python scripts/wm_backfill.py --dry-run

    # Process first 50 emails through WM (LLM calls)
    DELEGATED_USER=steven@aech.ai python scripts/wm_backfill.py --limit 50

    # Checkpoint current state
    DELEGATED_USER=steven@aech.ai python scripts/wm_backfill.py --checkpoint "after-50-emails"

    # List checkpoints
    DELEGATED_USER=steven@aech.ai python scripts/wm_backfill.py --list-checkpoints

    # Restore to checkpoint
    DELEGATED_USER=steven@aech.ai python scripts/wm_backfill.py --restore "after-50-emails"

    # Clear WM and start fresh (keeps emails)
    DELEGATED_USER=steven@aech.ai python scripts/wm_backfill.py --clear

    # Full backfill
    DELEGATED_USER=steven@aech.ai python scripts/wm_backfill.py
"""

import argparse
import asyncio
import logging
import os
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_eval_user_dir(user_email: str) -> Path:
    """Get eval user directory (this is what AECH_USER_DIR should point to)."""
    return Path(__file__).parent.parent / "data" / "wm-eval" / user_email


def get_eval_db(user_email: str) -> Path:
    """Get path to eval database."""
    return get_eval_user_dir(user_email) / ".inbox-assistant" / "inbox.sqlite"


def get_checkpoints_dir(user_email: str) -> Path:
    """Get checkpoints directory."""
    return get_eval_user_dir(user_email) / ".inbox-assistant" / "checkpoints"


def get_live_db_path(user_email: str) -> Path:
    """Get path to the live database."""
    live_data = Path(__file__).parent.parent.parent / "aech-main" / "data" / "users" / user_email / ".inbox-assistant"
    return live_data / "inbox.sqlite"


def setup_eval_env(user_email: str):
    """Copy live database to eval environment."""
    live_db = get_live_db_path(user_email)
    eval_db = get_eval_db(user_email)
    checkpoints_dir = get_checkpoints_dir(user_email)

    if not live_db.exists():
        logger.error(f"Live database not found: {live_db}")
        sys.exit(1)

    eval_db.parent.mkdir(parents=True, exist_ok=True)
    checkpoints_dir.mkdir(parents=True, exist_ok=True)

    # Copy database
    logger.info(f"Copying live database to eval environment...")
    logger.info(f"  From: {live_db}")
    logger.info(f"  To:   {eval_db}")
    shutil.copy2(live_db, eval_db)

    # Initialize WM tables
    os.environ["AECH_USER_DIR"] = str(get_eval_user_dir(user_email))
    from src.database import init_db
    init_db()

    # Create initial checkpoint
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    checkpoint_name = f"initial_{timestamp}"
    checkpoint_path = checkpoints_dir / f"{checkpoint_name}.sqlite"
    shutil.copy2(eval_db, checkpoint_path)
    logger.info(f"Created initial checkpoint: {checkpoint_name}")

    # Get stats
    conn = sqlite3.connect(eval_db)
    conn.row_factory = sqlite3.Row
    email_count = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
    conn.close()

    logger.info(f"\nEval environment ready:")
    logger.info(f"  - {email_count} emails")


def fresh_sync_eval_env(user_email: str):
    """Create fresh eval DB and sync all emails from M365 Graph API."""
    eval_db = get_eval_db(user_email)
    checkpoints_dir = get_checkpoints_dir(user_email)

    # Remove old eval DB if exists
    if eval_db.exists():
        logger.info(f"Removing existing eval database: {eval_db}")
        eval_db.unlink()

    eval_db.parent.mkdir(parents=True, exist_ok=True)
    checkpoints_dir.mkdir(parents=True, exist_ok=True)

    # Set env to use eval DB
    os.environ["AECH_USER_DIR"] = str(get_eval_user_dir(user_email))

    # Initialize fresh database with schema
    logger.info("Initializing fresh database...")
    from src.database import init_db, get_db_path
    init_db()
    logger.info(f"Database path resolved to: {get_db_path()}")

    # Now sync from M365
    logger.info("Syncing emails from Microsoft 365...")
    from src.poller import GraphPoller

    # aech_cli_msgraph sets INBOX_DB_PATH which overrides AECH_USER_DIR - fix it
    if "INBOX_DB_PATH" in os.environ:
        del os.environ["INBOX_DB_PATH"]
    logger.info(f"Database path after poller import: {get_db_path()}")

    poller = GraphPoller()

    # Get all folders
    folders = poller._graph_client.get_mail_folders(user_id=user_email).get("value", [])
    logger.info(f"Found {len(folders)} folders")

    total_synced = 0
    for folder in folders:
        folder_id = folder.get("id")
        folder_name = folder.get("displayName", "Unknown")
        msg_count = folder.get("totalItemCount", 0)

        if msg_count == 0:
            logger.info(f"  Skipping {folder_name} (empty)")
            continue

        logger.info(f"  Syncing {folder_name} ({msg_count} messages)...")
        synced = poller.full_sync_folder(folder_id, folder_name, fetch_body=True)
        total_synced += synced

    # Create initial checkpoint
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    checkpoint_name = f"fresh_sync_{timestamp}"
    checkpoint_path = checkpoints_dir / f"{checkpoint_name}.sqlite"
    shutil.copy2(eval_db, checkpoint_path)
    logger.info(f"Created checkpoint: {checkpoint_name}")

    # Verify conversation_id coverage
    conn = sqlite3.connect(eval_db)
    conn.row_factory = sqlite3.Row
    total = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
    with_conv = conn.execute("SELECT COUNT(*) FROM emails WHERE conversation_id IS NOT NULL").fetchone()[0]
    conn.close()

    logger.info(f"\nFresh sync complete:")
    logger.info(f"  - {total} emails synced")
    logger.info(f"  - {with_conv} with conversation_id ({100*with_conv//total if total else 0}%)")
    logger.info(f"  - Database: {eval_db}")
    logger.info(f"  - Checkpoints: {checkpoints_dir}")


def metadata_sync_eval_env(user_email: str):
    """
    Sync metadata only (no body content) for existing emails.

    This is a fast sync that updates:
    - webLink (folder-agnostic deep links)
    - conversationId
    - folder changes
    - read status

    Does NOT fetch message bodies, making it ~10x faster.
    """
    eval_db = get_eval_db(user_email)

    if not eval_db.exists():
        logger.error("Eval database not found. Run --fresh-sync first.")
        sys.exit(1)

    # Set env to use eval DB
    os.environ["AECH_USER_DIR"] = str(get_eval_user_dir(user_email))

    # Run migrations
    from src.database import init_db, get_db_path
    init_db()
    logger.info(f"Database: {get_db_path()}")

    # Import poller
    from src.poller import GraphPoller

    # aech_cli_msgraph sets INBOX_DB_PATH which overrides AECH_USER_DIR - fix it
    if "INBOX_DB_PATH" in os.environ:
        del os.environ["INBOX_DB_PATH"]

    poller = GraphPoller()

    # Get all folders
    folders = poller._graph_client.get_mail_folders(user_id=user_email).get("value", [])
    logger.info(f"Found {len(folders)} folders")

    total_synced = 0
    for folder in folders:
        folder_id = folder.get("id")
        folder_name = folder.get("displayName", "Unknown")
        msg_count = folder.get("totalItemCount", 0)

        if msg_count == 0:
            logger.info(f"  Skipping {folder_name} (empty)")
            continue

        logger.info(f"  Syncing {folder_name} ({msg_count} messages)...")
        # fetch_body=False makes this ~10x faster
        synced = poller.full_sync_folder(folder_id, folder_name, fetch_body=False)
        total_synced += synced

    # Verify webLink coverage
    conn = sqlite3.connect(eval_db)
    conn.row_factory = sqlite3.Row
    total = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
    with_weblink = conn.execute("SELECT COUNT(*) FROM emails WHERE web_link IS NOT NULL").fetchone()[0]
    conn.close()

    logger.info(f"\nMetadata sync complete:")
    logger.info(f"  - {total} emails updated")
    logger.info(f"  - {with_weblink} with web_link ({100*with_weblink//total if total else 0}%)")


def create_checkpoint(user_email: str, name: str):
    """Save current state as a checkpoint."""
    eval_db = get_eval_db(user_email)
    checkpoints_dir = get_checkpoints_dir(user_email)

    if not eval_db.exists():
        logger.error("Eval database not found. Run --setup first.")
        sys.exit(1)

    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = name.replace(" ", "_").replace("/", "-")
    checkpoint_path = checkpoints_dir / f"{safe_name}_{timestamp}.sqlite"
    shutil.copy2(eval_db, checkpoint_path)
    logger.info(f"Checkpoint saved: {checkpoint_path.name}")


def list_checkpoints(user_email: str):
    """List available checkpoints."""
    checkpoints_dir = get_checkpoints_dir(user_email)

    if not checkpoints_dir.exists():
        logger.info("No checkpoints directory found.")
        return

    checkpoints = sorted(checkpoints_dir.glob("*.sqlite"))
    if not checkpoints:
        logger.info("No checkpoints found.")
        return

    logger.info("Available checkpoints:")
    for cp in checkpoints:
        size_mb = cp.stat().st_size / (1024 * 1024)
        logger.info(f"  - {cp.stem} ({size_mb:.1f} MB)")


def restore_checkpoint(user_email: str, name: str):
    """Restore eval database from checkpoint."""
    eval_db = get_eval_db(user_email)
    checkpoints_dir = get_checkpoints_dir(user_email)

    matches = list(checkpoints_dir.glob(f"{name}*.sqlite"))
    if not matches:
        logger.error(f"No checkpoint matching '{name}' found.")
        list_checkpoints(user_email)
        sys.exit(1)

    if len(matches) > 1:
        logger.error(f"Multiple checkpoints match '{name}':")
        for m in matches:
            logger.error(f"  - {m.stem}")
        sys.exit(1)

    checkpoint_path = matches[0]
    shutil.copy2(checkpoint_path, eval_db)
    logger.info(f"Restored from: {checkpoint_path.name}")


def clear_working_memory(user_email: str):
    """Clear all working memory tables."""
    eval_db = get_eval_db(user_email)
    conn = sqlite3.connect(eval_db)
    tables = [
        "wm_threads", "wm_contacts", "wm_decisions",
        "wm_commitments", "wm_observations", "wm_projects"
    ]
    for table in tables:
        try:
            conn.execute(f"DELETE FROM {table}")
            logger.info(f"Cleared {table}")
        except sqlite3.OperationalError:
            logger.warning(f"Table {table} doesn't exist yet")
    conn.commit()
    conn.close()


def get_emails(user_email: str, limit: int | None, skip_processed: bool, oldest_first: bool):
    """Fetch emails from database."""
    eval_db = get_eval_db(user_email)
    conn = sqlite3.connect(eval_db)
    conn.row_factory = sqlite3.Row

    order = "ASC" if oldest_first else "DESC"
    query = "SELECT * FROM emails"

    if skip_processed:
        query += """
        WHERE NOT EXISTS (
            SELECT 1 FROM wm_threads wt
            WHERE wt.conversation_id = emails.conversation_id
        )
        """

    query += f" ORDER BY received_at {order}"

    if limit:
        query += f" LIMIT {limit}"

    emails = conn.execute(query).fetchall()
    conn.close()
    return emails


def get_stats(user_email: str):
    """Get current working memory stats."""
    eval_db = get_eval_db(user_email)
    conn = sqlite3.connect(eval_db)
    conn.row_factory = sqlite3.Row

    stats = {}
    tables = [
        ("threads", "wm_threads", None),
        ("contacts", "wm_contacts", None),
        ("pending_decisions", "wm_decisions", "WHERE is_resolved=0"),
        ("open_commitments", "wm_commitments", "WHERE is_completed=0"),
        ("observations", "wm_observations", None),
        ("projects", "wm_projects", None),
    ]

    for name, table, where in tables:
        try:
            q = f"SELECT COUNT(*) FROM {table}"
            if where:
                q += f" {where}"
            stats[name] = conn.execute(q).fetchone()[0]
        except sqlite3.OperationalError:
            stats[name] = 0

    conn.close()
    return stats


async def process_emails(user_email: str, emails: list, batch_size: int = 10, concurrency: int = 1):
    """Process emails through working memory with optional parallelism."""
    # Set env to use eval DB
    os.environ["AECH_USER_DIR"] = str(get_eval_user_dir(user_email))

    from src.working_memory.updater import WorkingMemoryUpdater

    updater = WorkingMemoryUpdater(user_email)
    total = len(emails)

    # Thread-safe counters
    import threading
    lock = threading.Lock()
    counters = {"processed": 0, "errors": 0, "completed": 0}

    semaphore = asyncio.Semaphore(concurrency)

    async def process_one(idx: int, email):
        async with semaphore:
            email_dict = dict(email)
            try:
                category_decision = {
                    "category": email_dict.get("category") or "unknown",
                    "requires_reply": False,
                    "labels": [],
                }

                await updater.process_email(email_dict, category_decision)

                with lock:
                    counters["processed"] += 1
                    counters["completed"] += 1
                    completed = counters["completed"]

                    # Log progress periodically
                    if completed % batch_size == 0 or completed == total:
                        pct = int(completed / total * 100)
                        subj = (email_dict.get("subject") or "")[:40]
                        logger.info(f"[{pct:3d}%] {completed}/{total} - {subj}")

            except Exception as e:
                with lock:
                    counters["errors"] += 1
                    counters["completed"] += 1
                subj = (email_dict.get("subject") or "")[:30]
                logger.error(f"Error processing '{subj}': {e}")

    if concurrency > 1:
        logger.info(f"Processing with concurrency={concurrency}")

    # Process all emails in parallel (limited by semaphore)
    tasks = [process_one(i, email) for i, email in enumerate(emails)]
    await asyncio.gather(*tasks)

    return counters["processed"], counters["errors"]


def main():
    parser = argparse.ArgumentParser(
        description="Backfill working memory from existing emails (eval environment)"
    )
    parser.add_argument("--setup", action="store_true", help="Setup eval environment from live DB")
    parser.add_argument("--fresh-sync", action="store_true", help="Create fresh DB and sync all emails from M365")
    parser.add_argument("--metadata-sync", action="store_true", help="Fast sync: update metadata (webLink, etc) without fetching bodies")
    parser.add_argument("--checkpoint", type=str, metavar="NAME", help="Create checkpoint with given name")
    parser.add_argument("--list-checkpoints", action="store_true", help="List available checkpoints")
    parser.add_argument("--restore", type=str, metavar="NAME", help="Restore from checkpoint")
    parser.add_argument("--clear", action="store_true", help="Clear working memory tables")
    parser.add_argument("--limit", type=int, default=None, help="Max emails to process")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed")
    parser.add_argument("--no-skip-processed", action="store_true", help="Process all emails")
    parser.add_argument("--newest-first", action="store_true", help="Process newest first")
    parser.add_argument("--batch-size", type=int, default=10, help="Progress update frequency")
    parser.add_argument("--concurrency", type=int, default=1, help="Number of parallel LLM calls (default: 1, recommended: 10)")
    args = parser.parse_args()

    user_email = os.environ.get("DELEGATED_USER")
    if not user_email:
        logger.error("DELEGATED_USER environment variable must be set")
        sys.exit(1)

    eval_db = get_eval_db(user_email)

    # Handle admin commands
    if args.setup:
        setup_eval_env(user_email)
        return

    if args.fresh_sync:
        fresh_sync_eval_env(user_email)
        return

    if args.metadata_sync:
        metadata_sync_eval_env(user_email)
        return

    if args.list_checkpoints:
        list_checkpoints(user_email)
        return

    if args.checkpoint:
        create_checkpoint(user_email, args.checkpoint)
        return

    if args.restore:
        restore_checkpoint(user_email, args.restore)
        return

    # Check eval DB exists
    if not eval_db.exists():
        logger.error("Eval database not found. Run with --setup first.")
        sys.exit(1)

    # Set env to use eval DB
    os.environ["AECH_USER_DIR"] = str(get_eval_user_dir(user_email))

    logger.info(f"User: {user_email}")
    logger.info(f"Using eval DB: {eval_db}")

    # Show current stats
    stats = get_stats(user_email)
    logger.info(f"Current working memory: {stats}")

    if args.clear:
        logger.info("Clearing working memory...")
        clear_working_memory(user_email)
        stats = get_stats(user_email)
        logger.info(f"After clear: {stats}")
        return

    # Get emails
    emails = get_emails(
        user_email,
        limit=args.limit,
        skip_processed=not args.no_skip_processed,
        oldest_first=not args.newest_first
    )

    if not emails:
        logger.info("No emails to process.")
        return

    logger.info(f"Found {len(emails)} emails to process")

    if emails:
        oldest_date = emails[0]["received_at"]
        newest_date = emails[-1]["received_at"]
        oldest = oldest_date[:10] if oldest_date else "?"
        newest = newest_date[:10] if newest_date else "?"
        if not args.newest_first:
            logger.info(f"Date range: {oldest} to {newest}")
        else:
            logger.info(f"Date range: {newest} to {oldest}")

    if args.dry_run:
        logger.info("\n[DRY RUN] Would process:")
        for i, email in enumerate(emails[:20]):
            recv = email["received_at"]
            date = recv[:10] if recv else "?"
            subj = (email["subject"] or "")[:50]
            sender = email["sender"] or "?"
            category = email["category"] or "?"
            logger.info(f"  {i+1}. [{date}] [{category}] {subj}")
            logger.info(f"      From: {sender}")
        if len(emails) > 20:
            logger.info(f"  ... and {len(emails) - 20} more")
        return

    logger.info(f"\nProcessing emails...")
    logger.info("-" * 60)

    processed, errors = asyncio.run(
        process_emails(user_email, emails, args.batch_size, args.concurrency)
    )

    logger.info("-" * 60)
    logger.info(f"Done! Processed: {processed}, Errors: {errors}")

    final_stats = get_stats(user_email)
    logger.info(f"\nWorking Memory now contains:")
    for key, val in final_stats.items():
        logger.info(f"  - {val} {key}")


if __name__ == "__main__":
    main()
