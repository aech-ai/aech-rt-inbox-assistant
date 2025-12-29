#!/usr/bin/env python3
"""
Full Email Processing Pipeline - Run everything from scratch.

Usage:
    DELEGATED_USER=user@example.com python scripts/full_pipeline.py

Or in docker-compose:
    docker compose run --rm inbox-assistant python scripts/full_pipeline.py
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pipeline")


def check_env():
    """Verify required environment variables."""
    user = os.getenv("DELEGATED_USER")
    if not user:
        logger.error("DELEGATED_USER environment variable required")
        sys.exit(1)
    logger.info(f"Running pipeline for: {user}")
    return user


def step_init_db():
    """Step 1: Initialize database schema."""
    logger.info("=" * 60)
    logger.info("STEP 1: Initialize Database")
    logger.info("=" * 60)

    from src.database import init_db, get_db_path

    db_path = get_db_path()
    logger.info(f"Database path: {db_path}")

    # Delete existing DB for fresh start
    if db_path.exists():
        logger.info("Removing existing database for fresh start...")
        db_path.unlink()

    init_db()
    logger.info("Database initialized with fresh schema")


def step_sync_emails():
    """Step 2: Sync all emails from Graph API."""
    logger.info("=" * 60)
    logger.info("STEP 2: Sync Emails from Graph API")
    logger.info("=" * 60)

    from src.poller import GraphPoller

    poller = GraphPoller()

    # Get all folders
    folders = poller.get_all_folders()
    logger.info(f"Found {len(folders)} mail folders")

    # Sync each folder
    total_messages = 0
    for folder in folders:
        folder_id = folder.get("id")
        folder_name = folder.get("displayName", "Unknown")

        if not folder_id:
            continue

        # Skip certain folders
        skip_folders = ["Deleted Items", "Junk Email", "Drafts", "Outbox"]
        if folder_name in skip_folders:
            logger.info(f"  Skipping {folder_name}")
            continue

        count = poller.full_sync_folder(folder_id, folder_name, fetch_body=True)
        total_messages += count
        logger.info(f"  {folder_name}: {count} messages")

    logger.info(f"Total emails synced: {total_messages}")
    return total_messages


def step_convert_bodies():
    """Step 3: Convert HTML bodies to markdown (backfill if needed)."""
    logger.info("=" * 60)
    logger.info("STEP 3: Convert Bodies to Markdown")
    logger.info("=" * 60)

    from src.database import get_connection
    from src.body_parser import parse_email_body

    conn = get_connection()
    cursor = conn.cursor()

    # Find emails needing conversion (shouldn't be many since poller does it now)
    cursor.execute("""
        SELECT id, body_html
        FROM emails
        WHERE body_html IS NOT NULL AND body_markdown IS NULL
    """)
    rows = cursor.fetchall()

    if not rows:
        logger.info("All emails already have body_markdown")
        conn.close()
        return 0

    logger.info(f"Converting {len(rows)} emails...")

    converted = 0
    for row in rows:
        try:
            parsed = parse_email_body(row["body_html"])
            cursor.execute(
                "UPDATE emails SET body_markdown = ?, signature_block = ? WHERE id = ?",
                (parsed.main_content, parsed.signature_block, row["id"]),
            )
            converted += 1
        except Exception as e:
            logger.error(f"Error converting {row['id']}: {e}")

    conn.commit()
    conn.close()

    logger.info(f"Converted {converted} emails")
    return converted


async def step_extract_content():
    """Step 4: Run LLM extraction for thread summaries."""
    logger.info("=" * 60)
    logger.info("STEP 4: LLM Content Extraction (thread summaries)")
    logger.info("=" * 60)

    from src.database import get_connection
    from src.working_memory.updater import WorkingMemoryUpdater

    user_email = os.getenv("DELEGATED_USER")
    updater = WorkingMemoryUpdater(user_email)

    conn = get_connection()

    # Find emails needing extraction (no thread_summary yet)
    rows = conn.execute("""
        SELECT id, conversation_id, subject, sender, received_at,
               body_markdown, body_preview, to_emails, cc_emails
        FROM emails
        WHERE thread_summary IS NULL
          AND body_markdown IS NOT NULL
        ORDER BY received_at ASC
        LIMIT 500
    """).fetchall()

    conn.close()

    if not rows:
        logger.info("All emails already have thread summaries")
        return 0

    logger.info(f"Processing {len(rows)} emails for LLM extraction...")

    processed = 0
    for row in rows:
        try:
            email = dict(row)
            await updater.process_email(email)
            processed += 1

            if processed % 10 == 0:
                logger.info(f"  Processed {processed}/{len(rows)} emails")
        except Exception as e:
            logger.error(f"Error processing {row['id']}: {e}")

    logger.info(f"LLM extraction complete: {processed} emails")
    return processed


def step_create_chunks():
    """Step 5: Create search chunks for emails."""
    logger.info("=" * 60)
    logger.info("STEP 5: Create Search Chunks")
    logger.info("=" * 60)

    from src.chunker import process_unindexed_emails, process_unindexed_attachments

    # Process emails
    email_results = process_unindexed_emails(limit=10000)
    logger.info(
        f"Email chunks: {email_results['processed']} emails, "
        f"{email_results['chunks_created']} chunks"
    )

    # Process attachments
    att_results = process_unindexed_attachments(limit=10000)
    logger.info(
        f"Attachment chunks: {att_results['processed']} attachments, "
        f"{att_results['chunks_created']} chunks"
    )

    return email_results["chunks_created"] + att_results["chunks_created"]


def step_generate_embeddings():
    """Step 6: Generate embeddings for chunks."""
    logger.info("=" * 60)
    logger.info("STEP 6: Generate Embeddings")
    logger.info("=" * 60)

    from src.embeddings import embed_pending_chunks

    results = embed_pending_chunks(limit=10000, batch_size=32)
    logger.info(f"Embeddings: {results['embedded']} chunks embedded")
    return results["embedded"]


def step_summary():
    """Print final summary."""
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE - Summary")
    logger.info("=" * 60)

    from src.database import get_connection

    conn = get_connection()

    # Count stats
    stats = {}
    stats["emails"] = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
    stats["with_markdown"] = conn.execute(
        "SELECT COUNT(*) FROM emails WHERE body_markdown IS NOT NULL"
    ).fetchone()[0]
    stats["with_summary"] = conn.execute(
        "SELECT COUNT(*) FROM emails WHERE thread_summary IS NOT NULL"
    ).fetchone()[0]
    stats["chunks"] = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    stats["with_embeddings"] = conn.execute(
        "SELECT COUNT(*) FROM chunks WHERE embedding IS NOT NULL"
    ).fetchone()[0]
    stats["threads"] = conn.execute("SELECT COUNT(*) FROM wm_threads").fetchone()[0]
    stats["contacts"] = conn.execute("SELECT COUNT(*) FROM wm_contacts").fetchone()[0]

    conn.close()

    logger.info(f"  Emails:           {stats['emails']}")
    logger.info(f"  With body_markdown: {stats['with_markdown']}")
    logger.info(f"  With thread_summary: {stats['with_summary']}")
    logger.info(f"  Search chunks:    {stats['chunks']}")
    logger.info(f"  With embeddings:  {stats['with_embeddings']}")
    logger.info(f"  Working memory threads: {stats['threads']}")
    logger.info(f"  Known contacts:   {stats['contacts']}")

    return stats


async def main():
    """Run the full pipeline."""
    logger.info("=" * 60)
    logger.info("FULL EMAIL PROCESSING PIPELINE")
    logger.info("=" * 60)

    user = check_env()

    # Run all steps
    step_init_db()
    step_sync_emails()
    step_convert_bodies()
    await step_extract_content()
    step_create_chunks()

    # Embeddings are optional - check if module exists
    try:
        step_generate_embeddings()
    except ImportError:
        logger.warning("Embeddings module not available, skipping")
    except Exception as e:
        logger.warning(f"Embeddings failed: {e}")

    step_summary()

    logger.info("=" * 60)
    logger.info("DONE")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
