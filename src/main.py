import argparse
import asyncio
import logging
import os
import time

from src.database import init_db, get_connection
from src.poller import GraphPoller
from src.organizer import Organizer
from src.working_memory.engine import run_memory_engine_cycle
from src.working_memory.updater import WorkingMemoryUpdater
from src.attachments import AttachmentProcessor
from src.chunker import process_unindexed_emails, process_unindexed_attachments
from src.embeddings import embed_pending_chunks

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def process_pending_content():
    """
    Process pending content for search indexing.
    Runs after email classification to keep search corpus up-to-date.
    """
    try:
        poller = GraphPoller()

        # 1. Fetch full bodies for emails missing them
        conn = get_connection()
        emails_needing_body = conn.execute("""
            SELECT id FROM emails
            WHERE (body_text IS NULL OR body_text = '')
            LIMIT 20
        """).fetchall()
        conn.close()

        if emails_needing_body:
            fetched = 0
            for row in emails_needing_body:
                email_id = row["id"]
                body_text, body_html = poller._get_message_body(email_id)
                if body_text:
                    conn = get_connection()
                    conn.execute(
                        "UPDATE emails SET body_text = ?, body_html = ? WHERE id = ?",
                        (body_text, body_html, email_id)
                    )
                    conn.commit()
                    conn.close()
                    fetched += 1
            if fetched > 0:
                logger.info(f"Fetched {fetched} email bodies")

        # 2. Extract attachments (download + OCR/text extraction)
        processor = AttachmentProcessor()
        att_results = await processor.process_pending_attachments_async(limit=50, concurrency=5)
        if att_results["completed"] > 0:
            logger.info(f"Extracted {att_results['completed']} attachments")

        # 3. LLM content extraction (quote/signature removal)
        conn = get_connection()
        pending_emails = conn.execute("""
            SELECT id, conversation_id, subject, sender, received_at,
                   body_text, body_preview, to_emails, cc_emails
            FROM emails
            WHERE extracted_body IS NULL
              AND (body_text IS NOT NULL OR body_preview IS NOT NULL)
            LIMIT 50
        """).fetchall()
        conn.close()

        if pending_emails:
            updater = WorkingMemoryUpdater()
            extracted = 0
            for row in pending_emails:
                email = dict(row)
                try:
                    await updater.process_email(email)
                    extracted += 1
                except Exception as e:
                    logger.warning(f"Content extraction failed for {email['id']}: {e}")
            if extracted > 0:
                logger.info(f"Extracted content from {extracted} emails")

        # 4. Index for search (chunking)
        email_results = process_unindexed_emails(limit=100)
        att_chunk_results = process_unindexed_attachments(limit=100)
        total_chunks = email_results.get("chunks_created", 0) + att_chunk_results.get("chunks_created", 0)
        if total_chunks > 0:
            logger.info(f"Created {total_chunks} search chunks")

        # 5. Generate embeddings
        embed_results = embed_pending_chunks(limit=100, batch_size=32)
        if embed_results["processed"] > 0:
            logger.info(f"Generated {embed_results['processed']} embeddings")

    except Exception as e:
        logger.warning(f"Content processing error (non-fatal): {e}")


def service_loop(user_email: str, poll_interval: int, run_once: bool, concurrency: int = 5, backfill: bool = False):
    logger.info("Initializing database...")
    init_db()

    logger.info("Initializing poller...")
    poller = GraphPoller()

    organizer = Organizer(poller, backfill=backfill)

    # Working memory engine configuration
    wm_engine_interval = int(os.environ.get("WM_ENGINE_INTERVAL", 300))  # Default 5 minutes
    last_wm_engine_run = 0.0

    logger.info("Starting Inbox Assistant Service")
    logger.info(f"User: {user_email}")
    logger.info(f"Poll Interval: {poll_interval}s")
    logger.info(f"Concurrency: {concurrency}")
    logger.info(f"Working Memory Engine Interval: {wm_engine_interval}s")
    if backfill:
        logger.info("Backfill mode: triggers suppressed (no Teams notifications)")

    while True:
        try:
            poller.poll_inbox()
            asyncio.run(organizer.organize_emails(concurrency=concurrency))

            # Process pending content for search (attachments, extraction, indexing, embeddings)
            asyncio.run(process_pending_content())

            # Run working memory engine periodically
            now = time.time()
            if now - last_wm_engine_run >= wm_engine_interval:
                try:
                    asyncio.run(run_memory_engine_cycle(user_email))
                    last_wm_engine_run = now
                except Exception as wm_err:
                    logger.warning(f"Working memory engine error: {wm_err}")
        except Exception as e:
            logger.error(f"Error in main loop: {e}")

        if run_once:
            break

        logger.debug(f"Sleeping for {poll_interval} seconds")
        time.sleep(poll_interval)


def run(argv=None):
    user_email = os.environ.get("DELEGATED_USER")
    if not user_email:
        raise ValueError("DELEGATED_USER environment variable must be set")

    parser = argparse.ArgumentParser(description="Aech Inbox Assistant service runner")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single poll/organize cycle and exit.",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=None,
        help="Override poll interval seconds (defaults to POLL_INTERVAL env or 5).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of emails to process in parallel (default: 5).",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Backfill mode: suppress triggers (no Teams notifications). Use for onboarding new accounts.",
    )
    args = parser.parse_args(argv)

    poll_interval = args.poll_interval or int(os.environ.get("POLL_INTERVAL", 5))
    service_loop(
        user_email,
        poll_interval,
        run_once=args.once,
        concurrency=args.concurrency,
        backfill=args.backfill,
    )


if __name__ == "__main__":
    run()
