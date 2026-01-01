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
from src.calendar_sync import sync_calendar, needs_sync
from src.action_executor import poll_and_execute_actions, has_pending_actions

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

        # 1. Fetch full bodies for emails we haven't tried to fetch yet
        # We check body_html IS NULL to avoid re-fetching calendar accepts/auto-replies
        # that have empty bodies (body_markdown would be '' but we've already tried)
        conn = get_connection()
        emails_needing_body = conn.execute("""
            SELECT id FROM emails
            WHERE body_html IS NULL
            LIMIT 20
        """).fetchall()
        conn.close()

        if emails_needing_body:
            from src.body_parser import html_to_markdown
            fetched = 0
            for row in emails_needing_body:
                email_id = row["id"]
                body_html = poller._get_message_body(email_id)
                # Always update to mark we've tried, even if body is empty
                body_markdown = html_to_markdown(body_html) if body_html else ""
                conn = get_connection()
                conn.execute(
                    "UPDATE emails SET body_markdown = ?, body_html = ? WHERE id = ?",
                    (body_markdown, body_html or "", email_id)
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

        # 3. Working memory analysis (recent emails only - older ones use search)
        conn = get_connection()
        pending_emails = conn.execute("""
            SELECT id, conversation_id, subject, sender, received_at,
                   body_markdown, body_preview, to_emails, cc_emails
            FROM emails
            WHERE extracted_body IS NULL
              AND (body_markdown IS NOT NULL OR body_preview IS NOT NULL)
              AND datetime(received_at) > datetime('now', '-30 days')
            LIMIT 50
        """).fetchall()
        conn.close()

        if pending_emails:
            user_email = os.environ.get("DELEGATED_USER", "")
            updater = WorkingMemoryUpdater(user_email)
            wm_concurrency = 10
            semaphore = asyncio.Semaphore(wm_concurrency)

            async def process_one(email: dict) -> bool:
                async with semaphore:
                    try:
                        await updater.process_email(email)
                        conn = get_connection()
                        conn.execute(
                            "UPDATE emails SET extracted_body = COALESCE(body_markdown, body_preview, '') WHERE id = ?",
                            (email["id"],)
                        )
                        conn.commit()
                        conn.close()
                        return True
                    except Exception as e:
                        logger.warning(f"Content extraction failed for {email['id']}: {e}")
                        return False

            logger.info(f"Processing {len(pending_emails)} emails for working memory (concurrency={wm_concurrency})")
            results = await asyncio.gather(*[process_one(dict(row)) for row in pending_emails])
            extracted = sum(1 for r in results if r)
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


async def _evaluate_sent_email_alerts(user_email: str, count: int) -> None:
    """Evaluate alert rules against recently synced sent emails."""
    try:
        from src.alerts import AlertRulesEngine

        conn = get_connection()
        # Get recently synced sent emails (last few minutes)
        sent_emails = conn.execute(
            """
            SELECT id, subject, sender, to_emails, cc_emails, received_at, body_preview
            FROM emails
            WHERE datetime(created_at) > datetime('now', '-5 minutes')
            ORDER BY received_at DESC
            LIMIT ?
            """,
            (count,),
        ).fetchall()
        conn.close()

        if not sent_emails:
            return

        alert_engine = AlertRulesEngine(user_email)

        for email in sent_emails:
            email_dict = dict(email)
            # For sent emails, classification is minimal (no LLM classification done)
            classification = {"labels": [], "urgency": "someday", "outlook_categories": []}

            triggered = await alert_engine.evaluate_email_rules(
                email_dict, classification, event_type="email_sent"
            )

            for t in triggered:
                alert_engine.emit_alert_trigger(
                    t["rule"],
                    "email_sent",
                    email_dict["id"],
                    email_dict,
                    t["match_reason"],
                )

        if sent_emails:
            logger.debug(f"Evaluated {len(sent_emails)} sent emails against alert rules")

    except Exception as e:
        logger.warning(f"Sent email alert evaluation error: {e}")


def service_loop(user_email: str, poll_interval: int, run_once: bool, concurrency: int = 5, backfill: bool = False):
    logger.info("Initializing database...")
    init_db()

    logger.info("Initializing poller...")
    poller = GraphPoller()

    organizer = Organizer(poller, backfill=backfill)

    # Working memory engine configuration
    wm_engine_interval = int(os.environ.get("WM_ENGINE_INTERVAL", 300))  # Default 5 minutes
    last_wm_engine_run = 0.0

    # Calendar sync configuration
    calendar_sync_interval = int(os.environ.get("CALENDAR_SYNC_INTERVAL", 300))  # Default 5 minutes

    # Delta sync configuration (handles deletions)
    delta_sync_interval = int(os.environ.get("DELTA_SYNC_INTERVAL", 300))  # Default 5 minutes
    last_delta_sync = 0.0
    inbox_folder_id = None  # Cached Inbox folder ID

    # Sent items sync for alert rules
    sent_sync_interval = int(os.environ.get("SENT_SYNC_INTERVAL", 300))  # Default 5 minutes
    last_sent_sync = 0.0
    sent_items_folder_id = None  # Cached Sent Items folder ID

    logger.info("Starting Inbox Assistant Service")
    logger.info(f"User: {user_email}")
    logger.info(f"Poll Interval: {poll_interval}s")
    logger.info(f"Concurrency: {concurrency}")
    logger.info(f"Working Memory Engine Interval: {wm_engine_interval}s")
    logger.info(f"Calendar Sync Interval: {calendar_sync_interval}s")
    logger.info(f"Delta Sync Interval: {delta_sync_interval}s")
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

            # Sync calendar periodically
            if needs_sync(calendar_sync_interval):
                try:
                    sync_calendar()
                except Exception as cal_err:
                    logger.warning(f"Calendar sync error: {cal_err}")

            # Delta sync periodically (handles deletions from Outlook)
            now = time.time()
            if now - last_delta_sync >= delta_sync_interval:
                try:
                    # Get Inbox folder ID (cache it)
                    if inbox_folder_id is None:
                        folders = poller.get_all_folders()
                        inbox = next((f for f in folders if f.get("displayName", "").lower() == "inbox"), None)
                        if inbox:
                            inbox_folder_id = inbox["id"]

                    if inbox_folder_id:
                        updated, deleted = poller.delta_sync_folder(inbox_folder_id, "Inbox", fetch_body=True)
                        if updated > 0 or deleted > 0:
                            logger.info(f"Delta sync: {updated} updated, {deleted} deleted")
                    last_delta_sync = now
                except Exception as sync_err:
                    logger.warning(f"Delta sync error: {sync_err}")

            # Sync sent items for alert rules (email_sent events)
            now = time.time()
            if now - last_sent_sync >= sent_sync_interval:
                try:
                    # Get Sent Items folder ID (cache it)
                    if sent_items_folder_id is None:
                        folders = poller.get_all_folders() if inbox_folder_id is None else None
                        if folders is None:
                            folders = poller.get_all_folders()
                        sent_folder = next(
                            (f for f in folders if f.get("displayName", "").lower() in ("sent items", "sent")),
                            None
                        )
                        if sent_folder:
                            sent_items_folder_id = sent_folder["id"]
                            logger.info(f"Cached Sent Items folder ID: {sent_items_folder_id[:20]}...")

                    if sent_items_folder_id and not backfill:
                        updated, deleted = poller.delta_sync_folder(
                            sent_items_folder_id, "Sent Items", fetch_body=False
                        )
                        if updated > 0:
                            logger.info(f"Sent items sync: {updated} new/updated")
                            # Evaluate alert rules for sent emails
                            asyncio.run(_evaluate_sent_email_alerts(user_email, updated))
                    last_sent_sync = now
                except Exception as sent_err:
                    logger.warning(f"Sent items sync error: {sent_err}")

            # Execute pending actions (from CLI)
            if has_pending_actions():
                try:
                    results = poll_and_execute_actions()
                    if results["executed"] > 0 or results["failed"] > 0:
                        logger.info(f"Actions: {results['executed']} executed, {results['failed']} failed")
                except Exception as action_err:
                    logger.warning(f"Action executor error: {action_err}")
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
