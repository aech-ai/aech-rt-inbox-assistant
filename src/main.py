import argparse
import asyncio
import logging
import os
import time

from src.attachments import AttachmentProcessor
from src.chunker import process_unindexed_attachments, process_unindexed_emails
from src.database import get_connection, init_db
from src.embeddings import embed_pending_chunks
from src.poller import GraphPoller

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


async def process_pending_content(concurrency: int = 5) -> None:
    """
    Keep the mailbox corpus queryable.

    This service is intentionally dumb: fetch bodies, persist attachments,
    extract text, and keep search indexes current. It does not perform role
    reasoning, notification policy, or calendar/action workflows.
    """
    try:
        poller = GraphPoller()

        conn = get_connection()
        emails_needing_body = conn.execute(
            """
            SELECT id FROM emails
            WHERE body_html IS NULL
            LIMIT 20
            """
        ).fetchall()
        conn.close()

        if emails_needing_body:
            from src.body_parser import html_to_markdown
            from src.chunker import create_email_chunks
            from src.embeddings import embed_chunks_for_source

            fetched = 0
            indexed = 0
            for row in emails_needing_body:
                email_id = row["id"]
                body_html = poller._get_message_body(email_id)
                body_markdown = html_to_markdown(body_html) if body_html else ""

                conn = get_connection()
                conn.execute(
                    """
                    UPDATE emails
                    SET body_markdown = ?, body_html = ?, updated_at = datetime('now')
                    WHERE id = ?
                    """,
                    (body_markdown, body_html or "", email_id),
                )
                conn.commit()
                conn.close()
                fetched += 1

                if body_markdown:
                    try:
                        chunks_created = create_email_chunks(email_id)
                        if chunks_created > 0:
                            embed_chunks_for_source("email", email_id)
                            indexed += 1
                    except Exception as exc:
                        logger.debug("Email indexing deferred for %s: %s", email_id, exc)

            logger.info("Fetched %s email bodies, indexed %s", fetched, indexed)

        processor = AttachmentProcessor()
        att_results = await processor.process_pending_attachments_async(limit=50, concurrency=concurrency)
        if att_results["completed"] > 0:
            logger.info("Stored/extracted %s attachments", att_results["completed"])

        email_results = process_unindexed_emails(limit=50)
        att_chunk_results = process_unindexed_attachments(limit=50)
        fallback_chunks = email_results.get("chunks_created", 0) + att_chunk_results.get("chunks_created", 0)
        if fallback_chunks > 0:
            logger.info("Fallback indexing created %s chunks", fallback_chunks)

        embed_results = embed_pending_chunks(limit=50, batch_size=32)
        if embed_results["processed"] > 0:
            logger.info("Fallback embedding wrote %s embeddings", embed_results["processed"])
    except Exception as exc:
        logger.error("Content processing error: %s", exc)
        raise


def _cache_folder_id(
    poller: GraphPoller,
    cache: dict[str, str | None],
    cache_key: str,
    display_names: tuple[str, ...],
) -> str | None:
    folder_id = cache.get(cache_key)
    if folder_id:
        return folder_id

    folders = poller.get_all_folders()
    folder = next(
        (f for f in folders if f.get("displayName", "").lower() in display_names),
        None,
    )
    if not folder:
        return None

    folder_id = folder["id"]
    cache[cache_key] = folder_id
    return folder_id


def service_loop(
    user_email: str,
    poll_interval: int,
    run_once: bool,
    concurrency: int = 5,
    sync_sent_items: bool = True,
) -> None:
    logger.info("Initializing database")
    init_db()

    logger.info("Initializing Graph poller")
    poller = GraphPoller()

    delta_sync_interval = int(os.environ.get("DELTA_SYNC_INTERVAL", 300))
    sent_sync_interval = int(os.environ.get("SENT_SYNC_INTERVAL", 300))
    last_delta_sync = 0.0
    last_sent_sync = 0.0
    folder_cache: dict[str, str | None] = {"inbox": None, "sent": None}

    logger.info("Starting inbox-assistant sync service")
    logger.info("User: %s", user_email)
    logger.info("Poll Interval: %ss", poll_interval)
    logger.info("Concurrency: %s", concurrency)
    logger.info("Delta Sync Interval: %ss", delta_sync_interval)
    logger.info("Sync Sent Items: %s", sync_sent_items)

    while True:
        try:
            poller.poll_inbox()
            asyncio.run(process_pending_content(concurrency=concurrency))

            now = time.time()
            if now - last_delta_sync >= delta_sync_interval:
                try:
                    inbox_folder_id = _cache_folder_id(poller, folder_cache, "inbox", ("inbox",))
                    if inbox_folder_id:
                        updated, deleted = poller.delta_sync_folder(
                            inbox_folder_id,
                            "Inbox",
                            fetch_body=True,
                        )
                        if updated > 0 or deleted > 0:
                            logger.info("Inbox delta sync: %s updated, %s deleted", updated, deleted)
                    last_delta_sync = now
                except Exception as exc:
                    logger.warning("Inbox delta sync error: %s", exc)

            now = time.time()
            if sync_sent_items and now - last_sent_sync >= sent_sync_interval:
                try:
                    sent_folder_id = _cache_folder_id(
                        poller,
                        folder_cache,
                        "sent",
                        ("sent items", "sent"),
                    )
                    if sent_folder_id:
                        updated, deleted = poller.delta_sync_folder(
                            sent_folder_id,
                            "Sent Items",
                            fetch_body=False,
                        )
                        if updated > 0 or deleted > 0:
                            logger.info("Sent Items delta sync: %s updated, %s deleted", updated, deleted)
                    last_sent_sync = now
                except Exception as exc:
                    logger.warning("Sent Items delta sync error: %s", exc)
        except Exception as exc:
            logger.error("Error in main loop: %s", exc)

        if run_once:
            break

        logger.debug("Sleeping for %s seconds", poll_interval)
        time.sleep(poll_interval)


def run(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Aech Inbox Assistant service runner")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single ingest/index cycle and exit.",
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
        help="Number of attachments to process in parallel (default: 5).",
    )
    parser.add_argument(
        "--no-sync-sent",
        action="store_true",
        help="Skip Sent Items delta sync.",
    )
    args = parser.parse_args(argv)

    user_email = os.environ.get("DELEGATED_USER")
    if not user_email:
        raise ValueError("DELEGATED_USER environment variable must be set")

    poll_interval = args.poll_interval or int(os.environ.get("POLL_INTERVAL", 5))
    service_loop(
        user_email,
        poll_interval,
        run_once=args.once,
        concurrency=args.concurrency,
        sync_sent_items=not args.no_sync_sent,
    )


if __name__ == "__main__":
    run()
