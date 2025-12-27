import argparse
import asyncio
import logging
import os
import time

from src.database import init_db
from src.poller import GraphPoller
from src.organizer import Organizer
from src.working_memory.engine import run_memory_engine_cycle

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


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
