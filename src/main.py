import argparse
import asyncio
import logging
import os
import time

from src.database import init_db, get_db_path
from src.poller import GraphPoller
from src.organizer import Organizer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def _service_loop(user_email: str, poll_interval: int, run_once: bool):
    db_path = get_db_path(user_email)
    logger.info(f"Initializing database at {db_path}")
    init_db(db_path)

    poller = GraphPoller(db_path, user_email)
    poller.ensure_standard_folders()
    organizer = Organizer(db_path, poller, user_email)

    logger.info("Starting Inbox Assistant Service")
    logger.info(f"User: {user_email}")
    logger.info(f"Poll Interval: {poll_interval}s")

    while True:
        try:
            poller.poll_inbox()
            asyncio.run(organizer.organize_emails())
        except Exception as e:
            logger.error(f"Error in main loop: {e}")

        if run_once:
            break

        logger.info(f"Sleeping for {poll_interval} seconds")
        time.sleep(poll_interval)


def run(argv=None):
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
    args = parser.parse_args(argv)

    user_email = os.environ.get("DELEGATED_USER", "unknown@example.com")
    poll_interval = args.poll_interval or int(os.environ.get("POLL_INTERVAL", 5))
    _service_loop(user_email, poll_interval, run_once=args.once)


if __name__ == "__main__":
    run()
