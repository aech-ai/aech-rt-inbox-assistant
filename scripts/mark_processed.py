#!/usr/bin/env python3
"""Mark emails as processed - internal ops tool (not for Agent Aech CLI).

Usage:
    DELEGATED_USER=user@example.com python scripts/mark_processed.py

This script marks all emails that have a thread_summary as processed,
useful for recovery after running fresh_start.sh pipeline.
"""

import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import get_connection


def mark_all_processed(dry_run: bool = False) -> int:
    """Mark all emails with thread_summary as processed.

    Args:
        dry_run: If True, only show count without updating

    Returns:
        Number of emails that would be/were marked as processed
    """
    conn = get_connection()

    # Count how many would be affected
    count = conn.execute("""
        SELECT COUNT(*) FROM emails
        WHERE processed_at IS NULL
          AND thread_summary IS NOT NULL
    """).fetchone()[0]

    if dry_run:
        print(f"Would mark {count} emails as processed (dry run)")
        conn.close()
        return count

    if count == 0:
        print("No emails to mark as processed")
        conn.close()
        return 0

    # Perform the update
    conn.execute("""
        UPDATE emails
        SET processed_at = datetime('now')
        WHERE processed_at IS NULL
          AND thread_summary IS NOT NULL
    """)
    conn.commit()
    conn.close()

    print(f"Marked {count} emails as processed")
    return count


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Mark emails as processed (internal ops tool)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show count without making changes"
    )
    args = parser.parse_args()

    mark_all_processed(dry_run=args.dry_run)
