#!/usr/bin/env python3
"""
One-time migration script to update wm_threads CHECK constraint.
Preserves all existing data.

Run: python scripts/migrate_wm_threads_status.py
"""
import os
import sys

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import get_connection


def migrate_wm_threads():
    conn = get_connection()
    cursor = conn.cursor()

    # Check current row count
    count = cursor.execute("SELECT COUNT(*) FROM wm_threads").fetchone()[0]
    print(f"Found {count} rows in wm_threads")

    # Create new table with updated CHECK constraint
    print("Creating new table with updated CHECK constraint...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wm_threads_new (
        id TEXT PRIMARY KEY,
        conversation_id TEXT NOT NULL UNIQUE,
        subject TEXT,
        participants_json TEXT NOT NULL DEFAULT '[]',
        status TEXT DEFAULT 'active' CHECK(status IN ('active', 'awaiting_reply', 'awaiting_action', 'stale', 'resolved', 'archived')),
        urgency TEXT DEFAULT 'this_week' CHECK(urgency IN ('immediate', 'today', 'this_week', 'someday')),
        started_at DATETIME,
        last_activity_at DATETIME,
        user_last_action_at DATETIME,
        summary TEXT,
        key_points_json TEXT NOT NULL DEFAULT '[]',
        pending_questions_json TEXT NOT NULL DEFAULT '[]',
        message_count INTEGER DEFAULT 0,
        user_is_cc BOOLEAN DEFAULT 0,
        needs_reply BOOLEAN DEFAULT 0,
        reply_deadline DATETIME,
        labels_json TEXT NOT NULL DEFAULT '[]',
        project_refs_json TEXT NOT NULL DEFAULT '[]',
        latest_email_id TEXT,
        latest_web_link TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Copy all data
    print("Copying data to new table...")
    cursor.execute("""
    INSERT INTO wm_threads_new
    SELECT * FROM wm_threads
    """)

    # Verify count matches
    new_count = cursor.execute("SELECT COUNT(*) FROM wm_threads_new").fetchone()[0]
    if new_count != count:
        print(f"ERROR: Row count mismatch! Original: {count}, New: {new_count}")
        conn.rollback()
        return False

    # Drop old table
    print("Dropping old table...")
    cursor.execute("DROP TABLE wm_threads")

    # Rename new table
    print("Renaming new table...")
    cursor.execute("ALTER TABLE wm_threads_new RENAME TO wm_threads")

    # Recreate indexes
    print("Recreating indexes...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_threads_status ON wm_threads(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_threads_urgency ON wm_threads(urgency)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wm_threads_needs_reply ON wm_threads(needs_reply)")

    conn.commit()
    print(f"Migration complete! {new_count} rows preserved.")
    return True


if __name__ == "__main__":
    migrate_wm_threads()
