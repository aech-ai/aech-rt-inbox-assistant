#!/usr/bin/env python3
"""
Migration script: inbox.sqlite -> assistant.sqlite

This script safely migrates data from the old inbox.sqlite to the new
assistant.sqlite database with the updated schema (includes calendar_events
and actions tables).

Usage:
    DELEGATED_USER=user@example.com python scripts/migrate_to_assistant_db.py

Or with explicit path:
    AECH_USER_DIR=/path/to/user/dir python scripts/migrate_to_assistant_db.py

Options:
    --dry-run    Show what would be migrated without making changes
    --force      Overwrite existing assistant.sqlite if it exists
"""

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import get_state_dir, init_db

try:
    import pysqlite3 as sqlite3
except ImportError:
    import sqlite3


def get_table_counts(conn: sqlite3.Connection) -> dict:
    """Get row counts for all tables."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_fts%'
    """)
    tables = [row[0] for row in cursor.fetchall()]

    counts = {}
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
        except sqlite3.Error:
            counts[table] = -1  # Table exists but can't count
    return counts


def migrate_table(src_conn: sqlite3.Connection, dst_conn: sqlite3.Connection, table: str) -> int:
    """Migrate a single table's data. Returns number of rows migrated."""
    src_cursor = src_conn.cursor()
    dst_cursor = dst_conn.cursor()

    # Get column names from source
    src_cursor.execute(f"PRAGMA table_info({table})")
    src_columns = {row[1] for row in src_cursor.fetchall()}

    # Get column names from destination
    dst_cursor.execute(f"PRAGMA table_info({table})")
    dst_columns = {row[1] for row in dst_cursor.fetchall()}

    # Only copy columns that exist in both
    common_columns = src_columns & dst_columns
    if not common_columns:
        return 0

    columns_str = ", ".join(common_columns)
    placeholders = ", ".join(["?"] * len(common_columns))

    # Fetch all data from source
    src_cursor.execute(f"SELECT {columns_str} FROM {table}")
    rows = src_cursor.fetchall()

    if not rows:
        return 0

    # Insert into destination
    dst_cursor.executemany(
        f"INSERT OR REPLACE INTO {table} ({columns_str}) VALUES ({placeholders})",
        rows
    )

    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Migrate inbox.sqlite to assistant.sqlite")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be migrated")
    parser.add_argument("--force", action="store_true", help="Overwrite existing assistant.sqlite")
    args = parser.parse_args()

    state_dir = get_state_dir()
    old_path = state_dir / "inbox.sqlite"
    new_path = state_dir / "assistant.sqlite"
    backup_path = state_dir / f"inbox.sqlite.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    print(f"State directory: {state_dir}")
    print(f"Old database:    {old_path}")
    print(f"New database:    {new_path}")
    print()

    # Check source exists
    if not old_path.exists():
        print(f"ERROR: Source database not found: {old_path}")
        print("Nothing to migrate.")
        sys.exit(1)

    # Check destination
    if new_path.exists():
        if args.force:
            print(f"WARNING: Removing existing {new_path}")
            if not args.dry_run:
                new_path.unlink()
        else:
            print(f"ERROR: Destination already exists: {new_path}")
            print("Use --force to overwrite, or delete it manually.")
            sys.exit(1)

    # Open source and show stats
    src_conn = sqlite3.connect(old_path)
    src_conn.row_factory = sqlite3.Row

    print("=== Source Database (inbox.sqlite) ===")
    src_counts = get_table_counts(src_conn)
    for table, count in sorted(src_counts.items()):
        print(f"  {table}: {count:,} rows")
    print()

    if args.dry_run:
        print("DRY RUN - no changes made")
        src_conn.close()
        return

    # Create fresh destination with new schema
    print("Creating fresh assistant.sqlite with new schema...")
    init_db(new_path)

    dst_conn = sqlite3.connect(new_path)
    dst_conn.row_factory = sqlite3.Row

    # Migrate each table
    print("\n=== Migrating Data ===")
    migrated = {}

    # Tables to migrate (in order to respect foreign keys)
    tables_to_migrate = [
        "emails",
        "triage_log",
        "user_preferences",
        "labels",
        "reply_tracking",
        "work_items",
        "sync_state",
        "attachments",
        "chunks",
        "wm_threads",
        "wm_contacts",
        "wm_projects",
        "wm_observations",
        "wm_decisions",
        "wm_commitments",
    ]

    for table in tables_to_migrate:
        if table in src_counts and src_counts[table] > 0:
            try:
                count = migrate_table(src_conn, dst_conn, table)
                migrated[table] = count
                print(f"  {table}: {count:,} rows migrated")
            except sqlite3.Error as e:
                print(f"  {table}: ERROR - {e}")
                migrated[table] = 0

    dst_conn.commit()

    # Verify migration
    print("\n=== Verification ===")
    dst_counts = get_table_counts(dst_conn)
    all_ok = True

    for table in tables_to_migrate:
        src_count = src_counts.get(table, 0)
        dst_count = dst_counts.get(table, 0)
        if src_count != dst_count:
            print(f"  WARNING: {table} - source has {src_count}, destination has {dst_count}")
            all_ok = False
        elif src_count > 0:
            print(f"  OK: {table} - {dst_count:,} rows")

    # Show new tables (empty but present)
    print("\n=== New Tables (empty, ready for use) ===")
    new_tables = ["calendar_events", "actions"]
    for table in new_tables:
        count = dst_counts.get(table, 0)
        print(f"  {table}: {count} rows")

    src_conn.close()
    dst_conn.close()

    if all_ok:
        # Backup old database
        print(f"\n=== Backing up old database ===")
        print(f"  {old_path} -> {backup_path}")
        shutil.copy2(old_path, backup_path)

        print("\n" + "=" * 50)
        print("MIGRATION COMPLETE")
        print("=" * 50)
        print(f"\nNew database: {new_path}")
        print(f"Old database backed up to: {backup_path}")
        print("\nYou can safely delete the old database once you've verified everything works:")
        print(f"  rm {old_path}")
    else:
        print("\n" + "=" * 50)
        print("MIGRATION COMPLETED WITH WARNINGS")
        print("=" * 50)
        print("Please review the warnings above before proceeding.")


if __name__ == "__main__":
    main()
