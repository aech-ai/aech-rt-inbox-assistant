#!/usr/bin/env python3
"""
Schema consolidation migration script.

Migrates data from old WM tables to the new unified facts table:
- wm_decisions → facts (fact_type='decision')
- wm_commitments → facts (fact_type='commitment')
- wm_observations → facts (fact_type='preference'|'relationship'|'pattern')

Also backfills any emails/attachments missing chunks and embeddings.

Usage:
    python scripts/consolidate_schema.py [--dry-run] [--backfill-only]
"""

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import get_connection, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def migrate_wm_decisions(conn, dry_run: bool = False) -> int:
    """Migrate wm_decisions to facts table."""
    cursor = conn.cursor()

    # Check if wm_decisions table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='wm_decisions'"
    )
    if not cursor.fetchone():
        logger.info("wm_decisions table not found, skipping")
        return 0

    # Get all unresolved decisions
    rows = cursor.execute("""
        SELECT id, question, context, source_email_id, requester,
               urgency, deadline, created_at
        FROM wm_decisions
        WHERE is_resolved = 0
    """).fetchall()

    migrated = 0
    for row in rows:
        fact_id = str(uuid.uuid4())

        if not dry_run:
            try:
                cursor.execute("""
                    INSERT INTO facts (
                        id, source_type, source_id, fact_type, fact_value,
                        context, confidence, status, due_date, extracted_at
                    ) VALUES (?, 'email', ?, 'decision', ?, ?, 0.9, 'active', ?, ?)
                """, (
                    fact_id,
                    row["source_email_id"],
                    row["question"],
                    row["context"],
                    row["deadline"],
                    row["created_at"] or datetime.now().isoformat(),
                ))
                migrated += 1
            except Exception as e:
                logger.warning(f"Failed to migrate decision {row['id']}: {e}")
        else:
            migrated += 1

    if migrated > 0:
        logger.info(f"Migrated {migrated} decisions to facts table")
    return migrated


def migrate_wm_commitments(conn, dry_run: bool = False) -> int:
    """Migrate wm_commitments to facts table."""
    cursor = conn.cursor()

    # Check if wm_commitments table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='wm_commitments'"
    )
    if not cursor.fetchone():
        logger.info("wm_commitments table not found, skipping")
        return 0

    # Get all incomplete commitments
    rows = cursor.execute("""
        SELECT id, description, to_whom, source_email_id,
               committed_at, due_by, created_at
        FROM wm_commitments
        WHERE is_completed = 0
    """).fetchall()

    migrated = 0
    for row in rows:
        fact_id = str(uuid.uuid4())

        metadata = {}
        if row["to_whom"]:
            metadata["to_whom"] = row["to_whom"]

        if not dry_run:
            try:
                cursor.execute("""
                    INSERT INTO facts (
                        id, source_type, source_id, fact_type, fact_value,
                        confidence, metadata_json, status, due_date, extracted_at
                    ) VALUES (?, 'email', ?, 'commitment', ?, 0.9, ?, 'active', ?, ?)
                """, (
                    fact_id,
                    row["source_email_id"],
                    row["description"],
                    json.dumps(metadata) if metadata else None,
                    row["due_by"],
                    row["created_at"] or datetime.now().isoformat(),
                ))
                migrated += 1
            except Exception as e:
                logger.warning(f"Failed to migrate commitment {row['id']}: {e}")
        else:
            migrated += 1

    if migrated > 0:
        logger.info(f"Migrated {migrated} commitments to facts table")
    return migrated


def migrate_wm_observations(conn, dry_run: bool = False) -> int:
    """Migrate wm_observations to facts table."""
    cursor = conn.cursor()

    # Check if wm_observations table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='wm_observations'"
    )
    if not cursor.fetchone():
        logger.info("wm_observations table not found, skipping")
        return 0

    # Map observation types to fact types
    type_mapping = {
        "context_learned": "preference",
        "person_introduced": "relationship",
        "meeting_scheduled": "pattern",
        "status_update": "preference",
        "project_mention": "pattern",
        "decision_made": "preference",
        "deadline_mentioned": "pattern",
        "commitment_made": "commitment",  # Already migrated separately
    }

    rows = cursor.execute("""
        SELECT id, type, content, source_email_id, importance,
               confidence, observed_at
        FROM wm_observations
        WHERE type != 'commitment_made'
    """).fetchall()

    migrated = 0
    for row in rows:
        fact_id = str(uuid.uuid4())
        fact_type = type_mapping.get(row["type"], "preference")

        if not dry_run:
            try:
                cursor.execute("""
                    INSERT INTO facts (
                        id, source_type, source_id, fact_type, fact_value,
                        confidence, status, extracted_at
                    ) VALUES (?, 'email', ?, ?, ?, ?, 'active', ?)
                """, (
                    fact_id,
                    row["source_email_id"],
                    fact_type,
                    row["content"],
                    row["confidence"] or 0.7,
                    row["observed_at"] or datetime.now().isoformat(),
                ))
                migrated += 1
            except Exception as e:
                logger.warning(f"Failed to migrate observation {row['id']}: {e}")
        else:
            migrated += 1

    if migrated > 0:
        logger.info(f"Migrated {migrated} observations to facts table")
    return migrated


def backfill_missing_chunks(conn, dry_run: bool = False, limit: int = 100) -> int:
    """Create chunks for emails/attachments that don't have any."""
    from chunker import create_email_chunks, create_attachment_chunks

    cursor = conn.cursor()

    # Find emails with body but no chunks
    email_rows = cursor.execute("""
        SELECT e.id
        FROM emails e
        LEFT JOIN chunks c ON c.source_type = 'email' AND c.source_id = e.id
        WHERE e.body_markdown IS NOT NULL
          AND e.body_markdown != ''
          AND c.id IS NULL
        LIMIT ?
    """, (limit,)).fetchall()

    email_chunks = 0
    for row in email_rows:
        if not dry_run:
            try:
                created = create_email_chunks(row["id"])
                email_chunks += created
            except Exception as e:
                logger.warning(f"Failed to chunk email {row['id']}: {e}")
        else:
            email_chunks += 1

    # Find attachments with extracted_text but no chunks
    att_rows = cursor.execute("""
        SELECT a.id
        FROM attachments a
        LEFT JOIN chunks c ON c.source_type = 'attachment' AND c.source_id = a.id
        WHERE a.extracted_text IS NOT NULL
          AND a.extracted_text != ''
          AND c.id IS NULL
        LIMIT ?
    """, (limit,)).fetchall()

    att_chunks = 0
    for row in att_rows:
        if not dry_run:
            try:
                created = create_attachment_chunks(row["id"])
                att_chunks += created
            except Exception as e:
                logger.warning(f"Failed to chunk attachment {row['id']}: {e}")
        else:
            att_chunks += 1

    if email_chunks > 0 or att_chunks > 0:
        logger.info(f"Created {email_chunks} email chunks, {att_chunks} attachment chunks")

    return email_chunks + att_chunks


def backfill_missing_embeddings(conn, dry_run: bool = False, limit: int = 100) -> int:
    """Generate embeddings for chunks that don't have any."""
    from embeddings import embed_pending_chunks

    if dry_run:
        cursor = conn.cursor()
        count = cursor.execute(
            "SELECT COUNT(*) FROM chunks WHERE embedding IS NULL"
        ).fetchone()[0]
        logger.info(f"Would embed {min(count, limit)} chunks")
        return min(count, limit)

    result = embed_pending_chunks(limit=limit)
    if result["processed"] > 0:
        logger.info(f"Embedded {result['processed']} chunks")
    return result["processed"]


def backfill_attachment_facts(conn, dry_run: bool = False, limit: int = 50) -> int:
    """Extract facts from attachments that don't have any yet."""
    import asyncio
    from facts import FactsExtractor

    cursor = conn.cursor()

    # Find attachments with extracted_text but no facts
    att_rows = cursor.execute("""
        SELECT a.id, a.filename, a.extracted_text
        FROM attachments a
        LEFT JOIN facts f ON f.source_type = 'attachment' AND f.source_id = a.id
        WHERE a.extracted_text IS NOT NULL
          AND LENGTH(a.extracted_text) > 100
          AND f.id IS NULL
        LIMIT ?
    """, (limit,)).fetchall()

    if dry_run:
        logger.info(f"Would extract facts from {len(att_rows)} attachments")
        return len(att_rows)

    if not att_rows:
        return 0

    extractor = FactsExtractor()
    facts_extracted = 0

    for row in att_rows:
        att_id = row["id"]
        filename = row["filename"] or "unknown"
        extracted_text = row["extracted_text"]

        try:
            facts = asyncio.run(
                extractor.extract_from_attachment(att_id, extracted_text, filename)
            )
            if facts:
                stored = extractor.store_facts("attachment", att_id, facts)
                facts_extracted += stored
                logger.debug(f"Extracted {stored} facts from {filename}")
        except Exception as e:
            logger.warning(f"Failed to extract facts from {filename}: {e}")

    if facts_extracted > 0:
        logger.info(f"Extracted {facts_extracted} facts from attachments")

    return facts_extracted


def run_migration(dry_run: bool = False, backfill_only: bool = False):
    """Run the full migration."""
    logger.info("=" * 60)
    logger.info("Schema Consolidation Migration")
    logger.info("=" * 60)

    if dry_run:
        logger.info("DRY RUN MODE - no changes will be made")

    # Initialize database (creates new tables/views if missing)
    logger.info("Initializing database schema...")
    init_db()

    conn = get_connection()

    try:
        stats = {
            "decisions_migrated": 0,
            "commitments_migrated": 0,
            "observations_migrated": 0,
            "chunks_created": 0,
            "embeddings_created": 0,
            "attachment_facts": 0,
        }

        if not backfill_only:
            # Migrate WM tables to facts
            logger.info("\n--- Migrating WM tables to facts ---")
            stats["decisions_migrated"] = migrate_wm_decisions(conn, dry_run)
            stats["commitments_migrated"] = migrate_wm_commitments(conn, dry_run)
            stats["observations_migrated"] = migrate_wm_observations(conn, dry_run)

            if not dry_run:
                conn.commit()

        # Backfill missing chunks and embeddings
        logger.info("\n--- Backfilling missing chunks/embeddings ---")
        stats["chunks_created"] = backfill_missing_chunks(conn, dry_run)

        if not dry_run:
            conn.commit()

        stats["embeddings_created"] = backfill_missing_embeddings(conn, dry_run)

        if not dry_run:
            conn.commit()

        # Backfill facts for attachments
        logger.info("\n--- Backfilling attachment facts ---")
        stats["attachment_facts"] = backfill_attachment_facts(conn, dry_run)

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("Migration Summary:")
        logger.info("=" * 60)
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")

        if dry_run:
            logger.info("\nDRY RUN complete - no changes were made")
        else:
            logger.info("\nMigration complete!")

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Consolidate schema - migrate WM tables to facts, backfill chunks/embeddings"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without making changes",
    )
    parser.add_argument(
        "--backfill-only",
        action="store_true",
        help="Only backfill chunks/embeddings, skip WM table migration",
    )

    args = parser.parse_args()

    # Ensure DELEGATED_USER is set
    if not os.environ.get("DELEGATED_USER"):
        logger.error("DELEGATED_USER environment variable must be set")
        sys.exit(1)

    run_migration(dry_run=args.dry_run, backfill_only=args.backfill_only)


if __name__ == "__main__":
    main()
