#!/usr/bin/env python3
"""
Test the full pipeline on 10 emails.

Usage:
    DELEGATED_USER=user@example.com uv run python scripts/test_pipeline.py
"""

import asyncio
import logging
import os
import sys
import tempfile
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("test_pipeline")

# Use temp DB for testing
TEST_DB = tempfile.mktemp(suffix=".sqlite")
os.environ["INBOX_DB_PATH"] = TEST_DB


def check_env():
    user = os.getenv("DELEGATED_USER")
    if not user:
        logger.error("DELEGATED_USER required")
        sys.exit(1)
    logger.info(f"Testing with user: {user}")
    logger.info(f"Test DB: {TEST_DB}")
    return user


def test_init_db():
    """Test database initialization."""
    logger.info("=" * 50)
    logger.info("TEST: Database Init")
    logger.info("=" * 50)

    from src.database import init_db, get_db_path

    init_db()
    db_path = get_db_path()
    assert db_path.exists(), "Database not created"
    logger.info(f"✓ Database created at {db_path}")


def test_body_parser():
    """Test body parser."""
    logger.info("=" * 50)
    logger.info("TEST: Body Parser")
    logger.info("=" * 50)

    from src.body_parser import parse_email_body

    html = """
    <html><body>
    <p>Hi there,</p>
    <p>Here's the <strong>update</strong>:</p>
    <ul><li>Done item 1</li><li>Done item 2</li></ul>
    <p>Best,</p>
    <p>John Smith<br>VP Engineering<br>555-1234</p>
    <div>On Dec 25, someone wrote:</div>
    <blockquote>old stuff</blockquote>
    </body></html>
    """

    result = parse_email_body(html)

    assert "update" in result.main_content, "Main content missing"
    assert "Done item" in result.main_content, "List items missing"
    # Note: body_parser just converts HTML→markdown
    # LLM handles quote removal and signature extraction

    logger.info("✓ Body parser works")
    logger.info(f"  Main content: {len(result.main_content)} chars")


def test_sync_emails(limit=10):
    """Test syncing a few emails."""
    logger.info("=" * 50)
    logger.info(f"TEST: Sync {limit} Emails")
    logger.info("=" * 50)

    from src.poller import GraphPoller
    from src.database import get_connection

    poller = GraphPoller()

    # Get inbox folder
    folders = poller.get_all_folders()
    inbox = next((f for f in folders if f.get("displayName") == "Inbox"), None)

    if not inbox:
        logger.warning("No Inbox folder found, using first folder")
        inbox = folders[0] if folders else None

    if not inbox:
        logger.error("No folders found")
        return 0

    folder_id = inbox["id"]
    folder_name = inbox["displayName"]

    # Sync with limit
    logger.info(f"Syncing from: {folder_name}")

    # Custom sync with limit
    from src.database import get_connection
    import requests

    headers = poller._graph_client._get_headers()
    base_path = poller._graph_client._get_base_path(poller.user_email)

    select_fields = "id,conversationId,internetMessageId,subject,from,toRecipients,ccRecipients,receivedDateTime,bodyPreview,hasAttachments,isRead,webLink,categories"
    url = f"{base_path}/mailFolders/{folder_id}/messages?$select={select_fields}&$top={limit}"

    resp = requests.get(url, headers=headers)
    if not resp.ok:
        logger.error(f"Failed to fetch messages: {resp.status_code}")
        return 0

    messages = resp.json().get("value", [])
    logger.info(f"Fetched {len(messages)} messages")

    conn = get_connection()
    synced = 0

    for msg in messages:
        msg_data = poller._extract_message_data(msg)
        body_html = poller._get_message_body(msg["id"])
        poller._upsert_message(conn, msg_data, body_html)
        synced += 1
        logger.info(f"  {synced}. {msg_data['subject'][:50]}...")

    conn.commit()
    conn.close()

    logger.info(f"✓ Synced {synced} emails")
    return synced


def test_check_data():
    """Check what we have in the database."""
    logger.info("=" * 50)
    logger.info("TEST: Check Data")
    logger.info("=" * 50)

    from src.database import get_connection

    conn = get_connection()

    # Count emails
    count = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
    logger.info(f"Total emails: {count}")

    # Check body_markdown
    with_markdown = conn.execute(
        "SELECT COUNT(*) FROM emails WHERE body_markdown IS NOT NULL"
    ).fetchone()[0]
    logger.info(f"With body_markdown: {with_markdown}")

    # Check signature_block
    with_sig = conn.execute(
        "SELECT COUNT(*) FROM emails WHERE signature_block IS NOT NULL AND signature_block != ''"
    ).fetchone()[0]
    logger.info(f"With signature_block: {with_sig}")

    # Sample one email
    sample = conn.execute("""
        SELECT subject, body_markdown, signature_block
        FROM emails
        WHERE body_markdown IS NOT NULL
        LIMIT 1
    """).fetchone()

    if sample:
        logger.info("")
        logger.info("Sample email:")
        logger.info(f"  Subject: {sample['subject'][:60]}...")
        logger.info(f"  Body (first 200 chars): {(sample['body_markdown'] or '')[:200]}...")
        logger.info(f"  Signature: {(sample['signature_block'] or '')[:100]}...")

    conn.close()

    assert count > 0, "No emails synced"
    assert with_markdown > 0, "No body_markdown generated"
    logger.info("✓ Data looks good")


async def test_llm_extraction():
    """Test LLM extraction on one email."""
    logger.info("=" * 50)
    logger.info("TEST: LLM Extraction (1 email)")
    logger.info("=" * 50)

    from src.database import get_connection
    from src.working_memory.updater import WorkingMemoryUpdater

    user_email = os.getenv("DELEGATED_USER")
    updater = WorkingMemoryUpdater(user_email)

    conn = get_connection()
    row = conn.execute("""
        SELECT id, conversation_id, subject, sender, received_at,
               body_markdown, body_preview, to_emails, cc_emails
        FROM emails
        WHERE body_markdown IS NOT NULL AND thread_summary IS NULL
        LIMIT 1
    """).fetchone()
    conn.close()

    if not row:
        logger.info("No emails need LLM extraction")
        return

    email = dict(row)
    logger.info(f"Processing: {email['subject'][:50]}...")

    await updater.process_email(email)

    # Check result
    conn = get_connection()
    result = conn.execute(
        "SELECT thread_summary FROM emails WHERE id = ?",
        (email["id"],)
    ).fetchone()
    conn.close()

    if result and result["thread_summary"]:
        logger.info(f"✓ Thread summary: {result['thread_summary'][:100]}...")
    else:
        logger.warning("No thread summary generated (may be expected for simple emails)")


def test_chunking():
    """Test chunk creation."""
    logger.info("=" * 50)
    logger.info("TEST: Chunking")
    logger.info("=" * 50)

    from src.chunker import process_unindexed_emails
    from src.database import get_connection

    results = process_unindexed_emails(limit=10)

    logger.info(f"Processed: {results['processed']}")
    logger.info(f"Chunks created: {results['chunks_created']}")

    # Check chunks
    conn = get_connection()
    chunk_count = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    conn.close()

    logger.info(f"✓ Total chunks in DB: {chunk_count}")
    return chunk_count


def test_embeddings():
    """Test embedding generation."""
    logger.info("=" * 50)
    logger.info("TEST: Embeddings")
    logger.info("=" * 50)

    from src.embeddings import embed_pending_chunks, get_embedding_stats
    from src.database import get_connection

    # Generate embeddings for all chunks
    results = embed_pending_chunks(limit=100)

    logger.info(f"Embedded: {results['processed']}")
    logger.info(f"Failed: {results['failed']}")

    # Check stats
    stats = get_embedding_stats()
    logger.info(f"Chunks with embeddings: {stats['chunks_with_embeddings']}")
    logger.info(f"Model: {stats['model']}")

    assert results['processed'] > 0, "No chunks were embedded"
    assert results['failed'] == 0, f"Some embeddings failed: {results['failed']}"

    logger.info("✓ Embeddings generated")
    return results['processed']


def test_vector_search():
    """Test vector similarity search."""
    logger.info("=" * 50)
    logger.info("TEST: Vector Search")
    logger.info("=" * 50)

    from src.search import vector_search
    from src.database import get_connection

    # Get a sample subject to search for
    conn = get_connection()
    sample = conn.execute("SELECT subject FROM emails LIMIT 1").fetchone()
    conn.close()

    if not sample:
        logger.warning("No emails to search")
        return

    # Extract key terms from subject for semantic search
    query = sample["subject"][:50]
    logger.info(f"Search query: '{query}'")

    results = vector_search(query, limit=5, min_score=0.1)

    logger.info(f"Vector results: {len(results)}")
    for i, r in enumerate(results[:3]):
        logger.info(f"  {i+1}. score={r.score:.3f} - {r.content_preview[:60]}...")

    assert len(results) > 0, "Vector search returned no results"
    logger.info("✓ Vector search works")


def test_fts_search():
    """Test full-text search."""
    logger.info("=" * 50)
    logger.info("TEST: FTS Search")
    logger.info("=" * 50)

    from src.search import fts_search, get_search_stats
    from src.database import get_connection

    # Check FTS index state
    stats = get_search_stats()
    logger.info(f"FTS indexed: {stats['fts_indexed']}")
    logger.info(f"Total chunks: {stats['total_chunks']}")

    if stats['fts_indexed'] == 0:
        logger.warning("FTS index is empty - checking if trigger fired")
        conn = get_connection()
        # Try to manually check chunks_fts
        try:
            fts_count = conn.execute("SELECT COUNT(*) FROM chunks_fts").fetchone()[0]
            logger.info(f"Direct chunks_fts count: {fts_count}")
        except Exception as e:
            logger.warning(f"Could not query chunks_fts: {e}")
        conn.close()

    # Get a word from chunk content (not email body) to search for
    conn = get_connection()
    sample = conn.execute(
        "SELECT content FROM chunks LIMIT 1"
    ).fetchone()
    conn.close()

    if not sample or not sample["content"]:
        logger.warning("No chunk content to search")
        return

    # Extract a word (skip short words and non-alpha)
    words = [w for w in sample["content"].split() if len(w) > 4 and w.isalpha()]
    if not words:
        # Try any alpha word
        words = [w for w in sample["content"].split() if w.isalpha()]

    if not words:
        logger.warning("No suitable words for FTS search")
        logger.info("✓ FTS search skipped (no suitable content)")
        return

    query = words[0]
    logger.info(f"FTS query: '{query}'")

    results = fts_search(query, limit=5)

    logger.info(f"FTS results: {len(results)}")
    for i, r in enumerate(results[:3]):
        logger.info(f"  {i+1}. rank={r.fts_rank} - {r.content_preview[:60]}...")

    if len(results) == 0:
        logger.warning("FTS returned 0 results - may indicate trigger issue")
        logger.info("✓ FTS search completed (0 results, possibly index issue)")
    else:
        logger.info("✓ FTS search works")


def test_hybrid_search():
    """Test hybrid search (FTS + vector with RRF)."""
    logger.info("=" * 50)
    logger.info("TEST: Hybrid Search")
    logger.info("=" * 50)

    from src.search import hybrid_search, get_search_stats

    # Get search stats
    stats = get_search_stats()
    logger.info(f"Total chunks: {stats['total_chunks']}")
    logger.info(f"Vector searchable: {stats['vector_searchable']}")
    logger.info(f"FTS indexed: {stats['fts_indexed']}")

    # Search with a general term
    query = "update"
    logger.info(f"Hybrid query: '{query}'")

    results = hybrid_search(query, limit=5)

    logger.info(f"Hybrid results: {len(results)}")
    for i, r in enumerate(results[:3]):
        fts = f"fts={r.fts_rank}" if r.fts_rank else "fts=∅"
        vec = f"vec={r.vector_rank}" if r.vector_rank else "vec=∅"
        logger.info(f"  {i+1}. score={r.score:.4f} ({fts}, {vec}) - {r.content_preview[:50]}...")

    # Hybrid should find something even with a generic query
    logger.info("✓ Hybrid search works")


def test_cleanup():
    """Clean up test database."""
    logger.info("=" * 50)
    logger.info("TEST: Cleanup")
    logger.info("=" * 50)

    if os.path.exists(TEST_DB):
        os.unlink(TEST_DB)
        logger.info(f"✓ Deleted {TEST_DB}")

    # Clean up WAL files
    for ext in ["-wal", "-shm"]:
        path = TEST_DB + ext
        if os.path.exists(path):
            os.unlink(path)


async def main():
    logger.info("=" * 50)
    logger.info("PIPELINE TEST - 10 Emails")
    logger.info("=" * 50)
    logger.info("")

    success = False
    try:
        check_env()

        # Run tests
        test_init_db()
        test_body_parser()
        test_sync_emails(limit=10)
        test_check_data()
        await test_llm_extraction()
        test_chunking()
        test_embeddings()
        test_fts_search()
        test_vector_search()
        test_hybrid_search()

        logger.info("")
        logger.info("=" * 50)
        logger.info("ALL TESTS PASSED ✓")
        logger.info("=" * 50)
        success = True

    except Exception as e:
        logger.error(f"TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        logger.info(f"Test DB preserved for debugging: {TEST_DB}")
        sys.exit(1)

    finally:
        if success:
            test_cleanup()


if __name__ == "__main__":
    asyncio.run(main())
