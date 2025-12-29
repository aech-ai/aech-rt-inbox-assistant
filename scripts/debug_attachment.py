#!/usr/bin/env python3
"""
Debug script to trace attachment extraction step by step.
Run inside Docker: docker compose run --rm backfill python /app/scripts/debug_attachment.py
"""

import os
import sys
import sqlite3
import subprocess
import tempfile
from pathlib import Path

# Ensure we can import from src
sys.path.insert(0, "/app")

def get_db_path():
    return "/home/agentaech/.inbox-assistant/assistant.sqlite"

def get_pending_attachment():
    """Get one pending attachment for testing."""
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get a PDF that's pending
    row = cursor.execute("""
        SELECT a.id, a.email_id, a.filename, a.content_type, a.size_bytes, a.extraction_status
        FROM attachments a
        WHERE a.content_type = 'application/pdf'
        AND a.extraction_status IN ('pending', 'failed')
        ORDER BY a.size_bytes ASC
        LIMIT 1
    """).fetchone()

    if not row:
        # Try any pending attachment
        row = cursor.execute("""
            SELECT a.id, a.email_id, a.filename, a.content_type, a.size_bytes, a.extraction_status
            FROM attachments a
            WHERE a.extraction_status IN ('pending', 'failed')
            ORDER BY a.size_bytes ASC
            LIMIT 1
        """).fetchone()

    conn.close()
    return dict(row) if row else None

def download_attachment(email_id: str, attachment_id: str) -> bytes:
    """Download attachment from Graph API."""
    import requests
    from aech_cli_msgraph.graph import GraphClient

    user_email = os.getenv("DELEGATED_USER")
    print(f"  DELEGATED_USER: {user_email}")

    client = GraphClient()
    headers = client._get_headers()
    base_path = client._get_base_path(user_email)

    url = f"{base_path}/messages/{email_id}/attachments/{attachment_id}/$value"
    print(f"  Graph URL: {url}")

    resp = requests.get(url, headers=headers)
    print(f"  Response status: {resp.status_code}")
    print(f"  Response headers: {dict(resp.headers)}")

    if not resp.ok:
        print(f"  ERROR: {resp.text[:500]}")
        return None

    return resp.content

def test_documents_cli(file_path: str, output_dir: str):
    """Test the documents CLI directly."""
    print(f"\n=== Testing documents CLI ===")
    print(f"  Input: {file_path}")
    print(f"  Output dir: {output_dir}")

    cmd = [
        "aech-cli-documents",
        "convert-to-markdown",
        file_path,
        "--output-dir",
        output_dir,
    ]
    print(f"  Command: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=120,
    )

    print(f"  Return code: {result.returncode}")
    print(f"  STDOUT: {result.stdout}")
    print(f"  STDERR: {result.stderr[:1000] if result.stderr else '(empty)'}")

    # Check what files were created
    output_path = Path(output_dir)
    files = list(output_path.glob("*"))
    print(f"  Files created: {files}")

    for f in files:
        print(f"    {f.name}: {f.stat().st_size} bytes")
        if f.suffix == ".md":
            content = f.read_text()[:500]
            print(f"    Content preview: {content}")

    return result.returncode == 0 and any(f.suffix == ".md" for f in files)

def main():
    print("=" * 60)
    print("Attachment Extraction Debug Script")
    print("=" * 60)

    # Step 1: Get a test attachment
    print("\n=== Step 1: Get test attachment from DB ===")
    att = get_pending_attachment()
    if not att:
        print("  No pending attachments found!")

        # Show what we have
        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        print("\n  Attachment status breakdown:")
        for row in cursor.execute("""
            SELECT extraction_status, COUNT(*) as cnt
            FROM attachments
            GROUP BY extraction_status
        """):
            print(f"    {row['extraction_status']}: {row['cnt']}")

        print("\n  Sample attachments:")
        for row in cursor.execute("""
            SELECT id, filename, content_type, extraction_status
            FROM attachments
            LIMIT 5
        """):
            print(f"    {row['filename']} ({row['content_type']}): {row['extraction_status']}")

        conn.close()
        return

    print(f"  Attachment ID: {att['id']}")
    print(f"  Email ID: {att['email_id']}")
    print(f"  Filename: {att['filename']}")
    print(f"  Content-Type: {att['content_type']}")
    print(f"  Size: {att['size_bytes']} bytes")
    print(f"  Status: {att['extraction_status']}")

    # Step 2: Download the attachment
    print("\n=== Step 2: Download from Graph API ===")
    content = download_attachment(att['email_id'], att['id'])
    if not content:
        print("  FAILED to download attachment")
        return

    print(f"  Downloaded {len(content)} bytes")
    print(f"  First 100 bytes (hex): {content[:100].hex()}")

    # Step 3: Save to temp file
    print("\n=== Step 3: Save to temp file ===")
    suffix = Path(att['filename']).suffix or ".bin"

    # Use a persistent location for debugging
    debug_dir = Path("/tmp/attachment_debug")
    debug_dir.mkdir(exist_ok=True)

    temp_file = debug_dir / f"test{suffix}"
    temp_file.write_bytes(content)
    print(f"  Saved to: {temp_file}")
    print(f"  File exists: {temp_file.exists()}")
    print(f"  File size: {temp_file.stat().st_size}")

    # Step 4: Test documents CLI
    output_dir = debug_dir / "output"
    output_dir.mkdir(exist_ok=True)

    success = test_documents_cli(str(temp_file), str(output_dir))

    print("\n=== Result ===")
    if success:
        print("  SUCCESS! Documents CLI produced output.")
    else:
        print("  FAILED! Documents CLI did not produce output.")

    print(f"\n  Debug files saved to: {debug_dir}")
    print(f"  You can inspect them with: ls -la {debug_dir}")

if __name__ == "__main__":
    main()
