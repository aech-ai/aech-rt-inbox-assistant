"""
Attachment processing pipeline for Email Corpus Intelligence.

Downloads attachments from Microsoft Graph API and extracts text using
aech-cli-documents for searchable indexing.
"""

import asyncio
import hashlib
import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests
from aech_cli_msgraph.graph import GraphClient

from .database import get_connection

logger = logging.getLogger(__name__)

# Content types that we can extract text from
EXTRACTABLE_TYPES = {
    # Documents
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/vnd.ms-excel",
    "application/vnd.ms-powerpoint",
    # Text
    "text/plain",
    "text/csv",
    "text/html",
    "text/markdown",
    # NOTE: Images excluded for now - most are email signatures (noise).
    # Future: add image description extraction via vision model.
}

# Filename patterns to skip (email signatures, logos, etc.)
SKIP_FILENAME_PATTERNS = {
    "image001", "image002", "image003", "image004", "image005",
    "signature", "logo", "banner", "footer", "header",
}


class AttachmentProcessor:
    """
    Processes email attachments: downloads from Graph API and extracts text.
    """

    def __init__(self):
        self.user_email = os.getenv("DELEGATED_USER")
        if not self.user_email:
            raise ValueError("DELEGATED_USER environment variable must be set")

        self._graph_client = GraphClient()

    def _get_pending_attachments(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get attachments that need processing."""
        conn = get_connection()
        cursor = conn.cursor()
        rows = cursor.execute(
            """
            SELECT a.id, a.email_id, a.filename, a.content_type, a.size_bytes
            FROM attachments a
            WHERE a.extraction_status = 'pending'
            ORDER BY a.size_bytes ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def _download_attachment(self, email_id: str, attachment_id: str) -> Optional[bytes]:
        """Download attachment content from Graph API."""
        try:
            assert self.user_email is not None
            headers = self._graph_client._get_headers()
            base_path = self._graph_client._get_base_path(self.user_email)
            url = f"{base_path}/messages/{email_id}/attachments/{attachment_id}/$value"

            resp = requests.get(url, headers=headers)
            if resp.ok:
                return resp.content
            else:
                logger.warning(
                    f"Failed to download attachment {attachment_id}: {resp.status_code}"
                )
                return None
        except Exception as e:
            logger.error(f"Error downloading attachment: {e}")
            return None

    def _extract_text_with_documents_cli(
        self, content: bytes, filename: str, content_type: str
    ) -> Optional[str]:
        """
        Extract text from attachment using aech-cli-documents.
        Falls back to simple text extraction for plain text files.
        """
        # For plain text, just decode
        if content_type in ("text/plain", "text/csv", "text/markdown"):
            try:
                return content.decode("utf-8", errors="replace")
            except Exception:
                return content.decode("latin-1", errors="replace")

        # For HTML, strip tags
        if content_type == "text/html":
            try:
                from html import unescape
                import re

                text = content.decode("utf-8", errors="replace")
                text = re.sub(r"<[^>]+>", " ", text)
                text = unescape(text)
                text = re.sub(r"\s+", " ", text).strip()
                return text
            except Exception as e:
                logger.warning(f"Failed to parse HTML: {e}")
                return None

        # For other types, use aech-cli-documents
        try:
            # Write to temp file
            suffix = Path(filename).suffix or ".bin"
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
                f.write(content)
                temp_path = f.name

            try:
                # Create temp output directory
                with tempfile.TemporaryDirectory() as output_dir:
                    # Call documents CLI (convert-to-markdown extracts text as markdown)
                    result = subprocess.run(
                        [
                            "aech-cli-documents",
                            "convert-to-markdown",
                            temp_path,
                            "--output-dir",
                            output_dir,
                        ],
                        capture_output=True,
                        text=True,
                        timeout=120,
                    )

                    # Check for output files first - onnxruntime warnings may cause
                    # non-zero exit even when extraction succeeds
                    output_files = list(Path(output_dir).glob("*.md"))
                    if output_files:
                        return output_files[0].read_text()

                    # Try txt files
                    output_files = list(Path(output_dir).glob("*.txt"))
                    if output_files:
                        return output_files[0].read_text()

                    # Only report failure if no output was produced
                    stderr = result.stderr
                    stdout = result.stdout
                    # Log actual error for debugging (filter onnxruntime noise)
                    clean_stderr = "\n".join(
                        line for line in stderr.split("\n")
                        if "onnxruntime" not in line.lower() and line.strip()
                    )
                    if clean_stderr:
                        logger.warning(f"Documents CLI error for {filename}: {clean_stderr}")
                    if "Error" in stdout or "Error" in stderr:
                        # Extract the actual error message
                        logger.warning(f"Documents CLI output for {filename}: {stdout}")

                    logger.warning(f"No output file from documents CLI for {filename}")
                    return None

            finally:
                # Clean up temp file
                Path(temp_path).unlink(missing_ok=True)

        except subprocess.TimeoutExpired:
            logger.warning(f"Documents CLI timeout for {filename}")
            return None
        except FileNotFoundError:
            logger.warning("aech-cli-documents not found, skipping extraction")
            return None
        except Exception as e:
            logger.error(f"Error extracting text from {filename}: {e}")
            return None

    def _update_attachment_status(
        self,
        attachment_id: str,
        status: str,
        extracted_text: Optional[str] = None,
        error: Optional[str] = None,
        content_hash: Optional[str] = None,
    ) -> None:
        """Update attachment extraction status in database."""
        conn = get_connection()
        conn.execute(
            """
            UPDATE attachments SET
                extraction_status = ?,
                extracted_text = ?,
                extraction_error = ?,
                content_hash = ?,
                extracted_at = datetime('now')
            WHERE id = ?
            """,
            (status, extracted_text, error, content_hash, attachment_id),
        )
        conn.commit()
        conn.close()

    def process_attachment(self, attachment: Dict[str, Any]) -> bool:
        """
        Process a single attachment: download, extract text, update DB.
        Returns True if successful.
        """
        att_id = attachment["id"]
        email_id = attachment["email_id"]
        filename = attachment["filename"] or "unknown"
        content_type = attachment["content_type"] or ""

        logger.info(f"Processing attachment: {filename} ({content_type})")

        # Skip common noise files (email signatures, logos)
        filename_lower = filename.lower()
        filename_stem = Path(filename).stem.lower()
        if any(pattern in filename_stem for pattern in SKIP_FILENAME_PATTERNS):
            self._update_attachment_status(
                att_id, "skipped", error=f"Filename matches skip pattern: {filename}"
            )
            logger.debug(f"Skipping {filename} - matches skip pattern")
            return False

        # Check if we can extract from this type
        if content_type not in EXTRACTABLE_TYPES:
            self._update_attachment_status(
                att_id, "skipped", error=f"Content type not supported: {content_type}"
            )
            return False

        # Download
        content = self._download_attachment(email_id, att_id)
        if content is None:
            self._update_attachment_status(att_id, "failed", error="Download failed")
            return False

        # Compute hash for dedup
        content_hash = hashlib.sha256(content).hexdigest()[:32]

        # Check if we already have this content
        conn = get_connection()
        existing = conn.execute(
            "SELECT id FROM attachments WHERE content_hash = ? AND id != ? AND extracted_text IS NOT NULL",
            (content_hash, att_id),
        ).fetchone()
        conn.close()

        if existing:
            # Copy text from existing attachment
            conn = get_connection()
            try:
                row = conn.execute(
                    "SELECT extracted_text FROM attachments WHERE id = ?", (existing["id"],)
                ).fetchone()
            finally:
                conn.close()
            if row:
                self._update_attachment_status(
                    att_id, "completed", extracted_text=row["extracted_text"], content_hash=content_hash
                )
                # ATOMIC: Create chunks and embeddings immediately
                self._index_attachment(att_id, filename)
                logger.info(f"Used cached extraction for {filename}")
                return True

        # Extract text
        extracted_text = self._extract_text_with_documents_cli(content, filename, content_type)

        if extracted_text:
            self._update_attachment_status(
                att_id, "completed", extracted_text=extracted_text, content_hash=content_hash
            )
            # ATOMIC: Create chunks and embeddings immediately
            self._index_attachment(att_id, filename)
            logger.info(f"Successfully extracted text from {filename} ({len(extracted_text)} chars)")
            return True
        else:
            self._update_attachment_status(
                att_id, "failed", error="Text extraction returned empty", content_hash=content_hash
            )
            return False

    def _index_attachment(self, attachment_id: str, filename: str) -> None:
        """
        Create chunks, embeddings, and extract facts from an attachment.
        This ensures content is fully searchable right away.
        """
        # 1. Create chunks and embeddings for text search
        try:
            from .chunker import create_attachment_chunks
            from .embeddings import embed_chunks_for_source

            chunks_created = create_attachment_chunks(attachment_id)
            if chunks_created > 0:
                embedded = embed_chunks_for_source("attachment", attachment_id)
                logger.debug(f"Indexed {filename}: {chunks_created} chunks, {embedded} embeddings")
        except Exception as e:
            # Don't fail the extraction if indexing fails - it can be retried
            logger.warning(f"Failed to index attachment {filename}: {e}")

        # 2. Extract structured facts (tax IDs, amounts, dates, etc.)
        try:
            facts_stored = self._extract_attachment_facts(attachment_id, filename)
            if facts_stored > 0:
                logger.info(f"Extracted {facts_stored} facts from {filename}")
        except Exception as e:
            # Don't fail if facts extraction fails - text search still works
            logger.warning(f"Failed to extract facts from {filename}: {e}")

    def _extract_attachment_facts(self, attachment_id: str, filename: str) -> int:
        """
        Extract structured facts from attachment text using LLM.

        Facts include: tax IDs, amounts, addresses, phone numbers, contract numbers, etc.
        These are stored in the unified facts table for structured search.
        """
        import asyncio

        # Get the extracted text
        conn = get_connection()
        row = conn.execute(
            "SELECT extracted_text FROM attachments WHERE id = ?",
            (attachment_id,)
        ).fetchone()
        conn.close()

        if not row or not row["extracted_text"]:
            return 0

        extracted_text = row["extracted_text"]

        # Skip very short documents (unlikely to have useful facts)
        if len(extracted_text.strip()) < 100:
            return 0

        from .facts import FactsExtractor
        extractor = FactsExtractor()

        # Run the async LLM extraction
        try:
            facts = asyncio.run(
                extractor.extract_from_attachment(attachment_id, extracted_text, filename)
            )
        except Exception as e:
            logger.warning(f"Facts extraction LLM call failed for {filename}: {e}")
            return 0

        if facts:
            stored = extractor.store_facts("attachment", attachment_id, facts)
            return stored

        return 0

    def process_pending_attachments(
        self,
        limit: int = 50,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, int]:
        """
        Process all pending attachments.
        Returns counts of completed/failed/skipped.

        Args:
            limit: Maximum number of attachments to process
            progress_callback: Optional callback(current, total, filename) for progress updates
        """
        attachments = self._get_pending_attachments(limit)
        total = len(attachments)
        logger.info(f"Processing {total} pending attachments")

        results = {"completed": 0, "failed": 0, "skipped": 0}

        for i, att in enumerate(attachments):
            filename = att.get("filename", "unknown")

            # Report progress before processing
            if progress_callback:
                progress_callback(i + 1, total, filename)

            try:
                success = self.process_attachment(att)
                if success:
                    results["completed"] += 1
                else:
                    # Check actual status to bucket correctly
                    conn = get_connection()
                    row = conn.execute(
                        "SELECT extraction_status FROM attachments WHERE id = ?",
                        (att["id"],),
                    ).fetchone()
                    conn.close()
                    if row and row["extraction_status"] == "skipped":
                        results["skipped"] += 1
                    else:
                        results["failed"] += 1
            except Exception as e:
                logger.error(f"Error processing attachment {att['id']}: {e}")
                self._update_attachment_status(att["id"], "failed", error=str(e))
                results["failed"] += 1

        logger.info(
            f"Attachment processing complete: {results['completed']} completed, "
            f"{results['failed']} failed, {results['skipped']} skipped"
        )
        return results

    async def process_pending_attachments_async(
        self,
        limit: int = 50,
        concurrency: int = 5,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, int]:
        """
        Process pending attachments concurrently.

        Args:
            limit: Maximum number of attachments to process
            concurrency: Number of concurrent workers
            progress_callback: Optional callback(current, total, filename) for progress updates
        """
        attachments = self._get_pending_attachments(limit)
        total = len(attachments)
        logger.info(f"Processing {total} pending attachments with concurrency={concurrency}")

        results = {"completed": 0, "failed": 0, "skipped": 0}
        processed_count = 0
        lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(concurrency)

        async def process_one(att: Dict[str, Any]) -> None:
            nonlocal processed_count
            filename = att.get("filename", "unknown")

            async with semaphore:
                # Report progress
                async with lock:
                    processed_count += 1
                    current = processed_count
                if progress_callback:
                    progress_callback(current, total, filename)

                try:
                    # Run synchronous processing in thread pool
                    success = await asyncio.to_thread(self.process_attachment, att)

                    async with lock:
                        if success:
                            results["completed"] += 1
                        else:
                            # Check actual status
                            conn = get_connection()
                            row = conn.execute(
                                "SELECT extraction_status FROM attachments WHERE id = ?",
                                (att["id"],),
                            ).fetchone()
                            conn.close()
                            if row and row["extraction_status"] == "skipped":
                                results["skipped"] += 1
                            else:
                                results["failed"] += 1
                except Exception as e:
                    logger.error(f"Error processing attachment {att['id']}: {e}")
                    self._update_attachment_status(att["id"], "failed", error=str(e))
                    async with lock:
                        results["failed"] += 1

        # Process all attachments concurrently
        await asyncio.gather(*[process_one(att) for att in attachments])

        logger.info(
            f"Attachment processing complete: {results['completed']} completed, "
            f"{results['failed']} failed, {results['skipped']} skipped"
        )
        return results

    def get_extraction_stats(self) -> Dict[str, Any]:
        """Get statistics about attachment extraction."""
        conn = get_connection()
        cursor = conn.cursor()

        stats = {}

        cursor.execute(
            """
            SELECT extraction_status, COUNT(*) as count
            FROM attachments
            GROUP BY extraction_status
            """
        )
        for row in cursor.fetchall():
            stats[row["extraction_status"] or "unknown"] = row["count"]

        cursor.execute("SELECT SUM(LENGTH(extracted_text)) FROM attachments WHERE extracted_text IS NOT NULL")
        result = cursor.fetchone()[0]
        stats["total_extracted_chars"] = result if result else 0

        conn.close()
        return stats
