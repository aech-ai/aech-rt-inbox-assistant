"""
Chunking and quote-stripping for Email Corpus Intelligence.

Key design decisions:
- Replies: Strip quoted content (recipient already has it)
- Forwards: Parse into "virtual" emails (content is NEW to recipient)
- Attachments: Chunk only if >2000 chars
- Preserve conversation threading via metadata
"""

import json
import logging
import re
from dataclasses import dataclass
from typing import Dict, List, Optional

from .database import get_connection

logger = logging.getLogger(__name__)

# ============================================================================
# Forward Detection
# ============================================================================

FORWARD_SUBJECT_PATTERNS = [
    re.compile(r"^(Fwd?|FW|Forwarded):\s*", re.IGNORECASE),
]

FORWARD_BODY_MARKERS = [
    re.compile(r"-{3,}\s*Forwarded message\s*-{3,}", re.IGNORECASE),
    re.compile(r"-{3,}\s*Original Message\s*-{3,}", re.IGNORECASE),
    re.compile(r"Begin forwarded message:", re.IGNORECASE),
]

# Pattern to extract headers from forwarded message blocks
# Matches: "From: ... Sent: ... To: ... Subject: ..."
FORWARDED_HEADER_PATTERN = re.compile(
    r"(?:^|\n)"
    r"(?:From:\s*(?P<from>.+?)(?:\n|$))?"
    r"(?:Sent:\s*(?P<sent>.+?)(?:\n|$))?"
    r"(?:Date:\s*(?P<date>.+?)(?:\n|$))?"
    r"(?:To:\s*(?P<to>.+?)(?:\n|$))?"
    r"(?:Cc:\s*(?P<cc>.+?)(?:\n|$))?"
    r"(?:Subject:\s*(?P<subject>.+?)(?:\n|$))?",
    re.IGNORECASE | re.MULTILINE
)

# Gmail-style forwarded header
GMAIL_FORWARD_HEADER = re.compile(
    r"On\s+(?P<date>[^,]+),\s*(?P<from>.+?)\s+wrote:",
    re.IGNORECASE
)

# ============================================================================
# Reply Detection (for stripping)
# ============================================================================

REPLY_QUOTE_PATTERNS = [
    # "On <date>, <name> wrote:" patterns (but NOT at start of forward)
    re.compile(r"^On .+ wrote:?\s*$", re.MULTILINE | re.IGNORECASE),
    # ">..." line-by-line quotes (3+ consecutive)
    re.compile(r"(^>.*\n){3,}", re.MULTILINE),
    # "Sent from my iPhone/Android" signatures
    re.compile(r"^Sent from my (iPhone|Android|iPad|Galaxy).*$", re.MULTILINE | re.IGNORECASE),
    # Outlook-style separators
    re.compile(r"^_{3,}\s*$", re.MULTILINE),
]

# Minimum length for a "useful" chunk
MIN_CHUNK_LENGTH = 50

# Target chunk size for documents (only used for attachments)
DOCUMENT_CHUNK_SIZE = 1500
DOCUMENT_CHUNK_OVERLAP = 200


@dataclass
class VirtualEmail:
    """A message extracted from a forwarded email chain."""
    sender: Optional[str]
    recipients: Optional[str]
    date: Optional[str]
    subject: Optional[str]
    body: str
    source_email_id: str  # The forwarding email that contained this
    position: int  # Position in the chain (0 = most recent forwarded)


def is_forward(subject: str, body: str) -> bool:
    """Detect if an email is a forward (vs a reply)."""
    # Check subject
    for pattern in FORWARD_SUBJECT_PATTERNS:
        if pattern.search(subject or ""):
            return True

    # Check body for forward markers
    for pattern in FORWARD_BODY_MARKERS:
        if pattern.search(body or ""):
            return True

    return False


def parse_forwarded_chain(body: str, source_email_id: str) -> List[VirtualEmail]:
    """
    Parse a forwarded email to extract individual messages from the chain.
    Returns list of VirtualEmail objects, ordered from newest to oldest in chain.
    """
    if not body:
        return []

    virtual_emails = []

    # Split on forward markers
    split_patterns = [
        r"-{3,}\s*Forwarded message\s*-{3,}",
        r"-{3,}\s*Original Message\s*-{3,}",
        r"Begin forwarded message:",
    ]

    # Combine patterns
    combined = "|".join(f"({p})" for p in split_patterns)
    parts = re.split(combined, body, flags=re.IGNORECASE)

    # Filter out None and empty parts
    parts = [p for p in parts if p and p.strip() and not re.match(combined, p, re.IGNORECASE)]

    for i, part in enumerate(parts):
        # Try to extract headers from this part
        headers = extract_headers_from_block(part)

        # Get the body (everything after headers)
        body_text = remove_headers_from_block(part)

        if body_text and len(body_text.strip()) >= MIN_CHUNK_LENGTH:
            virtual_emails.append(VirtualEmail(
                sender=headers.get("from"),
                recipients=headers.get("to"),
                date=headers.get("date") or headers.get("sent"),
                subject=headers.get("subject"),
                body=body_text.strip(),
                source_email_id=source_email_id,
                position=i,
            ))

    return virtual_emails


def extract_headers_from_block(text: str) -> Dict[str, str]:
    """Extract email headers from a text block."""
    headers = {}

    # Try Outlook-style headers
    match = FORWARDED_HEADER_PATTERN.search(text[:500])  # Headers usually at start
    if match:
        for key in ["from", "sent", "date", "to", "cc", "subject"]:
            value = match.group(key)
            if value:
                headers[key] = value.strip()

    # Try Gmail-style "On date, person wrote:"
    if not headers.get("from"):
        gmail_match = GMAIL_FORWARD_HEADER.search(text[:500])
        if gmail_match:
            headers["from"] = gmail_match.group("from").strip()
            headers["date"] = gmail_match.group("date").strip()

    return headers


def remove_headers_from_block(text: str) -> str:
    """Remove header lines from start of text block to get just the body."""
    lines = text.split("\n")
    body_start = 0

    # Skip header-like lines at the start
    header_prefixes = ("from:", "to:", "cc:", "sent:", "date:", "subject:", ">")

    for i, line in enumerate(lines):
        line_lower = line.lower().strip()
        if any(line_lower.startswith(p) for p in header_prefixes):
            body_start = i + 1
        elif line.strip() == "":
            # Empty line often separates headers from body
            if body_start > 0:
                body_start = i + 1
                break
        elif body_start > 0:
            # We've found headers and now hit content
            break

    return "\n".join(lines[body_start:]).strip()


def strip_quoted_replies(text: str) -> str:
    """
    Strip quoted replies from email body, keeping only the new content.
    Returns the cleaned text.
    """
    if not text:
        return ""

    # Find the earliest quote marker
    earliest_pos = len(text)

    for pattern in REPLY_QUOTE_PATTERNS:
        match = pattern.search(text)
        if match and match.start() < earliest_pos:
            earliest_pos = match.start()

    # Take text before the quote
    clean_text = text[:earliest_pos].strip()

    # If we stripped too much (less than 50 chars), fall back to original
    if len(clean_text) < MIN_CHUNK_LENGTH and len(text) > MIN_CHUNK_LENGTH:
        # Try to find a sensible cutoff by looking for double newlines
        # followed by quote-like content
        parts = text.split("\n\n")
        clean_parts = []
        for part in parts:
            # Stop if this part looks like a quote start
            if any(p.match(part.strip()) for p in REPLY_QUOTE_PATTERNS):
                break
            clean_parts.append(part)
        clean_text = "\n\n".join(clean_parts).strip()

    # Final fallback: if still too short, use first 2000 chars of original
    if len(clean_text) < MIN_CHUNK_LENGTH and len(text) > MIN_CHUNK_LENGTH:
        clean_text = text[:2000].strip()

    return clean_text


def chunk_document(text: str, chunk_size: int = DOCUMENT_CHUNK_SIZE, overlap: int = DOCUMENT_CHUNK_OVERLAP) -> List[str]:
    """
    Split a long document into overlapping chunks.
    Tries to break at paragraph/sentence boundaries.
    """
    if not text or len(text) <= chunk_size:
        return [text] if text else []

    chunks = []
    start = 0

    while start < len(text):
        end = start + chunk_size

        # If not at the end, try to find a good break point
        if end < len(text):
            # Look for paragraph break
            para_break = text.rfind("\n\n", start + chunk_size // 2, end)
            if para_break > start:
                end = para_break

            else:
                # Look for sentence end
                for sep in [". ", ".\n", "! ", "!\n", "? ", "?\n"]:
                    sent_break = text.rfind(sep, start + chunk_size // 2, end)
                    if sent_break > start:
                        end = sent_break + 1  # Include the period
                        break

        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)

        # Move start with overlap
        start = end - overlap
        if start <= 0 or (end >= len(text) and start < end):
            break
        if start >= len(text):
            break

    return chunks


def generate_chunk_id(source_type: str, source_id: str, chunk_index: int) -> str:
    """Generate a stable, unique chunk ID."""
    return f"{source_type}:{source_id}:{chunk_index}"


@dataclass
class ProcessedEmail:
    """Result of processing an email for indexing."""
    email_id: str
    conversation_id: Optional[str]
    subject: str
    sender: str
    received_at: str
    clean_body: str
    original_length: int
    clean_length: int
    is_forward: bool
    virtual_emails: List[VirtualEmail]


def process_email_for_indexing(email_id: str) -> Optional[ProcessedEmail]:
    """
    Process an email for search indexing.

    Requires body_markdown (parsed from HTML during ingestion).
    Returns None if email not found or has no body_markdown.
    """
    conn = get_connection()
    row = conn.execute(
        """
        SELECT id, conversation_id, subject, sender, received_at,
               body_markdown, body_preview
        FROM emails WHERE id = ?
        """,
        (email_id,),
    ).fetchone()
    conn.close()

    if not row:
        return None

    # Use body_markdown (parsed from HTML)
    clean_body = row["body_markdown"]
    if not clean_body:
        logger.debug(f"Email {email_id} has no body_markdown - skipping")
        return None

    subject = row["subject"] or ""

    if len(clean_body) < MIN_CHUNK_LENGTH:
        logger.debug(f"Email {email_id} has insufficient content")
        return None

    return ProcessedEmail(
        email_id=email_id,
        conversation_id=row["conversation_id"],
        subject=subject,
        sender=row["sender"],
        received_at=row["received_at"],
        clean_body=clean_body,
        original_length=len(row["body_preview"] or ""),
        clean_length=len(clean_body),
        is_forward=False,
        virtual_emails=[],
    )


def create_email_chunk(email_data: ProcessedEmail) -> int:
    """
    Create chunk entries for an email and its virtual emails (from forwards).
    Applies document chunking for large email bodies.
    Returns total number of chunks created.
    """
    chunks_created = 0
    conn = get_connection()
    next_chunk_index = 0  # Track next available chunk index

    # Create chunks for the main email content (if any)
    if email_data.clean_body and len(email_data.clean_body) >= MIN_CHUNK_LENGTH:
        # Split large email bodies into chunks (same as attachments)
        if len(email_data.clean_body) > DOCUMENT_CHUNK_SIZE:
            body_chunks = chunk_document(email_data.clean_body)
        else:
            body_chunks = [email_data.clean_body]

        for chunk_idx, chunk_text in enumerate(body_chunks):
            chunk_id = generate_chunk_id("email", email_data.email_id, chunk_idx)

            metadata = {
                "conversation_id": email_data.conversation_id,
                "subject": email_data.subject,
                "sender": email_data.sender,
                "received_at": email_data.received_at,
                "is_forward": email_data.is_forward,
                "chunk_of": len(body_chunks),  # Track total chunks for this email
            }

            conn.execute(
                """
                INSERT INTO chunks (id, source_type, source_id, chunk_index, content,
                                   char_offset_start, char_offset_end, metadata_json)
                VALUES (?, 'email', ?, ?, ?, 0, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    content = excluded.content,
                    char_offset_end = excluded.char_offset_end,
                    metadata_json = excluded.metadata_json
                """,
                (
                    chunk_id,
                    email_data.email_id,
                    chunk_idx,
                    chunk_text,
                    len(chunk_text),
                    json.dumps(metadata),
                ),
            )
            chunks_created += 1

        next_chunk_index = len(body_chunks)

    # Create chunks for virtual emails (extracted from forwards)
    for virtual in email_data.virtual_emails:
        # Use chunk_index offset so they cascade-delete with the parent email
        chunk_index = next_chunk_index + virtual.position
        chunk_id = generate_chunk_id("email", email_data.email_id, chunk_index)

        metadata = {
            "source_email_id": email_data.email_id,
            "position_in_chain": virtual.position,
            "extracted_sender": virtual.sender,
            "extracted_recipients": virtual.recipients,
            "extracted_date": virtual.date,
            "extracted_subject": virtual.subject,
            "virtual_id": f"virtual:{email_data.email_id}:{virtual.position}",
            # Link to parent conversation if available
            "parent_conversation_id": email_data.conversation_id,
            "is_virtual": True,
        }

        conn.execute(
            """
            INSERT INTO chunks (id, source_type, source_id, chunk_index, content,
                               char_offset_start, char_offset_end, metadata_json)
            VALUES (?, 'email', ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                content = excluded.content,
                char_offset_end = excluded.char_offset_end,
                metadata_json = excluded.metadata_json
            """,
            (
                chunk_id,
                email_data.email_id,
                chunk_index,
                virtual.body,
                0,
                len(virtual.body),
                json.dumps(metadata),
            ),
        )
        chunks_created += 1

    conn.commit()
    conn.close()

    if email_data.virtual_emails:
        logger.info(
            f"Created {chunks_created} chunks for email {email_data.email_id} "
            f"(1 main + {len(email_data.virtual_emails)} virtual from forward)"
        )

    return chunks_created


def create_email_chunks(email_id: str) -> int:
    """
    Create chunks for a single email.
    Wrapper that processes and chunks in one call.
    Returns number of chunks created.
    """
    email_data = process_email_for_indexing(email_id)
    if not email_data:
        return 0
    return create_email_chunk(email_data)


def create_attachment_chunks(attachment_id: str) -> int:
    """
    Create chunks for an attachment's extracted text.
    Only chunks if text is longer than DOCUMENT_CHUNK_SIZE.
    Returns number of chunks created.
    """
    conn = get_connection()
    row = conn.execute(
        """
        SELECT a.id, a.email_id, a.filename, a.extracted_text, e.conversation_id, e.received_at
        FROM attachments a
        LEFT JOIN emails e ON a.email_id = e.id
        WHERE a.id = ? AND a.extracted_text IS NOT NULL
        """,
        (attachment_id,),
    ).fetchone()
    conn.close()

    if not row or not row["extracted_text"]:
        return 0

    text = row["extracted_text"]

    # Decide whether to chunk
    if len(text) <= DOCUMENT_CHUNK_SIZE:
        chunks = [text]
    else:
        chunks = chunk_document(text)

    conn = get_connection()

    # Delete existing chunks for this attachment
    conn.execute("DELETE FROM chunks WHERE source_type = 'attachment' AND source_id = ?", (attachment_id,))

    for i, chunk_text in enumerate(chunks):
        chunk_id = generate_chunk_id("attachment", attachment_id, i)

        metadata = {
            "email_id": row["email_id"],
            "conversation_id": row["conversation_id"],
            "filename": row["filename"],
            "received_at": row["received_at"],
            "chunk_of": len(chunks),
        }

        conn.execute(
            """
            INSERT INTO chunks (id, source_type, source_id, chunk_index, content, metadata_json)
            VALUES (?, 'attachment', ?, ?, ?, ?)
            """,
            (chunk_id, attachment_id, i, chunk_text, json.dumps(metadata)),
        )

    conn.commit()
    conn.close()

    return len(chunks)


def process_unindexed_emails(limit: int = 100) -> Dict[str, int]:
    """
    Process emails that haven't been chunked yet.
    Returns counts of processed/skipped.
    """
    conn = get_connection()

    # Find emails with body_markdown that don't have chunks yet
    rows = conn.execute(
        """
        SELECT e.id FROM emails e
        WHERE e.body_markdown IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM chunks c WHERE c.source_type = 'email' AND c.source_id = e.id
          )
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    results = {"processed": 0, "skipped": 0, "chunks_created": 0, "virtual_emails": 0}

    for row in rows:
        email_data = process_email_for_indexing(row["id"])
        if email_data:
            chunks = create_email_chunk(email_data)
            results["processed"] += 1
            results["chunks_created"] += chunks
            results["virtual_emails"] += len(email_data.virtual_emails)
        else:
            results["skipped"] += 1

    logger.info(
        f"Email chunking: {results['processed']} processed, {results['skipped']} skipped, "
        f"{results['chunks_created']} chunks ({results['virtual_emails']} from forwards)"
    )
    return results


def process_unindexed_attachments(limit: int = 100) -> Dict[str, int]:
    """
    Process attachments that have extracted text but no chunks.
    """
    conn = get_connection()

    rows = conn.execute(
        """
        SELECT a.id FROM attachments a
        WHERE a.extraction_status = 'completed'
          AND a.extracted_text IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM chunks c WHERE c.source_type = 'attachment' AND c.source_id = a.id
          )
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    results = {"processed": 0, "chunks_created": 0}

    for row in rows:
        num_chunks = create_attachment_chunks(row["id"])
        if num_chunks > 0:
            results["processed"] += 1
            results["chunks_created"] += num_chunks

    logger.info(
        f"Attachment chunking: {results['processed']} attachments, "
        f"{results['chunks_created']} chunks created"
    )
    return results
