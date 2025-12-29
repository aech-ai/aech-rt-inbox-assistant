"""
Embedding generation for Email Corpus Intelligence.

Uses sentence-transformers with configurable model (default: BAAI/bge-m3).
Embeddings are stored as BLOBs in SQLite for vector similarity search.

bge-m3 features:
- 8192 token context (handles long attachment chunks)
- Multi-lingual support
- Strong retrieval performance
"""

import json
import logging
import os
import struct
from typing import Any, Callable, Dict, List, Optional

from .database import get_connection

logger = logging.getLogger(__name__)

# Model configuration - configurable via environment
DEFAULT_MODEL = "BAAI/bge-m3"
MODEL_NAME = os.getenv("EMBEDDING_MODEL", DEFAULT_MODEL)
BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "8"))  # Lower default for memory efficiency

# Lazy-loaded model and dimension
_model = None
_embedding_dim = None


def get_model():
    """Lazy-load the sentence transformer model."""
    global _model, _embedding_dim
    if _model is None:
        try:
            from sentence_transformers import SentenceTransformer

            logger.info(f"Loading embedding model: {MODEL_NAME}")
            _model = SentenceTransformer(MODEL_NAME, trust_remote_code=True)

            # Auto-detect embedding dimension
            _embedding_dim = _model.get_sentence_embedding_dimension()
            logger.info(f"Model loaded successfully (dim={_embedding_dim})")
        except ImportError:
            logger.error(
                "sentence-transformers not installed. "
                "Install with: pip install sentence-transformers"
            )
            raise
    return _model


def get_embedding_dim() -> int:
    """Get the embedding dimension (loads model if needed)."""
    global _embedding_dim
    if _embedding_dim is None:
        get_model()
    return _embedding_dim or 0


def encode_text(text: str) -> bytes:
    """
    Encode text to embedding vector and serialize to bytes.
    Returns the embedding as a binary blob (float32 array).
    """
    model = get_model()
    embedding = model.encode(text, convert_to_numpy=True)

    # Serialize to bytes (float32)
    return struct.pack(f"{len(embedding)}f", *embedding)


def decode_embedding(blob: bytes) -> List[float]:
    """Deserialize embedding from bytes back to float list."""
    num_floats = len(blob) // 4
    return list(struct.unpack(f"{num_floats}f", blob))


def encode_batch(texts: List[str]) -> List[bytes]:
    """
    Encode multiple texts to embeddings in a batch.
    More efficient than encoding one at a time.
    """
    if not texts:
        return []

    model = get_model()
    embeddings = model.encode(texts, convert_to_numpy=True, batch_size=BATCH_SIZE)

    return [struct.pack(f"{len(emb)}f", *emb) for emb in embeddings]


def cosine_similarity(a: bytes, b: bytes) -> float:
    """Compute cosine similarity between two embedding blobs."""
    vec_a = decode_embedding(a)
    vec_b = decode_embedding(b)

    dot = sum(x * y for x, y in zip(vec_a, vec_b))
    norm_a = sum(x * x for x in vec_a) ** 0.5
    norm_b = sum(x * x for x in vec_b) ** 0.5

    if norm_a == 0 or norm_b == 0:
        return 0.0

    return dot / (norm_a * norm_b)


def prepare_email_text_for_embedding(
    content: str,
    subject: Optional[str] = None,
    sender: Optional[str] = None,
    received_at: Optional[str] = None,
) -> str:
    """
    Prepare email content for embedding by enriching with metadata.

    Enriched format improves retrieval quality by including searchable
    context that users might query (subject, sender, date).
    """
    parts = []

    if subject:
        parts.append(f"Subject: {subject}")
    if sender:
        # Extract name from "Name <email>" format if present
        sender_display = sender.split("<")[0].strip() if "<" in sender else sender
        parts.append(f"From: {sender_display}")
    if received_at:
        # Just the date portion
        date_part = received_at.split("T")[0] if "T" in received_at else received_at
        parts.append(f"Date: {date_part}")

    if parts:
        parts.append("")  # Blank line before content

    parts.append(content)

    return "\n".join(parts)


def prepare_attachment_text_for_embedding(
    content: str,
    filename: Optional[str] = None,
    email_subject: Optional[str] = None,
    email_sender: Optional[str] = None,
) -> str:
    """
    Prepare attachment content for embedding by enriching with metadata.
    """
    parts = []

    if filename:
        parts.append(f"Attachment: {filename}")
    if email_subject:
        parts.append(f"From email: {email_subject}")
    if email_sender:
        sender_display = email_sender.split("<")[0].strip() if "<" in email_sender else email_sender
        parts.append(f"Sender: {sender_display}")

    if parts:
        parts.append("")  # Blank line before content

    parts.append(content)

    return "\n".join(parts)


def embed_chunk(chunk_id: str) -> bool:
    """
    Generate and store embedding for a single chunk.
    Returns True if successful.
    """
    conn = get_connection()
    row = conn.execute(
        "SELECT content FROM chunks WHERE id = ? AND embedding IS NULL",
        (chunk_id,),
    ).fetchone()

    if not row:
        conn.close()
        return False

    try:
        embedding = encode_text(row["content"])

        conn.execute(
            "UPDATE chunks SET embedding = ? WHERE id = ?",
            (embedding, chunk_id),
        )
        conn.commit()
        conn.close()
        return True

    except Exception as e:
        logger.error(f"Error embedding chunk {chunk_id}: {e}")
        conn.close()
        return False


def embed_pending_chunks(
    limit: int = 1000,
    enrich: bool = True,
    batch_size: int = 50,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Dict[str, int]:
    """
    Generate embeddings for chunks that don't have them yet.
    Uses batch processing for efficiency with progress reporting.

    Args:
        limit: Maximum number of chunks to process
        enrich: If True, enrich text with email/attachment metadata for better retrieval
        batch_size: Number of chunks to process per batch (for progress updates)
        progress_callback: Optional callback(processed, total) for progress updates
    """
    conn = get_connection()

    # Get chunks without embeddings, with source metadata
    rows = conn.execute(
        """
        SELECT
            c.id, c.content, c.source_type, c.source_id, c.metadata_json,
            e.subject as email_subject, e.sender as email_sender, e.received_at,
            a.filename as attachment_filename,
            ae.subject as attachment_email_subject, ae.sender as attachment_email_sender
        FROM chunks c
        LEFT JOIN emails e ON c.source_type = 'email' AND c.source_id = e.id
        LEFT JOIN attachments a ON c.source_type = 'attachment' AND c.source_id = a.id
        LEFT JOIN emails ae ON a.email_id = ae.id
        WHERE c.embedding IS NULL
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    if not rows:
        return {"processed": 0, "failed": 0, "total_pending": 0}

    # Check total pending for progress reporting
    conn = get_connection()
    total_pending = conn.execute(
        "SELECT COUNT(*) FROM chunks WHERE embedding IS NULL"
    ).fetchone()[0]
    conn.close()

    # Prepare all texts first
    all_chunk_ids = []
    all_texts = []

    for row in rows:
        all_chunk_ids.append(row["id"])
        content = row["content"] or ""

        if enrich:
            if row["source_type"] == "email":
                text = prepare_email_text_for_embedding(
                    content=content,
                    subject=row["email_subject"],
                    sender=row["email_sender"],
                    received_at=row["received_at"],
                )
            elif row["source_type"] == "attachment":
                text = prepare_attachment_text_for_embedding(
                    content=content,
                    filename=row["attachment_filename"],
                    email_subject=row["attachment_email_subject"],
                    email_sender=row["attachment_email_sender"],
                )
            else:
                # Virtual emails or other types
                metadata = {}
                if row["metadata_json"]:
                    try:
                        metadata = json.loads(row["metadata_json"])
                    except json.JSONDecodeError:
                        pass

                text = prepare_email_text_for_embedding(
                    content=content,
                    subject=metadata.get("extracted_subject"),
                    sender=metadata.get("extracted_sender"),
                    received_at=metadata.get("extracted_date"),
                )
        else:
            text = content

        all_texts.append(text)

    total_to_process = len(all_texts)
    logger.info(f"Generating embeddings for {total_to_process} chunks ({total_pending} total pending)")

    success = 0
    failed = 0

    # Process in batches with progress updates
    for batch_start in range(0, total_to_process, batch_size):
        batch_end = min(batch_start + batch_size, total_to_process)
        batch_ids = all_chunk_ids[batch_start:batch_end]
        batch_texts = all_texts[batch_start:batch_end]

        try:
            embeddings = encode_batch(batch_texts)
        except Exception as e:
            logger.error(f"Batch embedding failed: {e}")
            failed += len(batch_ids)
            continue

        # Store embeddings
        conn = get_connection()
        for chunk_id, embedding in zip(batch_ids, embeddings):
            try:
                conn.execute(
                    "UPDATE chunks SET embedding = ? WHERE id = ?",
                    (embedding, chunk_id),
                )
                success += 1
            except Exception as e:
                logger.error(f"Failed to store embedding for {chunk_id}: {e}")
                failed += 1

        conn.commit()
        conn.close()

        # Report progress
        if progress_callback:
            progress_callback(batch_end, total_to_process)

    logger.info(f"Embedding complete: {success} success, {failed} failed")
    return {
        "processed": success,
        "failed": failed,
        "total_pending": total_pending - success,
    }


def search_by_similarity(query: str, limit: int = 20, min_score: float = 0.3) -> List[Dict[str, Any]]:
    """
    Search chunks by semantic similarity to query.
    Returns list of results with scores.
    """
    query_embedding = encode_text(query)

    conn = get_connection()

    # Get all chunks with embeddings
    rows = conn.execute(
        """
        SELECT id, source_type, source_id, chunk_index, content, metadata_json, embedding
        FROM chunks
        WHERE embedding IS NOT NULL
        """
    ).fetchall()
    conn.close()

    # Compute similarities
    results = []
    for row in rows:
        if not row["embedding"]:
            continue

        score = cosine_similarity(query_embedding, row["embedding"])

        if score >= min_score:
            results.append({
                "chunk_id": row["id"],
                "source_type": row["source_type"],
                "source_id": row["source_id"],
                "chunk_index": row["chunk_index"],
                "content": row["content"][:500],  # Preview
                "score": score,
                "metadata": row["metadata_json"],
            })

    # Sort by score descending
    results.sort(key=lambda x: x["score"], reverse=True)

    return results[:limit]


def get_embedding_stats() -> Dict[str, Any]:
    """Get statistics about embeddings."""
    conn = get_connection()
    cursor = conn.cursor()

    stats = {}

    cursor.execute("SELECT COUNT(*) FROM chunks WHERE embedding IS NOT NULL")
    stats["chunks_with_embeddings"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM chunks WHERE embedding IS NULL")
    stats["chunks_without_embeddings"] = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT source_type, COUNT(*) as count
        FROM chunks
        WHERE embedding IS NOT NULL
        GROUP BY source_type
        """
    )
    stats["by_source_type"] = {row["source_type"]: row["count"] for row in cursor.fetchall()}

    stats["model"] = MODEL_NAME

    conn.close()
    return stats
