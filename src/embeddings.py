"""
Embedding generation for Email Corpus Intelligence.

Supports either local sentence-transformers models or an OpenAI-compatible
embeddings endpoint such as LM Studio. Embeddings are stored as BLOBs in
SQLite for vector similarity search.
"""

import json
import logging
import os
import struct
from typing import Any, Callable, Dict, List, Optional

from .database import get_connection

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "BAAI/bge-m3"
DEFAULT_BACKEND = "sentence-transformers"

# Lazy-loaded providers and dimension
_model = None
_model_config = None
_client = None
_client_config = None
_embedding_dim = None


def _get_backend() -> str:
    backend = os.getenv("EMBEDDING_BACKEND", "").strip().lower()
    if not backend and os.getenv("EMBEDDING_BASE_URL", "").strip():
        return "openai-compatible"
    if backend in {"lmstudio", "openai", "openai-compatible", "openai_compatible"}:
        return "openai-compatible"
    if not backend:
        return DEFAULT_BACKEND
    return backend


def _get_model_name() -> str:
    return os.getenv("EMBEDDING_MODEL", DEFAULT_MODEL)


def _get_batch_size() -> int:
    return int(os.getenv("EMBEDDING_BATCH_SIZE", "8"))


def _get_base_url() -> str:
    return os.getenv("EMBEDDING_BASE_URL", "").strip()


def _get_api_key() -> str:
    return os.getenv("EMBEDDING_API_KEY", "").strip() or "lm-studio"


def _get_timeout_seconds() -> float:
    return float(os.getenv("EMBEDDING_TIMEOUT_SECONDS", "60"))


def _pack_embedding(embedding: List[float]) -> bytes:
    return struct.pack(f"{len(embedding)}f", *embedding)


def get_model():
    """Lazy-load the sentence transformer model."""
    global _model, _model_config, _embedding_dim
    model_name = _get_model_name()
    config = (_get_backend(), model_name)
    if _model is None or _model_config != config:
        try:
            from sentence_transformers import SentenceTransformer

            logger.info("Loading sentence-transformers embedding model: %s", model_name)
            _model = SentenceTransformer(model_name, trust_remote_code=True)
            _model_config = config

            # Auto-detect embedding dimension
            _embedding_dim = _model.get_sentence_embedding_dimension()
            logger.info("Embedding model loaded successfully (dim=%s)", _embedding_dim)
        except ImportError:
            logger.error(
                "sentence-transformers not installed. "
                "Install with: pip install sentence-transformers"
            )
            raise
    return _model


def _get_openai_client():
    """Lazy-load an OpenAI-compatible embeddings client."""
    global _client, _client_config
    base_url = _get_base_url()
    if not base_url:
        raise RuntimeError(
            "EMBEDDING_BASE_URL is required for the OpenAI-compatible embedding backend"
        )

    config = (_get_backend(), base_url, _get_api_key(), _get_timeout_seconds())
    if _client is None or _client_config != config:
        from openai import OpenAI

        logger.info(
            "Connecting to OpenAI-compatible embedding backend at %s with model %s",
            base_url,
            _get_model_name(),
        )
        _client = OpenAI(
            base_url=base_url,
            api_key=_get_api_key(),
            timeout=_get_timeout_seconds(),
        )
        _client_config = config

    return _client


def _encode_openai_compatible(texts: List[str]) -> List[bytes]:
    global _embedding_dim

    client = _get_openai_client()
    response = client.embeddings.create(model=_get_model_name(), input=texts)
    ordered = [item.embedding for item in sorted(response.data, key=lambda item: item.index)]

    if ordered and _embedding_dim is None:
        _embedding_dim = len(ordered[0])
        logger.info("Detected embedding dimension from OpenAI-compatible backend: %s", _embedding_dim)

    return [_pack_embedding(embedding) for embedding in ordered]


def get_embedding_dim() -> int:
    """Get the embedding dimension (loads model if needed)."""
    global _embedding_dim
    if _embedding_dim is None:
        backend = _get_backend()
        if backend == "sentence-transformers":
            get_model()
        elif backend == "openai-compatible":
            _encode_openai_compatible(["dimension probe"])
        else:
            raise ValueError(f"Unsupported embedding backend: {backend}")
    return _embedding_dim or 0


def encode_text(text: str) -> bytes:
    """
    Encode text to embedding vector and serialize to bytes.
    Returns the embedding as a binary blob (float32 array).
    """
    backend = _get_backend()
    if backend == "sentence-transformers":
        model = get_model()
        embedding = model.encode(text, convert_to_numpy=True)
        return _pack_embedding(embedding)
    if backend == "openai-compatible":
        return _encode_openai_compatible([text])[0]
    raise ValueError(f"Unsupported embedding backend: {backend}")


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

    backend = _get_backend()
    if backend == "sentence-transformers":
        model = get_model()
        embeddings = model.encode(texts, convert_to_numpy=True, batch_size=_get_batch_size())
        return [_pack_embedding(embedding) for embedding in embeddings]
    if backend == "openai-compatible":
        return _encode_openai_compatible(texts)
    raise ValueError(f"Unsupported embedding backend: {backend}")


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


def embed_chunks_for_source(source_type: str, source_id: str, enrich: bool = True) -> int:
    """
    Generate embeddings for all chunks belonging to a specific source.
    Used for atomic indexing - call immediately after creating chunks.

    Args:
        source_type: 'email' or 'attachment'
        source_id: The email or attachment ID

    Returns:
        Number of chunks embedded
    """
    conn = get_connection()

    # Get chunks for this source that need embeddings
    rows = conn.execute(
        """
        SELECT
            c.id, c.content, c.metadata_json,
            e.subject as email_subject, e.sender as email_sender, e.received_at,
            a.filename as attachment_filename,
            ae.subject as attachment_email_subject, ae.sender as attachment_email_sender
        FROM chunks c
        LEFT JOIN emails e ON c.source_type = 'email' AND c.source_id = e.id
        LEFT JOIN attachments a ON c.source_type = 'attachment' AND c.source_id = a.id
        LEFT JOIN emails ae ON a.email_id = ae.id
        WHERE c.source_type = ? AND c.source_id = ? AND c.embedding IS NULL
        """,
        (source_type, source_id),
    ).fetchall()
    conn.close()

    if not rows:
        return 0

    # Prepare texts for batch embedding
    chunk_ids = []
    texts = []

    for row in rows:
        chunk_ids.append(row["id"])
        content = row["content"] or ""

        if enrich:
            if source_type == "email":
                text = prepare_email_text_for_embedding(
                    content=content,
                    subject=row["email_subject"],
                    sender=row["email_sender"],
                    received_at=row["received_at"],
                )
            elif source_type == "attachment":
                text = prepare_attachment_text_for_embedding(
                    content=content,
                    filename=row["attachment_filename"],
                    email_subject=row["attachment_email_subject"],
                    email_sender=row["attachment_email_sender"],
                )
            else:
                # Virtual emails - use metadata
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

        texts.append(text)

    # Generate embeddings
    try:
        embeddings = encode_batch(texts)
    except Exception as e:
        logger.error(f"Embedding generation failed for {source_type}:{source_id}: {e}")
        return 0

    # Store embeddings
    conn = get_connection()
    embedded = 0
    for chunk_id, embedding in zip(chunk_ids, embeddings):
        try:
            conn.execute(
                "UPDATE chunks SET embedding = ? WHERE id = ?",
                (embedding, chunk_id),
            )
            embedded += 1
        except Exception as e:
            logger.error(f"Failed to store embedding for {chunk_id}: {e}")

    conn.commit()
    conn.close()

    logger.debug(f"Embedded {embedded} chunks for {source_type}:{source_id}")
    return embedded


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
