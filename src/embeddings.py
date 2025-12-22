"""
Embedding generation for Email Corpus Intelligence.

Uses sentence-transformers (all-MiniLM-L6-v2) for local embedding generation.
Embeddings are stored as BLOBs in SQLite for vector similarity search.
"""

import logging
import struct
from typing import Any, Dict, List, Optional

from .database import get_connection

logger = logging.getLogger(__name__)

# Model configuration
MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
BATCH_SIZE = 32

# Lazy-loaded model
_model = None


def get_model():
    """Lazy-load the sentence transformer model."""
    global _model
    if _model is None:
        try:
            from sentence_transformers import SentenceTransformer

            logger.info(f"Loading embedding model: {MODEL_NAME}")
            _model = SentenceTransformer(MODEL_NAME)
            logger.info(f"Model loaded successfully (dim={EMBEDDING_DIM})")
        except ImportError:
            logger.error(
                "sentence-transformers not installed. "
                "Install with: pip install sentence-transformers"
            )
            raise
    return _model


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


def embed_pending_chunks(limit: int = 100) -> Dict[str, int]:
    """
    Generate embeddings for chunks that don't have them yet.
    Uses batch processing for efficiency.
    """
    conn = get_connection()

    # Get chunks without embeddings
    rows = conn.execute(
        """
        SELECT id, content FROM chunks
        WHERE embedding IS NULL
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    if not rows:
        return {"processed": 0, "failed": 0}

    chunk_ids = [r["id"] for r in rows]
    texts = [r["content"] for r in rows]

    logger.info(f"Generating embeddings for {len(texts)} chunks")

    try:
        embeddings = encode_batch(texts)
    except Exception as e:
        logger.error(f"Batch embedding failed: {e}")
        return {"processed": 0, "failed": len(texts)}

    # Store embeddings
    conn = get_connection()
    success = 0
    failed = 0

    for chunk_id, embedding in zip(chunk_ids, embeddings):
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

    logger.info(f"Embedding complete: {success} success, {failed} failed")
    return {"processed": success, "failed": failed}


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

    conn.close()
    return stats
