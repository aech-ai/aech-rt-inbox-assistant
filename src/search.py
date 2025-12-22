"""
Hybrid search for Email Corpus Intelligence.

Combines FTS5 keyword search with vector similarity search using
Reciprocal Rank Fusion (RRF) for optimal results.
"""

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .database import get_connection
from .embeddings import encode_text, cosine_similarity, decode_embedding

logger = logging.getLogger(__name__)

# RRF constant (higher = more weight to later ranks)
RRF_K = 60


@dataclass
class SearchResult:
    """A single search result."""

    chunk_id: str
    source_type: str  # 'email' or 'attachment'
    source_id: str
    content_preview: str
    score: float
    fts_rank: Optional[int] = None
    vector_rank: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None


def fts_search(query: str, limit: int = 50) -> List[SearchResult]:
    """
    Full-text search using FTS5 with BM25 ranking.
    Searches both email and chunk FTS indexes.
    """
    conn = get_connection()
    results = []

    # Search chunks FTS
    try:
        rows = conn.execute(
            """
            SELECT c.id, c.source_type, c.source_id, c.content, c.metadata_json,
                   bm25(chunks_fts) as rank
            FROM chunks_fts
            JOIN chunks c ON chunks_fts.id = c.id
            WHERE chunks_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (query, limit),
        ).fetchall()

        for i, row in enumerate(rows):
            results.append(
                SearchResult(
                    chunk_id=row["id"],
                    source_type=row["source_type"],
                    source_id=row["source_id"],
                    content_preview=row["content"][:300] if row["content"] else "",
                    score=abs(row["rank"]),  # BM25 returns negative scores
                    fts_rank=i + 1,
                    metadata=json.loads(row["metadata_json"]) if row["metadata_json"] else None,
                )
            )

    except Exception as e:
        logger.warning(f"FTS search failed: {e}")

    conn.close()
    return results


def vector_search(query: str, limit: int = 50, min_score: float = 0.25) -> List[SearchResult]:
    """
    Semantic similarity search using embeddings.
    """
    try:
        query_embedding = encode_text(query)
    except Exception as e:
        logger.warning(f"Failed to encode query: {e}")
        return []

    conn = get_connection()

    # Get all chunks with embeddings
    rows = conn.execute(
        """
        SELECT id, source_type, source_id, content, metadata_json, embedding
        FROM chunks
        WHERE embedding IS NOT NULL
        """
    ).fetchall()
    conn.close()

    # Compute similarities
    scored = []
    for row in rows:
        if not row["embedding"]:
            continue

        score = cosine_similarity(query_embedding, row["embedding"])

        if score >= min_score:
            scored.append((row, score))

    # Sort by score descending
    scored.sort(key=lambda x: x[1], reverse=True)

    results = []
    for i, (row, score) in enumerate(scored[:limit]):
        results.append(
            SearchResult(
                chunk_id=row["id"],
                source_type=row["source_type"],
                source_id=row["source_id"],
                content_preview=row["content"][:300] if row["content"] else "",
                score=score,
                vector_rank=i + 1,
                metadata=json.loads(row["metadata_json"]) if row["metadata_json"] else None,
            )
        )

    return results


def rrf_merge(
    fts_results: List[SearchResult],
    vector_results: List[SearchResult],
    k: int = RRF_K,
) -> List[SearchResult]:
    """
    Merge results using Reciprocal Rank Fusion (RRF).

    RRF score = 1 / (k + rank_in_list)
    Total score = sum of RRF scores across both lists
    """
    # Map chunk_id -> result
    merged: Dict[str, SearchResult] = {}

    # Process FTS results
    for result in fts_results:
        if result.chunk_id not in merged:
            merged[result.chunk_id] = SearchResult(
                chunk_id=result.chunk_id,
                source_type=result.source_type,
                source_id=result.source_id,
                content_preview=result.content_preview,
                score=0.0,
                metadata=result.metadata,
            )

        merged[result.chunk_id].fts_rank = result.fts_rank
        merged[result.chunk_id].score += 1.0 / (k + result.fts_rank)

    # Process vector results
    for result in vector_results:
        if result.chunk_id not in merged:
            merged[result.chunk_id] = SearchResult(
                chunk_id=result.chunk_id,
                source_type=result.source_type,
                source_id=result.source_id,
                content_preview=result.content_preview,
                score=0.0,
                metadata=result.metadata,
            )

        merged[result.chunk_id].vector_rank = result.vector_rank
        merged[result.chunk_id].score += 1.0 / (k + result.vector_rank)

    # Sort by combined score
    results = list(merged.values())
    results.sort(key=lambda x: x.score, reverse=True)

    return results


def hybrid_search(
    query: str,
    limit: int = 20,
    mode: str = "hybrid",
) -> List[SearchResult]:
    """
    Perform search with specified mode.

    Modes:
    - 'hybrid': Combine FTS and vector search with RRF
    - 'fts': FTS only (keyword matching)
    - 'vector': Vector search only (semantic similarity)
    """
    if mode == "fts":
        return fts_search(query, limit)

    elif mode == "vector":
        return vector_search(query, limit)

    elif mode == "hybrid":
        # Get more results from each source for better fusion
        fts_results = fts_search(query, limit * 2)
        vector_results = vector_search(query, limit * 2)

        # Merge with RRF
        merged = rrf_merge(fts_results, vector_results)

        return merged[:limit]

    else:
        raise ValueError(f"Unknown search mode: {mode}")


def search_with_source_details(
    query: str,
    limit: int = 20,
    mode: str = "hybrid",
) -> List[Dict[str, Any]]:
    """
    Search and enrich results with source details (email subject, attachment filename, etc.)
    """
    results = hybrid_search(query, limit, mode)

    conn = get_connection()
    enriched = []

    for result in results:
        item = {
            "chunk_id": result.chunk_id,
            "source_type": result.source_type,
            "source_id": result.source_id,
            "content_preview": result.content_preview,
            "score": result.score,
            "fts_rank": result.fts_rank,
            "vector_rank": result.vector_rank,
        }

        # Get source details
        if result.source_type == "email":
            row = conn.execute(
                """
                SELECT subject, sender, received_at, conversation_id
                FROM emails WHERE id = ?
                """,
                (result.source_id,),
            ).fetchone()

            if row:
                item["email_subject"] = row["subject"]
                item["email_sender"] = row["sender"]
                item["email_date"] = row["received_at"]
                item["conversation_id"] = row["conversation_id"]

        elif result.source_type == "attachment":
            row = conn.execute(
                """
                SELECT a.filename, a.content_type, e.subject as email_subject,
                       e.sender as email_sender, e.received_at, e.conversation_id
                FROM attachments a
                LEFT JOIN emails e ON a.email_id = e.id
                WHERE a.id = ?
                """,
                (result.source_id,),
            ).fetchone()

            if row:
                item["filename"] = row["filename"]
                item["content_type"] = row["content_type"]
                item["email_subject"] = row["email_subject"]
                item["email_sender"] = row["email_sender"]
                item["email_date"] = row["received_at"]
                item["conversation_id"] = row["conversation_id"]

        elif result.source_type == "virtual_email":
            # Virtual emails are extracted from forwards - metadata is in the chunk
            item["is_virtual"] = True
            if result.metadata:
                metadata = result.metadata if isinstance(result.metadata, dict) else json.loads(result.metadata)
                item["email_sender"] = metadata.get("extracted_sender", "Unknown (from forward)")
                item["email_subject"] = metadata.get("extracted_subject", "")
                item["email_date"] = metadata.get("extracted_date", "")
                item["extracted_from"] = metadata.get("source_email_id")
                item["position_in_chain"] = metadata.get("position_in_chain")

                # Try to get the forwarding email's details
                source_email_id = metadata.get("source_email_id")
                if source_email_id:
                    row = conn.execute(
                        "SELECT subject, sender, received_at FROM emails WHERE id = ?",
                        (source_email_id,),
                    ).fetchone()
                    if row:
                        item["forwarded_by"] = row["sender"]
                        item["forwarded_at"] = row["received_at"]
                        item["forward_subject"] = row["subject"]

        enriched.append(item)

    conn.close()
    return enriched


def get_search_stats() -> Dict[str, Any]:
    """Get statistics about searchable content."""
    conn = get_connection()
    cursor = conn.cursor()

    stats = {}

    # Total searchable chunks
    cursor.execute("SELECT COUNT(*) FROM chunks")
    stats["total_chunks"] = cursor.fetchone()[0]

    # Chunks with embeddings (vector searchable)
    cursor.execute("SELECT COUNT(*) FROM chunks WHERE embedding IS NOT NULL")
    stats["vector_searchable"] = cursor.fetchone()[0]

    # By source type
    cursor.execute(
        """
        SELECT source_type, COUNT(*) as count
        FROM chunks
        GROUP BY source_type
        """
    )
    stats["by_source_type"] = {row["source_type"]: row["count"] for row in cursor.fetchall()}

    # FTS index size
    try:
        cursor.execute("SELECT COUNT(*) FROM chunks_fts")
        stats["fts_indexed"] = cursor.fetchone()[0]
    except Exception:
        stats["fts_indexed"] = 0

    conn.close()
    return stats
