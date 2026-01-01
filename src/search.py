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
            metadata = json.loads(row["metadata_json"]) if row["metadata_json"] else None
            source_type = "virtual_email" if metadata and metadata.get("is_virtual") else row["source_type"]
            results.append(
                SearchResult(
                    chunk_id=row["id"],
                    source_type=source_type,
                    source_id=row["source_id"],
                    content_preview=row["content"][:300] if row["content"] else "",
                    score=abs(row["rank"]),  # BM25 returns negative scores
                    fts_rank=i + 1,
                    metadata=metadata,
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
        metadata = json.loads(row["metadata_json"]) if row["metadata_json"] else None
        source_type = "virtual_email" if metadata and metadata.get("is_virtual") else row["source_type"]
        results.append(
            SearchResult(
                chunk_id=row["id"],
                source_type=source_type,
                source_id=row["source_id"],
                content_preview=row["content"][:300] if row["content"] else "",
                score=score,
                vector_rank=i + 1,
                metadata=metadata,
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
                item["conversation_id"] = metadata.get("parent_conversation_id")

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


@dataclass
class UnifiedSearchResult:
    """A result from unified search - can be a chunk or a fact."""

    id: str
    result_type: str  # 'email', 'attachment', 'fact', 'virtual_email'
    source_id: str
    content_preview: str
    score: float

    # Optional fields based on result type
    email_subject: Optional[str] = None
    email_sender: Optional[str] = None
    email_date: Optional[str] = None
    conversation_id: Optional[str] = None
    filename: Optional[str] = None
    fact_type: Optional[str] = None
    fact_value: Optional[str] = None
    web_link: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


def search_facts(query: str, limit: int = 20) -> List[UnifiedSearchResult]:
    """
    Search the facts table using FTS.

    Returns fact results formatted as UnifiedSearchResult.
    """
    conn = get_connection()
    results = []

    try:
        rows = conn.execute(
            """
            SELECT
                f.id, f.source_type, f.source_id, f.fact_type, f.fact_value,
                f.context, f.confidence, f.entity_normalized, f.status, f.due_date,
                e.subject as email_subject, e.sender as email_sender,
                e.received_at, e.web_link, e.conversation_id,
                a.filename as attachment_filename,
                bm25(facts_fts) as rank
            FROM facts_fts ft
            JOIN facts f ON ft.id = f.id
            LEFT JOIN emails e ON f.source_type = 'email' AND f.source_id = e.id
            LEFT JOIN attachments a ON f.source_type = 'attachment' AND f.source_id = a.id
            WHERE facts_fts MATCH ?
            AND f.status = 'active'
            ORDER BY rank
            LIMIT ?
            """,
            (query, limit),
        ).fetchall()

        for row in rows:
            # Build content preview from fact value and context
            preview = row["fact_value"]
            if row["context"]:
                preview = f"{preview} - {row['context'][:200]}"

            results.append(
                UnifiedSearchResult(
                    id=row["id"],
                    result_type="fact",
                    source_id=row["source_id"],
                    content_preview=preview[:300],
                    score=abs(row["rank"]),
                    email_subject=row["email_subject"],
                    email_sender=row["email_sender"],
                    email_date=row["received_at"],
                    conversation_id=row["conversation_id"],
                    filename=row["attachment_filename"],
                    fact_type=row["fact_type"],
                    fact_value=row["fact_value"],
                    web_link=row["web_link"],
                )
            )

    except Exception as e:
        logger.warning(f"Facts search failed: {e}")

    conn.close()
    return results


def _apply_recency_weight(
    results: List[UnifiedSearchResult],
    decay_days: int = 30,
) -> List[UnifiedSearchResult]:
    """
    Apply recency weighting to search results.

    More recent results get a boost, with decay over time.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)

    for result in results:
        if result.email_date:
            try:
                # Parse date and compute days ago
                date_str = result.email_date.replace("Z", "+00:00")
                email_date = datetime.fromisoformat(date_str)
                if email_date.tzinfo is None:
                    email_date = email_date.replace(tzinfo=timezone.utc)

                days_ago = (now - email_date).days

                # Recency boost: decay from 1.0 to 0.5 over decay_days
                recency_factor = max(0.5, 1.0 - (days_ago / (decay_days * 2)))
                result.score *= recency_factor
            except Exception:
                pass

    return results


def unified_search(
    query: str,
    limit: int = 20,
    mode: str = "hybrid",
    source_types: Optional[List[str]] = None,
    include_facts: bool = True,
    recency_weight: bool = True,
) -> List[UnifiedSearchResult]:
    """
    Unified search interface for all content.

    Searches emails, attachments, and facts with a single query.

    Args:
        query: Search query
        limit: Maximum results to return
        mode: Search mode ('hybrid', 'fts', 'vector')
        source_types: Filter to specific types ('email', 'attachment', 'fact')
        include_facts: Whether to search facts table
        recency_weight: Apply recency boost to results

    Returns:
        Combined, ranked list of results
    """
    results: List[UnifiedSearchResult] = []

    # 1. Search chunks (emails + attachments)
    chunk_results = hybrid_search(query, limit * 2, mode)

    conn = get_connection()

    for chunk_result in chunk_results:
        # Determine actual source type
        source_type = chunk_result.source_type
        if source_type == "virtual_email":
            source_type = "email"  # Group with emails for filtering

        # Skip if not in requested source types
        if source_types and source_type not in source_types:
            continue

        # Build unified result
        unified = UnifiedSearchResult(
            id=chunk_result.chunk_id,
            result_type=chunk_result.source_type,
            source_id=chunk_result.source_id,
            content_preview=chunk_result.content_preview,
            score=chunk_result.score,
            metadata=chunk_result.metadata,
        )

        # Enrich with source details
        if chunk_result.source_type == "email":
            row = conn.execute(
                """
                SELECT subject, sender, received_at, conversation_id, web_link
                FROM emails WHERE id = ?
                """,
                (chunk_result.source_id,),
            ).fetchone()

            if row:
                unified.email_subject = row["subject"]
                unified.email_sender = row["sender"]
                unified.email_date = row["received_at"]
                unified.conversation_id = row["conversation_id"]
                unified.web_link = row["web_link"]

        elif chunk_result.source_type == "attachment":
            row = conn.execute(
                """
                SELECT a.filename, e.subject, e.sender, e.received_at,
                       e.conversation_id, e.web_link
                FROM attachments a
                LEFT JOIN emails e ON a.email_id = e.id
                WHERE a.id = ?
                """,
                (chunk_result.source_id,),
            ).fetchone()

            if row:
                unified.filename = row["filename"]
                unified.email_subject = row["subject"]
                unified.email_sender = row["sender"]
                unified.email_date = row["received_at"]
                unified.conversation_id = row["conversation_id"]
                unified.web_link = row["web_link"]

        elif chunk_result.source_type == "virtual_email":
            unified.result_type = "virtual_email"
            if chunk_result.metadata:
                meta = chunk_result.metadata if isinstance(chunk_result.metadata, dict) else json.loads(chunk_result.metadata)
                unified.email_sender = meta.get("extracted_sender")
                unified.email_subject = meta.get("extracted_subject")
                unified.email_date = meta.get("extracted_date")
                unified.conversation_id = meta.get("parent_conversation_id")

        results.append(unified)

    conn.close()

    # 2. Search facts (if requested and not filtered out)
    if include_facts and (source_types is None or "fact" in source_types):
        fact_results = search_facts(query, limit)
        results.extend(fact_results)

    # 3. Apply recency weighting
    if recency_weight:
        results = _apply_recency_weight(results)

    # 4. Sort by final score and limit
    results.sort(key=lambda x: x.score, reverse=True)

    return results[:limit]


def get_unified_search_stats() -> Dict[str, Any]:
    """Get statistics about all searchable content."""
    chunk_stats = get_search_stats()

    conn = get_connection()
    cursor = conn.cursor()

    # Facts stats
    cursor.execute("SELECT COUNT(*) FROM facts WHERE status = 'active'")
    chunk_stats["active_facts"] = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT fact_type, COUNT(*) as count
        FROM facts
        WHERE status = 'active'
        GROUP BY fact_type
        """
    )
    chunk_stats["facts_by_type"] = {row["fact_type"]: row["count"] for row in cursor.fetchall()}

    conn.close()
    return chunk_stats
