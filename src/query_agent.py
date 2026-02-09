"""PydanticAI tool-using agent for natural-language inbox queries."""

from __future__ import annotations

import hashlib
import importlib.util
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext


def _load_local_module(module_name: str, filename: str):
    """Load module from this directory without depending on sys.path precedence."""
    module_path = Path(__file__).resolve().parent / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load local module {module_name} from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_database_mod = _load_local_module("inbox_database_local", "database.py")
_model_utils_mod = _load_local_module("inbox_model_utils_local", "model_utils.py")
_triggers_mod = _load_local_module("inbox_triggers_local", "triggers.py")

get_connection = _database_mod.get_connection
get_model_settings = _model_utils_mod.get_model_settings
parse_model_string = _model_utils_mod.parse_model_string
make_dedupe_key = _triggers_mod.make_dedupe_key
write_trigger = _triggers_mod.write_trigger


@dataclass
class QueryAgentDeps:
    """Runtime dependencies for query agent runs."""

    user_email: str
    default_limit: int = 5


class SearchHit(BaseModel):
    """Normalized email hit returned by retrieval tools."""

    email_id: str
    subject: str = ""
    sender: str = ""
    received_at: str | None = None
    conversation_id: str | None = None
    web_link: str | None = None
    snippet: str = ""
    score: float = 0.0
    evidence_types: list[str] = Field(default_factory=list)
    wm_needs_reply: bool | None = None
    next_action_owner: str | None = None
    role_context_note: str | None = None


class EmailDetail(BaseModel):
    """Detailed email context for grounded responses."""

    email_id: str
    subject: str = ""
    sender: str = ""
    received_at: str | None = None
    web_link: str | None = None
    body_preview: str = ""
    body_excerpt: str = ""
    thread_summary: str | None = None
    wm_needs_reply: bool | None = None
    next_action_owner: str | None = None
    sender_org_relation: str | None = None
    value_flow_direction: str | None = None
    role_context_note: str | None = None
    role_context_confidence: float | None = None


class NudgeReceipt(BaseModel):
    """Result of nudging user via trigger."""

    status: Literal["queued"]
    trigger_type: str
    dedupe_key: str
    title: str
    urgency: str


class QueryAgentResponse(BaseModel):
    """Typed final response for user query handling."""

    answer: str = Field(
        description="Direct answer to user question with helpful context and links."
    )
    matched_emails: list[SearchHit] = Field(
        default_factory=list,
        description="Most relevant emails for the user query.",
    )
    suggested_next_steps: list[str] = Field(default_factory=list)
    clarification_question: str | None = None


def _score_from_rank(rank: float | None) -> float:
    if rank is None:
        return 0.0
    return 1.0 / (1.0 + abs(float(rank)))


def _recency_factor(received_at: str | None, decay_days: int = 30) -> float:
    if not received_at:
        return 1.0
    try:
        dt = datetime.fromisoformat(str(received_at).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        days_ago = (datetime.now(timezone.utc) - dt).days
        return max(0.5, 1.0 - (days_ago / (decay_days * 2)))
    except Exception:
        return 1.0


def _fetch_email_row(conn, email_id: str):
    return conn.execute(
        """
        SELECT id, subject, sender, received_at, conversation_id, web_link,
               body_preview, body_markdown, thread_summary,
               wm_needs_reply, next_action_owner, sender_org_relation,
               value_flow_direction, role_context_note, role_context_confidence
        FROM emails
        WHERE id = ?
        """,
        (email_id,),
    ).fetchone()


def _fetch_derived_learning_row(conn, learning_id: str):
    return conn.execute(
        """
        SELECT id, learning_type, summary, confidence, metadata_json, last_seen_at
        FROM derived_learnings
        WHERE id = ?
        """,
        (learning_id,),
    ).fetchone()


def _add_hit(
    aggregate: dict[str, dict[str, Any]],
    row: dict[str, Any],
    evidence_type: str,
    snippet: str,
    score: float,
) -> None:
    email_id = row["id"]
    if email_id not in aggregate:
        aggregate[email_id] = {
            "email_id": row["id"],
            "subject": row["subject"] or "",
            "sender": row["sender"] or "",
            "received_at": row["received_at"],
            "conversation_id": row["conversation_id"],
            "web_link": row["web_link"],
            "snippet": snippet[:300],
            "score": score,
            "evidence_types": {evidence_type},
            "wm_needs_reply": row["wm_needs_reply"],
            "next_action_owner": row["next_action_owner"],
            "role_context_note": row["role_context_note"],
        }
        return

    aggregate[email_id]["score"] = max(aggregate[email_id]["score"], score)
    aggregate[email_id]["evidence_types"].add(evidence_type)
    if snippet and not aggregate[email_id]["snippet"]:
        aggregate[email_id]["snippet"] = snippet[:300]


def _search_and_enrich(query: str, limit: int, mode: str) -> list[SearchHit]:
    """
    Retrieve and aggregate best email hits with enrichment.

    mode controls weighting emphasis:
    - fts: email/content facts by FTS ranking only
    - hybrid/vector: currently same retrieval corpus, with broader source coverage
    """
    if mode not in {"hybrid", "fts", "vector"}:
        raise ValueError(f"Invalid mode: {mode}")

    conn = get_connection()
    try:
        aggregate: dict[str, dict[str, Any]] = {}
        email_cache: dict[str, Any] = {}
        fanout = max(limit * 5, 25)

        # 1) Email subject/body FTS
        email_rows = conn.execute(
            """
            SELECT e.id, e.subject, e.sender, e.received_at, e.conversation_id, e.web_link,
                   e.body_preview, e.wm_needs_reply, e.next_action_owner, e.role_context_note,
                   bm25(emails_fts) AS rank
            FROM emails_fts
            JOIN emails e ON emails_fts.id = e.id
            WHERE emails_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (query, fanout),
        ).fetchall()

        for row in email_rows:
            score = _score_from_rank(row["rank"]) * _recency_factor(row["received_at"])
            _add_hit(
                aggregate=aggregate,
                row=dict(row),
                evidence_type="email_fts",
                snippet=row["body_preview"] or "",
                score=score,
            )
            email_cache[row["id"]] = row

        # 2) Chunk FTS (captures longer body/attachment content)
        chunk_rows = conn.execute(
            """
            SELECT c.source_type, c.source_id, c.content, bm25(chunks_fts) AS rank
            FROM chunks_fts
            JOIN chunks c ON c.id = chunks_fts.id
            WHERE chunks_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (query, fanout),
        ).fetchall()

        for row in chunk_rows:
            email_id: str | None = None
            source_type = row["source_type"]
            if source_type == "email":
                email_id = row["source_id"]
                evidence_type = "chunk_email"
            elif source_type == "attachment":
                r = conn.execute(
                    "SELECT email_id FROM attachments WHERE id = ?",
                    (row["source_id"],),
                ).fetchone()
                email_id = r["email_id"] if r else None
                evidence_type = "chunk_attachment"
            else:
                continue

            if not email_id:
                continue
            email_row = email_cache.get(email_id) or _fetch_email_row(conn, email_id)
            if email_row is None:
                continue
            email_cache[email_id] = email_row
            score = _score_from_rank(row["rank"]) * _recency_factor(email_row["received_at"])
            _add_hit(
                aggregate=aggregate,
                row=dict(email_row),
                evidence_type=evidence_type,
                snippet=row["content"] or "",
                score=score,
            )

        # 3) Fact FTS (captures extracted entities/details)
        fact_rows = conn.execute(
            """
            SELECT f.id, f.source_type, f.source_id, f.fact_value, f.context, bm25(facts_fts) AS rank
            FROM facts_fts
            JOIN facts f ON f.id = facts_fts.id
            WHERE facts_fts MATCH ?
            AND f.status = 'active'
            ORDER BY rank
            LIMIT ?
            """,
            (query, fanout),
        ).fetchall()

        for row in fact_rows:
            email_id: str | None = None
            if row["source_type"] == "email":
                email_id = row["source_id"]
            elif row["source_type"] == "attachment":
                r = conn.execute(
                    "SELECT email_id FROM attachments WHERE id = ?",
                    (row["source_id"],),
                ).fetchone()
                email_id = r["email_id"] if r else None
            if not email_id:
                continue

            email_row = email_cache.get(email_id) or _fetch_email_row(conn, email_id)
            if email_row is None:
                continue
            email_cache[email_id] = email_row
            score = _score_from_rank(row["rank"]) * _recency_factor(email_row["received_at"])
            snippet = f"{row['fact_value'] or ''} {row['context'] or ''}".strip()
            _add_hit(
                aggregate=aggregate,
                row=dict(email_row),
                evidence_type="fact_fts",
                snippet=snippet,
                score=score,
            )

        # 4) Derived learnings (compliance-safe retained knowledge)
        derived_rows = conn.execute(
            """
            SELECT d.id, d.learning_type, d.summary, d.confidence, d.last_seen_at, bm25(derived_learnings_fts) AS rank
            FROM derived_learnings_fts
            JOIN derived_learnings d ON d.id = derived_learnings_fts.id
            WHERE derived_learnings_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (query, fanout),
        ).fetchall()

        derived_hits: list[SearchHit] = []
        for row in derived_rows:
            derived_hits.append(
                SearchHit(
                    email_id=f"derived:{row['id']}",
                    subject=f"Derived learning ({row['learning_type']})",
                    sender="Derived Knowledge",
                    received_at=row["last_seen_at"],
                    conversation_id=None,
                    web_link=None,
                    snippet=(row["summary"] or "")[:300],
                    score=_score_from_rank(row["rank"]),
                    evidence_types=["derived_learning"],
                    wm_needs_reply=None,
                    next_action_owner=None,
                    role_context_note=None,
                )
            )

        hits = []
        for item in aggregate.values():
            item["evidence_types"] = sorted(item["evidence_types"])
            hits.append(SearchHit(**item))

        hits.extend(derived_hits)
        hits.sort(key=lambda h: (h.score, h.received_at or ""), reverse=True)
        return hits[:limit]
    except sqlite3.OperationalError as exc:
        raise RuntimeError(
            "Query agent retrieval failed due to schema mismatch. "
            "Run the inbox service once to apply DB migrations before using `ask`."
        ) from exc
    finally:
        conn.close()


def _build_query_agent() -> Agent[QueryAgentDeps, QueryAgentResponse]:
    model_string = os.getenv(
        "QUERY_AGENT_MODEL",
        os.getenv("MODEL_NAME", "openai-responses:gpt-5-mini"),
    )
    model_name, _ = parse_model_string(model_string)
    model_settings = get_model_settings(model_string)

    instructions = """
You are Agent Aech's inbox query assistant.
Use tools to retrieve grounded evidence before answering.

Rules:
- For "remember/find/show that email" style requests, call search_inbox first.
- Prefer linking directly to emails via web_link when available.
- Do not fabricate IDs, links, or facts.
- If only derived-learning hits are available, state that source emails were removed by retention policy.
- If results are ambiguous, ask one clarification question.
- Use nudge_user only when asked or when user explicitly wants a reminder/nudge.
- Keep answers concise and practical.
"""

    agent = Agent(
        model_name,
        deps_type=QueryAgentDeps,
        output_type=QueryAgentResponse,
        instructions=instructions,
        model_settings=model_settings,
    )

    @agent.tool
    def search_inbox(
        ctx: RunContext[QueryAgentDeps],
        query: str,
        limit: int = 0,
        mode: Literal["hybrid", "fts", "vector"] = "hybrid",
    ) -> list[SearchHit]:
        """Search inbox corpus and return enriched email hits."""
        effective_limit = limit if limit > 0 else ctx.deps.default_limit
        return _search_and_enrich(query=query, limit=effective_limit, mode=mode)

    @agent.tool
    def get_email(ctx: RunContext[QueryAgentDeps], email_id: str) -> EmailDetail:
        """Get detailed context for one email ID."""
        conn = get_connection()
        try:
            if email_id.startswith("derived:"):
                learning_id = email_id.split(":", 1)[1]
                learning = _fetch_derived_learning_row(conn, learning_id)
                if learning is None:
                    raise ValueError(f"Derived learning not found: {email_id}")
                metadata_text = learning["metadata_json"] or ""
                return EmailDetail(
                    email_id=email_id,
                    subject=f"Derived learning ({learning['learning_type']})",
                    sender="Derived Knowledge",
                    received_at=learning["last_seen_at"],
                    web_link=None,
                    body_preview=learning["summary"] or "",
                    body_excerpt=f"{learning['summary'] or ''}\n{metadata_text}".strip()[:1500],
                    thread_summary=None,
                    wm_needs_reply=None,
                    next_action_owner=None,
                    sender_org_relation=None,
                    value_flow_direction=None,
                    role_context_note=None,
                    role_context_confidence=learning["confidence"],
                )

            row = _fetch_email_row(conn, email_id)
            if row is None:
                raise ValueError(f"Email not found: {email_id}")

            body_markdown = row["body_markdown"] or ""
            return EmailDetail(
                email_id=row["id"],
                subject=row["subject"] or "",
                sender=row["sender"] or "",
                received_at=row["received_at"],
                web_link=row["web_link"],
                body_preview=row["body_preview"] or "",
                body_excerpt=body_markdown[:1500],
                thread_summary=row["thread_summary"],
                wm_needs_reply=row["wm_needs_reply"],
                next_action_owner=row["next_action_owner"],
                sender_org_relation=row["sender_org_relation"],
                value_flow_direction=row["value_flow_direction"],
                role_context_note=row["role_context_note"],
                role_context_confidence=row["role_context_confidence"],
            )
        finally:
            conn.close()

    @agent.tool
    def nudge_user(
        ctx: RunContext[QueryAgentDeps],
        title: str,
        message: str,
        urgency: Literal["immediate", "today", "this_week", "someday"] = "today",
    ) -> NudgeReceipt:
        """Create a user nudge trigger for Teams."""
        user_email = (ctx.deps.user_email or "").strip().lower()
        if not user_email:
            raise ValueError("Cannot nudge user without user_email in agent deps")

        fingerprint = hashlib.sha256(
            f"{user_email}|{title}|{message}|{urgency}".encode("utf-8")
        ).hexdigest()[:24]
        dedupe_key = make_dedupe_key("assistant_query_nudge", user_email, fingerprint)

        write_trigger(
            user_email,
            "working_memory_nudge",
            {
                "type": "assistant_query_nudge",
                "urgency": urgency,
                "title": title,
                "message": message,
            },
            dedupe_key=dedupe_key,
            routing={"channel": "teams"},
        )

        return NudgeReceipt(
            status="queued",
            trigger_type="working_memory_nudge",
            dedupe_key=dedupe_key,
            title=title,
            urgency=urgency,
        )

    return agent


_QUERY_AGENT: Agent[QueryAgentDeps, QueryAgentResponse] | None = None


def _get_query_agent() -> Agent[QueryAgentDeps, QueryAgentResponse]:
    global _QUERY_AGENT
    if _QUERY_AGENT is None:
        _QUERY_AGENT = _build_query_agent()
    return _QUERY_AGENT


async def run_query_agent(
    user_email: str,
    user_prompt: str,
    max_results: int = 5,
) -> dict[str, Any]:
    """Run the query agent and return JSON-serializable output."""
    deps = QueryAgentDeps(
        user_email=user_email.strip().lower(),
        default_limit=max_results,
    )
    result = await _get_query_agent().run(user_prompt, deps=deps)
    output = result.output.model_dump(mode="json")
    usage = result.usage()
    output["usage"] = {
        "request_tokens": usage.request_tokens,
        "response_tokens": usage.response_tokens,
    }
    return output
