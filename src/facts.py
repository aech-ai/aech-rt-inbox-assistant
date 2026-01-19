"""
Unified facts extraction and storage for Email Corpus Intelligence.

Consolidates extraction of:
- Decisions (from WM) - questions requiring user response
- Commitments (from WM) - promises made by user
- Key details - tax IDs, amounts, addresses, phone numbers, etc.
- Observations - patterns and insights learned
"""

import json
import logging
import os
import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from .database import get_connection
from .model_utils import parse_model_string, get_model_settings

logger = logging.getLogger(__name__)


class FactType(str, Enum):
    """Types of facts that can be extracted."""

    # Action items
    DECISION = "decision"
    COMMITMENT = "commitment"
    ACTION_ITEM = "action_item"

    # Key details
    TAX_ID = "tax_id"
    BUSINESS_NUMBER = "business_number"
    ACCOUNT_NUMBER = "account_number"
    AMOUNT = "amount"
    ADDRESS = "address"
    PHONE = "phone"
    DEADLINE = "deadline"
    PERSON_NAME = "person_name"
    COMPANY_NAME = "company_name"
    CONTRACT_NUMBER = "contract_number"

    # Observations
    PREFERENCE = "preference"
    RELATIONSHIP = "relationship"
    PATTERN = "pattern"

    OTHER = "other"


class ExtractedFact(BaseModel):
    """A single fact extracted from content."""

    fact_type: FactType
    fact_value: str
    context: str = ""  # Surrounding text for disambiguation
    confidence: float = Field(default=0.8, ge=0.0, le=1.0)
    entity_normalized: str | None = None  # Normalized form (dates, phones, etc.)
    due_date: str | None = None  # For deadlines, commitments (ISO format)
    metadata: dict[str, Any] = Field(default_factory=dict)


class FactsExtraction(BaseModel):
    """AI-extracted facts from email/attachment content."""

    facts: list[ExtractedFact] = Field(default_factory=list)


def _build_facts_agent() -> Agent:
    """Build the AI agent for facts extraction."""
    model_string = os.getenv(
        "FACTS_MODEL",
        os.getenv("MODEL_NAME", "openai:gpt-4o-mini")
    )
    model_name, _ = parse_model_string(model_string)
    model_settings = get_model_settings(model_string)

    system_prompt = """
You are an expert at extracting structured facts from emails and documents.
Your goal is to identify key business information that users might search for later.

## What to Extract

### Key Details (ALWAYS extract when present)
- **tax_id**: EIN, TIN, VAT numbers (format: XX-XXXXXXX or similar)
- **business_number**: Registration numbers, company IDs, license numbers
- **account_number**: Bank accounts, customer IDs, membership numbers
- **amount**: Dollar amounts, prices, totals, invoices. Normalize to numeric (e.g., "$1,234.56" → "1234.56")
- **address**: Physical addresses, mailing addresses. Include full address.
- **phone**: Phone numbers. Normalize to E.164 format when possible (e.g., +1-555-123-4567)
- **deadline**: Due dates, expiry dates. Use ISO format (YYYY-MM-DD).
- **person_name**: Key contacts mentioned with context about who they are
- **company_name**: Organizations, vendors, clients discussed
- **contract_number**: Agreement IDs, PO numbers, reference numbers

### Action Items (extract when user needs to act)
- **decision**: Someone is asking the user to make a choice or provide input
- **commitment**: The user promised to do something specific
- **action_item**: A task mentioned that user should be aware of

### Observations (for patterns and learning)
- **preference**: User preferences mentioned (preferred times, methods, etc.)
- **relationship**: Organizational relationships, reporting structures
- **pattern**: Recurring patterns (weekly meetings, monthly reports)

## Guidelines
- For each fact, provide:
  - fact_value: The actual value (the tax ID number, the amount, etc.)
  - context: 1-2 sentences of surrounding text that gives context
  - confidence: How confident you are (0.0-1.0). Lower for ambiguous content.
  - entity_normalized: Normalized form where applicable (dates → ISO, phones → E.164)
  - due_date: Only for deadlines and commitments with dates

## What NOT to Extract
- Generic greetings or closings
- Already-known user information (their own email, name)
- Boilerplate text from templates
- Marketing copy or promotional language
- Content that would be obvious from the email metadata (sender, subject)

Return an empty list if no significant facts are found.
"""

    return Agent(
        model_name,
        output_type=FactsExtraction,
        instructions=system_prompt,
        model_settings=model_settings,
    )


class FactsExtractor:
    """Extracts and stores facts from emails and attachments."""

    def __init__(self):
        self._agent: Agent | None = None

    def _get_agent(self) -> Agent:
        """Lazy-load the extraction agent."""
        if self._agent is None:
            self._agent = _build_facts_agent()
        return self._agent

    async def extract_from_email(
        self,
        email_id: str,
        body: str,
        subject: str | None = None,
        sender: str | None = None,
    ) -> list[ExtractedFact]:
        """
        Extract facts from an email.

        Args:
            email_id: The email ID
            body: Email body text (markdown or plain)
            subject: Email subject for context
            sender: Sender for context

        Returns:
            List of extracted facts
        """
        if not body or len(body.strip()) < 50:
            return []

        # Build context for the agent
        context_parts = []
        if subject:
            context_parts.append(f"Subject: {subject}")
        if sender:
            context_parts.append(f"From: {sender}")
        context_parts.append("")
        context_parts.append(body)

        prompt = "\n".join(context_parts)

        try:
            agent = self._get_agent()
            result = await agent.run(prompt)
            return result.output.facts
        except Exception as e:
            logger.warning(f"Facts extraction failed for email {email_id}: {e}")
            return []

    async def extract_from_attachment(
        self,
        attachment_id: str,
        extracted_text: str,
        filename: str | None = None,
    ) -> list[ExtractedFact]:
        """
        Extract facts from an attachment's extracted text.

        Args:
            attachment_id: The attachment ID
            extracted_text: Text extracted from the attachment
            filename: Filename for context

        Returns:
            List of extracted facts
        """
        if not extracted_text or len(extracted_text.strip()) < 50:
            return []

        # Build context for the agent
        context_parts = []
        if filename:
            context_parts.append(f"Document: {filename}")
        context_parts.append("")
        # Limit text length to avoid token limits
        text = extracted_text[:10000] if len(extracted_text) > 10000 else extracted_text
        context_parts.append(text)

        prompt = "\n".join(context_parts)

        try:
            agent = self._get_agent()
            result = await agent.run(prompt)
            return result.output.facts
        except Exception as e:
            logger.warning(f"Facts extraction failed for attachment {attachment_id}: {e}")
            return []

    def store_facts(
        self,
        source_type: str,
        source_id: str,
        facts: list[ExtractedFact],
    ) -> int:
        """
        Persist extracted facts to the database.

        Args:
            source_type: 'email' or 'attachment'
            source_id: The email or attachment ID
            facts: List of facts to store

        Returns:
            Number of facts stored
        """
        if not facts:
            return 0

        conn = get_connection()
        stored = 0

        for fact in facts:
            fact_id = str(uuid.uuid4())

            try:
                conn.execute(
                    """
                    INSERT INTO facts (
                        id, source_type, source_id, fact_type, fact_value,
                        context, confidence, entity_normalized, metadata_json,
                        status, due_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)
                    """,
                    (
                        fact_id,
                        source_type,
                        source_id,
                        fact.fact_type.value,
                        fact.fact_value,
                        fact.context,
                        fact.confidence,
                        fact.entity_normalized,
                        json.dumps(fact.metadata) if fact.metadata else None,
                        fact.due_date,
                    ),
                )
                stored += 1
            except Exception as e:
                logger.warning(f"Failed to store fact: {e}")

        conn.commit()
        conn.close()

        if stored > 0:
            logger.debug(f"Stored {stored} facts for {source_type}:{source_id}")

        return stored


def search_facts(
    query: str,
    fact_types: list[str] | None = None,
    status: str = "active",
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Search facts using FTS.

    Args:
        query: Search query
        fact_types: Optional list of fact types to filter
        status: Status filter ('active', 'resolved', 'expired')
        limit: Maximum results

    Returns:
        List of matching facts with source info
    """
    conn = get_connection()

    # Build query
    if fact_types:
        placeholders = ",".join("?" for _ in fact_types)
        type_filter = f"AND f.fact_type IN ({placeholders})"
        params = [query, status] + fact_types + [limit]
    else:
        type_filter = ""
        params = [query, status, limit]

    rows = conn.execute(
        f"""
        SELECT
            f.id, f.source_type, f.source_id, f.fact_type, f.fact_value,
            f.context, f.confidence, f.entity_normalized, f.metadata_json,
            f.status, f.due_date, f.extracted_at,
            e.subject as email_subject, e.sender as email_sender, e.received_at,
            a.filename as attachment_filename
        FROM facts_fts ft
        JOIN facts f ON ft.id = f.id
        LEFT JOIN emails e ON f.source_type = 'email' AND f.source_id = e.id
        LEFT JOIN attachments a ON f.source_type = 'attachment' AND f.source_id = a.id
        WHERE facts_fts MATCH ?
          AND f.status = ?
          {type_filter}
        ORDER BY f.extracted_at DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    conn.close()

    results = []
    for row in rows:
        result = {
            "id": row["id"],
            "source_type": row["source_type"],
            "source_id": row["source_id"],
            "fact_type": row["fact_type"],
            "fact_value": row["fact_value"],
            "context": row["context"],
            "confidence": row["confidence"],
            "entity_normalized": row["entity_normalized"],
            "status": row["status"],
            "due_date": row["due_date"],
            "extracted_at": row["extracted_at"],
        }

        # Add source info
        if row["source_type"] == "email":
            result["email_subject"] = row["email_subject"]
            result["email_sender"] = row["email_sender"]
            result["email_date"] = row["received_at"]
        elif row["source_type"] == "attachment":
            result["filename"] = row["attachment_filename"]

        # Parse metadata
        if row["metadata_json"]:
            try:
                result["metadata"] = json.loads(row["metadata_json"])
            except json.JSONDecodeError:
                pass

        results.append(result)

    return results


def get_facts_for_source(source_type: str, source_id: str) -> list[dict[str, Any]]:
    """
    Get all facts for a specific email or attachment.

    Args:
        source_type: 'email' or 'attachment'
        source_id: The source ID

    Returns:
        List of facts
    """
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT id, fact_type, fact_value, context, confidence,
               entity_normalized, metadata_json, status, due_date, extracted_at
        FROM facts
        WHERE source_type = ? AND source_id = ?
        ORDER BY extracted_at DESC
        """,
        (source_type, source_id),
    ).fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_pending_action_items(limit: int = 50) -> list[dict[str, Any]]:
    """
    Get pending decisions and commitments.

    Returns:
        List of pending action items with source info
    """
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT
            f.id, f.fact_type, f.fact_value, f.context, f.due_date,
            f.extracted_at, f.source_type, f.source_id,
            e.subject, e.sender, e.received_at, e.web_link
        FROM facts f
        LEFT JOIN emails e ON f.source_type = 'email' AND f.source_id = e.id
        WHERE f.fact_type IN ('decision', 'commitment', 'action_item')
          AND f.status = 'active'
        ORDER BY
            CASE WHEN f.due_date IS NOT NULL THEN 0 ELSE 1 END,
            f.due_date ASC,
            f.extracted_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    return [dict(row) for row in rows]


def resolve_fact(fact_id: str, resolution: str | None = None) -> bool:
    """
    Mark a fact as resolved.

    Args:
        fact_id: The fact ID
        resolution: Optional resolution text

    Returns:
        True if updated
    """
    conn = get_connection()
    cursor = conn.execute(
        """
        UPDATE facts
        SET status = 'resolved', resolved_at = datetime('now')
        WHERE id = ? AND status = 'active'
        """,
        (fact_id,),
    )
    conn.commit()
    updated = cursor.rowcount > 0
    conn.close()
    return updated
