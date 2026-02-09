"""Working Memory Updater - processes emails to update working memory state."""

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic_ai import Agent

from ..agent_profile import append_agent_profile
from ..database import get_connection
from ..model_utils import parse_model_string, get_model_settings
from .models import EmailAnalysis, ObservationType

logger = logging.getLogger(__name__)


def _build_wm_analysis_agent() -> Agent:
    """Build the AI agent for email analysis and working memory extraction."""
    # Use WM_MODEL if set, otherwise fall back to MODEL_NAME
    # Default to mini model - WM analysis requires moderate reasoning
    model_string = os.getenv(
        "WM_MODEL",
        os.getenv("MODEL_NAME", "openai-responses:gpt-5-mini")
    )
    model_name, _ = parse_model_string(model_string)
    model_settings = get_model_settings(model_string)

    system_prompt = """
You are an executive assistant analyzing emails to update working memory.
Your goal is to extract structured intelligence that helps maintain continuous awareness.

## Email Type Classification
First, classify the email type:
- NEWSLETTER: Mass-sent content (digests, updates, marketing). Urgency: someday. No reply needed.
- AUTOMATED: System notifications, receipts, password resets. Urgency: someday. No reply needed.
- TRANSACTIONAL: Order confirmations, account alerts. Urgency: someday. No reply needed.
- DIRECT: Personal correspondence requiring human attention.

For newsletters/automated/transactional emails:
- no projects, no decisions, no commitments
- still extract lightweight learning signals about user interests (topic, source, relevance)

## For DIRECT emails (user is in TO):
- Identify if a reply is needed and by when
- Extract decisions ONLY if someone explicitly asks a question requiring user input
- Note commitments ONLY if the user explicitly promised to do something specific
- Assess urgency based on content and sender relationship

## For CC'd emails (user is in CC only):
- Focus on PASSIVE LEARNING - observe but don't suggest actions
- Note context that might be useful later (project updates, team dynamics)
- Do NOT suggest the user needs to reply unless explicitly called out

## Project Extraction (STRICT RULES)
Only extract as projects:
- Explicit named initiatives the user is working on (e.g., "Agent Aech deployment", "AWS integration")
- Business deals or partnerships being discussed
- Internal company projects with clear ownership

DO NOT extract as projects:
- Products or services mentioned (Microsoft 365, Azure, OpenAI)
- News topics from newsletters (CHIPS Act, AI regulations, market trends)
- Vendor names or categories (Finance, Travel, Microsoft)
- Generic activities (subscription management, password reset)
- Conference/event names unless user is presenting or organizing

## Output Guidelines
- thread_summary_update: One sentence about what this email adds. Be concise.
- key_points: 1-3 actionable facts (can be empty for newsletters/automated)
- pending_questions: Only explicit questions awaiting user's answer
- decisions_requested: Must include the actual question and context. Empty if none.
- commitments_made: Must include what was promised and to whom. Empty if none.
- observations: Brief context learned.
  For newsletters/automated content, include at most 1-2 concise "interest_signal" observations
  when content indicates recurring topics the principal cares about.
- project_mentions: Apply strict rules above. Return empty list for newsletters.
- suggested_urgency: immediate/today/this_week/someday
- needs_reply: true ONLY if human response is expected from user
- next_action_owner: principal/counterparty/shared/unknown (principal = mailbox owner)
- sender_org_relation: internal/external/unknown relative to principal organization
- value_flow_direction: toward_principal/away_from_principal/not_applicable/unknown
- role_context_note: one sentence to prevent principal/counterparty role inversion
- role_context_confidence: confidence score 0-1

## Content Extraction (CRITICAL for search indexing)
- extracted_new_content: Extract ONLY the new content written by the sender in THIS email.
  EXCLUDE all of the following:
  - Quoted replies (text after "On X wrote:", "> " prefixed lines, forwarded headers)
  - Email signatures (name, title, phone, address blocks)
  - Legal disclaimers and confidentiality notices
  - "Sent from my iPhone/Android" footers
  - Forwarded message headers (From:, To:, Subject:, Date: blocks)
  Return ONLY the fresh content the sender actually wrote. This is used for search indexing.

## Thread Summary
- thread_summary: Generate a 1-3 sentence summary of the conversation thread.
  This should capture:
  - What the thread is about
  - Key participants and their roles
  - Current state (waiting on someone, decision made, etc.)
  Keep it concise - this provides context for understanding individual emails.

## Signature Extraction
- signature_block: Extract the sender's email signature if present.
  Include: name, job title, company, phone numbers, addresses, social links.
  Return empty string if no signature is found.
  This is valuable context about who the sender is.

## Principal Identity
- PRINCIPAL_MAILBOX and PRINCIPAL_DOMAIN describe who the assistant works for.
- Do not invert roles: infer context relative to principal, not sender-centric defaults.
- If uncertain, use unknown (do not guess).

## Inbox Cleanup Action
- suggested_action: Recommend 'keep', 'archive', or 'delete'.
  DELETE: calendar accepts/declines/tentative, delivery receipts, read receipts,
          out-of-office auto-replies, unsubscribe confirmations,
          expired auth/verification codes (check expiry time vs current time),
          and low-value newsletters/digests/marketing after extracting interest signals.
  ARCHIVE: FYI-only notifications or automated reports that should remain in mailbox.
  KEEP: real conversations, actionable items, unexpired auth codes.
"""

    instructions = append_agent_profile(
        system_prompt,
        capability_name="working_memory",
    )

    return Agent(
        model_name,
        output_type=EmailAnalysis,
        instructions=instructions,
        model_settings=model_settings,
    )


class WorkingMemoryUpdater:
    """Updates working memory based on incoming emails."""

    def __init__(self, user_email: str):
        self.user_email = user_email
        self.user_domain = user_email.split("@")[-1].lower() if "@" in user_email else ""
        self._agent: Agent | None = None

    def _get_agent(self) -> Agent:
        """Lazy-load the analysis agent."""
        if self._agent is None:
            self._agent = _build_wm_analysis_agent()
        return self._agent

    def is_user_cc(self, email: dict) -> bool:
        """
        Determine if user is CC'd vs direct recipient.

        Returns True if user is in CC but NOT in TO.
        """
        to_emails = json.loads(email.get("to_emails") or "[]")
        cc_emails = json.loads(email.get("cc_emails") or "[]")

        user_lower = self.user_email.lower()
        to_lower = [str(e).lower() for e in to_emails]
        cc_lower = [str(e).lower() for e in cc_emails]

        return user_lower in cc_lower and user_lower not in to_lower

    async def process_email(
        self,
        email: dict,
        category_decision: dict | None = None,
    ) -> EmailAnalysis:
        """
        Process an email and update working memory.

        Args:
            email: Email row from database (dict-like)
            category_decision: Optional categorization result from organizer
        """
        is_cc = self.is_user_cc(email)

        # Run AI analysis
        context = self._build_analysis_context(email, is_cc, category_decision)
        try:
            result = await self._get_agent().run(context)
            analysis = result.output
        except Exception as e:
            raise RuntimeError(f"WM analysis failed for email {email.get('id')}: {e}") from e

        # Log LLM usage for cost tracking
        try:
            usage = result.usage()
            model = os.getenv("WM_MODEL", os.getenv("MODEL_NAME", "gpt-5-mini"))
            logger.info(
                f"LLM_USAGE task=wm_analysis model={model} "
                f"in={usage.request_tokens} out={usage.response_tokens}"
            )
        except Exception:
            pass  # Usage tracking is best-effort

        conn = get_connection()
        try:
            # Record observations to facts table (especially from CC emails)
            if is_cc or analysis.observations:
                self._record_observations(conn, email, analysis, is_cc)

            # Track pending decisions to facts table (only from direct emails)
            if not is_cc:
                for decision in analysis.decisions_requested:
                    self._add_pending_decision(conn, email, decision)

            # Track commitments to facts table
            for commitment in analysis.commitments_made:
                self._add_commitment(conn, email, commitment)

            # Store LLM-extracted content and mark as processed
            conn.execute(
                """UPDATE emails
                   SET body_markdown = COALESCE(?, body_markdown),
                       thread_summary = ?,
                       signature_block = COALESCE(?, signature_block),
                       wm_needs_reply = ?,
                       wm_reply_deadline = ?,
                       next_action_owner = ?,
                       sender_org_relation = ?,
                       value_flow_direction = ?,
                       role_context_note = ?,
                       role_context_confidence = ?,
                       suggested_action = ?,
                       wm_processed_at = COALESCE(wm_processed_at, datetime('now')),
                       processed_at = COALESCE(processed_at, datetime('now'))
                   WHERE id = ?""",
                (analysis.extracted_new_content, analysis.thread_summary,
                 analysis.signature_block, analysis.needs_reply,
                 analysis.reply_deadline, analysis.next_action_owner,
                 analysis.sender_org_relation, analysis.value_flow_direction,
                 analysis.role_context_note, analysis.role_context_confidence,
                 analysis.suggested_action, email.get("id")),
            )

            conn.commit()
            logger.debug(
                f"Working memory updated for email {email.get('id')} (CC={is_cc})"
            )
            return analysis
        except Exception as e:
            logger.error(f"Failed to update working memory for {email.get('id')}: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def _build_analysis_context(
        self,
        email: dict,
        is_cc: bool,
        category_decision: dict | None,
    ) -> str:
        """Build context string for AI analysis."""
        mode = "CC (passive learning - observe only)" if is_cc else "DIRECT (may need action)"
        category = (category_decision or {}).get("category", "Unknown")
        requires_reply = (category_decision or {}).get("requires_reply", False)
        labels = (category_decision or {}).get("labels", [])

        # Get body - prefer full body_markdown, fall back to preview
        body = email.get("body_markdown") or email.get("body_preview") or ""
        # Truncate very long bodies
        if len(body) > 4000:
            body = body[:4000] + "..."

        return f"""
EMAIL MODE: {mode}
CATEGORY: {category}
REQUIRES_REPLY (from triage): {requires_reply}
LABELS (from triage): {json.dumps(labels)}
PRINCIPAL_MAILBOX: {self.user_email}
PRINCIPAL_DOMAIN: {self.user_domain or "unknown"}

FROM: {email.get('sender', 'Unknown')}
TO: {email.get('to_emails', '[]')}
CC: {email.get('cc_emails', '[]')}
SUBJECT: {email.get('subject', '')}
RECEIVED: {email.get('received_at', '')}
CONVERSATION_ID: {email.get('conversation_id', '')}

BODY:
{body}
"""

    def _record_observations(
        self,
        conn,
        email: dict,
        analysis: EmailAnalysis,
        is_cc: bool,
    ) -> None:
        """Record observations to the unified facts table."""
        now = datetime.now(timezone.utc).isoformat()

        # If CC and no explicit observations, create a generic one
        if is_cc and not analysis.observations:
            observation = {
                "type": ObservationType.CONTEXT_LEARNED.value,
                "content": f"Observed thread: {email.get('subject', 'Unknown')}",
                "importance": 0.3,
            }
            analysis.observations.append(observation)

        # Map observation types to fact types
        obs_to_fact_type = {
            "context_learned": "preference",
            "person_introduced": "relationship",
            "meeting_scheduled": "pattern",
            "status_update": "preference",
            "project_mention": "pattern",
            "decision_made": "preference",
            "deadline_mentioned": "pattern",
            "interest_signal": "pattern",
            "commitment_made": "commitment",  # Should be handled by _add_commitment
        }

        for obs in analysis.observations:
            obs_type = obs.get("type", ObservationType.CONTEXT_LEARNED.value)
            # Skip commitments - handled separately
            if obs_type == "commitment_made":
                continue

            # Map to fact type
            fact_type = obs_to_fact_type.get(obs_type, "preference")

            # Build metadata
            metadata = {
                "observation_type": obs_type,
                "conversation_id": email.get("conversation_id"),
            }

            conn.execute(
                """
                INSERT INTO facts (
                    id, source_type, source_id, fact_type, fact_value,
                    confidence, metadata_json, status, extracted_at
                ) VALUES (?, 'email', ?, ?, ?, ?, ?, 'active', ?)
                """,
                (
                    str(uuid.uuid4()),
                    email.get("id"),
                    fact_type,
                    obs.get("content", ""),
                    obs.get("confidence", 0.5),
                    json.dumps(metadata) if metadata else None,
                    now,
                ),
            )
            self._upsert_derived_learning(
                conn=conn,
                learning_type=(obs_type if obs_type == "interest_signal" else fact_type),
                summary=obs.get("content", ""),
                confidence=float(obs.get("confidence", 0.5) or 0.5),
                metadata=metadata,
            )

    def _add_pending_decision(
        self,
        conn,
        email: dict,
        decision: dict[str, Any],
    ) -> None:
        """Add a pending decision to the unified facts table."""
        now = datetime.now(timezone.utc).isoformat()

        # Build metadata with options and requester
        metadata = {}
        if decision.get("options"):
            metadata["options"] = decision.get("options")
        metadata["requester"] = email.get("sender", "")
        metadata["conversation_id"] = email.get("conversation_id")

        conn.execute(
            """
            INSERT INTO facts (
                id, source_type, source_id, fact_type, fact_value,
                context, confidence, metadata_json, status, due_date, extracted_at
            ) VALUES (?, 'email', ?, 'decision', ?, ?, 0.9, ?, 'active', ?, ?)
            """,
            (
                str(uuid.uuid4()),
                email.get("id"),
                decision.get("question", ""),
                decision.get("context", ""),
                json.dumps(metadata) if metadata else None,
                decision.get("deadline"),
                now,
            ),
        )
        self._upsert_derived_learning(
            conn=conn,
            learning_type="pattern",
            summary=f"Pending decision requested: {decision['question']}",
            confidence=float(decision.get("confidence", 0.7) or 0.7),
            metadata=metadata,
        )

    def _add_commitment(
        self,
        conn,
        email: dict,
        commitment: dict[str, Any],
    ) -> None:
        """Add a commitment to the unified facts table."""
        now = datetime.now(timezone.utc).isoformat()

        # Build metadata with to_whom
        metadata = {
            "to_whom": commitment.get("to_whom") or email.get("sender", ""),
            "conversation_id": email.get("conversation_id"),
        }

        conn.execute(
            """
            INSERT INTO facts (
                id, source_type, source_id, fact_type, fact_value,
                confidence, metadata_json, status, due_date, extracted_at
            ) VALUES (?, 'email', ?, 'commitment', ?, 0.9, ?, 'active', ?, ?)
            """,
            (
                str(uuid.uuid4()),
                email.get("id"),
                commitment.get("description", ""),
                json.dumps(metadata) if metadata else None,
                commitment.get("due_by"),
                now,
            ),
        )
        self._upsert_derived_learning(
            conn=conn,
            learning_type="pattern",
            summary=f"Commitment pattern: {commitment['description']}",
            confidence=float(commitment.get("confidence", 0.7) or 0.7),
            metadata=metadata,
        )

    def _upsert_derived_learning(
        self,
        conn,
        learning_type: str,
        summary: str,
        confidence: float,
        metadata: dict[str, Any] | None,
    ) -> None:
        """Store durable derived knowledge independent from email-row retention."""
        clean_summary = str(summary or "").strip()
        if not clean_summary:
            return

        normalized = " ".join(clean_summary.lower().split())
        learning_key = hashlib.sha256(
            f"{learning_type}|{normalized}".encode("utf-8")
        ).hexdigest()
        now = datetime.now(timezone.utc).isoformat()

        conn.execute(
            """
            INSERT INTO derived_learnings (
                id, learning_key, learning_type, summary, confidence,
                metadata_json, seen_count, created_at, updated_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
            ON CONFLICT(learning_key) DO UPDATE SET
                learning_type = excluded.learning_type,
                summary = CASE
                    WHEN excluded.confidence >= derived_learnings.confidence
                    THEN excluded.summary
                    ELSE derived_learnings.summary
                END,
                confidence = CASE
                    WHEN excluded.confidence >= derived_learnings.confidence
                    THEN excluded.confidence
                    ELSE derived_learnings.confidence
                END,
                metadata_json = COALESCE(excluded.metadata_json, derived_learnings.metadata_json),
                seen_count = derived_learnings.seen_count + 1,
                updated_at = excluded.updated_at,
                last_seen_at = excluded.last_seen_at
            """,
            (
                str(uuid.uuid4()),
                learning_key,
                learning_type if learning_type in {"interest_signal", "preference", "relationship", "pattern", "other"} else "other",
                clean_summary[:500],
                max(0.0, min(1.0, float(confidence))),
                json.dumps(metadata) if metadata else None,
                now,
                now,
                now,
            ),
        )
