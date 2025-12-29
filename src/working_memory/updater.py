"""Working Memory Updater - processes emails to update working memory state."""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic_ai import Agent

from ..database import get_connection
from .models import EmailAnalysis, ObservationType, UrgencyLevel

logger = logging.getLogger(__name__)


def _build_wm_analysis_agent() -> Agent:
    """Build the AI agent for email analysis and working memory extraction."""
    # Use WM_MODEL if set, otherwise fall back to MODEL_NAME
    # Default to mini model - WM analysis requires moderate reasoning
    model_name = os.getenv(
        "WM_MODEL",
        os.getenv("MODEL_NAME", "openai-responses:gpt-5-mini")
    )

    system_prompt = """
You are an executive assistant analyzing emails to update working memory.
Your goal is to extract structured intelligence that helps maintain continuous awareness.

## Email Type Classification
First, classify the email type:
- NEWSLETTER: Mass-sent content (digests, updates, marketing). Urgency: someday. No reply needed.
- AUTOMATED: System notifications, receipts, password resets. Urgency: someday. No reply needed.
- TRANSACTIONAL: Order confirmations, account alerts. Urgency: someday. No reply needed.
- DIRECT: Personal correspondence requiring human attention.

For newsletters/automated/transactional emails: extract minimal info, no projects, no decisions.

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
- key_points: 1-3 actionable facts (skip for newsletters/automated)
- pending_questions: Only explicit questions awaiting user's answer
- decisions_requested: Must include the actual question and context. Empty if none.
- commitments_made: Must include what was promised and to whom. Empty if none.
- observations: Brief context learned. Skip generic newsletter content.
- project_mentions: Apply strict rules above. Return empty list for newsletters.
- suggested_urgency: immediate/today/this_week/someday
- needs_reply: true ONLY if human response is expected from user

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

## Inbox Cleanup Action
- suggested_action: Recommend 'keep', 'archive', or 'delete'.
  DELETE: calendar accepts/declines/tentative, delivery receipts, read receipts,
          out-of-office auto-replies, unsubscribe confirmations,
          expired auth/verification codes (check expiry time vs current time).
  ARCHIVE: newsletters already read, FYI-only notifications, automated reports.
  KEEP: real conversations, actionable items, unexpired auth codes.
"""

    return Agent(
        model_name,
        output_type=EmailAnalysis,
        system_prompt=system_prompt,
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
    ) -> None:
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

        except Exception as e:
            logger.warning(f"Working memory analysis failed for {email.get('id')}: {e}")
            # Fall back to basic updates without AI analysis
            analysis = EmailAnalysis()

        conn = get_connection()
        try:
            # Update or create thread
            self._update_thread(conn, email, analysis, is_cc)

            # Update contacts
            self._update_contacts(conn, email, analysis)

            # Record observations (especially from CC emails)
            if is_cc or analysis.observations:
                self._record_observations(conn, email, analysis, is_cc)

            # Track pending decisions (only from direct emails)
            if not is_cc:
                for decision in analysis.decisions_requested:
                    self._add_pending_decision(conn, email, decision)

            # Track commitments
            for commitment in analysis.commitments_made:
                self._add_commitment(conn, email, commitment)

            # Update projects
            self._update_projects(conn, email, analysis)

            # Store LLM-extracted content
            conn.execute(
                """UPDATE emails
                   SET body_markdown = COALESCE(?, body_markdown),
                       thread_summary = ?,
                       signature_block = COALESCE(?, signature_block),
                       suggested_action = ?
                   WHERE id = ?""",
                (analysis.extracted_new_content, analysis.thread_summary,
                 analysis.signature_block, analysis.suggested_action, email.get("id")),
            )

            conn.commit()
            logger.debug(
                f"Working memory updated for email {email.get('id')} (CC={is_cc})"
            )
        except Exception as e:
            logger.error(f"Failed to update working memory for {email.get('id')}: {e}")
            conn.rollback()
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

        # Get body - prefer full body_markdown, fall back to preview
        body = email.get("body_markdown") or email.get("body_preview") or ""
        # Truncate very long bodies
        if len(body) > 4000:
            body = body[:4000] + "..."

        return f"""
EMAIL MODE: {mode}
CATEGORY: {category}
REQUIRES_REPLY (from triage): {requires_reply}

FROM: {email.get('sender', 'Unknown')}
TO: {email.get('to_emails', '[]')}
CC: {email.get('cc_emails', '[]')}
SUBJECT: {email.get('subject', '')}
RECEIVED: {email.get('received_at', '')}
CONVERSATION_ID: {email.get('conversation_id', '')}

BODY:
{body}
"""

    def _update_thread(
        self,
        conn,
        email: dict,
        analysis: EmailAnalysis,
        is_cc: bool,
    ) -> None:
        """Update or create thread record."""
        # Use conversation_id if available, otherwise fall back to email id
        # This means emails without conversation_id become single-message threads
        conversation_id = email.get("conversation_id") or email.get("id")
        if not conversation_id:
            return

        now = datetime.now(timezone.utc).isoformat()
        received_at = email.get("received_at") or now

        # Check if thread exists
        existing = conn.execute(
            "SELECT * FROM wm_threads WHERE conversation_id = ?",
            (conversation_id,),
        ).fetchone()

        if existing:
            # Update existing thread
            message_count = (existing["message_count"] or 0) + 1

            # Merge key points
            existing_points = json.loads(existing["key_points_json"] or "[]")
            new_points = (existing_points + analysis.key_points)[-10:]  # Keep last 10

            # Update pending questions (replace with latest)
            pending_q = analysis.pending_questions or json.loads(
                existing["pending_questions_json"] or "[]"
            )

            conn.execute(
                """
                UPDATE wm_threads SET
                    last_activity_at = ?,
                    message_count = ?,
                    summary = COALESCE(?, summary),
                    key_points_json = ?,
                    pending_questions_json = ?,
                    needs_reply = CASE WHEN ? THEN 1 ELSE needs_reply END,
                    urgency = CASE WHEN ? != 'this_week' THEN ? ELSE urgency END,
                    latest_email_id = ?,
                    latest_web_link = ?,
                    updated_at = ?
                WHERE conversation_id = ?
                """,
                (
                    received_at,
                    message_count,
                    analysis.thread_summary_update,
                    json.dumps(new_points),
                    json.dumps(pending_q),
                    analysis.needs_reply,
                    analysis.suggested_urgency.value,
                    analysis.suggested_urgency.value,
                    email.get("id"),
                    email.get("web_link"),  # Folder-agnostic deep link from Graph API
                    now,
                    conversation_id,
                ),
            )
        else:
            # Create new thread
            participants = set()
            sender = email.get("sender", "")
            if sender:
                participants.add(sender)
            participants.update(json.loads(email.get("to_emails") or "[]"))
            participants.update(json.loads(email.get("cc_emails") or "[]"))
            participants.discard("")

            conn.execute(
                """
                INSERT INTO wm_threads (
                    id, conversation_id, subject, participants_json,
                    status, urgency, started_at, last_activity_at,
                    summary, key_points_json, pending_questions_json,
                    message_count, user_is_cc, needs_reply,
                    latest_email_id, latest_web_link, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    conversation_id,
                    email.get("subject", ""),
                    json.dumps(list(participants)),
                    "active",
                    analysis.suggested_urgency.value,
                    received_at,
                    received_at,
                    analysis.thread_summary_update or "",
                    json.dumps(analysis.key_points),
                    json.dumps(analysis.pending_questions),
                    1,
                    is_cc,
                    analysis.needs_reply,
                    email.get("id"),
                    email.get("web_link"),  # Folder-agnostic deep link from Graph API
                    now,
                    now,
                ),
            )

    def _update_contacts(self, conn, email: dict, analysis: EmailAnalysis) -> None:
        """Update contact records for all participants."""
        sender = (email.get("sender") or "").lower().strip()
        if not sender:
            return

        now = datetime.now(timezone.utc).isoformat()
        received_at = email.get("received_at") or now

        # Update sender (they initiated)
        self._upsert_contact(
            conn, sender, received_at, now, they_initiated=True, is_cc=False
        )

        # Update CC recipients
        cc_emails = json.loads(email.get("cc_emails") or "[]")
        for cc_email in cc_emails:
            cc_lower = str(cc_email).lower().strip()
            if cc_lower and cc_lower != self.user_email.lower():
                self._upsert_contact(
                    conn, cc_lower, received_at, now, they_initiated=False, is_cc=True
                )

        # Update TO recipients (excluding user)
        to_emails = json.loads(email.get("to_emails") or "[]")
        for to_email in to_emails:
            to_lower = str(to_email).lower().strip()
            if to_lower and to_lower != self.user_email.lower():
                self._upsert_contact(
                    conn, to_lower, received_at, now, they_initiated=False, is_cc=False
                )

    def _upsert_contact(
        self,
        conn,
        email: str,
        received_at: str,
        now: str,
        they_initiated: bool = False,
        is_cc: bool = False,
    ) -> None:
        """Insert or update a contact record."""
        domain = email.split("@")[-1] if "@" in email else ""
        is_internal = domain == self.user_domain

        existing = conn.execute(
            "SELECT id FROM wm_contacts WHERE email = ?",
            (email,),
        ).fetchone()

        if existing:
            # Build incremental update
            updates = ["last_interaction_at = ?", "total_interactions = total_interactions + 1", "updated_at = ?"]
            params: list[Any] = [received_at, now]

            if they_initiated:
                updates.append("they_initiated_count = they_initiated_count + 1")
            if is_cc:
                updates.append("cc_count = cc_count + 1")

            params.append(email)
            conn.execute(
                f"UPDATE wm_contacts SET {', '.join(updates)} WHERE email = ?",
                params,
            )
        else:
            conn.execute(
                """
                INSERT INTO wm_contacts (
                    id, email, is_internal, first_seen_at,
                    last_interaction_at, total_interactions,
                    they_initiated_count, cc_count, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    email,
                    is_internal,
                    received_at,
                    received_at,
                    1,
                    1 if they_initiated else 0,
                    1 if is_cc else 0,
                    now,
                    now,
                ),
            )

    def _record_observations(
        self,
        conn,
        email: dict,
        analysis: EmailAnalysis,
        is_cc: bool,
    ) -> None:
        """Record observations from email analysis."""
        now = datetime.now(timezone.utc).isoformat()

        # If CC and no explicit observations, create a generic one
        if is_cc and not analysis.observations:
            observation = {
                "type": ObservationType.CONTEXT_LEARNED.value,
                "content": f"Observed thread: {email.get('subject', 'Unknown')}",
                "importance": 0.3,
            }
            analysis.observations.append(observation)

        for obs in analysis.observations:
            obs_type = obs.get("type", ObservationType.CONTEXT_LEARNED.value)
            # Validate observation type against enum
            try:
                ObservationType(obs_type)
            except ValueError:
                obs_type = ObservationType.CONTEXT_LEARNED.value

            conn.execute(
                """
                INSERT INTO wm_observations (
                    id, type, content, source_email_id, source_thread_id,
                    importance, confidence, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    obs_type,
                    obs.get("content", ""),
                    email.get("id"),
                    email.get("conversation_id"),
                    obs.get("importance", 0.5),
                    obs.get("confidence", 0.5),
                    now,
                ),
            )

    def _add_pending_decision(
        self,
        conn,
        email: dict,
        decision: dict[str, Any],
    ) -> None:
        """Add a pending decision."""
        now = datetime.now(timezone.utc).isoformat()

        # Map urgency string to enum value
        urgency_str = decision.get("urgency", "this_week")
        try:
            urgency = UrgencyLevel(urgency_str).value
        except ValueError:
            urgency = UrgencyLevel.THIS_WEEK.value

        conn.execute(
            """
            INSERT INTO wm_decisions (
                id, question, context, options_json,
                source_email_id, source_thread_id, requester, urgency,
                deadline, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                decision.get("question", ""),
                decision.get("context", ""),
                json.dumps(decision.get("options", [])),
                email.get("id"),
                email.get("conversation_id"),
                email.get("sender", ""),
                urgency,
                decision.get("deadline"),
                now,
                now,
            ),
        )

    def _add_commitment(
        self,
        conn,
        email: dict,
        commitment: dict[str, Any],
    ) -> None:
        """Add a commitment."""
        now = datetime.now(timezone.utc).isoformat()

        conn.execute(
            """
            INSERT INTO wm_commitments (
                id, description, to_whom, source_email_id,
                committed_at, due_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                commitment.get("description", ""),
                commitment.get("to_whom") or email.get("sender", ""),
                email.get("id"),
                now,
                commitment.get("due_by"),
                now,
            ),
        )

    def _update_projects(
        self,
        conn,
        email: dict,
        analysis: EmailAnalysis,
    ) -> None:
        """Update or create project records based on mentions."""
        if not analysis.project_mentions:
            return

        now = datetime.now(timezone.utc).isoformat()
        received_at = email.get("received_at") or now

        for project_name in analysis.project_mentions:
            if not project_name or len(project_name) < 2:
                continue

            # Normalize project name for matching
            project_lower = project_name.lower().strip()

            # Look for existing project with similar name
            existing = conn.execute(
                "SELECT * FROM wm_projects WHERE LOWER(name) = ?",
                (project_lower,),
            ).fetchone()

            if existing:
                # Update existing project
                threads = json.loads(existing["related_threads_json"] or "[]")
                conversation_id = email.get("conversation_id")
                if conversation_id and conversation_id not in threads:
                    threads.append(conversation_id)

                conn.execute(
                    """
                    UPDATE wm_projects SET
                        last_activity_at = ?,
                        related_threads_json = ?,
                        confidence = MIN(1.0, confidence + 0.1),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        received_at,
                        json.dumps(threads[-20:]),  # Keep last 20
                        now,
                        existing["id"],
                    ),
                )
            else:
                # Create new project
                conn.execute(
                    """
                    INSERT INTO wm_projects (
                        id, name, related_threads_json,
                        first_mentioned_at, last_activity_at,
                        confidence, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        project_name,
                        json.dumps(
                            [email.get("conversation_id")]
                            if email.get("conversation_id")
                            else []
                        ),
                        received_at,
                        received_at,
                        0.3,  # Low initial confidence for inferred projects
                        now,
                        now,
                    ),
                )
