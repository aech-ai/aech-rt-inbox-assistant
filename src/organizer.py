import logging
import json
from typing import Optional
from pydantic import BaseModel, Field
from pydantic_ai import Agent
from .database import get_connection
from .poller import GraphPoller
from .triggers import write_trigger
from .folders_config import STANDARD_FOLDERS, FOLDER_ALIASES

logger = logging.getLogger(__name__)

class EmailCategory(BaseModel):
    category: str = Field(description="The category of the email (e.g., Work, Personal, Spam, Newsletter, Finance, Urgent)")
    reason: str = Field(description="The reason for this categorization")
    action: str = Field(description="Recommended action: 'move', 'delete', 'mark_important', 'none'")
    destination_folder: Optional[str] = Field(description="Name of the folder to move to, if action is 'move'")

import os

def _build_agent() -> Agent:
    """Create the AI agent lazily to avoid requiring API keys at import time."""
    model_name = os.getenv("MODEL_NAME", "openai-responses:gpt-4.1-mini")
    allowed_folders = ", ".join(STANDARD_FOLDERS)
    allowed_categories = ", ".join([
        "Work", "Personal", "Newsletter", "Social", "Updates", "Security Notifications",
        "Receipts", "Promotions", "Finance", "Urgent", "Delete"
    ])
    system_prompt = f"""
You are an expert email organizer. Analyze the email and choose a category, action, and destination folder.

Categories allowed:
- Must be exactly one of: {allowed_categories}
- Use 'Delete' only when the email is spam/phishing/junk where deletion is safe.
- Use 'Urgent' only for truly time-sensitive items.

Rules for destination_folder:
- Must be exactly one of: {allowed_folders}
- Do NOT invent new folders or subfolders; no slashes or custom names.
- If nothing precise fits, choose the closest from the list (e.g., LinkedIn or networking → Social; general work topics → Work; generic notifications → Updates; digests → Newsletters; security alerts → Security Notifications; order/receipts → Receipts).
- If no move is appropriate, set action='none' and leave destination_folder null.

Actions allowed:
- move: only when destination_folder is in the approved list above.
- delete: only when the email is clearly junk, spam, phishing, or otherwise safe to remove. Do NOT set a destination_folder for delete.
- mark_important: only for genuinely urgent/time-sensitive items.
- none: when no action is needed.

For suspected junk that should be reviewed (not hard-deleted), use action='move' with destination_folder='Should Delete'.
"""
    return Agent(
        model_name,
        output_type=EmailCategory,
        system_prompt=system_prompt,
    )

class Organizer:
    def __init__(self, db_path: str, poller: GraphPoller, user_email: str):
        self.db_path = db_path
        self.poller = poller
        self.user_email = user_email
        self.agent: Optional[Agent] = None

    def _get_agent(self) -> Agent:
        if self.agent is None:
            self.agent = _build_agent()
        return self.agent

    async def organize_emails(self):
        """Iterate over unprocessed emails and organize them."""
        conn = get_connection(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM emails WHERE processed_at IS NULL")
        emails = cursor.fetchall()
        conn.close()

        for email in emails:
            await self._process_email(email)

    async def _process_email(self, email):
        conn = get_connection(self.db_path)
        logger.info(f"Processing email {email['id']}: {email['subject']}")
        
        # Construct context for AI
        email_content = f"Subject: {email['subject']}\nSender: {email['sender']}\nPreview: {email['body_preview']}"
        
        try:
            # Run AI Agent
            result = await self._get_agent().run(email_content)
            decision = result.output
            
            logger.info(f"AI Decision for {email['id']}: {decision}")
            
            # Execute Action
            self._execute_action(email['id'], decision)
            
            # Log to Triage Log
            conn.execute("""
            INSERT INTO triage_log (email_id, action, destination_folder, reason)
            VALUES (?, ?, ?, ?)
            """, (email['id'], decision.action, decision.destination_folder, decision.reason))
            
            # Mark as processed
            row_exists = conn.execute("SELECT 1 FROM emails WHERE id = ?", (email['id'],)).fetchone()
            if not row_exists:
                logger.error(f"Email {email['id']} missing from DB; skipping update.")
            else:
                conn.execute(
                    "UPDATE emails SET processed_at = CURRENT_TIMESTAMP, category = ? WHERE id = ?", 
                    (decision.category, email['id'])
                )
                logger.debug(f"Marked email {email['id']} processed with category {decision.category}")
            conn.commit()
            
            # Create Trigger if Urgent
            if decision.category.lower() == "urgent" or decision.action == "mark_important":
                write_trigger(self.user_email, "urgent_email", {
                    "subject": email['subject'],
                    "id": email['id'],
                    "reason": decision.reason
                })
                
        except Exception as e:
            conn.rollback()
            integrity = None
            try:
                integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
            except Exception as ic:
                integrity = f"integrity_check failed: {ic}"
            logger.error(
                f"Error processing email {email['id']}: {e} (args={getattr(e, 'args', None)})",
                exc_info=True,
                extra={"integrity_check": integrity},
            )
        finally:
            conn.close()

    def _normalize_folder_name(self, folder_name: str) -> Optional[str]:
        """
        Normalize and validate folder name against standard folders.
        Returns standardized folder name if valid, None otherwise.
        """
        if not folder_name:
            return None
            
        # Check exact match (case-insensitive)
        for std_folder in STANDARD_FOLDERS:
            if folder_name.lower() == std_folder.lower():
                return std_folder
        
        # Check aliases
        lower_name = folder_name.lower()
        if lower_name in FOLDER_ALIASES:
            return FOLDER_ALIASES[lower_name]
        
        # Not a valid standard folder
        logger.warning(f"Folder '{folder_name}' not in standard folder list. Skipping.")
        return None

    def _execute_action(self, message_id: str, decision: EmailCategory):
        if decision.action == 'move' and decision.destination_folder:
            # Normalize folder name
            normalized_folder = self._normalize_folder_name(decision.destination_folder)
            if not normalized_folder:
                return

            self.poller.move_email(message_id, normalized_folder)
        elif decision.action == 'delete':
            self.poller.delete_email(message_id)
