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

def _build_agent(available_folders: list[str]) -> Agent:
    """Create the AI agent with dynamic folder list."""
    model_name = os.getenv("MODEL_NAME", "openai-responses:gpt-4.1-mini")
    
    # Use provided folders, falling back to standard if empty (shouldn't happen if poller works)
    folders_list = available_folders if available_folders else STANDARD_FOLDERS
    allowed_folders_str = ", ".join(folders_list)
    
    cleanup_strategy = os.getenv("CLEANUP_STRATEGY", "medium").lower()
    
    allowed_categories = ", ".join([
        "Work", "Personal", "Newsletter", "Social", "Updates", "Security Notifications",
        "Receipts", "Promotions", "Finance", "Urgent", "Cold Outreach", "Delete"
    ])
    system_prompt = f"""
You are an expert email organizer. Your goal is to deeply understand the INTENT of each email and categorize it accordingly.

### 1. Intent Analysis (CRITICAL)
- **Do not rely on keywords alone.** Look at the Subject, Sender, and Body together to determine the *primary purpose* of the email.
- **Analyze the underlying message**: Is this a status update? A travel itinerary? A work request? A social notification?
- **Context matters**: A "Travel" keyword in a status update (e.g., "I am travelling") does not make the email a "Travel" document. "Travel" is for *your* bookings and itineraries.
- **Sender matters**: Who is sending this? Is it a service, a colleague, or a friend?

### 2. Few-Shot Examples (Guide your reasoning)

**Example 1: The "False Positive" Travel**
- **Subject**: Automatic reply: Project Roadmap Q4
- **Body**: I am currently travelling with limited access to email until Nov 27th.
- **Analysis**: The sender is unavailable. This information is temporary and has no long-term value.
- **Category**: "Delete".
- **Action**: move -> Should Delete.

**Example 2: Real Travel**
- **Subject**: Flight Confirmation: SFO to LHR
- **Body**: Your flight UA901 is confirmed. Seat 4A.
- **Analysis**: This is a booking for *you*.
- **Category**: "Travel".
- **Action**: move -> Travel.

**Example 3: Social Notification**
- **Subject**: You appeared in 5 searches this week
- **Sender**: LinkedIn <notifications@linkedin.com>
- **Analysis**: Automated platform notification.
- **Category**: "Social".
- **Action**: move -> Social.

**Example 4: Work via Social Platform**
- **Subject**: Project collaboration inquiry
- **Sender**: James Dolan via LinkedIn
- **Body**: Hi Steven, I'd like to discuss the Q4 roadmap...
- **Analysis**: Although from LinkedIn, the *content* is a direct work request.
- **Category**: "Work".
- **Action**: move -> Work.

**Example 5: Newsletter vs Promotion**
- **Subject**: The Daily Tech Digest: AI Agents on the rise
- **Body**: Here are the top stories in tech today...
- **Analysis**: Informational content, recurring.
- **Category**: "Newsletters".
- **Action**: move -> Newsletters.

### 3. Cleanup Strategy (Current Level: {cleanup_strategy.upper()})
- **Goal**: Suggest removal of clutter by moving it to the "Should Delete" folder. NEVER hard delete.
- **Low**: Only move obvious spam/phishing/junk to "Should Delete".
- **Medium**: Move spam + old/irrelevant newsletters (> 3 months) to "Should Delete".
- **Aggressive**: Move spam + any newsletter/promo > 1 month + cold outreach to "Should Delete".

### 4. Categories allowed:
- Must be exactly one of: {allowed_categories}
- Use 'Delete' (mapped to "Should Delete" folder) for items that match the cleanup strategy.
- Use 'Cold Outreach' for unsolicited sales/networking emails.
- Use 'Urgent' only for truly time-sensitive items.

### 5. Rules for destination_folder:
- Must be exactly one of: {allowed_folders_str}
- **CRITICAL**: You MUST choose a folder from the list above. Do NOT invent new folders.
- If nothing precise fits, choose the closest from the list.
- If no move is appropriate, set action='none' and leave destination_folder null.

### 6. Actions allowed:
- move: only when destination_folder is in the approved list above.
- delete: **DEPRECATED**. Do NOT use 'delete'. Instead, use 'move' with destination_folder='Should Delete'.
- mark_important: only for genuinely urgent/time-sensitive items.
- none: when no action is needed.
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
        self.current_folders: list[str] = []

    def _get_agent(self, folders: list[str]) -> Agent:
        # Rebuild agent if folders have changed or if agent is not initialized
        # Sort folders to ensure consistent comparison
        sorted_folders = sorted(folders)
        if self.agent is None or sorted_folders != self.current_folders:
            logger.info("Rebuilding agent with updated folder list")
            self.agent = _build_agent(folders)
            self.current_folders = sorted_folders
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
            # Run AI Agent with STANDARD_FOLDERS
            result = await self._get_agent(STANDARD_FOLDERS).run(email_content)
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
