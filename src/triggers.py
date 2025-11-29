import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

TRIGGERS_FILE = Path("/home/agentaech/rt-triggers.json")

def write_trigger(user_email: str, trigger_type: str, payload: Dict[str, Any]):
    """Write a trigger to the shared triggers file."""
    trigger = {
        "id": str(uuid.uuid4()),
        "user": user_email,
        "type": trigger_type,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "payload": payload
    }
    
    # Read existing triggers
    if TRIGGERS_FILE.exists():
        try:
            with open(TRIGGERS_FILE, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            data = {}
    else:
        data = {}
    
    # Append new trigger
    if "inbox-assistant" not in data:
        data["inbox-assistant"] = []
    
    data["inbox-assistant"].append(trigger)
    
    # Write back atomically (using a temp file would be better but keeping it simple for now)
    with open(TRIGGERS_FILE, "w") as f:
        json.dump(data, f, indent=2)
