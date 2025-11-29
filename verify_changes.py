import os
import sys
import json
from unittest.mock import MagicMock, patch
# Mock dependencies to avoid needing real DB/Graph/Requests/Pydantic
sys.modules['aech_cli_msgraph.graph'] = MagicMock()
sys.modules['src.database'] = MagicMock()
sys.modules['requests'] = MagicMock()
import requests

# Mock Pydantic and Pydantic AI
pydantic_mock = MagicMock()
pydantic_mock.BaseModel = object
pydantic_mock.Field = MagicMock()
sys.modules['pydantic'] = pydantic_mock

pydantic_ai_mock = MagicMock()
pydantic_ai_mock.Agent = MagicMock()
sys.modules['pydantic_ai'] = pydantic_ai_mock

# Set env var for testing
os.environ['FOLDER_PREFIX'] = 'test_prefix_'

from src.poller import GraphPoller
from src.organizer import _build_agent

def test_folder_prefix():
    print("Testing Folder Prefix...")
    poller = GraphPoller("dummy.db", "test@example.com")
    
    # Check if prefix is loaded
    if poller.folder_prefix != 'test_prefix_':
        print(f"FAIL: Expected prefix 'test_prefix_', got '{poller.folder_prefix}'")
        return

    # Check move_email logic (mocking _run_cli)
    poller._run_cli = MagicMock(return_value='{}')
    poller.move_email("msg123", "Work")
    
    # Verify the CLI was called with the prefixed folder
    args = poller._run_cli.call_args[0][0]
    expected_folder = "test_prefix_Work"
    if expected_folder in args:
        print(f"PASS: move_email called with '{expected_folder}'")
    else:
        print(f"FAIL: move_email called with {args}, expected '{expected_folder}'")

def test_system_prompt():
    print("\nTesting System Prompt & Dynamic Folders...")
    # Reset mock to capture new call
    sys.modules['pydantic_ai'].Agent.reset_mock()
    
    # Test with custom dynamic folders
    custom_folders = ["CustomFolderA", "CustomFolderB"]
    _build_agent(custom_folders)
    
    # Get the system_prompt argument passed to Agent constructor
    call_args = sys.modules['pydantic_ai'].Agent.call_args
    if not call_args:
        print("FAIL: Agent constructor not called")
        return
        
    kwargs = call_args.kwargs
    prompt = kwargs.get('system_prompt')
    
    if not prompt:
        print("FAIL: system_prompt not passed to Agent constructor")
        return
    
    # Check for Dynamic Folders
    if "CustomFolderA" in prompt and "CustomFolderB" in prompt:
        print("PASS: Dynamic folders present in system prompt")
    else:
        print("FAIL: Dynamic folders missing from system prompt")

    # Check for Cleanup Strategy
    required_phrases = [
        "Example 1: The \"False Positive\" Travel",
        "Automatic reply: Project Roadmap Q4",
        "Should Delete",
        "Example 2: Real Travel",
        "DEPRECATED",
        "INTENT of each email",
        "Few-Shot Examples"
    ]
    
    all_present = True
    for phrase in required_phrases:
        if phrase not in prompt:
            print(f"FAIL: Missing phrase '{phrase}' in system prompt")
            all_present = False
    
    if all_present:
        print("PASS: System prompt contains all required instructions (Cleanup, Intent, Examples)")

def test_cleanup_config():
    print("\nTesting Cleanup Config...")
    # Test that env var is read
    os.environ['CLEANUP_STRATEGY'] = 'aggressive'
    sys.modules['pydantic_ai'].Agent.reset_mock()
    _build_agent([])
    
    kwargs = sys.modules['pydantic_ai'].Agent.call_args.kwargs
    prompt = kwargs.get('system_prompt')
    
    if "Current Level: AGGRESSIVE" in prompt:
        print("PASS: Cleanup strategy 'AGGRESSIVE' reflected in prompt")
    else:
        print("FAIL: Cleanup strategy not reflected in prompt")

def test_reprocessing():
    print("\nTesting Reprocessing...")
    poller = GraphPoller("dummy.db", "test@example.com")
    
    # Mock requests.get
    mock_resp_folders = MagicMock()
    mock_resp_folders.ok = True
    mock_resp_folders.json.return_value = {
        "value": [{"id": "f1", "displayName": "Folder1"}],
        "@odata.nextLink": None
    }
    
    mock_resp_msgs = MagicMock()
    mock_resp_msgs.ok = True
    mock_resp_msgs.json.return_value = {
        "value": [{"id": "m1", "subject": "Subj1", "bodyPreview": "Prev1"}],
        "@odata.nextLink": None
    }
    
    requests.get.side_effect = [mock_resp_folders, mock_resp_msgs]
    
    # Mock DB
    with patch('src.poller.get_connection') as mock_get_conn:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        poller.reprocess_all_folders()
        
        # Verify DB calls
        # Check if cursor.execute was called with INSERT...processed_at=NULL
        calls = mock_cursor.execute.call_args_list
        found_upsert = False
        for call in calls:
            sql = call[0][0]
            if "processed_at=NULL" in sql and "INSERT INTO emails" in sql:
                found_upsert = True
                break
        
        if found_upsert:
            print("PASS: Reprocessing upserted emails with processed_at=NULL")
        else:
            print("FAIL: Reprocessing did not upsert emails correctly")

if __name__ == "__main__":
    test_folder_prefix()
    test_system_prompt()
    test_cleanup_config()
    test_reprocessing()
