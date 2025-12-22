-- Get recent triage decisions
SELECT 
    t.timestamp,
    t.action,
    t.destination_folder,
    e.subject,
    e.sender,
    t.reason
FROM triage_log t
JOIN emails e ON t.email_id = e.id
ORDER BY t.timestamp DESC
LIMIT 20;