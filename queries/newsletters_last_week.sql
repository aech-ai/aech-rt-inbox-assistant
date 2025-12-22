-- Get newsletters from the last week
SELECT 
    id,
    subject,
    sender,
    received_at,
    body_preview
FROM emails
WHERE category = 'Newsletters'
  AND datetime(received_at) > datetime('now', '-7 days')
ORDER BY received_at DESC;