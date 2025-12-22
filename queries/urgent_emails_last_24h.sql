-- Get urgent emails from the last 24 hours
SELECT 
    id,
    subject,
    sender,
    received_at,
    body_preview,
    category
FROM emails
WHERE category = 'Urgent'
  AND datetime(received_at) > datetime('now', '-24 hours')
ORDER BY received_at DESC;