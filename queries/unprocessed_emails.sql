-- Get unprocessed emails
SELECT 
    id,
    subject,
    sender,
    received_at,
    body_preview,
    is_read
FROM emails
WHERE processed_at IS NULL
ORDER BY received_at DESC;