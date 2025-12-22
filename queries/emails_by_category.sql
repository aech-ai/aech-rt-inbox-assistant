-- Count emails by category
SELECT 
    category,
    COUNT(*) as count,
    COUNT(CASE WHEN is_read = 0 THEN 1 END) as unread_count
FROM emails
WHERE category IS NOT NULL
GROUP BY category
ORDER BY count DESC;