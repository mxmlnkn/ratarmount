WITH main AS (
    SELECT
        f.path,
        f.name,
        x.value AS sha256
    FROM files f
    JOIN xattrs x USING (offsetheader)
    WHERE x.key = 'user.hash.sha256'
),
other_sha AS (
    {{OTHER_SHA_CTE}}
),
counts AS (
    SELECT
        m.sha256,
        COUNT(DISTINCT o.src) AS copies_in_other_backups
    FROM main m
    LEFT JOIN other_sha o USING (sha256)
    GROUP BY m.sha256
)
-- The final visible/printed SELECT statement.
SELECT m.path || '/' || m.name AS full_path, c.copies_in_other_backups
FROM main m
JOIN counts c USING (sha256)
WHERE c.copies_in_other_backups < {{THRESHOLD}};
