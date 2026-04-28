-- Query to find files where CRC32 extracted from filename doesn't match xattr value
-- Used for detecting corrupted or modified files in the ratarmount index

WITH RECURSIVE positions AS (
    -- Recursive CTE to find all bracket and parenthesis positions in filenames
    -- This helps locate potential CRC32 values enclosed in brackets or parentheses
    SELECT
        f.path,
        f.name,
        f.offsetheader,
        instr(f.name, c.type) AS pos,
        c.type AS bracket_type
    FROM files f
    CROSS JOIN (SELECT '[' AS type UNION ALL SELECT '(' AS type) AS c
    WHERE instr(f.name, c.type) > 0
    UNION ALL
    SELECT
        path,
        name,
        offsetheader,
        pos + instr(substr(name, pos + 1), c.type),
        c.type
    FROM positions
    CROSS JOIN (SELECT '[' AS type UNION ALL SELECT '(' AS type) AS c
    WHERE instr(substr(name, pos + 1), c.type) > 0
),
last_positions AS (
    -- Identify the last occurrence of brackets and parentheses in each filename
    -- This determines which delimiter pair contains the CRC32 value
    SELECT
        path,
        name,
        offsetheader,
        MAX(CASE WHEN bracket_type = '[' THEN pos END) AS last_bracket_pos,
        MAX(CASE WHEN bracket_type = '(' THEN pos END) AS last_paren_pos
    FROM positions
    GROUP BY path, name, offsetheader
),
extracted AS (
    -- Extract CRC32 candidate from filename based on last bracket/parenthesis position
    -- Prefers brackets over parentheses if both are present at similar positions
    SELECT
        f.path,
        f.name,
        f.offsetheader,
        CASE
            WHEN COALESCE(lp.last_bracket_pos, 0) > COALESCE(lp.last_paren_pos, 0)
            THEN substr(
                f.name,
                lp.last_bracket_pos + 1,
                instr(substr(f.name, lp.last_bracket_pos + 1), ']') - 1
            )
            WHEN COALESCE(lp.last_paren_pos, 0) > COALESCE(lp.last_bracket_pos, 0)
            THEN substr(
                f.name,
                lp.last_paren_pos + 1,
                instr(substr(f.name, lp.last_paren_pos + 1), ')') - 1
            )
            ELSE NULL
        END AS crc_candidate
    FROM files f
    LEFT JOIN last_positions lp USING (path, name, offsetheader)
),
filtered AS (
    -- Filter to only include valid 8-character hexadecimal CRC32 candidates
    -- Uses GLOB pattern to match exactly 8 hex characters (case insensitive)
    SELECT
        path,
        name,
        offsetheader,
        crc_candidate AS crc_from_name
    FROM extracted
    WHERE crc_candidate GLOB '[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]'
),
crc_attr AS (
    -- Extract CRC32 values from extended attributes with normalization
    -- Converts to lowercase and trims whitespace for consistent comparison
    SELECT
        offsetheader,
        lower(trim(value)) AS crc_from_xattr
    FROM xattrs
    WHERE key = 'user.hash.crc32'
)
-- Final comparison: find files where filename CRC32 doesn't match xattr CRC32
-- Excludes subtitle files, trash info files, and PNG files which often have different naming patterns
SELECT
    f.path || '/' || f.name AS full_path,
    lower(f.crc_from_name) AS crc_from_name,
    c.crc_from_xattr
FROM filtered f
JOIN crc_attr c USING (offsetheader)
WHERE f.crc_from_name IS NOT NULL
  AND lower(f.name) NOT LIKE '%.srt'
  AND lower(f.name) NOT LIKE '%.ass'
  AND lower(f.name) NOT LIKE '%.trashinfo'
  AND lower(f.name) NOT LIKE '%.png'
  AND lower(f.crc_from_name) != c.crc_from_xattr;
