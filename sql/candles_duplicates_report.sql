WITH
    -- Step 1: Identify duplicate groups and count the duplicates
    duplicates AS (
        SELECT
            secid,
            COUNT(*) AS duplicate_count
        FROM
            md_moex.candles
        GROUP BY
            secid,
            boardid,
            shortname,
            begin,
            end
        HAVING
            duplicate_count > 1
    ),
    -- Step 2: Count the total number of candles for each secid
    total_candles AS (
        SELECT
            secid,
            COUNT(*) AS total_count
        FROM
            md_moex.candles
        GROUP BY
            secid
    ),
    -- Step 3: Calculate the total number of duplicate entries for each secid
    duplicate_totals AS (
        SELECT
            secid,
            SUM(duplicate_count) AS total_duplicate_count
        FROM
            duplicates
        GROUP BY
            secid
    )

-- Step 4: Calculate the percentage of duplicates
SELECT
    t.secid,
    t.total_count,
    d.total_duplicate_count,
    (d.total_duplicate_count / t.total_count) * 100 AS duplicate_percentage
FROM
    total_candles t
LEFT JOIN
    duplicate_totals d
ON
    t.secid = d.secid;
