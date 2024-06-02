SELECT
    secid,
    boardid,
    shortname,
    COUNT(*) AS total_candles,
    MIN(begin) AS earliest_begin_date,
    MAX(end) AS latest_end_date
FROM
    md_moex.candles
GROUP BY
    secid,
    boardid,
    shortname
ORDER BY
    secid,
    boardid,
    shortname;
