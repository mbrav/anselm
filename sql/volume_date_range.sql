SELECT
    toDate(begin) AS day,
    formatReadableQuantity(sum(volume)) AS total_volume
FROM
    md_moex.trades
WHERE
    begin >= '2024-01-01' AND begin < '2024-06-01'
GROUP BY
    day
ORDER BY
    day;
