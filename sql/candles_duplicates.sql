SELECT
    secid,
    boardid,
    shortname,
    begin,
    end,
    COUNT(*) AS count
FROM
    md_moex.candles
GROUP BY
    secid,
    boardid,
    shortname,
    begin,
    end
HAVING
    count > 3;
