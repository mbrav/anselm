SELECT
    table,
    formatReadableSize(sum(bytes_on_disk)) AS size_on_disk,
    formatReadableSize(sum(data_compressed_bytes + data_uncompressed_bytes)) AS total_size
FROM
    system.parts
WHERE
    database = 'md_moex'
    AND table = 'candles'
    AND active = 1
GROUP BY
    table;
