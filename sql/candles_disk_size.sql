SELECT
    table,
    formatReadableSize(sum(bytes_on_disk)) AS bytes_on_disk,
    formatReadableSize(sum(data_compressed_bytes)) AS bytes_compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS bytes_uncompressed,
    formatReadableSize(sum(data_compressed_bytes + data_uncompressed_bytes)) AS bytes_total
FROM
    system.parts
WHERE
    database = 'md_moex'
    AND table = 'candles'
    AND active = 1
GROUP BY
    table;


