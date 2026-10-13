-- Rebuild the crypto_prices index if monitoring indicates index bloat,
-- or if the index needs recovery. REINDEX CONCURRENTLY keeps normal writes
-- available while it works, but uses additional I/O, time, and disk space.
-- Requires PostgreSQL 12+ and must not be run inside a transaction block.

REINDEX INDEX CONCURRENTLY api_raw.idx_crypto_prices_coin_time;

-- Confirm that the index exists and check its size after rebuilding.
SELECT
    schemaname,
    relname,
    indexrelname,
    idx_scan,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
WHERE schemaname = 'api_raw'
  AND relname = 'crypto_prices'
  AND indexrelname = 'idx_crypto_prices_coin_time';