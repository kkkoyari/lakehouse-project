-- Refresh planner statistics for api_raw.crypto_prices.
-- Run with psql as the table owner (or a role with the required privileges).

ANALYZE api_raw.crypto_prices;

-- Estimated table health and the last maintenance timestamps.
SELECT
    schemaname,
    relname,
    n_live_tup,
    n_dead_tup,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE schemaname = 'api_raw'
  AND relname = 'crypto_prices';

-- Index usage and on-disk size. These values are useful for tracking trends,
-- but PostgreSQL does not expose an exact index-fragmentation percentage here.
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
