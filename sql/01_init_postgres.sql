CREATE SCHEMA api_raw;

GRANT USAGE ON SCHEMA api_raw TO crypto_writer;
GRANT CREATE ON SCHEMA api_raw TO crypto_writer;

CREATE TABLE api_raw.crypto_prices (
    price_id BIGSERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    target_currency VARCHAR(10) NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    market_cap NUMERIC(24, 4),
    volume_24h NUMERIC(24, 4),
    change_24h NUMERIC(10, 6),
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_crypto_prices_coin_time 
ON api_raw.crypto_prices (coin_id, last_updated_at DESC);

GRANT SELECT, INSERT, UPDATE ON api_raw.crypto_prices TO crypto_writer;
GRANT USAGE, SELECT ON SEQUENCE api_raw.crypto_prices_price_id_seq TO crypto_writer;

ALTER TABLE api_raw.crypto_prices 
ADD COLUMN last_updated_at TIMESTAMP;
