CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS trades_enriched (
    trade_id BIGINT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    usd_value DOUBLE PRECISION NOT NULL,
    classification TEXT NOT NULL,
    z_score DOUBLE PRECISION,
    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (trade_id, time)
);

SELECT create_hypertable('trades_enriched', 'time', if_not_exists => TRUE);

CREATE MATERIALIZED VIEW IF NOT EXISTS trades_1min_ohlcv
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(quantity) AS volume,
    sum(usd_value) AS usd_volume,
    count(*) AS trade_count,
    count(*) FILTER (WHERE is_anomaly) AS anomaly_count,
    avg(z_score) AS avg_z_score
FROM trades_enriched
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'trades_1min_ohlcv',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute'
)
ON CONFLICT DO NOTHING;

CREATE OR REPLACE VIEW recent_anomalies AS
SELECT
    trade_id,
    time,
    symbol,
    price,
    usd_value,
    classification,
    z_score
FROM trades_enriched
WHERE is_anomaly = TRUE
  AND time > now() - interval '1 hour'
ORDER BY time DESC;

CREATE OR REPLACE VIEW live_stats AS
SELECT
    symbol,
    count(*) FILTER (WHERE time > now() - interval '1 minute') AS trades_1m,
    count(*) FILTER (WHERE time > now() - interval '5 minutes') AS trades_5m,
    sum(usd_value) FILTER (WHERE time > now() - interval '1 minute') AS vol_1m,
    sum(usd_value) FILTER (WHERE time > now() - interval '5 minutes') AS vol_5m,
    count(*) FILTER (WHERE is_anomaly AND time > now() - interval '5 minutes') AS anomalies_5m,
    max(time) AS last_trade,
    max(price) FILTER (WHERE time > now() - interval '1 minute') AS price_high_1m,
    min(price) FILTER (WHERE time > now() - interval '1 minute') AS price_low_1m,
    (array_agg(price ORDER BY time DESC))[1] AS latest_price
FROM trades_enriched
WHERE time > now() - interval '5 minutes'
GROUP BY symbol;
