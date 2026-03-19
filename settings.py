from __future__ import annotations

import os

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "trades_raw")
SYMBOL_PARTITION_MAP = {
    "BTCUSDT": 0,
    "ETHUSDT": 1,
}
BINANCE_WS_URL = os.getenv(
    "BINANCE_WS_URL",
    "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade",
)

# Postgres / TimescaleDB
DB_DSN = os.getenv("DB_DSN", "postgresql://vibestream:vibestream@localhost:55432/vibestream")
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "2"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "8"))

# Analytics windowing
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "60"))
WARMUP_MIN_SAMPLES = int(os.getenv("WARMUP_MIN_SAMPLES", "30"))
DATA_GAP_RESET_SECONDS = int(os.getenv("DATA_GAP_RESET_SECONDS", "10"))
STALE_PURGE_SECONDS = int(os.getenv("STALE_PURGE_SECONDS", "60"))
Z_SCORE_THRESHOLD = float(os.getenv("Z_SCORE_THRESHOLD", "3.0"))

# Classification thresholds
RETAIL_MAX = float(os.getenv("RETAIL_MAX", "500"))
PRO_MAX = float(os.getenv("PRO_MAX", "50000"))

# Consumer / sink tuning
INGESTION_RATE_THRESHOLD = int(os.getenv("INGESTION_RATE_THRESHOLD", "500"))
BATCH_FLUSH_INTERVAL_MS = int(os.getenv("BATCH_FLUSH_INTERVAL_MS", "100"))
BATCH_FLUSH_SIZE = int(os.getenv("BATCH_FLUSH_SIZE", "500"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = os.getenv(
    "LOG_FORMAT",
    "%(asctime)s %(levelname)s %(name)s | %(message)s",
)
