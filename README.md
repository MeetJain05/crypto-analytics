# VibeStream Alpha

Real-time crypto market anomaly detection pipeline built with Python, Kafka, TimescaleDB, FastAPI, and a React dashboard.

VibeStream ingests live Binance BTC/ETH trade streams, enriches each trade with statistical signals, flags anomalies in near real time, and serves both REST and WebSocket data to a live frontend.

## Features

- End-to-end streaming architecture (ingestion -> broker -> analytics -> storage -> API -> UI)
- Real-time anomaly detection using rolling-window z-score analysis
- Reliable pipeline behavior with batching, deferred offset commits, and reconnect logic
- Time-series optimized storage and OHLCV aggregation with TimescaleDB
- Live dashboard with both polling and WebSocket updates

## Architecture

```text
Binance WebSocket (aggTrade)
        |
        v
producer.py  ->  Kafka topic: trades_raw  ->  consumer.py + analytics.py
                                                 |
                                                 v
                                           db_sink.py
                                                 |
                                                 v
                                TimescaleDB (trades_enriched + aggregates)
                                                 |
                                                 v
                                         FastAPI (api.py)
                                    REST + WebSocket endpoints
                                                 |
                                                 v
                                Dashboard (index.html, app.js, styles.css)
```

## Implementation Details

- Live ingestion from Binance combined stream: `btcusdt@aggTrade`, `ethusdt@aggTrade`
- Trade enrichment:
  - USD value calculation: `price * quantity`
  - Trader classification: Retail / Pro / Whale
  - Rolling z-score anomaly detection on 60-second windows
- Stream processing:
  - Kafka topic partitioning by symbol
  - Batched sink writes for higher throughput
  - Offset commit after successful flush to avoid silent data loss
- API layer:
  - Health/readiness endpoints
  - Live trades, anomalies, OHLCV, and cross-asset comparison
  - WebSocket streaming for live trade/anomaly updates
- Frontend:
  - Real-time trades panel
  - Anomaly feed
  - Symbol comparison metrics
  - OHLCV visualization

## Tech Stack

- Python 3.11+
- FastAPI + Uvicorn
- Kafka (`apache/kafka:3.8.0`)
- TimescaleDB (`timescale/timescaledb:latest-pg16`)
- asyncpg, pydantic, confluent-kafka, websockets
- React + Recharts (CDN)
- Docker Compose

## Project Structure

```text
producer.py         # Binance -> Kafka
consumer.py         # Kafka -> enrichment -> DB
analytics.py        # rolling stats + anomaly detection
db_sink.py          # batched DB writer
api.py              # FastAPI REST + WebSocket service
schemas.py          # pydantic models
settings.py         # env-driven configuration
sql/init.sql        # schema + hypertable + continuous aggregates
index.html
app.js
styles.css          # dashboard frontend
docker-compose.yml  # Kafka, Kafka UI, TimescaleDB
```

## Quick Start

### 1. Start infrastructure

```bash
docker compose up -d
docker compose ps
```

### 2. Create virtual environment and install dependencies

Windows CMD:

```cmd
python -m venv .venv
.venv\Scripts\activate.bat
pip install -r requirements.txt
```

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Run services in separate terminals

```bash
python producer.py
python consumer.py
uvicorn api:app --host 0.0.0.0 --port 8000
python -m http.server 3000
```

### 4. Open applications

- Dashboard: `http://localhost:3000/index.html`
- API docs: `http://localhost:8000/docs`
- Kafka UI: `http://localhost:8080`

## API Summary

### REST

- `GET /health`
- `GET /ready`
- `GET /live_trades?limit=50&symbol=BTCUSDT`
- `GET /anomalies?limit=20&symbol=ETHUSDT&minutes=60`
- `GET /stats`
- `GET /comparison`
- `GET /ohlcv/{symbol}?minutes=60`

### WebSocket

- `WS /ws/trades`
- `WS /ws/anomalies`

## Configuration

Environment variables are read from `settings.py` with safe defaults.

Common overrides:

- `KAFKA_BOOTSTRAP` (default: `localhost:9092`)
- `KAFKA_TOPIC_RAW` (default: `trades_raw`)
- `DB_DSN` (default: `postgresql://vibestream:vibestream@localhost:55432/vibestream`)
- `WINDOW_SECONDS` (default: `60`)
- `WARMUP_MIN_SAMPLES` (default: `30`)
- `Z_SCORE_THRESHOLD` (default: `3.0`)
- `BATCH_FLUSH_INTERVAL_MS` (default: `100`)
- `BATCH_FLUSH_SIZE` (default: `500`)

## Engineering Highlights

- Real-time event-driven analytics pipeline for live crypto trade streams.
- Rolling-window anomaly detection with z-score thresholding and warm-up/gap handling.
- Durable stream-to-database flow using batched writes and deferred Kafka offset commits.
- FastAPI backend exposing both REST and WebSocket interfaces.
- Frontend dashboard for live trades, anomalies, OHLCV, and BTC/ETH comparative insights.

## Notes

- This project is intended for local development and portfolio demonstration.
- Binance/WebSocket availability and local network quality can affect stream continuity.
- For production, add auth, observability, deployment automation, and stricter CORS/security controls.
