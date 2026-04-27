# VibeStream Alpha

VibeStream is a robust, real-time cryptocurrency market anomaly detection pipeline. Built entirely from scratch, it leverages modern stream processing techniques to ingest live trading data from Binance, enrich it with statistical insights, and detect market anomalies in real-time.

This project was developed to demonstrate core Big Data engineering principles: handling high-velocity data streams, decoupled architectures, stateful stream processing, and time-series-optimized storage.

## Features
- **Real-Time Data Ingestion:** Connects directly to the Binance WebSocket API (`@aggTrade`) for continuous, low-latency data feeds of BTC/USDT and ETH/USDT pairs.
- **Message Broker Architecture:** Utilizes Apache Kafka to decouple ingestion from processing, providing fault tolerance and buffering against high-volatility market spikes.
- **Stateful Stream Processing:** Implements a rolling-window Z-Score algorithm in Python to dynamically detect mathematical anomalies based on live market conditions, automatically handling periods of low variance and data gaps.
- **Time-Series Storage:** Persists enriched trade data into TimescaleDB, taking advantage of Continuous Aggregates to efficiently generate 1-minute OHLCV charts.
- **Interactive Live Dashboard:** A full-stack web interface built with React and FastAPI. Offers historical data retrieval via REST endpoints and pushes live updates frame-by-frame using WebSockets.

## Architecture Pipeline
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

## Technology Stack
- **Backend Core:** Python 3.11+, asyncio
- **Message Broker:** Apache Kafka (`apache/kafka:3.8.0`), `confluent-kafka`
- **Database:** TimescaleDB (PostgreSQL 16), `asyncpg`
- **API Server:** FastAPI, Uvicorn
- **Frontend UI:** React, Recharts, standard HTML/CSS/JS
- **Containerization:** Docker & Docker Compose

## Project Structure
- `producer.py`: Asynchronously connects to Binance, parses raw trades, and publishes them to Kafka topic partitions.
- `consumer.py`: Stateful Kafka consumer that retrieves raw trades, applies enrichment and anomaly detection, and ensures fault-tolerant offset commits.
- `analytics.py`: The stream processing engine. Maintains rolling 60-second in-memory windows, normalizes trade values, classifies trader sizes, and calculates regularized Z-scores.
- `db_sink.py`: Handles high-throughput, batched inserts into TimescaleDB using asynchronous locking and deferred callback commits.
- `api.py`: FastAPI application serving REST endpoints (for historical data and aggregations) and WebSockets (for live dashboard push updates).
- `schemas.py`: Pydantic models enforcing data integrity throughout the pipeline.
- `settings.py`: Environment-driven configuration variables.
- `sql/init.sql`: Database schema definition including hypertables and continuous aggregate policies.
- `index.html` / `app.js` / `styles.css`: The frontend analytics dashboard.
- `docker-compose.yml`: Infrastructure configuration for Kafka and TimescaleDB.

## Getting Started

### 1. Start Infrastructure
Launch the Kafka broker, Kafka UI, and TimescaleDB container via Docker Compose:
```bash
docker compose up -d
```

### 2. Environment Setup
Create a Python virtual environment and install the required dependencies:
```bash
# Windows
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Linux/macOS
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Launch Services
Run each of the following components in separate terminal windows:
```bash
# 1. Start the Ingestion Producer
python producer.py

# 2. Start the Analytics Consumer
python consumer.py

# 3. Start the Backend API
uvicorn api:app --host 0.0.0.0 --port 8000

# 4. Serve the Frontend Dashboard
python -m http.server 3000
```

### 4. View Dashboard
Navigate to `http://localhost:3000/index.html` in your web browser to view the real-time analytics terminal. 
API Documentation is available at `http://localhost:8000/docs`.

## Engineering Highlights
- **Fault-Tolerant Delivery:** Built with deferred offset commits. The consumer only commits a Kafka offset after a batch of trades has been safely written to the database, ensuring zero data loss during process crashes.
- **Dynamic Anomaly Detection:** The Z-score algorithm uses a regularized standard deviation floor, preventing division-by-zero errors in extremely flat markets while still accurately capturing outliers.
- **Cross-Asset Correlation:** The analytics engine performs continuous Pearson correlation and relative divergence tracking between Bitcoin and Ethereum order flow.
- **Optimized Throughput:** Micro-batching writes to TimescaleDB prevents connection pool exhaustion and database lock-ups during periods of extreme market volatility (e.g. Flash Crashes).
