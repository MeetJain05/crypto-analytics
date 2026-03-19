# VibeStream Alpha Setup

## Project Layout

- `producer.py`: Streams Binance aggTrades to Kafka.
- `consumer.py`: Consumes Kafka, enriches trades, writes to TimescaleDB.
- `api.py`: FastAPI service exposing stats, anomalies, OHLCV, and WebSockets.
- `index.html`, `styles.css`, `app.js`: Frontend dashboard.
- `settings.py`: Runtime configuration via environment variables.
- `docker-compose.yml`: Kafka, Kafka UI, TimescaleDB.
- `sql/init.sql`: Database schema and continuous aggregate setup.

## Prerequisites

- Docker Desktop (with Compose)
- Python 3.11+
- Windows PowerShell or CMD

## 1. Start Infrastructure

From the project root:

```bash
docker compose up -d
```

Wait about 20-40 seconds for services to initialize, then check:

```bash
docker compose ps
```

Expected:

- `vibestream-kafka` running
- `vibestream-timescaledb` running
- `vibestream-kafka-ui` running
- `kafka-init` exited successfully

## 2. Create and Activate Virtual Environment

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Windows CMD:

```cmd
python -m venv .venv
.venv\Scripts\activate.bat
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## 3. Start Backend Processes

Open three terminals at the project root. Activate `.venv` in each terminal.

Terminal 1 (producer):

```bash
python producer.py
```

Terminal 2 (consumer):

```bash
python consumer.py
```

Terminal 3 (API):

```bash
uvicorn api:app --host 0.0.0.0 --port 8000
```

## 4. Start Frontend

From the project root in a fourth terminal:

```bash
python -m http.server 3000
```

Open:

- Frontend dashboard: `http://localhost:3000/index.html`
- API docs: `http://localhost:8000/docs`
- Kafka UI: `http://localhost:8080`

The frontend is already integrated with backend defaults:

- HTTP API: `http://localhost:8000`
- WebSocket API: `ws://localhost:8000`

To use a different backend URL, set this before opening the page:

```html
<script>
  window.VIBESTREAM_API = "http://your-host:8000";
</script>
```

## 5. Quick Verification

```bash
curl http://localhost:8000/health
curl "http://localhost:8000/live_trades?limit=5"
curl "http://localhost:8000/anomalies?limit=5"
curl http://localhost:8000/stats
curl http://localhost:8000/comparison
```

## 6. Optional Environment Overrides

All settings are in `settings.py` and can be overridden by environment variables.

Common ones:

- `KAFKA_BOOTSTRAP` (default: `localhost:9092`)
- `DB_DSN` (default: `postgresql://vibestream:vibestream@localhost:55432/vibestream`)
- `LOG_LEVEL` (default: `INFO`)
- `Z_SCORE_THRESHOLD` (default: `3.0`)
