#!/usr/bin/env python3
# ============================================================
# VibeStream-Alpha: FastAPI — Complete Rewrite
# ============================================================
# Fixes & Additions:
#   + CORS middleware (was missing → frontend blocked)
#   + /live_trades endpoint (was missing)
#   + /anomalies clean route (was /stats/anomalies only)
#   + /stats summary endpoint (was missing)
#   + /comparison cross-asset endpoint (was missing)
#   + WebSocket /ws/trades for push updates (was missing)
#   + Serialization helpers for datetime/Decimal types
# ============================================================

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

import asyncpg
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from settings import DB_DSN
from analytics import AnalyticsEngine

log = logging.getLogger("vibestream.api")

# ── Shared state (injected at startup by the consumer process) ─
_engine: Optional[AnalyticsEngine] = None
_db_pool: Optional[asyncpg.Pool] = None
_start_time = time.monotonic()

# WebSocket connection registry
_ws_clients: set[WebSocket] = set()


def set_engine(engine: AnalyticsEngine) -> None:
    global _engine
    _engine = engine


# ── JSON serializer for asyncpg Row types ──────────────────
def _serialize(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Not serializable: {type(obj)}")


def _rows_to_json(rows) -> list[dict]:
    return json.loads(json.dumps([dict(r) for r in rows], default=_serialize))


# ── App lifecycle ──────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _db_pool
    _db_pool = await asyncpg.create_pool(
        DB_DSN,
        min_size=2,
        max_size=8,
        server_settings={"application_name": "vibestream-api"},
    )
    log.info("✅ API DB pool ready")
    yield
    if _db_pool:
        await _db_pool.close()


app = FastAPI(
    title="VibeStream-Alpha API",
    description="Real-time crypto anomaly detection engine",
    version="2.0.0",
    lifespan=lifespan,
)

# ── CORS — FIX: was missing, frontend on different port was blocked ──
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # Tighten to specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════
# Ops
# ══════════════════════════════════════════════════════════

@app.get("/health", tags=["ops"])
async def health():
    return {"status": "ok", "uptime_seconds": round(time.monotonic() - _start_time, 1)}


@app.get("/ready", tags=["ops"])
async def readiness():
    if not _db_pool:
        raise HTTPException(503, "DB pool not initialized")
    try:
        async with _db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ready"}
    except Exception as exc:
        raise HTTPException(503, f"DB unreachable: {exc}")


# ══════════════════════════════════════════════════════════
# Live Trades — NEW endpoint the frontend needs
# ══════════════════════════════════════════════════════════

@app.get("/live_trades", tags=["data"])
async def live_trades(
    limit: int = Query(default=50, le=200),
    symbol: Optional[str] = None,
):
    """
    Most recent enriched trades, newest-first.
    Used by the frontend live trade stream panel.
    """
    if not _db_pool:
        raise HTTPException(503, "DB not ready")

    where_sym = "AND symbol = $2" if symbol else ""
    params: list = [limit // 2 if not symbol else limit]

    if symbol:
        async with _db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT trade_id, time, symbol, price, quantity,
                       usd_value, classification, z_score, is_anomaly
                FROM trades_enriched
                WHERE time > NOW() - INTERVAL '5 minutes'
                {where_sym}
                ORDER BY time DESC
                LIMIT $1
                """,
                *params,
                *(symbol.upper(),) if symbol else (),
            )
        return _rows_to_json(rows)

    # No symbol filter — fetch per-symbol to guarantee both BTC and ETH appear
    async with _db_pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT trade_id, time, symbol, price, quantity,
                   usd_value, classification, z_score, is_anomaly
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY symbol ORDER BY time DESC
                ) AS rn
                FROM trades_enriched
                WHERE time > NOW() - INTERVAL '5 minutes'
            ) sub
            WHERE rn <= $1
            ORDER BY time DESC
            """,
            params[0],
        )
    return _rows_to_json(rows)



# ══════════════════════════════════════════════════════════
# Anomalies — Clean route
# ══════════════════════════════════════════════════════════

@app.get("/anomalies", tags=["data"])
async def anomalies(
    limit: int = Query(default=20, le=100),
    symbol: Optional[str] = None,
    minutes: int = Query(default=60, le=1440),
):
    """Recent anomalies (|z| > 3.0), newest-first."""
    if not _db_pool:
        raise HTTPException(503, "DB not ready")

    symbol_filter = "AND symbol = $3" if symbol else ""
    params: list = [limit, minutes]
    if symbol:
        params.append(symbol.upper())

    async with _db_pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT trade_id, time, symbol, price, usd_value,
                   classification, z_score, is_anomaly
            FROM trades_enriched
            WHERE is_anomaly = TRUE
              AND time > NOW() - ($2 * INTERVAL '1 minute')
            {symbol_filter}
            ORDER BY time DESC
            LIMIT $1
            """,
            *params,
        )
    return _rows_to_json(rows)


# ══════════════════════════════════════════════════════════
# Stats Summary — NEW
# ══════════════════════════════════════════════════════════

@app.get("/stats", tags=["analytics"])
async def stats():
    """
    Aggregated summary: trade counts, USD volumes, anomaly rates,
    plus per-symbol rolling window state from the analytics engine.
    """
    if not _db_pool:
        raise HTTPException(503, "DB not ready")

    async with _db_pool.acquire() as conn:
        # Last 1-minute counts and volumes per symbol
        db_stats = await conn.fetch("""
            SELECT
                symbol,
                COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '1 minute')  AS trades_1m,
                COUNT(*) FILTER (WHERE time > NOW() - INTERVAL '5 minutes') AS trades_5m,
                SUM(usd_value) FILTER (WHERE time > NOW() - INTERVAL '1 minute')  AS vol_1m,
                SUM(usd_value) FILTER (WHERE time > NOW() - INTERVAL '5 minutes') AS vol_5m,
                COUNT(*) FILTER (WHERE is_anomaly AND time > NOW() - INTERVAL '5 minutes') AS anomalies_5m,
                MAX(time) as last_trade,
                MAX(price) FILTER (WHERE time > NOW() - INTERVAL '1 minute') AS price_high_1m,
                MIN(price) FILTER (WHERE time > NOW() - INTERVAL '1 minute') AS price_low_1m,
                (array_agg(price ORDER BY time DESC))[1] AS latest_price
            FROM trades_enriched
            WHERE time > NOW() - INTERVAL '5 minutes'
            GROUP BY symbol
        """)

    result = {
        "timestamp": datetime.utcnow().isoformat(),
        "symbols": _rows_to_json(db_stats),
    }

    # Attach live window stats from analytics engine
    if _engine:
        result["windows"] = {
            "BTCUSDT": _engine.window_stats("BTCUSDT"),
            "ETHUSDT": _engine.window_stats("ETHUSDT"),
        }

    return result


# ══════════════════════════════════════════════════════════
# OHLCV
# ══════════════════════════════════════════════════════════

@app.get("/ohlcv/{symbol}", tags=["data"])
async def ohlcv(symbol: str, minutes: int = Query(default=60, le=1440)):
    """
    1-minute OHLCV candles built directly from trades_enriched.
    Avoids the 1-minute lag from the continuous aggregate's end_offset policy.
    """
    if not _db_pool:
        raise HTTPException(503, "DB not ready")
    async with _db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                time_bucket('1 minute', time) AS bucket,
                first(price, time)            AS open,
                max(price)                    AS high,
                min(price)                    AS low,
                last(price, time)             AS close,
                sum(quantity)                 AS volume,
                sum(usd_value)                AS usd_volume,
                count(*)                      AS trade_count,
                count(*) FILTER (WHERE is_anomaly) AS anomaly_count,
                avg(z_score)                  AS avg_z_score
            FROM trades_enriched
            WHERE symbol = $1
              AND time > NOW() - ($2 * INTERVAL '1 minute')
            GROUP BY bucket
            ORDER BY bucket ASC
            """,
            symbol.upper(), minutes,
        )
    return _rows_to_json(rows)


# ══════════════════════════════════════════════════════════
# Cross-Asset Comparison — NEW
# ══════════════════════════════════════════════════════════

@app.get("/comparison", tags=["analytics"])
async def comparison():
    """
    BTC vs ETH real-time comparison from the analytics engine:
      - Volume (USD) in rolling 60s window
      - Average trade size
      - Volatility coefficient of variation
      - Pearson rolling correlation
      - Recent z-score divergence (relative anomaly detection)
    """
    if _engine:
        return _engine.cross_asset_stats()

    # Fallback path: API and consumer run as separate processes in local dev,
    # so in-memory analytics state may not be attached to the API process.
    if not _db_pool:
        raise HTTPException(503, "DB not ready")

    async with _db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                symbol,
                COUNT(*) AS window_size,
                SUM(usd_value) AS volume_usd,
                AVG(usd_value) AS avg_trade_usd,
                STDDEV_POP(usd_value) AS stdev_usd,
                MIN(usd_value) AS min_usd,
                MAX(usd_value) AS max_usd
            FROM trades_enriched
            WHERE symbol IN ('BTCUSDT', 'ETHUSDT')
              AND time > NOW() - INTERVAL '60 seconds'
            GROUP BY symbol
            """
        )

    by_symbol = {r["symbol"]: dict(r) for r in rows}

    def _summary(symbol: str) -> dict:
        data = by_symbol.get(symbol)
        if not data:
            return {"symbol": symbol, "status": "no_data"}
        mean = float(data.get("avg_trade_usd") or 0)
        stdev = float(data.get("stdev_usd") or 0)
        return {
            "symbol": symbol,
            "window_size": int(data.get("window_size") or 0),
            "volume_usd": round(float(data.get("volume_usd") or 0), 2),
            "avg_trade_usd": round(mean, 2),
            "stdev_usd": round(stdev, 2),
            "volatility_cv": round(stdev / mean, 4) if mean > 0 else None,
            "min_usd": round(float(data.get("min_usd") or 0), 2),
            "max_usd": round(float(data.get("max_usd") or 0), 2),
            "warmed_up": int(data.get("window_size") or 0) >= 30,
        }

    btc = _summary("BTCUSDT")
    eth = _summary("ETHUSDT")

    btc_vol = float(btc.get("volume_usd") or 0)
    eth_vol = float(eth.get("volume_usd") or 0)
    total = btc_vol + eth_vol

    return {
        "status": "ok",
        "window_seconds": 60,
        "source": "db_fallback",
        "btc": btc,
        "eth": eth,
        "rolling_correlation": None,
        "btc_recent_z": None,
        "eth_recent_z": None,
        "divergence_alert": False,
        "btc_volume_share": round(btc_vol / total, 4) if total > 0 else None,
        "eth_volume_share": round(eth_vol / total, 4) if total > 0 else None,
    }


# ══════════════════════════════════════════════════════════
# Window Debug (analytics engine state)
# ══════════════════════════════════════════════════════════

@app.get("/window/{symbol}", tags=["analytics"])
async def window_stats(symbol: str):
    if not _engine:
        raise HTTPException(503, "Analytics engine not initialized")
    return _engine.window_stats(symbol.upper())


# ══════════════════════════════════════════════════════════
# WebSocket — NEW: push-based live feed
# ══════════════════════════════════════════════════════════

@app.websocket("/ws/trades")
async def ws_trades(websocket: WebSocket):
    """
    WebSocket endpoint for live enriched trade push.
    Broadcasts to all connected clients whenever the HTTP polling
    would return new data. Clients receive JSON objects matching
    the /live_trades schema.

    If the DB pool is not ready, falls back to polling.
    """
    await websocket.accept()
    _ws_clients.add(websocket)
    log.info("WebSocket client connected | total=%d", len(_ws_clients))

    try:
        last_trade_id: Optional[int] = None

        while True:
            await asyncio.sleep(0.5)  # 500ms push interval

            if not _db_pool:
                continue

            try:
                async with _db_pool.acquire() as conn:
                    # Fetch the latest N trades PER SYMBOL so both BTC and ETH
                    # always appear, even when one symbol has much higher throughput.
                    rows = await conn.fetch("""
                        SELECT trade_id, time, symbol, price, quantity,
                               usd_value, classification, z_score, is_anomaly
                        FROM (
                            SELECT *, ROW_NUMBER() OVER (
                                PARTITION BY symbol ORDER BY time DESC
                            ) AS rn
                            FROM trades_enriched
                            WHERE time > NOW() - INTERVAL '2 seconds'
                        ) sub
                        WHERE rn <= 20
                        ORDER BY time DESC
                    """)

                if rows:
                    trades = _rows_to_json(rows)
                    await websocket.send_json({"type": "trades", "data": trades})

            except Exception as exc:
                log.debug("WS fetch error: %s", exc)

    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(websocket)
        log.info("WebSocket client disconnected | total=%d", len(_ws_clients))


@app.websocket("/ws/anomalies")
async def ws_anomalies(websocket: WebSocket):
    """WebSocket push for anomaly alerts."""
    await websocket.accept()
    _ws_clients.add(websocket)

    try:
        last_seen_time: Optional[str] = None

        while True:
            await asyncio.sleep(1.0)

            if not _db_pool:
                continue

            try:
                async with _db_pool.acquire() as conn:
                    rows = await conn.fetch("""
                        SELECT trade_id, time, symbol, price, usd_value,
                               classification, z_score
                        FROM trades_enriched
                        WHERE is_anomaly = TRUE
                          AND time > NOW() - INTERVAL '5 seconds'
                        ORDER BY time DESC
                        LIMIT 10
                    """)

                if rows:
                    data = _rows_to_json(rows)
                    await websocket.send_json({"type": "anomalies", "data": data})

            except Exception as exc:
                log.debug("WS anomaly fetch error: %s", exc)

    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(websocket)
