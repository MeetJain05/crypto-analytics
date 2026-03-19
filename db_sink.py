#!/usr/bin/env python3
# ============================================================
# VibeStream-Alpha: Database Sink — FIXED
# Fixes: B3 (deferred commits via callbacks), B4 (broken re-queue lock)
# ============================================================

from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Optional

import asyncpg

from schemas import EnrichedTrade
from settings import (
    DB_DSN, DB_POOL_MIN, DB_POOL_MAX,
    BATCH_FLUSH_INTERVAL_MS, BATCH_FLUSH_SIZE,
)

log = logging.getLogger("vibestream.db_sink")

UPSERT_SQL = """
INSERT INTO trades_enriched (
    trade_id, time, symbol, price, quantity,
    usd_value, classification, z_score, is_anomaly
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (trade_id, time) DO NOTHING
"""


class SinkMetrics:
    def __init__(self) -> None:
        self.rows_written: int = 0
        self.rows_skipped: int = 0
        self.batches_flushed: int = 0
        self.flush_errors: int = 0
        self._start: float = time.monotonic()

    def record_flush(self, attempted: int) -> None:
        self.rows_written += attempted
        self.batches_flushed += 1

    def log_summary(self) -> None:
        log.info(
            "💾 DB Sink | written=%d batches=%d errors=%d uptime=%.0fs",
            self.rows_written, self.batches_flushed,
            self.flush_errors, time.monotonic() - self._start,
        )


class DatabaseSink:
    """
    Batched async writer to TimescaleDB.

    B3 Fix — Deferred Kafka commits:
      Kafka offsets must be committed only AFTER the data reaches the DB.
      `register_post_flush_callback(cb)` lets the consumer register its
      commit function. It fires after every successful executemany().

    B4 Fix — Broken re-queue:
      Original: `async with asyncio.Lock()` created a new unguarded lock each
      time. Since _flush_buffer_locked runs while holding self._buffer_lock,
      re-queuing just requires direct assignment — no extra lock needed.
    """

    def __init__(self) -> None:
        self._pool: Optional[asyncpg.Pool] = None
        self._buffer: list[tuple] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        self.metrics = SinkMetrics()
        self._post_flush_callbacks: list[Callable] = []

    def register_post_flush_callback(self, cb: Callable) -> None:
        """Register async callback called after each successful DB flush."""
        self._post_flush_callbacks.append(cb)

    async def connect(self) -> None:
        log.info("🔌 Connecting to TimescaleDB: %s", DB_DSN.split("@")[-1])
        self._pool = await asyncpg.create_pool(
            DB_DSN,
            min_size=DB_POOL_MIN,
            max_size=DB_POOL_MAX,
            command_timeout=30,
            server_settings={"application_name": "vibestream-consumer", "jit": "off"},
        )
        log.info("✅ DB pool ready (min=%d max=%d)", DB_POOL_MIN, DB_POOL_MAX)
        self._flush_task = asyncio.create_task(
            self._batch_flush_loop(), name="db-batch-flusher"
        )

    async def close(self) -> None:
        log.info("🛑 Closing database sink...")
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        await self._flush_buffer(reason="shutdown")
        if self._pool:
            await self._pool.close()
        self.metrics.log_summary()

    async def write(self, trade: EnrichedTrade) -> None:
        async with self._buffer_lock:
            self._buffer.append(trade.db_tuple())
            if len(self._buffer) >= BATCH_FLUSH_SIZE:
                await self._flush_buffer_locked(reason="size_limit")

    async def write_many(self, trades: list[EnrichedTrade]) -> None:
        async with self._buffer_lock:
            self._buffer.extend(t.db_tuple() for t in trades)
            if len(self._buffer) >= BATCH_FLUSH_SIZE:
                await self._flush_buffer_locked(reason="bulk_size_limit")

    async def _batch_flush_loop(self) -> None:
        interval = BATCH_FLUSH_INTERVAL_MS / 1000.0
        log.info("⏱️  Batch flusher | interval=%.0fms size_limit=%d",
                 BATCH_FLUSH_INTERVAL_MS, BATCH_FLUSH_SIZE)
        while True:
            await asyncio.sleep(interval)
            async with self._buffer_lock:
                if self._buffer:
                    await self._flush_buffer_locked(reason="timer")

    async def _flush_buffer(self, reason: str = "manual") -> None:
        async with self._buffer_lock:
            await self._flush_buffer_locked(reason=reason)

    async def _flush_buffer_locked(self, reason: str = "unknown") -> None:
        """Must be called while holding self._buffer_lock."""
        if not self._buffer:
            return

        batch = self._buffer.copy()
        self._buffer.clear()

        if not self._pool:
            log.error("❌ No DB pool — re-queuing %d records", len(batch))
            self.metrics.flush_errors += 1
            # B4 FIX: already hold _buffer_lock; assign directly, no new Lock()
            self._buffer = batch + self._buffer
            return

        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(UPSERT_SQL, batch)

            self.metrics.record_flush(len(batch))
            log.debug("💾 Flushed | reason=%s rows=%d", reason, len(batch))

            # B3 FIX: fire callbacks so consumer commits Kafka offsets
            # only after data is confirmed written to the DB
            for cb in self._post_flush_callbacks:
                try:
                    await cb()
                except Exception as e:
                    log.error("Post-flush callback error: %s", e)

        except (asyncpg.PostgresConnectionError, asyncpg.TooManyConnectionsError) as exc:
            log.error("DB connection error: %s — re-queuing batch", exc)
            self.metrics.flush_errors += 1
            # B4 FIX: re-queue inside lock scope, no new Lock() object
            self._buffer = batch + self._buffer

        except Exception as exc:
            log.error("Flush error: %s", exc, exc_info=True)
            self.metrics.flush_errors += 1
            self._buffer = batch + self._buffer
