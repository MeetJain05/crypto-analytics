#!/usr/bin/env python3
# ============================================================
# VibeStream-Alpha: Kafka Consumer — FIXED
# Fixes: B3 (deferred commits), B7 (deprecated get_event_loop)
# ============================================================

from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from settings import (
    KAFKA_BOOTSTRAP, KAFKA_TOPIC_RAW,
    LOG_LEVEL, LOG_FORMAT,
    INGESTION_RATE_THRESHOLD, BATCH_FLUSH_INTERVAL_MS,
)
from schemas import RawTrade
from analytics import AnalyticsEngine
from db_sink import DatabaseSink

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("vibestream.consumer")


class ConsumerMetrics:
    RATE_WINDOW_SECONDS: float = 5.0

    def __init__(self) -> None:
        self.messages_consumed: int = 0
        self.messages_enriched: int = 0
        self.messages_failed: int = 0
        self.anomalies_detected: int = 0
        self._start: float = time.monotonic()
        self._rate_count: int = 0
        self._rate_window_start: float = time.monotonic()

    def record(self, is_anomaly: bool = False) -> None:
        self.messages_consumed += 1
        self.messages_enriched += 1
        self._rate_count += 1
        if is_anomaly:
            self.anomalies_detected += 1

    @property
    def ingestion_rate(self) -> float:
        elapsed = time.monotonic() - self._rate_window_start
        if elapsed >= self.RATE_WINDOW_SECONDS:
            rate = self._rate_count / elapsed
            self._rate_count = 0
            self._rate_window_start = time.monotonic()
            return rate
        return self._rate_count / max(elapsed, 0.001)

    def log_summary(self) -> None:
        log.info(
            "📊 Consumer | consumed=%d enriched=%d failed=%d anomalies=%d "
            "rate=%.1f msg/s uptime=%.0fs",
            self.messages_consumed, self.messages_enriched,
            self.messages_failed, self.anomalies_detected,
            self.ingestion_rate, time.monotonic() - self._start,
        )


class VibeStreamConsumer:
    """
    Async Kafka consumer.

    B3 Fix — Deferred Offset Commits:
      Original: commit called immediately after `sink.write()` which only
      buffers in memory. If the process crashed before the 100ms batch flush,
      the Kafka offset was committed but the data was never in the DB → silent
      data loss.

      Fix: Don't commit per-message. Instead:
        1. Track the latest seen Message per partition in _pending_commits
        2. Register a post-flush callback with the sink
        3. After each successful DB flush, commit the tracked offsets
      This guarantees: committed offset ⟺ data in TimescaleDB.
    """

    STATS_LOG_INTERVAL: float = 30.0
    POLL_TIMEOUT: float = 0.1
    HIGH_RATE_LOG_INTERVAL: float = 5.0

    def __init__(self) -> None:
        self.metrics = ConsumerMetrics()
        self.engine = AnalyticsEngine()
        self.sink = DatabaseSink()
        self._running = False
        self._consumer: Optional[Consumer] = None
        self._last_stats_log: float = time.monotonic()
        self._last_high_rate_warn: float = 0.0

        # B3 fix: track the latest message per partition for deferred commits
        self._pending_commits: dict[int, Message] = {}

    def _build_kafka_consumer(self) -> Consumer:
        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "vibestream-enrichment-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 30_000,
            "heartbeat.interval.ms": 5_000,
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 100,
            "max.poll.interval.ms": 300_000,
        }
        log.info("🔌 Kafka consumer connecting to %s", KAFKA_BOOTSTRAP)
        consumer = Consumer(conf)
        consumer.subscribe([KAFKA_TOPIC_RAW])
        log.info("✅ Subscribed to topic: %s", KAFKA_TOPIC_RAW)
        return consumer

    def _deserialize(self, msg: Message) -> Optional[RawTrade]:
        try:
            raw = json.loads(msg.value().decode("utf-8"))
            # Reconstitute timezone-aware datetime from ISO string
            et = raw["event_time"]
            dt = datetime.fromisoformat(et)
            if dt.tzinfo is None:
                # Guard against naive datetimes from older producers
                dt = dt.replace(tzinfo=timezone.utc)
            raw["event_time"] = dt
            return RawTrade(**raw)
        except Exception as exc:
            log.error(
                "❌ Deserialization failed | partition=%d offset=%d err=%s",
                msg.partition(), msg.offset(), exc,
            )
            self.metrics.messages_failed += 1
            return None

    async def _commit_pending_offsets(self) -> None:
        """
        B3 Fix: called by the DB sink after every successful flush.
        Commits the latest seen offset for each partition.
        """
        if not self._pending_commits or not self._consumer:
            return
        for partition, msg in list(self._pending_commits.items()):
            try:
                await asyncio.to_thread(self._consumer.commit, message=msg, asynchronous=False)
                log.debug("✅ Committed partition=%d offset=%d", partition, msg.offset())
            except Exception as exc:
                log.error("Commit error partition=%d: %s", partition, exc)
        self._pending_commits.clear()

    def _check_flash_crash(self) -> None:
        rate = self.metrics.ingestion_rate
        now = time.monotonic()
        if (
            rate > INGESTION_RATE_THRESHOLD
            and now - self._last_high_rate_warn > self.HIGH_RATE_LOG_INTERVAL
        ):
            log.warning(
                "⚡ FLASH CRASH MODE | rate=%.0f msg/s > threshold=%d "
                "| batch writer engaged (flush=%dms / %d rows)",
                rate, INGESTION_RATE_THRESHOLD,
                BATCH_FLUSH_INTERVAL_MS, 500,
            )
            self._last_high_rate_warn = now

    async def _consume_loop(self) -> None:
        log.info("🚀 Consumer loop started")

        while self._running:
            msg = await asyncio.to_thread(self._consumer.poll, self.POLL_TIMEOUT)

            if msg is None:
                continue

            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    log.debug("EOF partition=%d offset=%d", msg.partition(), msg.offset())
                elif err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    log.error("Topic not found: %s", KAFKA_TOPIC_RAW)
                    await asyncio.sleep(5)
                else:
                    log.error("Kafka error: %s", err)
                continue

            trade = self._deserialize(msg)
            if trade is None:
                # Commit immediately for poison-pill messages (avoid infinite replay)
                await asyncio.to_thread(self._consumer.commit, message=msg, asynchronous=False)
                continue

            try:
                enriched = self.engine.enrich(trade)
            except Exception as exc:
                log.error("Analytics error trade=%d: %s", trade.trade_id, exc, exc_info=True)
                self.metrics.messages_failed += 1
                await asyncio.to_thread(self._consumer.commit, message=msg, asynchronous=False)
                continue

            await self.sink.write(enriched)
            self.metrics.record(is_anomaly=enriched.is_anomaly)

            # B3 fix: track latest message per partition instead of committing immediately.
            # The DB sink's post-flush callback will commit these after DB write succeeds.
            self._pending_commits[msg.partition()] = msg

            self._check_flash_crash()

            now = time.monotonic()
            if now - self._last_stats_log >= self.STATS_LOG_INTERVAL:
                self.metrics.log_summary()
                for sym in ["BTCUSDT", "ETHUSDT"]:
                    log.info("📐 Window [%s]: %s", sym, self.engine.window_stats(sym))
                self.sink.metrics.log_summary()
                self._last_stats_log = now

    async def start(self) -> None:
        await self.sink.connect()
        # B3 fix: register deferred commit callback
        self.sink.register_post_flush_callback(self._commit_pending_offsets)

        self._consumer = self._build_kafka_consumer()
        self._running = True
        log.info("✅ VibeStream Consumer ready")

        try:
            await self._consume_loop()
        except KafkaException as exc:
            log.error("Fatal Kafka error: %s", exc)

    async def stop(self) -> None:
        log.info("🛑 Shutting down consumer...")
        self._running = False
        # Allow the poll loop to exit naturally before closing
        await asyncio.sleep(self.POLL_TIMEOUT * 2)
        if self._consumer:
            self._consumer.close()
            log.info("Kafka consumer closed")
        await self.sink.close()
        self.metrics.log_summary()


async def main() -> None:
    consumer = VibeStreamConsumer()

    # B7 fix: use asyncio.get_running_loop() inside async context
    loop = asyncio.get_running_loop()

    def _shutdown(sig_name: str) -> None:
        log.info("Received %s — initiating graceful shutdown", sig_name)
        loop.create_task(consumer.stop())

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig.name: _shutdown(s))
    except NotImplementedError:
        # Windows doesn't support add_signal_handler
        log.debug("Signal handlers not available on this platform")

    try:
        await consumer.start()
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
