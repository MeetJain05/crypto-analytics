from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Optional

import websockets
from confluent_kafka import Producer, KafkaException
from pydantic import ValidationError

# ── Local imports ──────────────────────────────────────────
from settings import (
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC_RAW,
    SYMBOL_PARTITION_MAP,
    BINANCE_WS_URL,
    LOG_LEVEL,
    LOG_FORMAT,
)
from schemas import BinanceCombinedStream, RawTrade

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("vibestream.producer")
class ProducerMetrics:
    def __init__(self) -> None:
        self.messages_received: int = 0
        self.messages_published: int = 0
        self.messages_failed: int = 0
        self.parse_errors: int = 0
        self._start_time: float = time.monotonic()

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self._start_time

    @property
    def publish_rate(self) -> float:
        uptime = self.uptime_seconds
        return self.messages_published / uptime if uptime > 0 else 0.0

    def log_summary(self) -> None:
        log.info(
            "📊 Producer Stats | received=%d published=%d failed=%d "
            "parse_errors=%d rate=%.1f msg/s uptime=%.0fs",
            self.messages_received,
            self.messages_published,
            self.messages_failed,
            self.parse_errors,
            self.publish_rate,
            self.uptime_seconds,
        )

class VibeStreamProducer:
    """
    Async producer that:
      1. Opens a Binance combined WebSocket stream
      2. Parses aggTrade messages → RawTrade (unified schema)
      3. Publishes JSON to Kafka `trades_raw` with symbol partitioning
      4. Handles reconnection with exponential backoff
    """

    RECONNECT_BASE_DELAY: float = 1.0
    RECONNECT_MAX_DELAY: float = 60.0
    RECONNECT_BACKOFF: float = 2.0
    STATS_LOG_INTERVAL: float = 30.0

    def __init__(self) -> None:
        self.metrics = ProducerMetrics()
        self._running = False
        self._kafka: Optional[Producer] = None
        self._last_stats_log = time.monotonic()

    # ── Kafka Setup ────────────────────────────────────────
    def _build_kafka_producer(self) -> Producer:
        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            # Local single-broker dev mode: disable idempotence to avoid
            # fatal sequence desync errors after broker/topic restarts.
            "acks": "1",
            # Throughput: small linger for micro-batching
            "linger.ms": 5,
            "batch.size": 65536,        # 64 KB batch
            "compression.type": "lz4",
            # Retry on transient failures (idempotence also requires retries > 0)
            "retries": 5,
            "retry.backoff.ms": 250,
            "enable.idempotence": False,
        }
        log.info("🔌 Connecting to Kafka at %s", KAFKA_BOOTSTRAP)
        return Producer(conf)

    def _delivery_callback(self, err, msg) -> None:
        """Called by confluent-kafka after each message is ack'd or failed."""
        if err:
            self.metrics.messages_failed += 1
            log.error(
                "❌ Kafka delivery failed | topic=%s partition=%d err=%s",
                msg.topic(),
                msg.partition(),
                err,
            )
        else:
            self.metrics.messages_published += 1

    # ── Message Parsing ────────────────────────────────────
    def _parse_binance_message(self, raw: str) -> Optional[RawTrade]:
        """
        Parse a raw Binance WebSocket JSON string → RawTrade.
        Returns None on any parse/validation failure.
        """
        try:
            data = json.loads(raw)

            # Binance combined stream wraps payload in {"stream":..., "data":...}
            if "stream" not in data:
                return None

            msg = BinanceCombinedStream(**data)

            # Only process aggTrade events
            if msg.data.e != "aggTrade":
                return None

            return msg.data.to_raw_trade()

        except (json.JSONDecodeError, ValidationError, KeyError) as exc:
            self.metrics.parse_errors += 1
            log.debug("Parse error: %s | raw=%s", exc, raw[:120])
            return None

    # ── Publish to Kafka ───────────────────────────────────
    def _publish(self, trade: RawTrade) -> None:
        """
        Publish a RawTrade to Kafka with mandatory symbol partitioning.
        PRD §4.1: BTC → partition 0, ETH → partition 1
        """
        partition = SYMBOL_PARTITION_MAP.get(trade.symbol, -1)
        if partition == -1:
            log.warning("Unknown symbol %s — skipping partition assignment", trade.symbol)
            return

        payload = json.dumps(trade.to_kafka_value(), default=str).encode("utf-8")
        key = trade.to_kafka_key()

        self._kafka.produce(
            topic=KAFKA_TOPIC_RAW,
            key=key,
            value=payload,
            partition=partition,
            on_delivery=self._delivery_callback,
        )
        # Non-blocking poll to trigger delivery callbacks
        self._kafka.poll(0)

    # ── WebSocket Loop ─────────────────────────────────────
    async def _ws_loop(self) -> None:
        """
        Core WebSocket receive loop.
        Runs until self._running is False.
        """
        reconnect_delay = self.RECONNECT_BASE_DELAY

        while self._running:
            try:
                log.info("🌐 Opening WebSocket → %s", BINANCE_WS_URL)
                async with websockets.connect(
                    BINANCE_WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    reconnect_delay = self.RECONNECT_BASE_DELAY  # Reset on success
                    log.info("✅ WebSocket connected — streaming BTC + ETH aggTrades")

                    async for raw_msg in ws:
                        if not self._running:
                            break

                        self.metrics.messages_received += 1
                        trade = self._parse_binance_message(raw_msg)

                        if trade:
                            self._publish(trade)
                            log.debug(
                                "→ Kafka [partition %d] %s @ $%.2f qty=%.6f",
                                SYMBOL_PARTITION_MAP.get(trade.symbol, -1),
                                trade.symbol,
                                trade.price,
                                trade.quantity,
                            )

                        # Periodic stats
                        now = time.monotonic()
                        if now - self._last_stats_log >= self.STATS_LOG_INTERVAL:
                            self.metrics.log_summary()
                            self._last_stats_log = now

            except websockets.exceptions.ConnectionClosed as exc:
                log.warning("WebSocket closed: %s", exc)
            except Exception as exc:
                # Common on Windows when network briefly drops.
                if isinstance(exc, ConnectionResetError) and getattr(exc, "winerror", None) == 64:
                    log.warning("Transient network reset (WinError 64): %s", exc)
                else:
                    log.error("WebSocket error: %s", exc, exc_info=True)
            finally:
                # Flush pending Kafka messages before reconnecting
                if self._kafka:
                    remaining = self._kafka.flush(timeout=5)
                    if remaining > 0:
                        log.warning("⚠️  %d Kafka messages unflushed after disconnect", remaining)

            if self._running:
                log.info("🔄 Reconnecting in %.1fs...", reconnect_delay)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(
                    reconnect_delay * self.RECONNECT_BACKOFF,
                    self.RECONNECT_MAX_DELAY,
                )

    # ── Lifecycle ──────────────────────────────────────────
    async def start(self) -> None:
        self._kafka = self._build_kafka_producer()
        self._running = True
        log.info("🚀 VibeStream Producer starting up")
        await self._ws_loop()

    async def stop(self) -> None:
        log.info("🛑 Shutting down producer...")
        self._running = False
        if self._kafka:
            remaining = self._kafka.flush(timeout=10)
            log.info(
                "✅ Kafka flush complete | unflushed=%d", remaining
            )
        self.metrics.log_summary()


# ══════════════════════════════════════════════════════════
# Entry Point
# ══════════════════════════════════════════════════════════
async def main() -> None:
    producer = VibeStreamProducer()

    loop = asyncio.get_event_loop()

    def _shutdown(sig_name: str) -> None:
        log.info("Received %s — initiating graceful shutdown", sig_name)
        asyncio.create_task(producer.stop())

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig.name: _shutdown(s))
    except NotImplementedError:
        # Windows doesn't support add_signal_handler
        log.debug("Signal handlers not available on this platform")

    try:
        await producer.start()
    except asyncio.CancelledError:
        pass
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
