# ============================================================
# VibeStream-Alpha: Unified Trade Schema (PRD §2.1)
# ============================================================

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ── Classification Type ────────────────────────────────────
Classification = Literal["Retail", "Pro", "Whale"]


class RawTrade(BaseModel):
    """
    Normalized representation of an exchange aggregated trade.
    All WebSocket sources (Binance, Coinbase) map to this schema
    before being published to Kafka.

    PRD §2.1: symbol, price, quantity, event_time, trade_id
    """

    symbol: str = Field(..., description="Trading pair, e.g. BTCUSDT")
    price: float = Field(..., gt=0, description="Execution price in quote currency")
    quantity: float = Field(..., gt=0, description="Base asset quantity")
    event_time: datetime = Field(
        ..., description="Exchange-side event timestamp (NOT local time)"
    )
    trade_id: int = Field(..., description="Exchange-assigned unique trade ID")

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, v: str) -> str:
        return v.upper().strip()

    def to_kafka_key(self) -> bytes:
        """Partition key — symbol ensures chronological ordering per asset."""
        return self.symbol.encode("utf-8")

    def to_kafka_value(self) -> dict:
        return {
            "symbol": self.symbol,
            "price": self.price,
            "quantity": self.quantity,
            "event_time": self.event_time.isoformat(),
            "trade_id": self.trade_id,
        }


class EnrichedTrade(BaseModel):
    """
    Fully enriched trade with USD value, classification, and
    statistical anomaly data. Persisted to TimescaleDB.

    PRD §2.1: raw_fields + usd_value, classification, z_score, is_anomaly
    """

    # ── Raw fields (inherited from RawTrade) ──────────────
    symbol: str
    price: float
    quantity: float
    event_time: datetime
    trade_id: int

    # ── Enriched fields ───────────────────────────────────
    usd_value: float = Field(..., description="price × quantity in USD")
    classification: Classification
    z_score: Optional[float] = Field(
        None,
        description="Z-score vs 60s rolling window; None during warm-up",
    )
    is_anomaly: bool = Field(
        False,
        description="|z| > 3.0 AND window is warmed up",
    )

    @model_validator(mode="after")
    def validate_usd_value(self) -> "EnrichedTrade":
        expected = round(self.price * self.quantity, 4)
        if abs(self.usd_value - expected) > 0.01:
            raise ValueError(
                f"usd_value {self.usd_value} != price×qty {expected}"
            )
        return self

    def db_tuple(self) -> tuple:
        """Returns a tuple matching the TimescaleDB upsert parameter order."""
        return (
            self.trade_id,
            self.event_time,
            self.symbol,
            self.price,
            self.quantity,
            self.usd_value,
            self.classification,
            self.z_score,
            self.is_anomaly,
        )

    def __repr__(self) -> str:
        anomaly_tag = " 🚨ANOMALY" if self.is_anomaly else ""
        z_str = f"z={self.z_score:.2f}" if self.z_score is not None else "z=warming"
        return (
            f"[{self.symbol}] ${self.usd_value:,.2f} "
            f"({self.classification}) {z_str}{anomaly_tag}"
        )


# ── Binance WebSocket Message Parser ──────────────────────
class BinanceAggTrade(BaseModel):
    """
    Raw Binance aggregated trade message structure.
    Ref: https://binance-docs.github.io/apidocs/spot/en/#aggregate-trade-streams
    """

    e: str              # Event type: "aggTrade"
    E: int              # Event time (ms since epoch)
    s: str              # Symbol
    a: int              # Aggregate trade ID
    p: str              # Price
    q: str              # Quantity
    T: int              # Trade time (ms) — this is the canonical event time
    m: bool             # Was the buyer the market maker?

    def to_raw_trade(self) -> RawTrade:
        """Normalize Binance aggTrade → unified RawTrade.
        
        FIX B2: datetime.utcfromtimestamp() is deprecated in Python 3.12
        and returns a naive (timezone-unaware) datetime.
        Using datetime.fromtimestamp(ts, tz=timezone.utc) ensures:
          - Timezone-aware object throughout the pipeline
          - asyncpg writes correct TIMESTAMPTZ (not local time)
          - No implicit UTC assumption on non-UTC hosts
        """
        return RawTrade(
            symbol=self.s,
            price=float(self.p),
            quantity=float(self.q),
            event_time=datetime.fromtimestamp(self.T / 1000, tz=timezone.utc),
            trade_id=self.a,
        )


class BinanceCombinedStream(BaseModel):
    """
    Wrapper for Binance combined stream messages.
    Format: {"stream": "btcusdt@aggTrade", "data": {...}}
    """

    stream: str
    data: BinanceAggTrade
