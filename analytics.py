#!/usr/bin/env python3
# ============================================================
# VibeStream-Alpha: Analytics Engine (PRD §3)
# ============================================================
# Stateful per-symbol processor that maintains a 60-second
# sliding window and performs:
#   A. USD Value normalization
#   B. Behavioral classification (Retail / Pro / Whale)
#   C. Z-Score anomaly detection with warm-up guard
#
# PRD §4.2: State Store with collections.deque
# PRD §5: Edge cases (clock drift, stale data, data gaps)
# ============================================================

from __future__ import annotations

import logging
import statistics
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from settings import (
    WINDOW_SECONDS,
    WARMUP_MIN_SAMPLES,
    DATA_GAP_RESET_SECONDS,
    STALE_PURGE_SECONDS,
    Z_SCORE_THRESHOLD,
    RETAIL_MAX,
    PRO_MAX,
)
from schemas import Classification, EnrichedTrade, RawTrade

log = logging.getLogger("vibestream.analytics")


# ══════════════════════════════════════════════════════════
# WindowEntry: a single observation in the sliding window
# ══════════════════════════════════════════════════════════
@dataclass(slots=True)
class WindowEntry:
    """Lightweight record stored in the per-symbol deque."""
    event_time: datetime   # Exchange-side timestamp (PRD §5 clock drift)
    usd_value: float


# ══════════════════════════════════════════════════════════
# SymbolState: per-symbol state store (PRD §4.2)
# ══════════════════════════════════════════════════════════
@dataclass
class SymbolState:
    """
    Holds the rolling window and gap-detection bookkeeping
    for one symbol (BTC or ETH).

    The deque is unbounded in size but is filtered by event_time
    so entries older than WINDOW_SECONDS are evicted on each call.
    Using a deque (vs a maxlen deque) lets us correctly handle
    low-traffic periods where maxlen would evict by count, not time.
    """
    symbol: str
    window: deque[WindowEntry] = field(default_factory=deque)
    last_event_time: Optional[datetime] = None

    # Tracks whether we're in a "gap pause" (PRD §4.2 warm-up logic)
    in_gap_pause: bool = False

    # Timestamp of the first message received after a gap was detected.
    # The pause lifts only once we have ≥ WARMUP_MIN_SAMPLES entries
    # whose event_time >= this value (i.e. POST-gap data only).
    _gap_recovery_start: Optional[datetime] = field(default=None, repr=False)

    def _evict_stale(self, current_event_time: datetime) -> None:
        """
        Remove entries outside the 60-second rolling window.
        Uses exchange event_time (NOT local clock) per PRD §5.
        """
        cutoff_ts = current_event_time.timestamp() - WINDOW_SECONDS
        while self.window and self.window[0].event_time.timestamp() < cutoff_ts:
            self.window.popleft()

    def _check_and_handle_gap(self, current_event_time: datetime) -> bool:
        """
        Detect data gaps (PRD §4.2, §5).

        Returns True if we should suppress anomaly flagging.

        Rules:
          - Gap > STALE_PURGE_SECONDS (60s) → purge window entirely
          - Gap > DATA_GAP_RESET_SECONDS (10s) → enter gap pause
          - gap pause ends only once ≥ WARMUP_MIN_SAMPLES new (post-gap)
            entries have been ingested
        """
        if self.last_event_time is None:
            return False  # First ever message

        gap = current_event_time.timestamp() - self.last_event_time.timestamp()

        if gap > STALE_PURGE_SECONDS:
            # PRD §5: stream dropped > 60s → purge window
            log.warning(
                "🕳️  [%s] Stale data gap of %.1fs detected — purging rolling window",
                self.symbol, gap,
            )
            self.window.clear()
            self.in_gap_pause = True
            self._gap_recovery_start = current_event_time
            return True

        if gap > DATA_GAP_RESET_SECONDS:
            # PRD §4.2: disconnect > 10s → pause anomaly flagging.
            # Record the recovery start so is_warmed_up() can count only
            # entries accumulated AFTER the gap.
            log.info(
                "⚠️  [%s] Data gap of %.1fs — pausing anomaly detection",
                self.symbol, gap,
            )
            self.in_gap_pause = True
            self._gap_recovery_start = current_event_time
            return True

        return False

    def _post_gap_count(self) -> int:
        """Number of window entries received after the gap recovery start."""
        if self._gap_recovery_start is None:
            return len(self.window)
        cutoff = self._gap_recovery_start.timestamp()
        return sum(1 for e in self.window if e.event_time.timestamp() >= cutoff)

    def is_warmed_up(self) -> bool:
        """
        PRD §4.2: Window must have ≥ 30 data points before flagging.

        After a data gap, we require ≥ WARMUP_MIN_SAMPLES entries that
        arrived POST-gap before clearing the pause. This prevents the
        pre-existing window from masking the warm-up requirement.
        """
        if not self.in_gap_pause:
            # Normal path: just check total window size
            return len(self.window) >= WARMUP_MIN_SAMPLES

        # Gap-pause path: count only post-gap entries
        post_gap = self._post_gap_count()
        ready = post_gap >= WARMUP_MIN_SAMPLES
        if ready:
            log.info(
                "✅ [%s] Re-warmed with %d post-gap points — resuming anomaly detection",
                self.symbol, post_gap,
            )
            self.in_gap_pause = False
            self._gap_recovery_start = None
        return ready

    def add(self, entry: WindowEntry) -> None:
        self.window.append(entry)

    @property
    def values(self) -> list[float]:
        return [e.usd_value for e in self.window]

    @property
    def size(self) -> int:
        return len(self.window)


# ══════════════════════════════════════════════════════════
# AnalyticsEngine
# ══════════════════════════════════════════════════════════
class AnalyticsEngine:
    """
    Stateful, per-symbol enrichment pipeline.

    Thread-safety: Each symbol's SymbolState is independent.
    The engine is designed to be used from a single asyncio
    consumer task per symbol (enforced by Kafka partition assignment).
    """

    def __init__(self) -> None:
        # One state store per symbol
        self._states: dict[str, SymbolState] = {}
        log.info(
            "🧠 Analytics Engine initialized | window=%ds warmup=%d gap_reset=%ds purge=%ds z_threshold=%.1f",
            WINDOW_SECONDS, WARMUP_MIN_SAMPLES,
            DATA_GAP_RESET_SECONDS, STALE_PURGE_SECONDS,
            Z_SCORE_THRESHOLD,
        )

    def _get_state(self, symbol: str) -> SymbolState:
        if symbol not in self._states:
            self._states[symbol] = SymbolState(symbol=symbol)
            log.info("📌 New symbol state created: %s", symbol)
        return self._states[symbol]

    # ── A. USD Normalization ───────────────────────────────
    @staticmethod
    def compute_usd_value(price: float, quantity: float) -> float:
        """PRD §3A: USD_Value = Price × Quantity"""
        return round(price * quantity, 4)

    # ── B. Behavioral Classification ──────────────────────
    @staticmethod
    def classify(usd_value: float) -> Classification:
        """
        PRD §3B: Fixed threshold classification.
          Retail: < $500
          Pro:    $500 – $50,000
          Whale:  > $50,000
        """
        if usd_value < RETAIL_MAX:
            return "Retail"
        elif usd_value <= PRO_MAX:
            return "Pro"
        else:
            return "Whale"

    # ── C. Z-Score Anomaly Detection ──────────────────────
    @staticmethod
    def compute_z_score(value: float, values: list[float]) -> Optional[float]:
        """
        PRD §3C: z = (x - μ) / σ

        Improvement — Low-Variance Handling:
          In production, the window can occasionally have near-zero variance
          (e.g., a very stable trading period). Raw pstdev=0 → z=0 regardless
          of how extreme the new value is, causing missed anomalies.

          Fix: apply a σ floor of 0.1% of the mean (relative minimum dispersion).
          This is the "regularized z-score" pattern used in prod anomaly detectors.
          Effect: a value 10× the mean against a flat window gets z ≈ 9000,
          correctly flagged as extreme. Normal values still get z ≈ 0.
        """
        if len(values) < 2:
            return None

        mu = statistics.mean(values)

        try:
            sigma = statistics.pstdev(values)
        except statistics.StatisticsError:
            return None

        # Regularized σ floor: avoids z=0 on flat windows where any deviation is anomalous.
        # Use 0.1% of mean as minimum σ; fall back to 1.0 if mean is also ~0.
        sigma_floor = max(abs(mu) * 0.001, 1.0)
        sigma = max(sigma, sigma_floor)

        return (value - mu) / sigma

    # ── Main Enrich Pipeline ───────────────────────────────
    def enrich(self, trade: RawTrade) -> EnrichedTrade:
        """
        Full enrichment pipeline for a single RawTrade.

        Steps:
          1. Retrieve (or create) per-symbol state
          2. Detect data gaps
          3. Evict stale window entries
          4. Compute USD value
          5. Classify
          6. Add to window
          7. Compute Z-score (if warmed up)
          8. Flag anomaly
        """
        state = self._get_state(trade.symbol)
        event_time = trade.event_time

        # ── Step 2: gap detection ────────────────────────
        _ = state._check_and_handle_gap(event_time)

        # ── Step 3: evict expired window entries ─────────
        state._evict_stale(event_time)

        # ── Step 4: USD value ─────────────────────────────
        usd_value = self.compute_usd_value(trade.price, trade.quantity)

        # ── Step 5: classification ────────────────────────
        classification = self.classify(usd_value)

        # ── Step 7 & 8: Z-score + anomaly flag ───────────
        # FIX B5: Compute z-score BEFORE adding current trade to window.
        # The z-score measures how anomalous the current observation is
        # relative to the existing distribution. Including the observation
        # in its own distribution deflates the z-score (especially for
        # extreme outliers) because it pulls the mean toward itself.
        # Correct approach: snapshot window values BEFORE the new entry.
        z_score: Optional[float] = None
        is_anomaly = False

        if state.is_warmed_up():
            prior_values = state.values  # snapshot BEFORE adding current trade
            z_score = self.compute_z_score(usd_value, prior_values)

            if z_score is not None and abs(z_score) > Z_SCORE_THRESHOLD:
                is_anomaly = True
                log.warning(
                    "🚨 ANOMALY [%s] $%.2f | z=%.2f | class=%s | window_n=%d",
                    trade.symbol, usd_value, z_score, classification, state.size,
                )

        # ── Step 6: add to window (AFTER z-score) ────────
        entry = WindowEntry(event_time=event_time, usd_value=usd_value)
        state.add(entry)
        state.last_event_time = event_time

        enriched = EnrichedTrade(
            symbol=trade.symbol,
            price=trade.price,
            quantity=trade.quantity,
            event_time=event_time,
            trade_id=trade.trade_id,
            usd_value=usd_value,
            classification=classification,
            z_score=z_score,
            is_anomaly=is_anomaly,
        )

        log.debug("%s", enriched)
        return enriched

    # ── State Introspection ────────────────────────────────
    def window_stats(self, symbol: str) -> dict:
        """Returns a summary of the current window state for a symbol."""
        if symbol not in self._states:
            return {"symbol": symbol, "status": "no_data"}

        state = self._states[symbol]
        values = state.values

        result: dict = {
            "symbol": symbol,
            "window_size": state.size,
            "warmed_up": state.is_warmed_up(),
            "in_gap_pause": state.in_gap_pause,
            "last_event": state.last_event_time.isoformat() if state.last_event_time else None,
        }

        if values:
            result["mean_usd"] = round(statistics.mean(values), 2)
            result["stdev_usd"] = round(statistics.pstdev(values), 2)
            result["min_usd"] = round(min(values), 2)
            result["max_usd"] = round(max(values), 2)

        return result

    # ── NEW: Cross-Asset Analytics ─────────────────────────
    def cross_asset_stats(self) -> dict:
        """
        BTC vs ETH real-time comparison over the shared 60s window.
        Computes: volume, avg trade size, volatility ratio, Pearson correlation,
        and relative anomaly detection (one asset spikes while other is calm).
        """
        btc = self._states.get("BTCUSDT")
        eth = self._states.get("ETHUSDT")

        result: dict = {"status": "ok", "window_seconds": WINDOW_SECONDS}

        def _symbol_summary(state: Optional[SymbolState], symbol: str) -> dict:
            if not state or not state.values:
                return {"symbol": symbol, "status": "no_data"}
            vals = state.values
            mean = statistics.mean(vals)
            stdev = statistics.pstdev(vals) if len(vals) > 1 else 0.0
            return {
                "symbol": symbol,
                "window_size": state.size,
                "volume_usd": round(sum(vals), 2),
                "avg_trade_usd": round(mean, 2),
                "stdev_usd": round(stdev, 2),
                # Coefficient of variation: relative volatility (σ/μ)
                "volatility_cv": round(stdev / mean, 4) if mean > 0 else None,
                "min_usd": round(min(vals), 2),
                "max_usd": round(max(vals), 2),
                "warmed_up": state.is_warmed_up(),
            }

        result["btc"] = _symbol_summary(btc, "BTCUSDT")
        result["eth"] = _symbol_summary(eth, "ETHUSDT")

        # Correlation and relative anomaly require both symbols with data
        btc_vals = btc.values if btc else []
        eth_vals = eth.values if eth else []

        if len(btc_vals) >= 5 and len(eth_vals) >= 5:
            # Align windows to the same length (use the shorter one)
            n = min(len(btc_vals), len(eth_vals))
            b = btc_vals[-n:]
            e = eth_vals[-n:]

            corr = _pearson_correlation(b, e)
            result["rolling_correlation"] = round(corr, 4) if corr is not None else None

            # Relative anomaly: one asset's recent z-score diverges from the other.
            # Compare the last 5 trades' avg vs the full window avg for each asset.
            if len(b) >= 10 and len(e) >= 10:
                btc_recent_z = _recent_vs_window_z(b, n=5)
                eth_recent_z = _recent_vs_window_z(e, n=5)
                result["btc_recent_z"] = round(btc_recent_z, 3) if btc_recent_z is not None else None
                result["eth_recent_z"] = round(eth_recent_z, 3) if eth_recent_z is not None else None

                # Relative divergence: ETH hot while BTC cold (or vice versa)
                if btc_recent_z is not None and eth_recent_z is not None:
                    divergence = abs(eth_recent_z - btc_recent_z)
                    result["relative_divergence"] = round(divergence, 3)
                    result["divergence_alert"] = divergence > 2.0
        else:
            result["rolling_correlation"] = None
            result["btc_recent_z"] = None
            result["eth_recent_z"] = None

        # Volume dominance ratio
        btc_vol = result["btc"].get("volume_usd", 0) or 0
        eth_vol = result["eth"].get("volume_usd", 0) or 0
        total = btc_vol + eth_vol
        if total > 0:
            result["btc_volume_share"] = round(btc_vol / total, 4)
            result["eth_volume_share"] = round(eth_vol / total, 4)

        return result


# ── Cross-asset helper functions ───────────────────────────

def _pearson_correlation(x: list[float], y: list[float]) -> Optional[float]:
    """Pearson r between two equal-length series. Returns None if σ=0."""
    n = len(x)
    if n < 2:
        return None
    mean_x = statistics.mean(x)
    mean_y = statistics.mean(y)
    num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    den_x = sum((xi - mean_x) ** 2 for xi in x) ** 0.5
    den_y = sum((yi - mean_y) ** 2 for yi in y) ** 0.5
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


def _recent_vs_window_z(values: list[float], n: int = 5) -> Optional[float]:
    """
    Z-score of the recent n-trade average vs the full window distribution.
    Detects whether recent activity has diverged from the window baseline.
    """
    if len(values) < n + 2:
        return None
    window_vals = values[:-n]
    recent_mean = statistics.mean(values[-n:])
    mu = statistics.mean(window_vals)
    sigma = statistics.pstdev(window_vals)
    if sigma == 0:
        return 0.0
    return (recent_mean - mu) / sigma
