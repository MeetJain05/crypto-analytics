"use strict";
const { useState, useEffect, useRef, useCallback, useMemo } = React;
// Defensive boot: avoid hard crash if CDN globals fail to load.
const hasRecharts = typeof window !== "undefined" && !!window.Recharts;
const { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } =
  hasRecharts ? window.Recharts : {};

// ── CONFIG ────────────────────────────────────────────────
const API_BASE = (window.VIBESTREAM_API || "http://localhost:8000").replace(
  /\/$/,
  "",
);
const WS_BASE = API_BASE.replace(/^http/, "ws");

// ── FORMATTERS ────────────────────────────────────────────
const fmt = {
  price: (v) =>
    v == null
      ? "—"
      : "$" +
        Number(v).toLocaleString("en-US", {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }),
  usdK: (v) => {
    if (v == null || isNaN(v)) return "—";
    const n = Number(v);
    if (n >= 1e9) return "$" + (n / 1e9).toFixed(2) + "B";
    if (n >= 1e6) return "$" + (n / 1e6).toFixed(2) + "M";
    if (n >= 1e3) return "$" + (n / 1e3).toFixed(1) + "K";
    return "$" + n.toFixed(0);
  },
  z: (v) => (v == null ? "—" : (v >= 0 ? "+" : "") + Number(v).toFixed(2)),
  pct: (v) => (v == null ? "—" : (Number(v) * 100).toFixed(1) + "%"),
  num: (v) => (v == null ? "—" : Number(v).toLocaleString("en-US")),
  cv: (v) => (v == null ? "—" : Number(v).toFixed(3)),
  time: (v) => {
    if (!v) return "—";
    const d = new Date(v);
    return d.toLocaleTimeString("en-US", {
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  },
  timeMs: (v) => {
    if (!v) return "—";
    const d = new Date(v);
    return (
      d.toLocaleTimeString("en-US", { hour12: false }) +
      "." +
      String(d.getMilliseconds()).padStart(3, "0").slice(0, 2)
    );
  },
};
const symCls = (s) => (s && s.startsWith("BTC") ? "btc" : "eth");
const clsBadge = (c) =>
  c === "Whale"
    ? "cls-badge cls-W"
    : c === "Pro"
      ? "cls-badge cls-P"
      : "cls-badge cls-R";
const R = React.createElement;

// ── SAFE FETCH ────────────────────────────────────────────
async function apiFetch(path, fallback = null) {
  try {
    const r = await fetch(API_BASE + path, {
      signal: AbortSignal.timeout(4500),
    });
    if (!r.ok) return fallback;
    return r.json();
  } catch {
    return fallback;
  }
}

// ── usePolling ────────────────────────────────────────────
function usePolling(fn, ms) {
  const [data, setData] = useState(null);
  const [status, setStatus] = useState("connecting");

  useEffect(() => {
    let alive = true;
    const tick = async () => {
      const v = await fn();
      if (!alive) return;
      if (v !== null && v !== undefined) {
        setData(v);
        setStatus("live");
      } else {
        setStatus("error");
      }
    };
    tick();
    const id = setInterval(tick, ms);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [ms, fn]);

  return [data, status];
}

// ── useWebSocket ──────────────────────────────────────────
function useWebSocket(path, onMessage) {
  const [wsStatus, setWsStatus] = useState("connecting");
  const cbRef = useRef(onMessage);
  useEffect(() => {
    cbRef.current = onMessage;
  });

  useEffect(() => {
    let ws,
      retryTimer,
      retryMs = 1000,
      alive = true;
    const connect = () => {
      if (!alive) return;
      try {
        ws = new WebSocket(WS_BASE + path);
        ws.onopen = () => {
          if (alive) setWsStatus("live");
          retryMs = 1000;
        };
        ws.onmessage = (e) => {
          try {
            cbRef.current(JSON.parse(e.data));
          } catch {}
        };
        ws.onclose = () => {
          if (alive) {
            setWsStatus("connecting");
            retryTimer = setTimeout(connect, retryMs);
            retryMs = Math.min(retryMs * 1.6, 16000);
          }
        };
        ws.onerror = () => {
          if (alive) setWsStatus("error");
        };
      } catch {
        if (alive) {
          setWsStatus("error");
          retryTimer = setTimeout(connect, 3000);
        }
      }
    };
    connect();
    return () => {
      alive = false;
      clearTimeout(retryTimer);
      if (ws) ws.close();
    };
  }, [path]);

  return wsStatus;
}

// ── dedup helper ─────────────────────────────────────────
function dedup(arr, key = "trade_id", max = 80) {
  const seen = new Set(),
    out = [];
  for (const item of arr) {
    if (!seen.has(item[key])) {
      seen.add(item[key]);
      out.push(item);
    }
    if (out.length >= max) break;
  }
  return out;
}

// ══════════════════════════════════════════════════════════
// HEADER
// ══════════════════════════════════════════════════════════
function Header({ stats, wsStatus }) {
  const btc = stats?.symbols?.find((s) => s.symbol === "BTCUSDT");
  const eth = stats?.symbols?.find((s) => s.symbol === "ETHUSDT");
  const status =
    wsStatus === "live"
      ? "live"
      : wsStatus === "error"
        ? "error"
        : "connecting";

  return R(
    "header",
    { className: "header" },
    R(
      "div",
      { className: "logo" },
      "VIBE",
      R("em", null, "STREAM"),
      " · ALPHA",
    ),

    R(
      "div",
      { className: "ticker-strip" },
      btc &&
        R(
          "div",
          { className: "ticker-item" },
          R("span", { className: "ticker-sym btc" }, "BTC"),
          R("span", { className: "ticker-price" }, fmt.price(btc.latest_price)),
          R("span", { className: "ticker-meta" }, fmt.usdK(btc.vol_1m) + "/m"),
        ),
      btc && eth && R("span", { className: "ticker-sep" }, "·"),
      eth &&
        R(
          "div",
          { className: "ticker-item" },
          R("span", { className: "ticker-sym eth" }, "ETH"),
          R("span", { className: "ticker-price" }, fmt.price(eth.latest_price)),
          R("span", { className: "ticker-meta" }, fmt.usdK(eth.vol_1m) + "/m"),
        ),
      (btc || eth) && R("span", { className: "ticker-sep" }, "·"),
      R(
        "div",
        { className: "ticker-item" },
        R("span", { className: "ticker-meta" }, "ANOMALIES 5M"),
        R(
          "span",
          {
            style: {
              color: "var(--red)",
              fontWeight: 600,
              fontSize: 12,
              marginLeft: 5,
            },
          },
          ((btc?.anomalies_5m || 0) + (eth?.anomalies_5m || 0)).toString(),
        ),
      ),
    ),

    R(
      "div",
      { className: `status-pill ${status}` },
      R("div", { className: "pulse" }),
      status.toUpperCase(),
    ),
  );
}

// ══════════════════════════════════════════════════════════
// LIVE TRADE STREAM — col1, rows 1-2
// ══════════════════════════════════════════════════════════
function LiveTradeStream() {
  const [trades, setTrades] = useState([]);
  const [filter, setFilter] = useState("ALL");

  const mergeTrades = useCallback((incoming) => {
    setTrades((prev) => dedup([...incoming, ...prev], "trade_id", 100));
  }, []);

  // WebSocket push
  const wsStatus = useWebSocket(
    "/ws/trades",
    useCallback(
      (msg) => {
        if (msg.type === "trades" && Array.isArray(msg.data))
          mergeTrades(msg.data);
      },
      [mergeTrades],
    ),
  );

  // HTTP fallback when WS not live
  const [httpData] = usePolling(
    useCallback(() => apiFetch("/live_trades?limit=40", []), []),
    wsStatus === "live" ? 60000 : 1200,
  );
  useEffect(() => {
    if (wsStatus !== "live" && Array.isArray(httpData) && httpData.length)
      mergeTrades(httpData);
  }, [httpData, wsStatus, mergeTrades]);

  const filtered = useMemo(() => {
    let t = trades;
    if (filter === "BTC") t = t.filter((x) => x.symbol?.startsWith("BTC"));
    if (filter === "ETH") t = t.filter((x) => x.symbol?.startsWith("ETH"));
    if (filter === "WHALE") t = t.filter((x) => x.classification === "Whale");
    return t;
  }, [trades, filter]);

  const anomCount = filtered.filter((t) => t.is_anomaly).length;

  return R(
    "div",
    {
      className: "panel",
      // Explicit placement
      style: { gridColumn: "1", gridRow: "1 / 3" },
    },
    // header
    R(
      "div",
      { className: "panel-hd" },
      R("span", { className: "panel-title" }, "LIVE TRADES"),
      R(
        "div",
        { className: "panel-controls" },
        ["ALL", "BTC", "ETH", "WHALE"].map((f) =>
          R(
            "button",
            {
              key: f,
              className: `tab-btn ${filter === f ? "on" : ""}`,
              onClick: () => setFilter(f),
            },
            f,
          ),
        ),
      ),
      anomCount > 0
        ? R("span", { className: "badge badge-r" }, anomCount + " 🚨")
        : R("span", { className: "badge badge-g" }, filtered.length),
    ),

    // body
    filtered.length === 0
      ? R(
          "div",
          { className: "empty" },
          R(
            "span",
            null,
            wsStatus === "error" ? "Cannot reach API" : "Awaiting stream",
          ),
          R("span", { className: "dots", style: { fontSize: 10 } }),
        )
      : R(
          "div",
          { className: "panel-body" },
          filtered.map((t) =>
            R(
              "div",
              {
                key: t.trade_id,
                className: `t-row ${t.is_anomaly ? "hot" : ""}`,
              },
              R("span", { className: "t-time" }, fmt.timeMs(t.time)),
              R(
                "div",
                null,
                R(
                  "div",
                  { style: { display: "flex", alignItems: "center" } },
                  R(
                    "span",
                    { className: `t-sym ${symCls(t.symbol)}` },
                    t.symbol?.replace("USDT", ""),
                  ),
                  R("span", { className: "t-price" }, fmt.price(t.price)),
                ),
                t.z_score != null &&
                  R(
                    "div",
                    { className: "z-bar" },
                    R("div", {
                      className: "z-fill",
                      style: {
                        width:
                          Math.min((Math.abs(t.z_score) / 5) * 100, 100) + "%",
                        background:
                          Math.abs(t.z_score) > 3
                            ? "var(--red)"
                            : "var(--border-hot)",
                      },
                    }),
                  ),
              ),
              R("span", { className: "t-usd" }, fmt.usdK(t.usd_value)),
              R(
                "span",
                { className: clsBadge(t.classification) },
                t.classification === "Whale"
                  ? "W"
                  : t.classification === "Pro"
                    ? "PRO"
                    : "RET",
              ),
            ),
          ),
        ),
  );
}

// ══════════════════════════════════════════════════════════
// OHLCV CHART — col2, row1
// ══════════════════════════════════════════════════════════
function OHLCVChart() {
  const [sym, setSym] = useState("BTCUSDT");
  const [range, setRange] = useState(60);

  const [candles] = usePolling(
    useCallback(
      () => apiFetch(`/ohlcv/${sym}?minutes=${range}`, []),
      [sym, range],
    ),
    5000,
  );

  const data = useMemo(() => {
    if (!Array.isArray(candles)) return [];
    return candles.map((c) => ({
      t: fmt.time(c.bucket),
      close: +c.close || 0,
      vol: +c.usd_volume || 0,
      anom: c.anomaly_count || 0,
    }));
  }, [candles]);

  const color = sym.startsWith("BTC") ? "var(--btc)" : "var(--eth)";

  const svgPoints = useMemo(() => {
    if (!data.length) return "";
    const prices = data.map((d) => d.close || 0);
    const max = Math.max(...prices);
    const min = Math.min(...prices);
    const span = max - min || 1;
    const w = 100;
    const h = 54;
    return data
      .map((d, i) => {
        const x = data.length === 1 ? 0 : (i / (data.length - 1)) * w;
        const y = h - ((d.close - min) / span) * h;
        return `${x.toFixed(2)},${y.toFixed(2)}`;
      })
      .join(" ");
  }, [data]);

  if (!hasRecharts) {
    return R(
      "div",
      {
        className: "panel",
        style: { gridColumn: "2", gridRow: "1" },
      },
      R(
        "div",
        { className: "panel-hd" },
        R("span", { className: "panel-title" }, "OHLCV · 1MIN"),
        R(
          "div",
          { className: "panel-controls" },
          ["BTCUSDT", "ETHUSDT"].map((s) =>
            R(
              "button",
              {
                key: s,
                className: `tab-btn ${sym === s ? "on" : ""}`,
                onClick: () => setSym(s),
              },
              s.replace("USDT", ""),
            ),
          ),
          R("div", {
            style: {
              width: 1,
              background: "var(--border)",
              margin: "0 3px",
              alignSelf: "stretch",
            },
          }),
          [30, 60, 120].map((m) =>
            R(
              "button",
              {
                key: m,
                className: `tab-btn ${range === m ? "on" : ""}`,
                onClick: () => setRange(m),
              },
              m + "m",
            ),
          ),
        ),
      ),
      data.length === 0
        ? R(
            "div",
            { className: "empty" },
            R("span", null, "Building candles"),
            R("span", { className: "dots", style: { fontSize: 10 } }),
          )
        : R(
            "div",
            {
              className: "chart-area",
              style: { padding: "10px 12px" },
            },
            R(
              "svg",
              {
                viewBox: "0 0 100 54",
                preserveAspectRatio: "none",
                style: { width: "100%", height: "100%" },
              },
              R("polyline", {
                points: svgPoints,
                fill: "none",
                stroke: color,
                strokeWidth: "1.8",
                strokeLinecap: "round",
                strokeLinejoin: "round",
              }),
            ),
            R(
              "div",
              {
                style: {
                  display: "flex",
                  justifyContent: "space-between",
                  marginTop: 6,
                  fontSize: 10,
                  color: "var(--text-secondary)",
                },
              },
              R("span", null, `Fallback chart (${range}m)`),
              R(
                "span",
                null,
                `Last: ${fmt.price(data[data.length - 1]?.close)}`,
              ),
            ),
          ),
    );
  }

  const Tip = ({ active, payload }) => {
    if (!active || !payload?.length) return null;
    const d = payload[0]?.payload;
    return R(
      "div",
      {
        style: {
          background: "var(--bg-elevated)",
          border: "1px solid var(--border-hot)",
          padding: "7px 11px",
          borderRadius: 2,
          fontFamily: "var(--font-mono)",
          fontSize: 10,
        },
      },
      R(
        "div",
        { style: { color: "var(--text-secondary)", marginBottom: 3 } },
        d.t,
      ),
      R("div", { style: { color } }, "CLOSE " + fmt.price(d.close)),
      R(
        "div",
        { style: { color: "var(--text-secondary)" } },
        "VOL " + fmt.usdK(d.vol),
      ),
      d.anom > 0 &&
        R(
          "div",
          { style: { color: "var(--red)", marginTop: 2 } },
          "🚨 " + d.anom + " anomalies",
        ),
    );
  };

  return R(
    "div",
    {
      className: "panel",
      // Explicit placement
      style: { gridColumn: "2", gridRow: "1" },
    },
    R(
      "div",
      { className: "panel-hd" },
      R("span", { className: "panel-title" }, "OHLCV · 1MIN"),
      R(
        "div",
        { className: "panel-controls" },
        ["BTCUSDT", "ETHUSDT"].map((s) =>
          R(
            "button",
            {
              key: s,
              className: `tab-btn ${sym === s ? "on" : ""}`,
              onClick: () => setSym(s),
            },
            s.replace("USDT", ""),
          ),
        ),
        R("div", {
          style: {
            width: 1,
            background: "var(--border)",
            margin: "0 3px",
            alignSelf: "stretch",
          },
        }),
        [30, 60, 120].map((m) =>
          R(
            "button",
            {
              key: m,
              className: `tab-btn ${range === m ? "on" : ""}`,
              onClick: () => setRange(m),
            },
            m + "m",
          ),
        ),
      ),
    ),

    // Chart-area uses flex:1 + min-height:0, not height:100%
    data.length === 0
      ? R(
          "div",
          { className: "empty" },
          R("span", null, "Building candles"),
          R("span", { className: "dots", style: { fontSize: 10 } }),
        )
      : R(
          "div",
          { className: "chart-area" },
          // price chart — 68%
          R(
            "div",
            { style: { flex: "0 0 68%", minHeight: 0 } },
            R(
              ResponsiveContainer,
              { width: "100%", height: "100%" },
              R(
                AreaChart,
                { data, margin: { top: 6, right: 14, bottom: 0, left: 0 } },
                R(
                  "defs",
                  null,
                  R(
                    "linearGradient",
                    { id: "pg", x1: "0", y1: "0", x2: "0", y2: "1" },
                    R("stop", {
                      offset: "5%",
                      stopColor: color,
                      stopOpacity: 0.22,
                    }),
                    R("stop", {
                      offset: "95%",
                      stopColor: color,
                      stopOpacity: 0,
                    }),
                  ),
                ),
                R(XAxis, {
                  dataKey: "t",
                  tick: {
                    fill: "var(--text-dim)",
                    fontSize: 8,
                    fontFamily: "var(--font-mono)",
                  },
                  tickLine: false,
                  axisLine: false,
                  interval: "preserveStartEnd",
                }),
                R(YAxis, {
                  domain: ["auto", "auto"],
                  tick: {
                    fill: "var(--text-dim)",
                    fontSize: 8,
                    fontFamily: "var(--font-mono)",
                  },
                  tickLine: false,
                  axisLine: false,
                  width: 58,
                  tickFormatter: (v) => "$" + v.toLocaleString(),
                }),
                R(Tooltip, { content: R(Tip) }),
                R(Area, {
                  isAnimationActive: false,
                  type: "monotone",
                  dataKey: "close",
                  stroke: color,
                  strokeWidth: 1.5,
                  fill: "url(#pg)",
                  dot: false,
                  activeDot: { r: 3, fill: color },
                }),
              ),
            ),
          ),
          // volume chart — 32%
          R(
            "div",
            {
              style: {
                flex: "0 0 32%",
                minHeight: 0,
                borderTop: "1px solid var(--border)",
              },
            },
            R(
              ResponsiveContainer,
              { width: "100%", height: "100%" },
              R(
                AreaChart,
                { data, margin: { top: 4, right: 14, bottom: 0, left: 0 } },
                R(
                  "defs",
                  null,
                  R(
                    "linearGradient",
                    { id: "vg", x1: "0", y1: "0", x2: "0", y2: "1" },
                    R("stop", {
                      offset: "5%",
                      stopColor: "var(--text-dim)",
                      stopOpacity: 0.35,
                    }),
                    R("stop", {
                      offset: "95%",
                      stopColor: "var(--text-dim)",
                      stopOpacity: 0,
                    }),
                  ),
                ),
                R(XAxis, {
                  dataKey: "t",
                  tick: false,
                  axisLine: false,
                  tickLine: false,
                }),
                R(YAxis, {
                  tick: {
                    fill: "var(--text-dim)",
                    fontSize: 7,
                    fontFamily: "var(--font-mono)",
                  },
                  tickLine: false,
                  axisLine: false,
                  width: 58,
                  tickFormatter: fmt.usdK,
                }),
                R(Area, {
                  isAnimationActive: false,
                  type: "monotone",
                  dataKey: "vol",
                  stroke: "var(--text-dim)",
                  strokeWidth: 1,
                  fill: "url(#vg)",
                  dot: false,
                }),
              ),
            ),
          ),
        ),
  );
}

// ══════════════════════════════════════════════════════════
// ANOMALY FEED — right side of col2 row1? No: it's a separate panel
// Actually layout: anomaly feed sits in col3 row1, stats in col2 row2
// Wait — let me re-read the layout requirement:
//   col1 rows1-2: LiveTrades
//   col2 row1   : OHLCVChart
//   col2 row2   : StatsPanel
//   col3 rows1-2: ComparisonPanel (has anomaly feed inside)
// The PRD asks for an Anomaly Feed panel. We'll put it above comparison in col3 row1
// and Comparison in col3 row2. Actually let's put both in col3 spanning rows, with
// anomaly feed at top and comparison below — as a single scrollable panel.
// The cleanest approach: col3 spans both rows and has anomaly feed + comparison combined.
// ══════════════════════════════════════════════════════════

// ══════════════════════════════════════════════════════════
// STATS PANEL — col2, row2
// ══════════════════════════════════════════════════════════
function StatsPanel({ stats }) {
  const btc = stats?.symbols?.find((s) => s.symbol === "BTCUSDT");
  const eth = stats?.symbols?.find((s) => s.symbol === "ETHUSDT");
  const totalAnom = (btc?.anomalies_5m || 0) + (eth?.anomalies_5m || 0);
  const totalTr = (btc?.trades_1m || 0) + (eth?.trades_1m || 0);
  const totalVol = (+btc?.vol_1m || 0) + (+eth?.vol_1m || 0);
  const totalVol5 = (+btc?.vol_5m || 0) + (+eth?.vol_5m || 0);
  const wins = stats?.windows;

  const cells = [
    {
      lbl: "TRADES/MIN",
      val: fmt.num(totalTr),
      cls: "g",
      sub: `BTC ${btc?.trades_1m || 0} · ETH ${eth?.trades_1m || 0}`,
    },
    {
      lbl: "VOL/MIN",
      val: fmt.usdK(totalVol),
      cls: "",
      sub: `5m: ${fmt.usdK(totalVol5)}`,
    },
    {
      lbl: "ANOMALIES",
      val: String(totalAnom),
      cls: totalAnom > 0 ? "r" : "g",
      sub: `last 5 min`,
    },
    {
      lbl: "BTC",
      val: fmt.price(btc?.latest_price),
      cls: "b",
      sub: btc?.price_high_1m
        ? `H ${fmt.price(btc.price_high_1m)} · L ${fmt.price(btc.price_low_1m)}`
        : "—",
    },
    {
      lbl: "ETH",
      val: fmt.price(eth?.latest_price),
      cls: "e",
      sub: eth?.price_high_1m
        ? `H ${fmt.price(eth.price_high_1m)} · L ${fmt.price(eth.price_low_1m)}`
        : "—",
    },
    {
      lbl: "Z-WINDOW",
      val:
        wins?.BTCUSDT?.warmed_up && wins?.ETHUSDT?.warmed_up ? "LIVE" : "WARM",
      cls: wins?.BTCUSDT?.warmed_up && wins?.ETHUSDT?.warmed_up ? "g" : "a",
      sub: `n=${wins?.BTCUSDT?.window_size || "?"} · ${wins?.ETHUSDT?.window_size || "?"}`,
    },
  ];

  return R(
    "div",
    {
      className: "stats-grid",
      // Explicit grid placement
      style: { gridColumn: "2", gridRow: "2" },
    },
    cells.map((c, i) =>
      R(
        "div",
        { key: i, className: "stat-cell" },
        R("div", { className: "stat-lbl" }, c.lbl),
        R("div", { className: `stat-val ${c.cls}` }, c.val),
        R("div", { className: "stat-sub" }, c.sub),
      ),
    ),
  );
}

// ══════════════════════════════════════════════════════════
// RIGHT PANEL — col3, rows 1-2
// Contains: Anomaly Feed (top) + Cross-Asset Comparison (bottom)
// Combined into one scrollable panel so both panels are visible
// ══════════════════════════════════════════════════════════
function RightPanel() {
  // ── Anomaly state ──────────────────────────────────────
  const [anomalies, setAnomalies] = useState([]);

  const mergeAnomalies = useCallback((incoming) => {
    setAnomalies((prev) => dedup([...incoming, ...prev], "trade_id", 50));
  }, []);

  const wsAnom = useWebSocket(
    "/ws/anomalies",
    useCallback(
      (msg) => {
        if (msg.type === "anomalies" && Array.isArray(msg.data))
          mergeAnomalies(msg.data);
      },
      [mergeAnomalies],
    ),
  );

  const [httpAnom] = usePolling(
    useCallback(() => apiFetch("/anomalies?limit=20&minutes=30", []), []),
    wsAnom === "live" ? 60000 : 3000,
  );
  useEffect(() => {
    if (wsAnom !== "live" && Array.isArray(httpAnom) && httpAnom.length)
      mergeAnomalies(httpAnom);
  }, [httpAnom, wsAnom, mergeAnomalies]);

  // ── Comparison state ───────────────────────────────────
  const [cmp] = usePolling(
    useCallback(() => apiFetch("/comparison", null), []),
    1500,
  );

  const btc = cmp?.btc;
  const eth = cmp?.eth;
  const corr = cmp?.rolling_correlation;
  const corrPct = corr != null ? ((corr + 1) / 2) * 100 : 50;
  const volMax = Math.max(btc?.volume_usd || 0, eth?.volume_usd || 0, 1);
  const avgMax = Math.max(btc?.avg_trade_usd || 0, eth?.avg_trade_usd || 0, 1);
  const cvMax = Math.max(
    btc?.volatility_cv || 0,
    eth?.volatility_cv || 0,
    0.001,
  );

  return R(
    "div",
    {
      className: "panel",
      // Explicit placement
      style: { gridColumn: "3", gridRow: "1 / 3" },
    },

    // ── Anomaly Feed header ──────────────────────────────
    R(
      "div",
      { className: "panel-hd" },
      R("span", { className: "panel-title" }, "🚨 ANOMALY FEED"),
      R("span", { className: "badge badge-r" }, anomalies.length + " DETECTED"),
    ),

    // Anomaly items (fixed height, scrollable)
    R(
      "div",
      {
        style: {
          height: "38%",
          minHeight: 0,
          overflowY: "auto",
          borderBottom: "2px solid var(--border-hot)",
        },
      },
      anomalies.length === 0
        ? R(
            "div",
            { className: "empty" },
            R("span", { style: { fontSize: 10 } }, "Watching |z| > 3.0"),
            R("span", { className: "dots", style: { fontSize: 10 } }),
          )
        : anomalies.map((a) =>
            R(
              "div",
              { key: a.trade_id, className: "a-item" },
              R(
                "div",
                { className: "a-top" },
                R(
                  "span",
                  { className: `a-sym ${symCls(a.symbol)}` },
                  a.symbol?.replace("USDT", ""),
                ),
                R("span", { className: "a-z" }, "z " + fmt.z(a.z_score)),
              ),
              R(
                "div",
                { className: "a-bot" },
                R("span", { className: "a-val" }, fmt.usdK(a.usd_value)),
                R(
                  "span",
                  { className: clsBadge(a.classification) },
                  a.classification,
                ),
                R("span", { className: "a-t" }, fmt.time(a.time)),
              ),
            ),
          ),
    ),

    // ── Comparison section header ────────────────────────
    R(
      "div",
      { className: "panel-hd", style: { borderTop: "none" } },
      R("span", { className: "panel-title" }, "BTC vs ETH · 60s"),
      R(
        "span",
        { className: cmp ? "badge badge-g" : "badge badge-a" },
        cmp ? "LIVE" : "WARMING",
      ),
    ),

    // Comparison body (remaining height, scrollable)
    R(
      "div",
      { style: { flex: 1, minHeight: 0, overflowY: "auto" } },

      // Divergence alert
      cmp?.divergence_alert &&
        R(
          "div",
          { className: "div-alert" },
          R("span", null, "⚡"),
          R(
            "div",
            null,
            R("div", { className: "div-lbl" }, "DIVERGENCE ALERT"),
            R(
              "div",
              { className: "div-sub" },
              `BTC z${fmt.z(cmp.btc_recent_z)} · ETH z${fmt.z(cmp.eth_recent_z)}`,
            ),
          ),
        ),

      // Volume
      R(
        "div",
        { className: "cmp-section" },
        R("div", { className: "cmp-label" }, "USD Volume (window)"),
        R(
          "div",
          { className: "cmp-row" },
          R(
            "span",
            { className: "cmp-lbl", style: { color: "var(--btc)" } },
            "BTC",
          ),
          R(
            "div",
            { className: "bar-track" },
            R("div", {
              className: "bar-fill fill-btc",
              style: { width: `${((btc?.volume_usd || 0) / volMax) * 100}%` },
            }),
          ),
          R("span", { className: "cmp-val" }, fmt.usdK(btc?.volume_usd)),
        ),
        R(
          "div",
          { className: "cmp-row" },
          R(
            "span",
            { className: "cmp-lbl", style: { color: "var(--eth)" } },
            "ETH",
          ),
          R(
            "div",
            { className: "bar-track" },
            R("div", {
              className: "bar-fill fill-eth",
              style: { width: `${((eth?.volume_usd || 0) / volMax) * 100}%` },
            }),
          ),
          R("span", { className: "cmp-val" }, fmt.usdK(eth?.volume_usd)),
        ),
        cmp?.btc_volume_share > 0 &&
          R(
            "div",
            { style: { display: "flex", gap: 8, fontSize: 9, marginTop: 4 } },
            R(
              "span",
              { style: { color: "var(--btc)" } },
              fmt.pct(cmp.btc_volume_share) + " BTC",
            ),
            R("span", { style: { color: "var(--text-dim)" } }, "/"),
            R(
              "span",
              { style: { color: "var(--eth)" } },
              fmt.pct(cmp.eth_volume_share) + " ETH",
            ),
          ),
      ),

      // Avg trade
      R(
        "div",
        { className: "cmp-section" },
        R("div", { className: "cmp-label" }, "Avg Trade Size"),
        R(
          "div",
          { className: "cmp-row" },
          R(
            "span",
            { className: "cmp-lbl", style: { color: "var(--btc)" } },
            "BTC",
          ),
          R(
            "div",
            { className: "bar-track" },
            R("div", {
              className: "bar-fill fill-btc",
              style: {
                width: `${((btc?.avg_trade_usd || 0) / avgMax) * 100}%`,
              },
            }),
          ),
          R("span", { className: "cmp-val" }, fmt.usdK(btc?.avg_trade_usd)),
        ),
        R(
          "div",
          { className: "cmp-row" },
          R(
            "span",
            { className: "cmp-lbl", style: { color: "var(--eth)" } },
            "ETH",
          ),
          R(
            "div",
            { className: "bar-track" },
            R("div", {
              className: "bar-fill fill-eth",
              style: {
                width: `${((eth?.avg_trade_usd || 0) / avgMax) * 100}%`,
              },
            }),
          ),
          R("span", { className: "cmp-val" }, fmt.usdK(eth?.avg_trade_usd)),
        ),
      ),

      // Volatility
      R(
        "div",
        { className: "cmp-section" },
        R("div", { className: "cmp-label" }, "Volatility (σ/μ)"),
        R(
          "div",
          { className: "cmp-row" },
          R(
            "span",
            { className: "cmp-lbl", style: { color: "var(--btc)" } },
            "BTC",
          ),
          R(
            "div",
            { className: "bar-track" },
            R("div", {
              className: "bar-fill fill-vol",
              style: { width: `${((btc?.volatility_cv || 0) / cvMax) * 100}%` },
            }),
          ),
          R("span", { className: "cmp-val" }, fmt.cv(btc?.volatility_cv)),
        ),
        R(
          "div",
          { className: "cmp-row" },
          R(
            "span",
            { className: "cmp-lbl", style: { color: "var(--eth)" } },
            "ETH",
          ),
          R(
            "div",
            { className: "bar-track" },
            R("div", {
              className: "bar-fill fill-vol",
              style: { width: `${((eth?.volatility_cv || 0) / cvMax) * 100}%` },
            }),
          ),
          R("span", { className: "cmp-val" }, fmt.cv(eth?.volatility_cv)),
        ),
      ),

      // Correlation gauge
      R(
        "div",
        { className: "cmp-section" },
        R("div", { className: "cmp-label" }, "Pearson Correlation (60s)"),
        R(
          "div",
          { className: "corr-track" },
          R("div", { className: "corr-pin", style: { left: `${corrPct}%` } }),
        ),
        R(
          "div",
          { className: "corr-ends" },
          R("span", null, "-1.0"),
          R(
            "span",
            {
              style: {
                color:
                  corr == null
                    ? "var(--text-dim)"
                    : corr > 0.7
                      ? "var(--green)"
                      : corr < -0.3
                        ? "var(--red)"
                        : "var(--amber)",
              },
            },
            corr != null ? `r = ${corr.toFixed(3)}` : "warming…",
          ),
          R("span", null, "+1.0"),
        ),
      ),

      // Recent z comparison
      (cmp?.btc_recent_z != null || cmp?.eth_recent_z != null) &&
        R(
          "div",
          { className: "cmp-section" },
          R("div", { className: "cmp-label" }, "Recent Activity vs Baseline"),
          R(
            "div",
            { className: "z-cards" },
            R(
              "div",
              { className: "z-card" },
              R(
                "div",
                { className: "z-card-lbl", style: { color: "var(--btc)" } },
                "BTC RECENT Z",
              ),
              R(
                "div",
                {
                  className: "z-card-val",
                  style: {
                    color:
                      Math.abs(cmp?.btc_recent_z || 0) > 2
                        ? "var(--red)"
                        : Math.abs(cmp?.btc_recent_z || 0) > 1
                          ? "var(--amber)"
                          : "var(--text-secondary)",
                  },
                },
                fmt.z(cmp?.btc_recent_z),
              ),
            ),
            R(
              "div",
              { className: "z-card" },
              R(
                "div",
                { className: "z-card-lbl", style: { color: "var(--eth)" } },
                "ETH RECENT Z",
              ),
              R(
                "div",
                {
                  className: "z-card-val",
                  style: {
                    color:
                      Math.abs(cmp?.eth_recent_z || 0) > 2
                        ? "var(--red)"
                        : Math.abs(cmp?.eth_recent_z || 0) > 1
                          ? "var(--amber)"
                          : "var(--text-secondary)",
                  },
                },
                fmt.z(cmp?.eth_recent_z),
              ),
            ),
          ),
        ),

      // Window state
      R(
        "div",
        { className: "cmp-section" },
        R("div", { className: "cmp-label" }, "Analytics Window State"),
        R(
          "div",
          { className: "win-cards" },
          R(
            "div",
            {
              className: "win-card",
              style: {
                border: `1px solid ${btc?.warmed_up ? "rgba(0,255,136,0.15)" : "var(--border)"}`,
              },
            },
            R(
              "div",
              { className: "win-card-sym", style: { color: "var(--btc)" } },
              "BTC",
            ),
            R("div", { className: "win-card-n" }, btc?.window_size ?? "—"),
            R(
              "div",
              {
                className: "win-card-st",
                style: {
                  color: btc?.warmed_up ? "var(--green)" : "var(--amber)",
                },
              },
              btc?.warmed_up ? "WARMED" : "WARMING",
            ),
          ),
          R(
            "div",
            {
              className: "win-card",
              style: {
                border: `1px solid ${eth?.warmed_up ? "rgba(0,255,136,0.15)" : "var(--border)"}`,
              },
            },
            R(
              "div",
              { className: "win-card-sym", style: { color: "var(--eth)" } },
              "ETH",
            ),
            R("div", { className: "win-card-n" }, eth?.window_size ?? "—"),
            R(
              "div",
              {
                className: "win-card-st",
                style: {
                  color: eth?.warmed_up ? "var(--green)" : "var(--amber)",
                },
              },
              eth?.warmed_up ? "WARMED" : "WARMING",
            ),
          ),
        ),
      ),
    ),
  );
}

// ══════════════════════════════════════════════════════════
// APP ROOT
// ══════════════════════════════════════════════════════════
function App() {
  const [stats, statsStatus] = usePolling(
    useCallback(() => apiFetch("/stats", null), []),
    2000,
  );

  const header = useMemo(() => R(Header, { stats, wsStatus: statsStatus }), [stats, statsStatus]);
  const live = useMemo(() => R(LiveTradeStream), []);
  const ohlcv = useMemo(() => R(OHLCVChart), []);
  const statsPanel = useMemo(() => R(StatsPanel, { stats }), [stats]);
  const right = useMemo(() => R(RightPanel), []);

  return R(
    "div",
    { className: "terminal" },
    header,
    R(
      "div",
      { className: "main-grid" },
      live, // gridColumn:1 gridRow:1/3  (set inside component)
      ohlcv, // gridColumn:2 gridRow:1    (set inside component)
      statsPanel, // gridColumn:2 gridRow:2    (set inside component)
      right, // gridColumn:3 gridRow:1/3  (set inside component)
    ),
  );
}

try {
  if (!ReactDOM || typeof ReactDOM.createRoot !== "function") {
    throw new Error("ReactDOM.createRoot is not available");
  }
  ReactDOM.createRoot(document.getElementById("root")).render(R(App));
} catch (err) {
  const root = document.getElementById("root");
  if (root) {
    root.innerHTML =
      '<div style="padding:16px;color:#e8e8f0;font-family:monospace">Frontend failed to initialize. Open browser console for details.</div>';
  }
  console.error("Frontend bootstrap failed:", err);
}
