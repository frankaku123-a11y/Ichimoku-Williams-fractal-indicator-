
'use strict';

const express = require('express');
const WebSocket = require('ws');
const path = require('path');

const PORT = Number(process.env.PORT || 3000);
const DERIV_APP_ID = process.env.DERIV_APP_ID || '1089';
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
const SCAN_INTERVAL_MS = Math.max(60000, Number(process.env.SCAN_INTERVAL_MS || 60000));
const RECONNECT_DELAY_MS = 5000;
const RISK_REWARD = Math.max(1, Number(process.env.RISK_REWARD || 2));
const FRACTAL_N = 2;

const WS_URL = `wss://ws.derivws.com/websockets/v3?app_id=${encodeURIComponent(DERIV_APP_ID)}`;

const MARKETS = [
  { key: 'EUR/USD', type: 'forex', names: ['EUR/USD', 'EURUSD'] },
  { key: 'GBP/USD', type: 'forex', names: ['GBP/USD', 'GBPUSD'] },
  { key: 'USD/JPY', type: 'forex', names: ['USD/JPY', 'USDJPY'] },
  { key: 'GBP/JPY', type: 'forex', names: ['GBP/JPY', 'GBPJPY'] },
  { key: 'EUR/JPY', type: 'forex', names: ['EUR/JPY', 'EURJPY'] },
  { key: 'AUD/USD', type: 'forex', names: ['AUD/USD', 'AUDUSD'] },
  { key: 'USD/CAD', type: 'forex', names: ['USD/CAD', 'USDCAD'] },
  { key: 'USD/CHF', type: 'forex', names: ['USD/CHF', 'USDCHF'] },
  { key: 'AUD/JPY', type: 'forex', names: ['AUD/JPY', 'AUDJPY'] },
  { key: 'NZD/USD', type: 'forex', names: ['NZD/USD', 'NZDUSD'] },
  { key: 'EUR/GBP', type: 'forex', names: ['EUR/GBP', 'EURGBP'] },
  { key: 'EUR/AUD', type: 'forex', names: ['EUR/AUD', 'EURAUD'] },
  { key: 'GBP/AUD', type: 'forex', names: ['GBP/AUD', 'GBPAUD'] },
  { key: 'GBP/CAD', type: 'forex', names: ['GBP/CAD', 'GBPCAD'] },
  { key: 'AUD/CAD', type: 'forex', names: ['AUD/CAD', 'AUDCAD'] },
  { key: 'NZD/JPY', type: 'forex', names: ['NZD/JPY', 'NZDJPY'] },
  { key: 'CAD/JPY', type: 'forex', names: ['CAD/JPY', 'CADJPY'] },
  { key: 'CHF/JPY', type: 'forex', names: ['CHF/JPY', 'CHFJPY'] },
  { key: 'EUR/CHF', type: 'forex', names: ['EUR/CHF', 'EURCHF'] },
  { key: 'XAU/USD', type: 'metal', names: ['XAU/USD', 'XAUUSD', 'GOLD'] },
  { key: 'US 100', type: 'index', names: ['US Tech 100', 'US 100', 'USTEC', 'US100'] },
  { key: 'US 500', type: 'index', names: ['US 500', 'S&P 500', 'US500', 'SPX500'] },
  { key: 'US 30', type: 'index', names: ['Wall Street 30', 'US 30', 'US30', 'WS30'] }
];

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const state = {
  startedAt: new Date().toISOString(),
  connected: false,
  lastScan: null,
  lastError: null,
  marketCount: MARKETS.length,
  symbolsResolved: 0,
  results: MARKETS.map(m => ({
    market: m.key,
    type: m.type,
    symbol: null,
    status: 'Waiting for Deriv connection',
    signal: 'WAIT'
  }))
};

let socket;
let requestId = 1;
let reconnectTimer;
let scanTimer;
let scanRunning = false;
let activeSymbols = [];
let keepAliveTimer = null;

const pending = new Map();
const candleCache = new Map();
const lastAlertByMarket = new Map();

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function norm(s) {
  return String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function roundPrice(value, digits = 5) {
  return Number(Number(value).toFixed(digits));
}

function digitsFor(market, price) {
  if (market === 'XAU/USD') return 2;
  if (market.endsWith('/JPY')) return 3;
  if (market.startsWith('US ')) return 2;
  return price < 10 ? 5 : 3;
}

function safeSend(obj) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(obj));
  }
}

function derivRequest(payload, timeoutMs = 25000) {
  return new Promise((resolve, reject) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return reject(new Error('Deriv WebSocket is not connected'));
    }

    const req_id = requestId++;
    const timer = setTimeout(() => {
      pending.delete(req_id);
      reject(new Error('Deriv request timed out'));
    }, timeoutMs);

    pending.set(req_id, { resolve, reject, timer });
    safeSend({ ...payload, req_id });
  });
}

function connectDeriv() {
  clearTimeout(reconnectTimer);

  if (keepAliveTimer) clearInterval(keepAliveTimer);

  try {
    if (socket) socket.terminate();
  } catch (_) {}

  console.log(
    'Connecting to Deriv WebSocket:',
    WS_URL.replace(/app_id=[^&]+/, 'app_id=REDACTED')
  );

  socket = new WebSocket(WS_URL, {
    handshakeTimeout: 20000
  });

  socket.on('open', async () => {
    state.connected = true;
    state.lastError = null;
    console.log('Deriv WebSocket connected.');

    keepAliveTimer = setInterval(() => {
      if (socket && socket.readyState === WebSocket.OPEN) {
        safeSend({ ping: 1 });
      }
    }, 45000);

    try {
      const response = await derivRequest({
        active_symbols: 'brief',
        product_type: 'basic'
      });

      activeSymbols = response.active_symbols || [];
      resolveSymbols();

      if (!scanTimer) {
        scanTimer = setInterval(runScan, SCAN_INTERVAL_MS);
      }

      runScan();
    } catch (err) {
      state.lastError = err.message;
      state.connected = false;
      console.error('Initial Deriv request failed:', err.message);
      try {
        socket.close();
      } catch (_) {}
    }
  });

  socket.on('message', raw => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch (_) {
      return;
    }

    const item = data.req_id ? pending.get(data.req_id) : null;

    if (item) {
      clearTimeout(item.timer);
      pending.delete(data.req_id);

      if (data.error) {
        item.reject(new Error(data.error.message || 'Deriv API error'));
      } else {
        item.resolve(data);
      }
    }

    if (data.error && !item) {
      state.lastError = data.error.message || 'Deriv API error';
    }
  });

  socket.on('close', (code, reason) => {
    state.connected = false;

    if (keepAliveTimer) clearInterval(keepAliveTimer);
    keepAliveTimer = null;

    state.lastError =
      `WebSocket closed (${code}): ${reason ? reason.toString() : 'no reason supplied'}`;

    console.error(state.lastError);

    for (const [id, item] of pending) {
      clearTimeout(item.timer);
      item.reject(new Error('Deriv connection closed'));
      pending.delete(id);
    }

    reconnectTimer = setTimeout(connectDeriv, RECONNECT_DELAY_MS);
  });

  socket.on('error', err => {
    state.lastError = `Deriv WebSocket error: ${err.message}`;
    console.error(state.lastError);
  });
}

function resolveSymbols(activeSymbols) {
  const wanted = MARKETS.map(m => m.name);
  const found = {};

  for (const item of activeSymbols || []) {
    const symbol = String(item.underlying_symbol || item.symbol || "");
    const name = String(item.display_name || item.name || "");

    for (const market of MARKETS) {
      const aliases = [
        market.name,
        market.name.replace("/", ""),
        market.derivName || ""
      ].filter(Boolean);

      if (
        aliases.some(alias =>
          symbol.toUpperCase() === alias.toUpperCase() ||
          name.toUpperCase() === alias.toUpperCase()
        )
      ) {
        found[market.name] = symbol || name;
      }
    }
  }

  return found;
}

function toCandles(response) {
  const raw = response?.candles || response?.history?.candles;

  if (!Array.isArray(raw)) return [];

  return raw.map(c => ({
    time: Number(c.epoch ?? c.open_time),
    open: Number(c.open),
    high: Number(c.high),
    low: Number(c.low),
    close: Number(c.close)
  })).filter(c =>
    Number.isFinite(c.time) &&
    Number.isFinite(c.open) &&
    Number.isFinite(c.high) &&
    Number.isFinite(c.low) &&
    Number.isFinite(c.close)
  ).sort((a, b) => a.time - b.time);
}

function fetchCandles(symbol, granularity, count = 300) {
  return new Promise((resolve, reject) => {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return reject(new Error("Deriv WebSocket is not connected"));
    }

    const reqId = ++state.requestId;
    const timer = setTimeout(() => {
      state.pending.delete(reqId);
      reject(new Error("Candle request timed out"));
    }, 20000);

    state.pending.set(reqId, {
      resolve: response => {
        clearTimeout(timer);

        if (response.error) {
          reject(new Error(response.error.message || "Deriv API error"));
        } else {
          resolve(toCandles(response));
        }
      },
      reject: error => {
        clearTimeout(timer);
        reject(error);
      }
    });

    ws.send(JSON.stringify({
      ticks_history: symbol,
      adjust_start_time: 1,
      count,
      end: "latest",
      granularity,
      style: "candles",
      req_id: reqId
    }));
  });
}

function aggregateCandles(candles, seconds) {
  const groups = new Map();

  for (const candle of candles) {
    const bucket = Math.floor(candle.time / seconds) * seconds;

    if (!groups.has(bucket)) {
      groups.set(bucket, {
        time: bucket,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        count: 0
      });
    }

    const group = groups.get(bucket);
    group.high = Math.max(group.high, candle.high);
    group.low = Math.min(group.low, candle.low);
    group.close = candle.close;
    group.count++;
  }

  return [...groups.values()]
    .sort((a, b) => a.time - b.time)
    .map(({ count, ...candle }) => candle);
}

function donchian(candles, length, index) {
  if (index < length - 1) return null;

  const slice = candles.slice(index - length + 1, index + 1);
  return {
    high: Math.max(...slice.map(c => c.high)),
    low: Math.min(...slice.map(c => c.low))
  };
}

function ichimoku(candles, index, tenkanLength, kijunLength, spanLength) {
  const tenkanRange = donchian(candles, tenkanLength, index);
  const kijunRange = donchian(candles, kijunLength, index);
  const spanRange = donchian(candles, spanLength, index);

  if (!tenkanRange || !kijunRange || !spanRange) return null;

  const tenkan = (tenkanRange.high + tenkanRange.low) / 2;
  const kijun = (kijunRange.high + kijunRange.low) / 2;
  const spanA = (tenkan + kijun) / 2;
  const spanB = (spanRange.high + spanRange.low) / 2;

  return {
    tenkan,
    kijun,
    spanA,
    spanB,
    cloudTop: Math.max(spanA, spanB),
    cloudBottom: Math.min(spanA, spanB)
  };
}

function findFractals(candles, period = 2) {
  const highs = [];
  const lows = [];

  for (let i = period; i < candles.length - period; i++) {
    let isHigh = true;
    let isLow = true;

    for (let j = 1; j <= period; j++) {
      if (
        candles[i].high <= candles[i - j].high ||
        candles[i].high <= candles[i + j].high
      ) isHigh = false;

      if (
        candles[i].low >= candles[i - j].low ||
        candles[i].low >= candles[i + j].low
      ) isLow = false;
    }

    if (isHigh) highs.push({ index: i, ...candles[i] });
    if (isLow) lows.push({ index: i, ...candles[i] });
  }

  return { highs, lows };
}

function lineAt(first, second, index) {
  if (!first || !second || first.index === second.index) return null;

  const slope = (second.price - first.price) /
    (second.index - first.index);

  return first.price + slope * (index - first.index);
}

function assessFractalBreak(candles) {
  if (candles.length < 10) {
    return { direction: "WAIT", reason: "Not enough 15M candles" };
  }

  const { highs, lows } = findFractals(candles, 2);
  const lastIndex = candles.length - 1;
  const previous = candles[lastIndex - 1];
  const last = candles[lastIndex];

  const recentHighs = highs.slice(-2);
  const recentLows = lows.slice(-2);

  if (recentHighs.length >= 2) {
    const first = {
      index: recentHighs[0].index,
      price: recentHighs[0].high
    };
    const second = {
      index: recentHighs[1].index,
      price: recentHighs[1].high
    };

    const priorLine = lineAt(first, second, lastIndex - 1);
    const currentLine = lineAt(first, second, lastIndex);

    if (
      priorLine !== null &&
      currentLine !== null &&
      previous.close >= priorLine &&
      last.close < currentLine
    ) {
      return {
        direction: "SELL",
        reason: "15M candle closed below the fractal line",
        breakLevel: currentLine
      };
    }
  }

  if (recentLows.length >= 2) {
    const first = {
      index: recentLows[0].index,
      price: recentLows[0].low
    };
    const second = {
      index: recentLows[1].index,
      price: recentLows[1].low
    };

    const priorLine = lineAt(first, second, lastIndex - 1);
    const currentLine = lineAt(first, second, lastIndex);

    if (
      priorLine !== null &&
      currentLine !== null &&
      previous.close <= priorLine &&
      last.close > currentLine
    ) {
      return {
        direction: "BUY",
        reason: "15M candle closed above the fractal line",
        breakLevel: currentLine
      };
    }
  }

  return { direction: "WAIT", reason: "No confirmed fractal-line break" };
}

function getFiveMinuteConfirmation(candles, direction) {
  if (candles.length < 3) return false;

  const previous = candles[candles.length - 2];
  const last = candles[candles.length - 1];

  if (direction === "BUY") {
    return last.close > last.open && last.close > previous.high;
  }

  if (direction === "SELL") {
    return last.close < last.open && last.close < previous.low;
  }

  return false;
}

function get1HStructure(candles) {
  if (candles.length < 60) {
    return { direction: "WAIT", reason: "Not enough 1H candles" };
  }

  const index = candles.length - 2;
  const cloud = ichimoku(candles, index, 8, 24, 48);

  if (!cloud) return { direction: "WAIT", reason: "Ichimoku unavailable" };

  const close = candles[index].close;

  if (close > cloud.cloudTop && cloud.tenkan > cloud.kijun) {
    return { direction: "BUY", reason: "1H above cloud; Tenkan above Kijun" };
  }

  if (close < cloud.cloudBottom && cloud.tenkan < cloud.kijun) {
    return { direction: "SELL", reason: "1H below cloud; Tenkan below Kijun" };
  }

  return { direction: "WAIT", reason: "1H Ichimoku is not aligned" };
}

async function sendTelegram(message) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.log("Telegram not configured; skipping alert.");
    return;
  }

  const url =
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text: message,
        parse_mode: "HTML"
      })
    });

    const result = await response.json();

    if (!result.ok) {
      console.error("Telegram alert failed:", result.description);
    }
  } catch (error) {
    console.error("Telegram error:", error.message);
  }
}

async function scanMarket(market) {
  const symbol = state.symbols[market.name];

  if (!symbol) {
    return {
      pair: market.name,
      status: "WAIT",
      reason: "Deriv symbol not found"
    };
  }

  try {
    const [h1, m15, m5] = await Promise.all([
      fetchCandles(symbol, 3600, 500),
      fetchCandles(symbol, 900, 220),
      fetchCandles(symbol, 300, 220)
    ]);

    if (h1.length < 150 || m15.length < 10 || m5.length < 3) {
      return {
        pair: market.name,
        status: "WAIT",
        reason: "Not enough candle history returned"
      };
    }

    const h12 = aggregateCandles(h1, 43200);

    if (h12.length < 125) {
      return {
        pair: market.name,
        status: "WAIT",
        reason: "Not enough completed 12H history for Ichimoku"
      };
    }

    // Use the most recently completed candles, not the live candle.
    const h12Index = h12.length - 2;
    const cloud12 = ichimoku(h12, h12Index, 20, 60, 120);

    if (!cloud12) {
      return {
        pair: market.name,
        status: "WAIT",
        reason: "12H Ichimoku unavailable"
      };
    }

    const close12 = h12[h12Index].close;
    let direction12H = "WAIT";

    if (close12 > cloud12.cloudTop && cloud12.tenkan > cloud12.kijun) {
      direction12H = "BUY";
    } else if (
      close12 < cloud12.cloudBottom &&
      cloud12.tenkan < cloud12.kijun
    ) {
      direction12H = "SELL";
    }

    const structure1H = get1HStructure(h1);
    const trend15M = assessFractalBreak(m15);
    const direction = trend15M.direction;

    let status = "WAIT";
    let reason = "Waiting for all timeframes to align";
    let entry = null;
    let sl = null;
    let tp = null;

    if (
      direction !== "WAIT" &&
      direction12H === direction &&
      structure1H.direction === direction
    ) {
      const last15 = m15[m15.length - 1];
      const last5 = m5[m5.length - 2];

      const retestConfirmed =
        direction === "BUY"
          ? last15.low <= trend15M.breakLevel && last15.close > trend15M.breakLevel
          : last15.high >= trend15M.breakLevel && last15.close < trend15M.breakLevel;

      const confirmation = getFiveMinuteConfirmation(m5, direction);

      if (retestConfirmed && confirmation) {
        entry = last5.close;

        const recentCandles = m5.slice(-15, -1);
        const buffer = Math.max(
          Math.abs(entry) * 0.0002,
          (Math.max(...recentCandles.map(c => c.high)) -
            Math.min(...recentCandles.map(c => c.low))) * 0.1
        );

        if (direction === "BUY") {
          sl = Math.min(...recentCandles.map(c => c.low)) - buffer;
          tp = entry + (entry - sl) * RISK_REWARD;
        } else {
          sl = Math.max(...recentCandles.map(c => c.high)) + buffer;
          tp = entry - (sl - entry) * RISK_REWARD;
        }

        if (
          Number.isFinite(entry) &&
          Number.isFinite(sl) &&
          Number.isFinite(tp) &&
          entry > 0 &&
          sl > 0 &&
          tp > 0
        ) {
          status = direction;
          reason = "12H direction + 1H structure + 15M fractal break/retest + 5M confirmation";
        }
      } else {
        reason = !retestConfirmed
          ? "Waiting for 15M retest"
          : "Waiting for 5M confirmation";
      }
    } else if (direction12H === "WAIT") {
      reason = "12H Ichimoku direction is not confirmed";
    } else if (direction12H !== direction) {
      reason = "15M break does not match 12H direction";
    } else if (structure1H.direction !== direction) {
      reason = "1H structure does not confirm the setup";
    }

    const result = {
      pair: market.name,
      symbol,
      status,
      direction12H,
      structure1H: structure1H.direction,
      trend15M: direction,
      entry,
      sl,
      tp,
      reason,
      scannedAt: new Date().toISOString()
    };

    state.results[market.name] = result;

    if (status === "BUY" || status === "SELL") {
      const message = [
        `<b>DERIV ${status} SIGNAL</b>`,
        `Pair: ${market.name}`,
        `Entry: ${entry}`,
        `Stop Loss: ${sl}`,
        `Take Profit: ${tp}`,
        `Risk/Reward: 1:${RISK_REWARD}`,
        `12H: ${direction12H}`,
        `1H: ${structure1H.direction}`,
        `15M: ${direction}`,
        `Reason: ${reason}`,
        "<i>Signal only — no automatic trade executed.</i>"
      ].join("\n");

      await sendTelegram(message);
    }

    return result;
  } catch (error) {
    console.error(`Scan error for ${market.name}:`, error.message);

    return {
      pair: market.name,
      status: "ERROR",
      reason: error.message
    };
  }
}

async function runScan() {
  if (state.scanning) return;

  if (!state.connected) {
    console.log("Scan skipped: Deriv is not connected.");
    return;
  }

  state.scanning = true;

  try {
    for (const market of MARKETS) {
      if (!state.connected) break;

      const result = await scanMarket(market);
      state.results[market.name] = result;

      // Brief pause between markets to avoid flooding requests.
      await new Promise(resolve => setTimeout(resolve, 300));
    }

    state.lastScan = new Date().toISOString();
  } catch (error) {
    console.error("Scan error:", error.message);
  } finally {
    state.scanning = false;
  }
}

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    connected: state.connected,
    scanning: state.scanning,
    lastScan: state.lastScan
  });
});

app.get("/api/status", (req, res) => {
  res.json({
    connected: state.connected,
    scanning: state.scanning,
    lastScan: state.lastScan,
    error: state.error || null,
    markets: MARKETS.length,
    results: state.results
  });
});

app.get("/api/symbols", (req, res) => {
  res.json({
    connected: state.connected,
    symbols: state.symbols
  });
});

app.post("/api/scan", async (req, res) => {
  if (!state.connected) {
    return res.status(503).json({
      ok: false,
      error: "Deriv is not connected yet."
    });
  }

  if (state.scanning) {
    return res.json({
      ok: true,
      message: "A scan is already running."
    });
  }

  runScan().catch(error => {
    console.error("Manual scan failed:", error.message);
  });

  res.json({
    ok: true,
    message: "Scan started."
  });
});

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
  connectDeriv();
});
