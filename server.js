'use strict';

const express = require('express');
const WebSocket = require('ws');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const APP_ID = process.env.DERIV_APP_ID || '1089';
const RR = 2;
const SCAN_INTERVAL = 5 * 60 * 1000;
const REQUEST_TIMEOUT = 20000;
const RECONNECT_DELAY = 15000;

// Keep the original Deriv endpoints and alternate on reconnect.
const WS_URLS = [
  `wss://ws.derivws.com/websockets/v3?app_id=${encodeURIComponent(APP_ID)}`,
  `wss://ws.binaryws.com/websockets/v3?app_id=${encodeURIComponent(APP_ID)}`
];

const MARKETS = [
  'EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/CHF', 'USD/CAD',
  'AUD/USD', 'NZD/USD', 'EUR/GBP', 'EUR/JPY', 'GBP/JPY',
  'AUD/JPY', 'CAD/JPY', 'CHF/JPY', 'NZD/JPY', 'EUR/AUD',
  'EUR/CAD', 'EUR/CHF', 'EUR/NZD', 'GBP/AUD', 'GBP/CAD',
  'GBP/CHF', 'GBP/NZD', 'AUD/CAD', 'AUD/CHF', 'AUD/NZD',
  'CAD/CHF', 'NZD/CAD', 'NZD/CHF', 'XAU/USD', 'XAG/USD',
  'US 100', 'US 500', 'US 30'
];

const state = {
  connected: false,
  connecting: false,
  startedAt: new Date().toISOString(),
  lastScan: null,
  lastError: null,
  connectionAttempts: 0,
  endpoint: null,
  activeSymbolsLoaded: false,
  scanning: false,
  messages: [],
  markets: {},
  signals: [],
  counts: { checked: 0, signals: 0, errors: 0 }
};

for (const market of MARKETS) {
  state.markets[market] = {
    market,
    status: 'Waiting for connection',
    direction12H: '—',
    direction1H: '—',
    fractal15M: '—',
    retest15M: '—',
    confirm5M: '—',
    entry: null,
    stopLoss: null,
    takeProfit: null,
    updatedAt: null,
    error: null
  };
}

let socket = null;
let endpointIndex = 0;
let nextRequestId = 1;
let reconnectTimer = null;
let scanTimer = null;
let scanAgain = false;
let activeSymbols = [];
let lastTelegramAt = {};

const pending = new Map();
const breakoutMemory = new Map();

function log(...args) {
  const line = `[${new Date().toISOString()}] ${args.join(' ')}`;
  console.log(line);
  state.messages.push(line);
  if (state.messages.length > 150) state.messages.shift();
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function normalize(value) {
  return String(value || '')
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '');
}

function marketKey(market) {
  return normalize(market);
}

function setMarket(market, values) {
  if (!state.markets[market]) return;
  Object.assign(state.markets[market], values, {
    updatedAt: new Date().toISOString()
  });
}

function clearPending(reason) {
  for (const [id, item] of pending.entries()) {
    clearTimeout(item.timer);
    item.reject(new Error(reason));
    pending.delete(id);
  }
}

function scheduleReconnect() {
  if (reconnectTimer) return;

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, RECONNECT_DELAY);
}

function connect() {
  if (state.connecting) return;

  if (socket && (
    socket.readyState === WebSocket.OPEN ||
    socket.readyState === WebSocket.CONNECTING
  )) {
    return;
  }

  state.connecting = true;
  state.connectionAttempts++;

  const endpoint = WS_URLS[endpointIndex % WS_URLS.length];
  state.endpoint = endpoint;

  log(
    `Connecting to Deriv (attempt ${state.connectionAttempts}):`,
    endpoint
  );

  let newSocket;

  try {
    newSocket = new WebSocket(endpoint, {
      handshakeTimeout: REQUEST_TIMEOUT,
      perMessageDeflate: false
    });
    socket = newSocket;
  } catch (error) {
    state.connecting = false;
    state.lastError = error.message;
    log('WEBSOCKET CREATE ERROR:', error.message);
    endpointIndex = (endpointIndex + 1) % WS_URLS.length;
    scheduleReconnect();
    return;
  }

  newSocket.on('open', () => {
    if (socket !== newSocket) return;

    state.connected = true;
    state.connecting = false;
    state.lastError = null;

    log('WEBSOCKET CONNECTED successfully.');

    loadActiveSymbols().catch(error => {
      state.lastError = error.message;
      log('ACTIVE SYMBOLS ERROR:', error.message);
    });
  });

  newSocket.on('message', raw => {
    if (socket !== newSocket) return;

    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      log('Could not parse WebSocket message.');
      return;
    }

    if (data.error) {
      log(
        'DERIV API ERROR:',
        data.error.code || '',
        data.error.message || ''
      );
    }

    if (data.req_id && pending.has(data.req_id)) {
      const item = pending.get(data.req_id);
      clearTimeout(item.timer);
      pending.delete(data.req_id);

      if (data.error) {
        item.reject(new Error(
          `${data.error.code || 'API_ERROR'}: ${data.error.message || 'Request failed'}`
        ));
      } else {
        item.resolve(data);
      }
    }
  });

  newSocket.on('error', error => {
    if (socket !== newSocket) return;

    state.lastError = error.message;
    log('WEBSOCKET ERROR:', error.message);
  });

  newSocket.on('close', (code, reason) => {
    if (socket !== newSocket) return;

    state.connected = false;
    state.connecting = false;
    state.activeSymbolsLoaded = false;

    log(
      `WebSocket closed: ${code}`,
      reason ? reason.toString() : ''
    );

    clearPending(`WebSocket closed (${code})`);

    socket = null;
    endpointIndex = (endpointIndex + 1) % WS_URLS.length;

    if (scanTimer) {
      clearTimeout(scanTimer);
      scanTimer = null;
    }

    scheduleReconnect();
  });
}

function request(payload) {
  return new Promise((resolve, reject) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      reject(new Error('WebSocket is not connected.'));
      return;
    }

    const req_id = nextRequestId++;
    const message = { ...payload, req_id };

    const timer = setTimeout(() => {
      pending.delete(req_id);
      reject(new Error(
        `Request timed out: ${
          payload.ticks_history ||
          payload.active_symbols ||
          'Deriv request'
        }`
      ));
    }, REQUEST_TIMEOUT);

    pending.set(req_id, { resolve, reject, timer });

    try {
      socket.send(JSON.stringify(message), error => {
        if (error) {
          clearTimeout(timer);
          pending.delete(req_id);
          reject(error);
        }
      });
    } catch (error) {
      clearTimeout(timer);
      pending.delete(req_id);
      reject(error);
    }
  });
}

async function loadActiveSymbols() {
  log('Requesting Deriv active symbols...');

  const response = await request({
    active_symbols: 'brief',
    product_type: 'basic'
  });

  activeSymbols = Array.isArray(response.active_symbols)
    ? response.active_symbols
    : [];

  state.activeSymbolsLoaded = activeSymbols.length > 0;

  log(`Active symbols loaded: ${activeSymbols.length}`);

  if (!activeSymbols.length) {
    throw new Error('Deriv returned no active symbols.');
  }

  for (const market of MARKETS) {
    const found = resolveSymbol(market);

    if (found) {
      log(`SYMBOL FOUND: ${market} -> ${found.symbol || found.underlying_symbol}`);
      setMarket(market, { error: null });
    } else {
      log(`SYMBOL NOT FOUND: ${market}`);
      setMarket(market, {
        status: 'Symbol unavailable',
        error: 'No matching Deriv active symbol'
      });
    }
  }

  if (!scanTimer) runScan();
}

function resolveSymbol(market) {
  const wanted = normalize(market);

  const aliases = {
    XAUUSD: ['XAUUSD', 'GOLD'],
    XAGUSD: ['XAGUSD', 'SILVER'],
    US100: ['US100', 'NASDAQ100', 'USTECH100'],
    US500: ['US500', 'SP500', 'USA500'],
    US30: ['US30', 'DJ30', 'WALLSTREET30']
  };

  const targets = aliases[wanted] || [wanted];

  for (const item of activeSymbols) {
    const candidates = [
      item.underlying_symbol,
      item.symbol,
      item.display_name,
      item.underlying_symbol_name
    ].map(normalize);

    if (targets.some(target => candidates.includes(target))) {
      return item;
    }
  }

  for (const item of activeSymbols) {
    const combined = normalize([
      item.underlying_symbol,
      item.symbol,
      item.display_name,
      item.underlying_symbol_name
    ].join(' '));

    if (targets.some(target => combined.includes(target))) {
      if (wanted.length >= 6 || combined.includes(wanted)) return item;
    }
  }

  return null;
}

async function getCandles(symbol, granularity, count) {
  const response = await request({
    ticks_history: symbol,
    style: 'candles',
    granularity,
    count,
    end: 'latest'
  });

  if (!Array.isArray(response.candles)) {
    throw new Error(`No candles returned for ${symbol}, ${granularity}`);
  }

  return response.candles
    .map(c => ({
      time: Number(c.epoch),
      open: Number(c.open),
      high: Number(c.high),
      low: Number(c.low),
      close: Number(c.close)
    }))
    .filter(c =>
      Number.isFinite(c.time) &&
      Number.isFinite(c.open) &&
      Number.isFinite(c.high) &&
      Number.isFinite(c.low) &&
      Number.isFinite(c.close)
    )
    .sort((a, b) => a.time - b.time);
}

function completedCandles(candles, seconds) {
  const now = Math.floor(Date.now() / 1000);
  return candles.filter(c => c.time + seconds <= now);
}

function aggregate12H(hourly) {
  const buckets = new Map();

  for (const candle of hourly) {
    const bucketTime = Math.floor(candle.time / 43200) * 43200;

    if (!buckets.has(bucketTime)) {
      buckets.set(bucketTime, {
        time: bucketTime,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        lastTime: candle.time
      });
    } else {
      const b = buckets.get(bucketTime);
      b.high = Math.max(b.high, candle.high);
      b.low = Math.min(b.low, candle.low);

      if (candle.time >= b.lastTime) {
        b.close = candle.close;
        b.lastTime = candle.time;
      }
    }
  }

  const now = Math.floor(Date.now() / 1000);

  return [...buckets.values()]
    .filter(c => c.time + 43200 <= now)
    .sort((a, b) => a.time - b.time)
    .map(({ time, open, high, low, close }) => ({
      time, open, high, low, close
    }));
}

function midpoint(candles, period, index) {
  if (index < period - 1) return null;

  let high = -Infinity;
  let low = Infinity;

  for (let i = index - period + 1; i <= index; i++) {
    high = Math.max(high, candles[i].high);
    low = Math.min(low, candles[i].low);
  }

  return (high + low) / 2;
}

function ichimokuDirection(candles) {
  if (candles.length < 80) return 'INSUFFICIENT DATA';

  const i = candles.length - 1;
  const tenkan = midpoint(candles, 9, i);
  const kijun = midpoint(candles, 26, i);
  const spanA = (tenkan + kijun) / 2;
  const spanB = midpoint(candles, 52, i);
  const close = candles[i].close;

  if ([tenkan, kijun, spanA, spanB].some(v => v === null)) {
    return 'INSUFFICIENT DATA';
  }

  const cloudTop = Math.max(spanA, spanB);
  const cloudBottom = Math.min(spanA, spanB);

  if (close > cloudTop && tenkan > kijun) return 'BULLISH';
  if (close < cloudBottom && tenkan < kijun) return 'BEARISH';

  return 'NEUTRAL';
}

function findFractals(candles, type) {
  const result = [];

  for (let i = 2; i < candles.length - 2; i++) {
    const value = type === 'high'
      ? candles[i].high
      : candles[i].low;

    const left = candles.slice(i - 2, i);
    const right = candles.slice(i + 1, i + 3);

    const confirmed = type === 'high'
      ? left.every(c => value > c.high) &&
        right.every(c => value > c.high)
      : left.every(c => value < c.low) &&
        right.every(c => value < c.low);

    if (confirmed) {
      result.push({
        index: i,
        time: candles[i].time,
        value
      });
    }
  }

  return result;
}

function trendLineValue(a, b, time) {
  if (!a || !b || b.time === a.time) return null;

  return a.value +
    ((b.value - a.value) / (b.time - a.time)) *
    (time - a.time);
}

function fractalBreakAndRetest(market, candles) {
  if (candles.length < 20) {
    return { status: 'INSUFFICIENT DATA', direction: null };
  }

  const highs = findFractals(candles, 'high');
  const lows = findFractals(candles, 'low');
  const last = candles[candles.length - 1];
  const prev = candles[candles.length - 2];

  const recentHighs = highs.slice(-2);
  const recentLows = lows.slice(-2);

  if (recentHighs.length < 2 || recentLows.length < 2) {
    return { status: 'Waiting for confirmed fractals', direction: null };
  }

  const resistanceNow = trendLineValue(
    recentHighs[0], recentHighs[1], last.time
  );
  const resistancePrev = trendLineValue(
    recentHighs[0], recentHighs[1], prev.time
  );
  const supportNow = trendLineValue(
    recentLows[0], recentLows[1], last.time
  );
  const supportPrev = trendLineValue(
    recentLows[0], recentLows[1], prev.time
  );

  const key = marketKey(market);
  const remembered = breakoutMemory.get(key);

  if (
    resistanceNow !== null &&
    resistancePrev !== null &&
    prev.close <= resistancePrev &&
    last.close > resistanceNow
  ) {
    breakoutMemory.set(key, {
      direction: 'BUY',
      time: last.time,
      line: resistanceNow
    });

    return {
      status: 'BREAKOUT BUY — waiting for retest',
      direction: null
    };
  }

  if (
    supportNow !== null &&
    supportPrev !== null &&
    prev.close >= supportPrev &&
    last.close < supportNow
  ) {
    breakoutMemory.set(key, {
      direction: 'SELL',
      time: last.time,
      line: supportNow
    });

    return {
      status: 'BREAKOUT SELL — waiting for retest',
      direction: null
    };
  }

  if (!remembered) {
    return {
      status: 'Waiting for candle close beyond fractal line',
      direction: null
    };
  }

  if (last.time <= remembered.time) {
    return {
      status: `${remembered.direction} breakout recorded; waiting`,
      direction: null
    };
  }

  const tolerance = Math.max(
    Math.abs(remembered.line) * 0.0005,
    0.00001
  );

  const touched =
    Math.abs(last.low - remembered.line) <= tolerance ||
    Math.abs(last.high - remembered.line) <= tolerance ||
    (last.low <= remembered.line && last.high >= remembered.line);

  if (
    remembered.direction === 'BUY' &&
    touched &&
    last.close > remembered.line &&
    last.close > last.open
  ) {
    breakoutMemory.delete(key);
    return {
      status: 'BUY retest/rejection confirmed',
      direction: 'BUY'
    };
  }

  if (
    remembered.direction === 'SELL' &&
    touched &&
    last.close < remembered.line &&
    last.close < last.open
  ) {
    breakoutMemory.delete(key);
    return {
      status: 'SELL retest/rejection confirmed',
      direction: 'SELL'
    };
  }

  return {
    status: `${remembered.direction} breakout recorded; waiting for retest`,
    direction: null
  };
}

function confirm5M(candles, direction) {
  if (candles.length < 3) return false;

  const last = candles[candles.length - 1];
  const prev = candles[candles.length - 2];

  if (direction === 'BUY') {
    return last.close > last.open && last.close > prev.high;
  }

  if (direction === 'SELL') {
    return last.close < last.open && last.close < prev.low;
  }

  return false;
}

function makeTradeLevels(candles, direction) {
  const recent = candles.slice(-12);
  if (recent.length < 3) return null;

  const entry = candles[candles.length - 1].close;
  let stopLoss;

  if (direction === 'BUY') {
    stopLoss = Math.min(...recent.map(c => c.low));
    if (stopLoss >= entry) return null;

    const risk = entry - stopLoss;

    return {
      entry,
      stopLoss,
      takeProfit: entry + risk * RR,
      riskReward: `1:${RR}`
    };
  }

  stopLoss = Math.max(...recent.map(c => c.high));
  if (stopLoss <= entry) return null;

  const risk = stopLoss - entry;

  return {
    entry,
    stopLoss,
    takeProfit: entry - risk * RR,
    riskReward: `1:${RR}`
  };
}

function formatPrice(value) {
  if (!Number.isFinite(value)) return '—';
  return Number(value.toPrecision(8)).toString();
}

async function sendTelegram(textMessage, market) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;

  if (!token || !chatId) return;

  const now = Date.now();

  if (now - (lastTelegramAt[market] || 0) < 10 * 60 * 1000) {
    log('Telegram cooldown active for', market);
    return;
  }

  try {
    const response = await fetch(
      `https://api.telegram.org/bot${token}/sendMessage`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          text: textMessage
        })
      }
    );

    const result = await response.json();

    if (!response.ok || !result.ok) {
      throw new Error(result.description || `HTTP ${response.status}`);
    }

    lastTelegramAt[market] = now;
    log('Telegram alert sent for', market);
  } catch (error) {
    log('TELEGRAM ERROR:', error.message);
  }
}

async function scanMarket(market) {
  const resolved = resolveSymbol(market);

  if (!resolved) {
    setMarket(market, {
      status: 'Symbol unavailable',
      error: 'No matching Deriv active symbol'
    });
    return;
  }

  const symbol = resolved.underlying_symbol || resolved.symbol;

  try {
    const hourlyRaw = await getCandles(symbol, 3600, 1200);
    await sleep(250);

    const m15Raw = await getCandles(symbol, 900, 180);
    await sleep(250);

    const m5Raw = await getCandles(symbol, 300, 180);

    const h1 = completedCandles(hourlyRaw, 3600);
    const h12 = aggregate12H(h1);
    const m15 = completedCandles(m15Raw, 900);
    const m5 = completedCandles(m5Raw, 300);

    const direction12H = ichimokuDirection(h12);
    const direction1H = ichimokuDirection(h1);
    const fractal = fractalBreakAndRetest(market, m15);

    state.counts.checked++;

    setMarket(market, {
      status: 'Scanned',
      direction12H,
      direction1H,
      fractal15M: fractal.status,
      retest15M: fractal.direction || 'Waiting',
      confirm5M: 'Waiting',
      error: null
    });

    const higherDirection =
      direction12H === 'BULLISH' && direction1H === 'BULLISH'
        ? 'BUY'
        : direction12H === 'BEARISH' && direction1H === 'BEARISH'
          ? 'SELL'
          : null;

    if (!higherDirection || fractal.direction !== higherDirection) return;

    const confirmed = confirm5M(m5, higherDirection);

    setMarket(market, {
      confirm5M: confirmed
        ? `${higherDirection} confirmed`
        : 'Waiting for 5M confirmation'
    });

    if (!confirmed) return;

    const levels = makeTradeLevels(m5, higherDirection);
    if (!levels) return;

    const candleTime = m5[m5.length - 1].time;
    const id = `${marketKey(market)}-${higherDirection}-${candleTime}`;

    if (state.signals.some(signal => signal.id === id)) return;

    const signal = {
      id,
      market,
      symbol,
      direction: higherDirection,
      timeframe: '12H + 1H + 15M + 5M',
      entry: levels.entry,
      stopLoss: levels.stopLoss,
      takeProfit: levels.takeProfit,
      riskReward: levels.riskReward,
      candleTime,
      createdAt: new Date().toISOString()
    };

    state.signals.unshift(signal);
    state.signals = state.signals.slice(0, 100);
    state.counts.signals++;

    setMarket(market, {
      status: 'SIGNAL',
      entry: levels.entry,
      stopLoss: levels.stopLoss,
      takeProfit: levels.takeProfit,
      confirm5M: `${higherDirection} confirmed`
    });

    log(
      `SIGNAL ${market} ${higherDirection}`,
      `Entry=${formatPrice(levels.entry)}`,
      `SL=${formatPrice(levels.stopLoss)}`,
      `TP=${formatPrice(levels.takeProfit)}`
    );

    await sendTelegram([
      `DERIV SIGNAL — ${market}`,
      `Direction: ${higherDirection}`,
      `12H Ichimoku: ${direction12H}`,
      `1H Ichimoku: ${direction1H}`,
      '15M Williams Fractal: breakout + retest',
      '5M confirmation: confirmed',
      `Entry: ${formatPrice(levels.entry)}`,
      `Stop Loss: ${formatPrice(levels.stopLoss)}`,
      `Take Profit: ${formatPrice(levels.takeProfit)}`,
      `Risk/Reward: ${levels.riskReward}`,
      'Signal only — no trade was placed.'
    ].join('\n'), market);
  } catch (error) {
    state.counts.errors++;
    setMarket(market, {
      status: 'Scan error',
      error: error.message
    });
    log(`SCAN ERROR ${market}:`, error.message);
  }
}

async function runScan() {
  if (state.scanning) {
    scanAgain = true;
    return;
  }

  if (!state.connected || !state.activeSymbolsLoaded) {
    log('Scan skipped: Deriv connection or symbols not ready.');
    return;
  }

  state.scanning = true;
  log(`========== SCAN START (${MARKETS.length} markets) ==========`);

  try {
    for (const market of MARKETS) {
      if (!state.connected) break;

      await scanMarket(market);
      await sleep(200);
    }
  } finally {
    state.scanning = false;
    state.lastScan = new Date().toISOString();
    log('========== SCAN FINISHED ==========');

    if (scanAgain && state.connected) {
      scanAgain = false;
      setTimeout(runScan, 1000);
    } else if (state.connected) {
      scanTimer = setTimeout(() => {
        scanTimer = null;
        runScan();
      }, SCAN_INTERVAL);
    }
  }
}

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    connected: state.connected,
    connecting: state.connecting,
    activeSymbolsLoaded: state.activeSymbolsLoaded,
    endpoint: state.endpoint,
    attempts: state.connectionAttempts,
    lastError: state.lastError
  });
});

app.get('/api/status', (req, res) => {
  res.json({
    connected: state.connected,
    connecting: state.connecting,
    endpoint: state.endpoint,
    attempts: state.connectionAttempts,
    activeSymbolsLoaded: state.activeSymbolsLoaded,
    scanning: state.scanning,
    startedAt: state.startedAt,
    lastScan: state.lastScan,
    lastError: state.lastError,
    counts: state.counts,
    markets: Object.values(state.markets),
    signals: state.signals.slice(0, 50),
    logs: state.messages.slice(-80)
  });
});

app.get('/api/symbols', (req, res) => {
  res.json({
    configured: MARKETS,
    activeSymbolsCount: activeSymbols.length,
    resolved: MARKETS.map(market => {
      const found = resolveSymbol(market);
      return {
        market,
        symbol: found
          ? (found.underlying_symbol || found.symbol)
          : null
      };
    })
  });
});

app.post('/api/scan', (req, res) => {
  if (!state.connected || !state.activeSymbolsLoaded) {
    return res.status(503).json({
      ok: false,
      error: 'Deriv is not connected or active symbols are not loaded.'
    });
  }

  runScan();
  return res.json({ ok: true, message: 'Scan requested.' });
});

app.get('/', (req, res) => {
  res.type('html').send(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Deriv Ichimoku + Williams Fractal Scanner</title>
<style>
body{font-family:Arial,sans-serif;margin:16px;background:#101820;color:#f3f6f9}
h2{margin-bottom:6px}.muted{color:#aab7c4}
button{padding:10px 14px;margin:8px 0;background:#147d92;color:white;border:0;border-radius:6px}
.status{padding:10px;background:#1d2a35;border-radius:8px;margin:10px 0;overflow-wrap:anywhere}
table{border-collapse:collapse;width:100%;min-width:900px}
th,td{border:1px solid #334450;padding:8px;text-align:left;font-size:12px}
th{background:#1d2a35;position:sticky;top:0}
.wrap{overflow:auto}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#0b1116;padding:10px;border-radius:8px}
</style>
</head>
<body>
<h2>Deriv Ichimoku + Williams Fractal Scanner</h2>
<div class="muted">Signal-only dashboard · No automatic trades</div>
<div class="status" id="status">Loading status...</div>
<button onclick="scanNow()">Scan now</button>
<h3>Market status</h3>
<div class="wrap"><table>
<thead><tr><th>Market</th><th>Status</th><th>12H</th><th>1H</th><th>15M fractal</th><th>Retest</th><th>5M</th><th>Entry</th><th>SL</th><th>TP</th><th>Error</th></tr></thead>
<tbody id="markets"></tbody></table></div>
<h3>Recent signals</h3><div id="signals"></div>
<h3>Latest logs</h3><pre id="logs">Waiting for logs...</pre>
<script>
async function refresh(){
  try{
    const r=await fetch('/api/status');
    const d=await r.json();
    document.getElementById('status').textContent =
      'Connection: '+(d.connected?'CONNECTED':'DISCONNECTED')+
      ' | Connecting: '+d.connecting+
      ' | Active symbols loaded: '+d.activeSymbolsLoaded+
      ' | Attempts: '+d.attempts+
      ' | Scanning: '+d.scanning+
      ' | Last scan: '+(d.lastScan||'not yet')+
      ' | Last error: '+(d.lastError||'none');

    document.getElementById('markets').innerHTML=d.markets.map(m=>
      '<tr><td>'+esc(m.market)+'</td><td>'+esc(m.status)+'</td>'+
      '<td>'+esc(m.direction12H)+'</td><td>'+esc(m.direction1H)+'</td>'+
      '<td>'+esc(m.fractal15M)+'</td><td>'+esc(m.retest15M)+'</td>'+
      '<td>'+esc(m.confirm5M)+'</td><td>'+esc(price(m.entry))+'</td>'+
      '<td>'+esc(price(m.stopLoss))+'</td><td>'+esc(price(m.takeProfit))+'</td>'+
      '<td>'+esc(m.error||'')+'</td></tr>'
    ).join('');

    document.getElementById('signals').innerHTML=d.signals.length
      ? d.signals.map(s=>
        '<div class="status"><b>'+esc(s.market)+' '+esc(s.direction)+'</b><br>'+
        'Entry: '+esc(price(s.entry))+' | SL: '+esc(price(s.stopLoss))+
        ' | TP: '+esc(price(s.takeProfit))+' | '+esc(s.createdAt)+'</div>'
      ).join('')
      : '<div class="muted">No confirmed signals yet.</div>';

    document.getElementById('logs').textContent=(d.logs||[]).join('\\n');
  }catch(e){
    document.getElementById('status').textContent='Dashboard error: '+e.message;
  }
}

function esc(v){
  return String(v==null?'':v).replace(/[&<>"']/g,c=>({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[c]));
}

function price(v){
  if(v==null)return '—';
  return Number(v).toPrecision(8).replace(/0+$/,'').replace(/\\.$/,'');
}

async function scanNow(){
  try{
    const r=await fetch('/api/scan',{method:'POST'});
    const d=await r.json();
    if(!r.ok)alert(d.error||'Scan request failed');
    refresh();
  }catch(e){alert(e.message)}
}

refresh();
setInterval(refresh,5000);
</script>
</body>
</html>`);
});

app.listen(PORT, () => {
  log(`HTTP server listening on port ${PORT}`);
  log(`Markets configured: ${MARKETS.length}`);
  log('Signal-only mode enabled; no trades will be placed.');
  connect();
});

process.on('uncaughtException', error => {
  log('UNCAUGHT EXCEPTION:', error.stack || error.message);
});

process.on('unhandledRejection', error => {
  log('UNHANDLED REJECTION:', error && (error.stack || error.message || error));
});
