'use strict';

const express = require('express');
const WebSocket = require('ws');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const WS_URL = 'wss://ws.binaryws.com/websockets/v3';

const TELEGRAM_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';

const CONFIG = {
  scanIntervalMs: 300000,
  requestTimeoutMs: 20000,
  reconnectDelayMs: 3000,
  maxReconnectDelayMs: 30000,
  candleCount: 500,
  minimum12HCandles: 60,
  minimumCandles: 60,
  keepAliveMs: 30000,
  staleConnectionMs: 90000
};

const MARKETS = [
  'EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/CHF', 'USD/CAD',
  'AUD/USD', 'NZD/USD', 'EUR/GBP', 'EUR/JPY', 'GBP/JPY',
  'AUD/JPY', 'CAD/JPY', 'CHF/JPY', 'NZD/JPY', 'EUR/AUD',
  'EUR/CAD', 'EUR/CHF', 'EUR/NZD', 'GBP/AUD', 'GBP/CAD',
  'GBP/CHF', 'GBP/NZD', 'AUD/CAD', 'AUD/CHF', 'AUD/NZD',
  'CAD/CHF', 'NZD/CAD', 'NZD/CHF', 'XAU/USD', 'XAG/USD',
  'US 100', 'US 500', 'US 30'
];

const ALIASES = {
  'XAU/USD': ['XAUUSD', 'GOLDUSD', 'GOLD'],
  'XAG/USD': ['XAGUSD', 'SILVERUSD', 'SILVER'],
  'US 100': [
    'US100', 'USTECH100', 'NASDAQ100', 'NAS100', 'USNAS100'
  ],
  'US 500': [
    'US500', 'USSP500', 'SP500', 'S&P500', 'US500INDEX'
  ],
  'US 30': [
    'US30', 'WALLSTREET30', 'DOWJONES30', 'DJ30'
  ]
};

let ws = null;
let connected = false;
let connecting = false;
let reconnectTimer = null;
let keepAliveTimer = null;
let reconnectDelay = CONFIG.reconnectDelayMs;
let nextRequestId = 1;
let lastMessageAt = 0;

const pending = new Map();
const livePrices = {};
const signals = [];
const logs = [];

let availableSymbols = [];
let resolvedMarkets = [];
let unavailableMarkets = [...MARKETS];
let lastScanTime = null;
let scanRunning = false;
let totalScans = 0;
let lastError = null;

function log(message) {
  const item = {
    time: new Date().toISOString(),
    message: String(message)
  };

  logs.unshift(item);
  if (logs.length > 100) logs.length = 100;

  console.log(`[${item.time}] ${item.message}`);
}

function normalize(value) {
  return String(value || '')
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '');
}

function getSymbolCode(item) {
  return item.underlying_symbol ||
    item.symbol ||
    item.underlying ||
    '';
}

function getDisplayName(item) {
  return item.underlying_symbol_name ||
    item.display_name ||
    item.symbol_display_name ||
    '';
}

function resolveMarkets() {
  const resolved = [];
  const missing = [];

  for (const requestedName of MARKETS) {
    const target = normalize(requestedName);

    const aliases = [
      target,
      ...(ALIASES[requestedName] || []).map(normalize)
    ];

    let match = availableSymbols.find(item => {
      const name = normalize(getDisplayName(item));
      const code = normalize(getSymbolCode(item));

      return aliases.includes(name) ||
        aliases.includes(code);
    });

    if (!match && /^[A-Z]{6}$/.test(target)) {
      match = availableSymbols.find(item =>
        normalize(getSymbolCode(item)).endsWith(target)
      );
    }

    if (!match && ALIASES[requestedName]) {
      match = availableSymbols.find(item => {
        const name = normalize(getDisplayName(item));
        const code = normalize(getSymbolCode(item));

        return aliases.some(alias =>
          name.includes(alias) ||
          code === alias ||
          code.endsWith(alias)
        );
      });
    }

    if (match && getSymbolCode(match)) {
      resolved.push({
        requested: requestedName,
        symbol: getSymbolCode(match),
        displayName: getDisplayName(match)
      });
    } else {
      missing.push(requestedName);
    }
  }

  resolvedMarkets = resolved;
  unavailableMarkets = missing;

  log(`Resolved ${resolved.length} of ${MARKETS.length} markets.`);

  if (missing.length) {
    log(`Markets not found: ${missing.join(', ')}`);
  }

  return resolvedMarkets;
}

function rejectPending(error) {
  for (const [id, item] of pending.entries()) {
    clearTimeout(item.timer);
    item.reject(error);
    pending.delete(id);
  }
}

function request(payload) {
  return new Promise((resolve, reject) => {
    if (!ws || ws.readyState !== WebSocket.OPEN || !connected) {
      reject(new Error('Deriv WebSocket is not connected'));
      return;
    }

    const reqId = nextRequestId++;

    const timer = setTimeout(() => {
      pending.delete(reqId);
      reject(new Error(
        `Deriv request timed out: ${
          Object.keys(payload).join(', ')
        }`
      ));
    }, CONFIG.requestTimeoutMs);

    pending.set(reqId, { resolve, reject, timer });

    try {
      ws.send(JSON.stringify({
        ...payload,
        req_id: reqId
      }));
    } catch (error) {
      clearTimeout(timer);
      pending.delete(reqId);
      reject(error);
    }
  });
}

function sendPriceSubscriptions() {
  if (!connected || !ws || !resolvedMarkets.length) return;

  // Subscribe without waiting for each market's first price.
  for (const market of resolvedMarkets) {
    if (!connected || !ws || ws.readyState !== WebSocket.OPEN) {
      return;
    }

    const reqId = nextRequestId++;

    try {
      ws.send(JSON.stringify({
        ticks: market.symbol,
        subscribe: 1,
        req_id: reqId
      }));
    } catch (error) {
      log(
        `Subscription send failed for ${market.requested}: ` +
        error.message
      );
    }
  }

  log(`Requested live prices for ${resolvedMarkets.length} markets.`);
}

async function loadSymbols() {
  const response = await request({
    active_symbols: 'brief'
  });

  availableSymbols = response.active_symbols || [];

  if (!availableSymbols.length) {
    throw new Error('Deriv returned no active symbols.');
  }

  resolveMarkets();

  log(`Deriv returned ${availableSymbols.length} active symbols.`);

  sendPriceSubscriptions();
}

function stopKeepAlive() {
  if (keepAliveTimer) {
    clearInterval(keepAliveTimer);
    keepAliveTimer = null;
  }
}

function scheduleReconnect() {
  if (reconnectTimer) return;

  const delay = reconnectDelay;

  log(`Reconnecting in ${Math.round(delay / 1000)} seconds.`);

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, delay);

  reconnectDelay = Math.min(
    reconnectDelay * 2,
    CONFIG.maxReconnectDelayMs
  );
}

function startKeepAlive() {
  stopKeepAlive();

  keepAliveTimer = setInterval(() => {
    if (!connected || !ws || ws.readyState !== WebSocket.OPEN) {
      return;
    }

    // The Deriv API supports application-level ping requests.
    try {
      ws.send(JSON.stringify({ ping: 1 }));
    } catch (error) {
      log(`Ping failed: ${error.message}`);

      try {
        ws.terminate();
      } catch {}
      return;
    }

    // A silent connection should not remain marked as healthy.
    if (
      lastMessageAt &&
      Date.now() - lastMessageAt > CONFIG.staleConnectionMs
    ) {
      log('Connection appears stale. Restarting WebSocket.');

      try {
        ws.terminate();
      } catch {}
    }
  }, CONFIG.keepAliveMs);
}

function connect() {
  if (connecting || connected) return;

  connecting = true;
  log(`Connecting to Deriv using app ID ${APP_ID}...`);

  let socket;

  try {
    socket = new WebSocket(WS_URL);
    ws = socket;
  } catch (error) {
    connecting = false;
    lastError = error.message;
    log(`WebSocket creation failed: ${error.message}`);
    scheduleReconnect();
    return;
  }

  socket.on('open', async () => {
    if (ws !== socket) return;

    connected = true;
    connecting = false;
    reconnectDelay = CONFIG.reconnectDelayMs;
    lastError = null;
    lastMessageAt = Date.now();

    log('Deriv WebSocket connected.');

    startKeepAlive();

    try {
      await loadSymbols();
    } catch (error) {
      lastError = error.message;
      log(`Market setup failed: ${error.message}`);

      // Reconnect if initial setup cannot be completed.
      if (ws === socket) socket.terminate();
    }
  });

  socket.on('message', raw => {
    if (ws !== socket) return;

    lastMessageAt = Date.now();

    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      log('Received invalid WebSocket data.');
      return;
    }

    if (data.error) {
      const message = data.error.message || 'Unknown Deriv error';
      const reqId = data.req_id;

      if (reqId && pending.has(reqId)) {
        const item = pending.get(reqId);
        clearTimeout(item.timer);
        pending.delete(reqId);
        item.reject(new Error(message));
      } else {
        lastError = message;
        log(`Deriv error: ${message}`);
      }

      return;
    }

    if (data.msg_type === 'tick' && data.tick) {
      const tick = data.tick;

      livePrices[tick.symbol] = {
        symbol: tick.symbol,
        price: Number(tick.quote),
        epoch: tick.epoch,
        time: new Date(tick.epoch * 1000).toISOString()
      };
    }

    if (data.msg_type === 'active_symbols' && data.active_symbols) {
      availableSymbols = data.active_symbols;
      resolveMarkets();
    }

    if (data.req_id && pending.has(data.req_id)) {
      const item = pending.get(data.req_id);
      clearTimeout(item.timer);
      pending.delete(data.req_id);
      item.resolve(data);
    }
  });

  socket.on('error', error => {
    if (ws !== socket) return;

    lastError = error.message;
    log(`WebSocket error: ${error.message}`);
  });

  socket.on('close', (code, reason) => {
    if (ws !== socket) return;

    connected = false;
    connecting = false;

    log(
      `WebSocket closed. Code ${code}. ` +
      `${reason ? reason.toString() : 'No reason given'}`
    );

    stopKeepAlive();
    rejectPending(new Error('Deriv WebSocket disconnected'));

    ws = null;
    scheduleReconnect();
  });
}

async function getCandles(symbol, granularity, count = 200) {
  const response = await request({
    ticks_history: symbol,
    adjust_start_time: 1,
    count,
    end: 'latest',
    style: 'candles',
    granularity
  });

  if (!Array.isArray(response.candles)) {
    throw new Error(`No candle data returned for ${symbol}`);
  }

  return response.candles.map(c => ({
    time: Number(c.epoch),
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

function aggregateCandles(candles, targetSeconds) {
  const groups = new Map();

  for (const candle of candles) {
    const bucket =
      Math.floor(candle.time / targetSeconds) * targetSeconds;

    if (!groups.has(bucket)) {
      groups.set(bucket, {
        time: bucket,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close
      });
    } else {
      const group = groups.get(bucket);
      group.high = Math.max(group.high, candle.high);
      group.low = Math.min(group.low, candle.low);
      group.close = candle.close;
    }
  }

  return [...groups.values()].sort((a, b) => a.time - b.time);
}

function midpoint(candles, period, endIndex) {
  const start = endIndex - period + 1;

  if (start < 0) return null;

  let high = -Infinity;
  let low = Infinity;

  for (let i = start; i <= endIndex; i++) {
    high = Math.max(high, candles[i].high);
    low = Math.min(low, candles[i].low);
  }

  return (high + low) / 2;
}

function ichimokuDirection(candles) {
  if (candles.length < 52) return 'NEUTRAL';

  const i = candles.length - 1;
  const close = candles[i].close;

  const tenkan = midpoint(candles, 9, i);
  const kijun = midpoint(candles, 26, i);
  const spanA = (tenkan + kijun) / 2;
  const spanB = midpoint(candles, 52, i);

  if ([tenkan, kijun, spanA, spanB].some(v => v === null)) {
    return 'NEUTRAL';
  }

  const cloudTop = Math.max(spanA, spanB);
  const cloudBottom = Math.min(spanA, spanB);

  if (close > cloudTop && tenkan > kijun) return 'BUY';
  if (close < cloudBottom && tenkan < kijun) return 'SELL';

  return 'NEUTRAL';
}

function findFractals(candles) {
  const highs = [];
  const lows = [];

  // Two candles on the right confirm each Williams Fractal.
  for (let i = 2; i < candles.length - 2; i++) {
    const c = candles[i];

    if (
      c.high > candles[i - 1].high &&
      c.high > candles[i - 2].high &&
      c.high > candles[i + 1].high &&
      c.high > candles[i + 2].high
    ) {
      highs.push({
        index: i,
        price: c.high,
        time: c.time
      });
    }

    if (
      c.low < candles[i - 1].low &&
      c.low < candles[i - 2].low &&
      c.low < candles[i + 1].low &&
      c.low < candles[i + 2].low
    ) {
      lows.push({
        index: i,
        price: c.low,
        time: c.time
      });
    }
  }

  return { highs, lows };
}

function averageRange(candles, count = 14) {
  const recent = candles.slice(-count);

  if (!recent.length) return 0;

  return recent.reduce(
    (sum, c) => sum + (c.high - c.low),
    0
  ) / recent.length;
}

function findBreakoutRetest(candles, level, direction) {
  if (candles.length < 8) return false;

  const lastIndex = candles.length - 1;
  const latest = candles[lastIndex];
  const tolerance = averageRange(candles, 14) * 0.20;

  let breakoutIndex = -1;

  for (let i = Math.max(1, lastIndex - 6); i < lastIndex; i++) {
    const previous = candles[i - 1];
    const current = candles[i];

    if (direction === 'BUY') {
      if (previous.close <= level && current.close > level) {
        breakoutIndex = i;
      }
    } else {
      if (previous.close >= level && current.close < level) {
        breakoutIndex = i;
      }
    }
  }

  if (breakoutIndex < 0 || breakoutIndex >= lastIndex) {
    return false;
  }

  if (direction === 'BUY') {
    return latest.low <= level + tolerance &&
      latest.close > level &&
      latest.close > latest.open;
  }

  return latest.high >= level - tolerance &&
    latest.close < level &&
    latest.close < latest.open;
}

function evaluateSignal(
  market,
  candles12h,
  candles1h,
  candles15m,
  candles5m
) {
  const direction12h = ichimokuDirection(candles12h);
  const direction1h = ichimokuDirection(candles1h);

  if (
    direction12h === 'NEUTRAL' ||
    direction1h === 'NEUTRAL' ||
    direction12h !== direction1h
  ) {
    return null;
  }

  const direction = direction12h;
  const fractals = findFractals(candles15m);

  if (!fractals.highs.length || !fractals.lows.length) {
    return null;
  }

  const lastHigh = fractals.highs[fractals.highs.length - 1];
  const lastLow = fractals.lows[fractals.lows.length - 1];

  const latest5 = candles5m[candles5m.length - 1];
  const previous5 = candles5m[candles5m.length - 2];

  let entry;
  let stopLoss;
  let takeProfit;
  let reason;

  if (direction === 'BUY') {
    if (!findBreakoutRetest(candles15m, lastHigh.price, 'BUY')) {
      return null;
    }

    if (
      latest5.close <= latest5.open ||
      latest5.close <= previous5.close
    ) {
      return null;
    }

    entry = latest5.close;
    stopLoss = Math.min(
      lastLow.price,
      candles15m[candles15m.length - 1].low
    );

    if (stopLoss >= entry) return null;

    takeProfit = entry + 2 * (entry - stopLoss);

    reason =
      '12H/1H Ichimoku bullish; 15M fractal breakout and retest; ' +
      '5M bullish confirmation';
  } else {
    if (!findBreakoutRetest(candles15m, lastLow.price, 'SELL')) {
      return null;
    }

    if (
      latest5.close >= latest5.open ||
      latest5.close >= previous5.close
    ) {
      return null;
    }

    entry = latest5.close;
    stopLoss = Math.max(
      lastHigh.price,
      candles15m[candles15m.length - 1].high
    );

    if (stopLoss <= entry) return null;

    takeProfit = entry - 2 * (stopLoss - entry);

    reason =
      '12H/1H Ichimoku bearish; 15M fractal breakout and retest; ' +
      '5M bearish confirmation';
  }

  // Keep prices numeric; do not assume every market has 5 decimals.
  const digits = 5;

  return {
    id: `${market.symbol}-${Date.now()}`,
    market: market.requested,
    symbol: market.symbol,
    direction,
    entry: Number(entry.toFixed(digits)),
    stopLoss: Number(stopLoss.toFixed(digits)),
    takeProfit: Number(takeProfit.toFixed(digits)),
    riskReward: '1:2',
    strategy: 'Ichimoku + Williams Fractal',
    reason,
    time: new Date().toISOString(),
    status: 'SIGNAL ONLY'
  };
}

async function sendTelegram(message) {
  if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT_ID) return;

  try {
    const response = await fetch(
      `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: TELEGRAM_CHAT_ID,
          text: message
        })
      }
    );

    if (!response.ok) {
      log(`Telegram returned HTTP ${response.status}`);
    }
  } catch (error) {
    log(`Telegram error: ${error.message}`);
  }
}

async function scanMarket(market) {
  const candles4h = await getCandles(
    market.symbol,
    14400,
    CONFIG.candleCount
  );

  const candles12h = aggregateCandles(candles4h, 43200);

  const candles1h = await getCandles(market.symbol, 3600, 200);
  const candles15m = await getCandles(market.symbol, 900, 200);
  const candles5m = await getCandles(market.symbol, 300, 200);

  if (
    candles12h.length < CONFIG.minimum12HCandles ||
    candles1h.length < CONFIG.minimumCandles ||
    candles15m.length < CONFIG.minimumCandles ||
    candles5m.length < CONFIG.minimumCandles
  ) {
    throw new Error('Not enough historical candles');
  }

  return evaluateSignal(
    market,
    candles12h,
    candles1h,
    candles15m,
    candles5m
  );
}

async function runScan() {
  if (scanRunning) {
    return { message: 'A scan is already running.' };
  }

  if (!connected) {
    return { error: 'Deriv is not connected.' };
  }

  if (!resolvedMarkets.length) {
    return {
      error: 'No requested markets were resolved.',
      unavailableMarkets
    };
  }

  scanRunning = true;
  totalScans++;

  const found = [];
  const errors = [];

  try {
    for (const market of resolvedMarkets) {
      if (!connected) break;

      try {
        const signal = await scanMarket(market);

        if (!signal) continue;

        const duplicate = signals.some(existing =>
          existing.symbol === signal.symbol &&
          existing.direction === signal.direction &&
          Date.now() - new Date(existing.time).getTime() <
            15 * 60 * 1000
        );

        if (duplicate) continue;

        found.push(signal);
        signals.unshift(signal);

        if (signals.length > 100) signals.length = 100;

        await sendTelegram(
          `Deriv signal: ${signal.direction}\n` +
          `Market: ${signal.market}\n` +
          `Symbol: ${signal.symbol}\n` +
          `Entry: ${signal.entry}\n` +
          `Stop Loss: ${signal.stopLoss}\n` +
          `Take Profit: ${signal.takeProfit}\n` +
          `Risk/Reward: 1:2\n` +
          `Strategy: ${signal.strategy}\n` +
          `Signal only. No trade was placed.`
        );
      } catch (error) {
        errors.push({
          market: market.requested,
          symbol: market.symbol,
          error: error.message
        });

        log(`Scan failed for ${market.requested}: ${error.message}`);
      }
    }

    lastScanTime = new Date().toISOString();

    return {
      scanned: resolvedMarkets.length,
      unavailableMarkets,
      newSignals: found.length,
      signals: found,
      errors,
      time: lastScanTime
    };
  } finally {
    scanRunning = false;
  }
}

app.get('/', (req, res) => {
  res.type('html').send(`<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Deriv Signal Bot</title>
<style>
body{font-family:Arial,sans-serif;background:#101827;color:#f3f4f6;margin:0;padding:18px}
.card{background:#1e293b;border-radius:10px;padding:14px;margin:12px 0;overflow-wrap:anywhere}
button{background:#16a34a;color:white;border:0;border-radius:6px;padding:12px 18px;font-size:16px}
pre{white-space:pre-wrap;overflow-wrap:anywhere}
small{color:#94a3b8}
</style>
</head>
<body>
<h2>Deriv Signal Bot</h2>
<p>Ichimoku + Williams Fractal</p>
<div class="card" id="status">Checking connection...</div>
<button id="scanButton" onclick="scan()">Scan Markets Now</button>
<div class="card"><h3>Live Prices</h3><pre id="prices">Loading...</pre></div>
<div class="card"><h3>Resolved Markets</h3><pre id="markets">Loading...</pre></div>
<div class="card"><h3>Unavailable Markets</h3><pre id="missing">Loading...</pre></div>
<div class="card"><h3>Signals</h3><pre id="signals">Loading...</pre></div>
<div class="card"><h3>Recent Logs</h3><pre id="logs">Loading...</pre></div>
<small>Signal-only mode. No trades are placed.</small>
<script>
async function refresh(){
 try{
  const r=await fetch('/api/status');
  const d=await r.json();

  document.getElementById('status').textContent =
   'Connection: '+(d.connected?'CONNECTED':'DISCONNECTED')+
   ' | Resolved markets: '+d.resolvedMarkets.length+
   ' | Scans: '+d.totalScans+
   ' | Last scan: '+(d.lastScanTime||'Not yet')+
   (d.lastError?' | Error: '+d.lastError:'');

  document.getElementById('prices').textContent =
   JSON.stringify(d.livePrices,null,2);
  document.getElementById('markets').textContent =
   JSON.stringify(d.resolvedMarkets,null,2);
  document.getElementById('missing').textContent =
   JSON.stringify(d.unavailableMarkets,null,2);
  document.getElementById('signals').textContent =
   JSON.stringify(d.signals,null,2);
  document.getElementById('logs').textContent =
   JSON.stringify(d.logs.slice(0,15),null,2);
 }catch(e){
  document.getElementById('status').textContent =
   'Dashboard error: '+e.message;
 }
}

async function scan(){
 const b=document.getElementById('scanButton');
 b.disabled=true;
 b.textContent='Scanning...';

 try{
  const r=await fetch('/api/scan',{method:'POST'});
  const d=await r.json();
  alert(JSON.stringify(d,null,2));
 }catch(e){
  alert('Scan error: '+e.message);
 }

 b.disabled=false;
 b.textContent='Scan Markets Now';
 refresh();
}

refresh();
setInterval(refresh,5000);
</script>
</body>
</html>`);
});

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    connected,
    uptime: Math.round(process.uptime())
  });
});

app.get('/api/status', (req, res) => {
  res.json({
    connected,
    connecting,
    totalScans,
    lastScanTime,
    lastError,
    scanRunning,
    requestedMarkets: MARKETS,
    resolvedMarkets,
    unavailableMarkets,
    livePrices,
    signals: signals.slice(0, 30),
    logs: logs.slice(0, 30),
    uptime: Math.round(process.uptime())
  });
});

app.get('/api/symbols', (req, res) => {
  res.json({
    requestedMarkets: MARKETS,
    resolvedMarkets,
    unavailableMarkets,
    availableSymbols: availableSymbols.map(item => ({
      symbol: getSymbolCode(item),
      name: getDisplayName(item),
      market: item.market || item.market_display_name || '',
      submarket: item.submarket || item.submarket_display_name || ''
    }))
  });
});

app.post('/api/scan', async (req, res) => {
  try {
    const result = await runScan();
    res.json(result);
  } catch (error) {
    lastError = error.message;
    res.status(500).json({ error: error.message });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  log(`Server listening on port ${PORT}`);
  connect();
});

setInterval(() => {
  if (connected && !scanRunning) {
    runScan().catch(error => {
      lastError = error.message;
      log(`Scheduled scan failed: ${error.message}`);
    });
  }
}, CONFIG.scanIntervalMs);

process.on('SIGTERM', () => {
  log('Shutting down.');

  if (reconnectTimer) clearTimeout(reconnectTimer);
  stopKeepAlive();

  rejectPending(new Error('Server shutting down'));

  if (ws) {
    try {
      ws.close();
    } catch {}
  }

  process.exit(0);
});

process.on('unhandledRejection', error => {
  lastError = error && error.message
    ? error.message
    : String(error);

  log(`Unhandled rejection: ${lastError}`);
});
