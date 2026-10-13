'use strict';

const express = require('express');
const WebSocket = require('ws');

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT) || 3000;

// New Deriv public endpoint (no app ID needed)
const WS_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';

const TELEGRAM_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';

// Second data source for US indices (Twelve Data)
const TWELVE_KEY = process.env.TWELVE_DATA_API_KEY || '';

const INDEX_MARKETS = [
  { requested: 'US 100 (QQQ)', symbol: 'QQQ', htfLabel: '1D' },
  { requested: 'US 500 (SPY)', symbol: 'SPY', htfLabel: '1D' },
  { requested: 'US 30 (DIA)', symbol: 'DIA', htfLabel: '1D' }
];

const indexCache = new Map();

const CONFIG = {
  scanIntervalMs: 300000,
  requestTimeoutMs: 25000,
  connectionTimeoutMs: 20000,
  reconnectDelayMs: 3000,
  maxReconnectDelayMs: 60000,
  candleCount: 500,
  minimum12HCandles: 80,
  minimumCandles: 80,
  keepAliveMs: 30000,
  staleConnectionMs: 90000
};

const MARKETS = [
  'EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/CHF', 'USD/CAD',
  'AUD/USD', 'NZD/USD', 'EUR/GBP', 'EUR/JPY', 'GBP/JPY',
  'AUD/JPY', 'CAD/JPY', 'CHF/JPY', 'NZD/JPY', 'EUR/AUD',
  'EUR/CAD', 'EUR/CHF', 'EUR/NZD', 'GBP/AUD', 'GBP/CAD',
  'GBP/CHF', 'GBP/NZD', 'AUD/CAD', 'AUD/CHF', 'AUD/NZD',
  'CAD/CHF', 'NZD/CAD', 'NZD/CHF', 'XAU/USD', 'XAG/USD'
];

const ALIASES = {
  'XAU/USD': ['XAUUSD', 'GOLDUSD', 'GOLD'],
  'XAG/USD': ['XAGUSD', 'SILVERUSD', 'SILVER']
};

let ws = null;
let connected = false;
let connecting = false;
let reconnectTimer = null;
let keepAliveTimer = null;
let connectionTimer = null;
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

function getDecimals(market) {
  const name = String(market || '').toUpperCase();

  if (name === 'XAU/USD' || name === 'XAG/USD') return 3;
  if (name.startsWith('US ')) return 2;
  if (name.includes('JPY')) return 3;

  return 5;
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

      return aliases.includes(name) || aliases.includes(code);
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
        `Deriv request timed out: ${Object.keys(payload).join(', ')}`
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
      log(`Subscription failed for ${market.requested}: ${error.message}`);
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

function clearConnectionTimeout() {
  if (connectionTimer) {
    clearTimeout(connectionTimer);
    connectionTimer = null;
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

    try {
      ws.send(JSON.stringify({ ping: 1 }));
    } catch (error) {
      log(`Ping failed: ${error.message}`);

      try {
        ws.terminate();
      } catch {}

      return;
    }

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

  log('Connecting to Deriv...');
  log('WebSocket endpoint: api.derivws.com');

  let socket;

  try {
    socket = new WebSocket(WS_URL, {
      handshakeTimeout: CONFIG.connectionTimeoutMs,
      perMessageDeflate: false
    });

    ws = socket;
  } catch (error) {
    connecting = false;
    lastError = error.message;

    log(`WebSocket creation failed: ${error.message}`);

    scheduleReconnect();
    return;
  }

  connectionTimer = setTimeout(() => {
    if (ws !== socket || socket.readyState !== WebSocket.CONNECTING) {
      return;
    }

    lastError = 'WebSocket connection timed out';

    log('WebSocket connection timed out.');

    socket.terminate();
  }, CONFIG.connectionTimeoutMs + 1000);

  socket.on('open', async () => {
    if (ws !== socket) return;

    clearConnectionTimeout();

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

      if (ws === socket) {
        socket.terminate();
      }
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

      const epoch = Number(tick.epoch);
      const price = Number(tick.quote);

      if (Number.isFinite(epoch) && Number.isFinite(price)) {
        livePrices[tick.symbol] = {
          symbol: tick.symbol,
          price,
          epoch,
          time: new Date(epoch * 1000).toISOString()
        };
      }
    }

    if (data.req_id && pending.has(data.req_id)) {
      const item = pending.get(data.req_id);

      clearTimeout(item.timer);
      pending.delete(data.req_id);

      item.resolve(data);
    }
  });

  socket.on('unexpected-response', (req, response) => {
    if (ws !== socket) return;

    const status = response.statusCode;

    lastError = `WebSocket handshake rejected: HTTP ${status}`;

    log(`WebSocket handshake rejected: HTTP ${status}`);
    log(`Response server: ${response.headers.server || 'unknown'}`);
    log(`Response content type: ${response.headers['content-type'] || 'unknown'}`);

    response.resume();

    if (req && !req.destroyed) {
      req.destroy();
    }

    if (socket.readyState !== WebSocket.CLOSED) {
      socket.terminate();
    }
  });

  socket.on('error', error => {
    if (ws !== socket) return;

    lastError = error.message;

    log(`WebSocket error: ${error.message}`);
    log(`Error code: ${error.code || 'unknown'}`);
    log(`HTTP status: ${error.statusCode || 'unknown'}`);
  });

  socket.on('close', (code, reason) => {
    if (ws !== socket) return;

    clearConnectionTimeout();
    stopKeepAlive();

    connected = false;
    connecting = false;

    log(
      `WebSocket closed. Code ${code}. ` +
      `${reason ? reason.toString() : 'No reason given'}`
    );

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

// Fetch candles and remove the still-forming last candle
async function getClosedCandles(symbol, granularity, count) {
  const candles = await getCandles(symbol, granularity, count);
  return candles.slice(0, -1);
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

// Ichimoku with the 26-period forward shift of the cloud,
// so the cloud matches what TradingView draws at the current candle.
function ichimokuDirection(candles) {
  const DISPLACEMENT = 26;

  if (candles.length < 52 + DISPLACEMENT) return 'NEUTRAL';

  const i = candles.length - 1;
  const close = candles[i].close;

  const tenkan = midpoint(candles, 9, i);
  const kijun = midpoint(candles, 26, i);

  // Cloud at the current candle was calculated 26 candles ago
  const cloudIndex = i - DISPLACEMENT;
  const pastTenkan = midpoint(candles, 9, cloudIndex);
  const pastKijun = midpoint(candles, 26, cloudIndex);
  const spanA =
    pastTenkan !== null && pastKijun !== null
      ? (pastTenkan + pastKijun) / 2
      : null;
  const spanB = midpoint(candles, 52, cloudIndex);

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

  for (let i = 2; i < candles.length - 2; i++) {
    const c = candles[i];

    if (
      c.high > candles[i - 1].high &&
      c.high > candles[i - 2].high &&
      c.high > candles[i + 1].high &&
      c.high > candles[i + 2].high
    ) {
      highs.push({ index: i, price: c.high, time: c.time });
    }

    if (
      c.low < candles[i - 1].low &&
      c.low < candles[i - 2].low &&
      c.low < candles[i + 1].low &&
      c.low < candles[i + 2].low
    ) {
      lows.push({ index: i, price: c.low, time: c.time });
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
  if (!Array.isArray(candles) || candles.length < 10) {
    return false;
  }

  const lastIndex = candles.length - 1;
  const latest = candles[lastIndex];
  const average = averageRange(candles, 14);

  if (!latest || average <= 0) return false;

  // Avoid treating a candle that barely crosses the level as a breakout.
  const breakoutBuffer = average * 0.05;
  const retestTolerance = average * 0.20;

  // The breakout must happen on a recent CLOSED candle.
  // The latest candle must be a later retest/rejection candle.
  const firstIndex = Math.max(1, lastIndex - 4);

  for (let i = firstIndex; i < lastIndex; i++) {
    const previous = candles[i - 1];
    const breakout = candles[i];

    let validBreakout = false;

    if (direction === 'BUY') {
      validBreakout =
        previous.close <= level &&
        breakout.close > level + breakoutBuffer &&
        breakout.close > breakout.open;
    } else {
      validBreakout =
        previous.close >= level &&
        breakout.close < level - breakoutBuffer &&
        breakout.close < breakout.open;
    }

    if (!validBreakout) continue;

    // Require a later candle to retest the broken level and reject it.
    if (direction === 'BUY') {
      const retested = latest.low <= level + retestTolerance;
      const heldAbove = latest.close > level;
      const bullishRejection = latest.close > latest.open;

      if (retested && heldAbove && bullishRejection) {
        return true;
      }
    } else {
      const retested = latest.high >= level - retestTolerance;
      const heldBelow = latest.close < level;
      const bearishRejection = latest.close < latest.open;

      if (retested && heldBelow && bearishRejection) {
        return true;
      }
    }
  }

  return false;
}



function evaluateSignal(
  market,
  candles12h,
  candles1h,
  candles15m,
  candles5m
) {
  if (
    !candles12h || candles12h.length < 30 ||
    !candles1h || candles1h.length < 30 ||
    !candles15m || candles15m.length < 10 ||
    !candles5m || candles5m.length < 3
  ) {
    return null;
  }

  // 1. Confirm 12H and 1H Ichimoku direction
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

  // 2. Find confirmed 15M Williams fractals
  const fractals = findFractals(candles15m);

  if (!fractals.highs.length || !fractals.lows.length) {
    return null;
  }

  const lastHigh = fractals.highs[fractals.highs.length - 1];
  const lastLow = fractals.lows[fractals.lows.length - 1];

  // 3. Use the latest completed candles
  const latest5 = candles5m[candles5m.length - 1];
  const previous5 = candles5m[candles5m.length - 2];
  const latest15 = candles15m[candles15m.length - 1];

  if (!latest5 || !previous5 || !latest15) {
    return null;
  }

  let entry;
  let stopLoss;
  let takeProfit;
  let reason;

  // 4. BUY setup
  if (direction === 'BUY') {
    // Require a 15M fractal breakout and retest
    if (
      !findBreakoutRetest(
        candles15m,
        lastHigh.price,
        'BUY'
      )
    ) {
      return null;
    }

    // Require bullish 5M confirmation
    if (
      latest5.close <= latest5.open ||
      latest5.close <= previous5.close
    ) {
      return null;
    }

    entry = latest5.close;

    // Stop below the recent swing low
    stopLoss = Math.min(
      lastLow.price,
      latest15.low
    );

    if (
      !Number.isFinite(entry) ||
      !Number.isFinite(stopLoss) ||
      stopLoss >= entry
    ) {
      return null;
    }

    // 1:2 risk-reward
    takeProfit = entry + 2 * (entry - stopLoss);

    reason =
      (market.htfLabel || '12H') +
      '/1H Ichimoku bullish; 15M fractal breakout and retest; ' +
      '5M bullish confirmation';

  // 5. SELL setup
  } else if (direction === 'SELL') {
    // Require a 15M fractal breakout and retest
    if (
      !findBreakoutRetest(
        candles15m,
        lastLow.price,
        'SELL'
      )
    ) {
      return null;
    }

    // Require bearish 5M confirmation
    if (
      latest5.close >= latest5.open ||
      latest5.close >= previous5.close
    ) {
      return null;
    }

    entry = latest5.close;

    // Stop above the recent swing high
    stopLoss = Math.max(
      lastHigh.price,
      latest15.high
    );

    if (
      !Number.isFinite(entry) ||
      !Number.isFinite(stopLoss) ||
      stopLoss <= entry
    ) {
      return null;
    }

    // 1:2 risk-reward
    takeProfit = entry - 2 * (stopLoss - entry);

    reason =
      (market.htfLabel || '12H') +
      '/1H Ichimoku bearish; 15M fractal breakout and retest; ' +
      '5M bearish confirmation';

  } else {
    return null;
  }

  // 6. Format prices
  const digits = getDecimals(market.requested);

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
  // All candles below exclude the still-forming candle
  const candles4h = await getClosedCandles(
    market.symbol,
    14400,
    CONFIG.candleCount
  );

  const candles12h = aggregateCandles(candles4h, 43200);

  const candles1h = await getClosedCandles(market.symbol, 3600, 200);
  const candles15m = await getClosedCandles(market.symbol, 900, 200);
  const candles5m = await getClosedCandles(market.symbol, 300, 200);

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

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// US cash session: Mon-Fri 9:30-16:00 New York time
function isUsMarketOpen() {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    weekday: 'short',
    hour: 'numeric',
    minute: 'numeric',
    hour12: false
  }).formatToParts(new Date());

  const get = type => parts.find(p => p.type === type).value;
  const day = get('weekday');

  if (day === 'Sat' || day === 'Sun') return false;

  const minutes = (Number(get('hour')) % 24) * 60 + Number(get('minute'));

  return minutes >= 570 && minutes < 960;
}

// Free plan allows 8 requests per minute: stay at 7 or fewer
const twelveCalls = [];

async function twelveThrottle() {
  while (true) {
    const now = Date.now();

    while (twelveCalls.length && now - twelveCalls[0] > 60000) {
      twelveCalls.shift();
    }

    if (twelveCalls.length < 7) break;

    await sleep(60000 - (now - twelveCalls[0]) + 100);
  }

  twelveCalls.push(Date.now());
}

// Fetch candles from Twelve Data and drop the still-forming last candle
async function getTwelveCandles(symbol, interval, size) {
  await twelveThrottle();

  const url = new URL('https://api.twelvedata.com/time_series');

  url.searchParams.set('symbol', symbol);
  url.searchParams.set('interval', interval);
  url.searchParams.set('outputsize', String(size));
  url.searchParams.set('timezone', 'UTC');
  url.searchParams.set('order', 'ASC');
  url.searchParams.set('apikey', TWELVE_KEY);

  const response = await fetch(url, {
    signal: AbortSignal.timeout(20000)
  });

  const data = await response.json();

  if (data.status === 'error' || !Array.isArray(data.values)) {
    throw new Error(`Twelve Data: ${data.message || 'no data returned'}`);
  }

  const candles = data.values
    .map(v => {
      const iso = v.datetime.length > 10
        ? v.datetime.replace(' ', 'T') + 'Z'
        : v.datetime + 'T00:00:00Z';

      return {
        time: Math.floor(new Date(iso).getTime() / 1000),
        open: Number(v.open),
        high: Number(v.high),
        low: Number(v.low),
        close: Number(v.close)
      };
    })
    .filter(c =>
      Number.isFinite(c.time) &&
      Number.isFinite(c.open) &&
      Number.isFinite(c.high) &&
      Number.isFinite(c.low) &&
      Number.isFinite(c.close)
    )
    .sort((a, b) => a.time - b.time);

  return candles.slice(0, -1);
}

// Slow timeframes are cached to save API credits
async function getCachedTwelveCandles(symbol, interval, size, ttlMs) {
  const key = `${symbol}|${interval}`;
  const cached = indexCache.get(key);

  if (cached && Date.now() - cached.time < ttlMs) {
    return cached.candles;
  }

  const candles = await getTwelveCandles(symbol, interval, size);

  indexCache.set(key, { time: Date.now(), candles });

  return candles;
}

async function scanIndex(market) {
  const daily = await getCachedTwelveCandles(
    market.symbol, '1day', 120, 6 * 60 * 60 * 1000
  );

  const candles1h = await getCachedTwelveCandles(
    market.symbol, '1h', 200, 60 * 60 * 1000
  );

  if (
    daily.length < CONFIG.minimumCandles ||
    candles1h.length < CONFIG.minimumCandles
  ) {
    throw new Error('Not enough historical candles');
  }

  const dailyDirection = ichimokuDirection(daily);
  const hourDirection = ichimokuDirection(candles1h);

  // Only spend credits on 15M/5M when the bigger trend agrees
  if (
    dailyDirection === 'NEUTRAL' ||
    hourDirection === 'NEUTRAL' ||
    dailyDirection !== hourDirection
  ) {
    return null;
  }

  const candles15m = await getTwelveCandles(market.symbol, '15min', 200);
  const candles5m = await getTwelveCandles(market.symbol, '5min', 200);

  if (
    candles15m.length < CONFIG.minimumCandles ||
    candles5m.length < CONFIG.minimumCandles
  ) {
    throw new Error('Not enough historical candles');
  }

  // Skip stale data (holiday or early close)
  const lastTime = candles5m[candles5m.length - 1].time;

  if (Date.now() / 1000 - lastTime > 30 * 60) return null;

  return evaluateSignal(
    market,
    daily,
    candles1h,
    candles15m,
    candles5m
  );
}

async function recordSignal(signal, found) {
  const duplicate = signals.some(existing =>
    existing.symbol === signal.symbol &&
    existing.direction === signal.direction &&
    Date.now() - new Date(existing.time).getTime() < 15 * 60 * 1000
  );

  if (duplicate) return;

  found.push(signal);
  signals.unshift(signal);

  if (signals.length > 100) signals.length = 100;

  await sendTelegram(
    `Signal: ${signal.direction}\n` +
    `Market: ${signal.market}\n` +
    `Symbol: ${signal.symbol}\n` +
    `Entry: ${signal.entry}\n` +
    `Stop Loss: ${signal.stopLoss}\n` +
    `Take Profit: ${signal.takeProfit}\n` +
    `Risk/Reward: 1:2\n` +
    `Strategy: ${signal.strategy}\n` +
    `Signal only. No trade was placed.`
  );
}

async function runScan() {
  if (scanRunning) {
    return { message: 'A scan is already running.' };
  }

  const scanIndices = Boolean(TWELVE_KEY) && isUsMarketOpen();
  const scanDeriv = connected && resolvedMarkets.length > 0;

  if (!scanDeriv && !scanIndices) {
    return {
      error: connected
        ? 'No requested markets were resolved.'
        : 'Deriv is not connected.',
      unavailableMarkets
    };
  }

  scanRunning = true;
  totalScans++;

  const found = [];
  const errors = [];
  let scanned = 0;
  let indicesScanned = 0;

  try {
    if (scanDeriv) {
      for (const market of resolvedMarkets) {
        if (!connected) break;

        try {
          const signal = await scanMarket(market);

          scanned++;

          if (signal) await recordSignal(signal, found);
        } catch (error) {
          errors.push({
            market: market.requested,
            symbol: market.symbol,
            error: error.message
          });

          log(`Scan failed for ${market.requested}: ${error.message}`);
        }
      }
    }

    if (scanIndices) {
      for (const market of INDEX_MARKETS) {
        try {
          const signal = await scanIndex(market);

          indicesScanned++;

          if (signal) await recordSignal(signal, found);
        } catch (error) {
          errors.push({
            market: market.requested,
            symbol: market.symbol,
            error: error.message
          });

          log(`Scan failed for ${market.requested}: ${error.message}`);

          // Out of credits or rate limited: stop hitting the API
          if (/credit|limit|quota/i.test(error.message)) break;
        }

        await sleep(1500);
      }
    }

    lastScanTime = new Date().toISOString();

    return {
      scanned,
      indicesScanned,
      usMarketOpen: isUsMarketOpen(),
      indexDataEnabled: Boolean(TWELVE_KEY),
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
    indexMarkets: INDEX_MARKETS,
    usMarketOpen: isUsMarketOpen(),
    indexDataEnabled: Boolean(TWELVE_KEY),
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
  if (!scanRunning) {
    runScan().catch(error => {
      lastError = error.message;
      log(`Scheduled scan failed: ${error.message}`);
    });
  }
}, CONFIG.scanIntervalMs);

process.on('SIGTERM', () => {
  log('Shutting down.');

  if (reconnectTimer) clearTimeout(reconnectTimer);

  clearConnectionTimeout();
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
