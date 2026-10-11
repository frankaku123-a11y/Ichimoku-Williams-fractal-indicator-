
'use strict';

const express = require('express');
const WebSocket = require('ws');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const RR = 2;
const SCAN_INTERVAL = 300000;
const TIMEOUT = 20000;

const WS_URLS = [
  'wss://ws.binaryws.com/websockets/v3',
  'wss://ws.derivws.com/websockets/v3?app_id=1086
];

const MARKETS = [
  'EUR/USD', 'GBP/USD', 'USD/JPY',
  'USD/CHF', 'USD/CAD', 'AUD/USD',
  'NZD/USD', 'EUR/GBP', 'EUR/JPY',
  'GBP/JPY', 'AUD/JPY', 'CAD/JPY',
  'CHF/JPY', 'NZD/JPY', 'EUR/AUD',
  'EUR/CAD', 'EUR/CHF', 'EUR/NZD',
  'GBP/AUD', 'GBP/CAD', 'GBP/CHF',
  'GBP/NZD', 'AUD/CAD', 'AUD/CHF',
  'AUD/NZD', 'CAD/CHF', 'NZD/CAD',
  'NZD/CHF', 'XAU/USD', 'XAG/USD',
  'US 100', 'US 500', 'US 30'
];

const state = {
  connected: false,
  connecting: false,
  activeSymbolsLoaded: false,
  scanning: false,
  attempts: 0,
  lastError: null,
  lastScan: null,
  lastPriceAt: null,
  endpoint: null,
  markets: {},
  signals: [],
  logs: [],
  counts: {
    checked: 0,
    signals: 0,
    errors: 0
  }
};

for (const market of MARKETS) {
  state.markets[market] = {
    market,
    symbol: null,
    price: null,
    priceAt: null,
    status: 'Waiting',
    direction12H: '—',
    direction1H: '—',
    fractal15M: '—',
    retest15M: '—',
    confirm5M: '—',
    entry: null,
    stopLoss: null,
    takeProfit: null,
    error: null
  };
}

let ws = null;
let endpointIndex = 0;
let generation = 0;
let requestId = 1;
let heartbeat = null;
let reconnectTimer = null;
let scanTimer = null;
let scanAgain = false;
let activeSymbols = [];

const pending = new Map();
const breakoutMemory = new Map();
const subscriptions = new Set();
const telegramCooldown = new Map();

function log(...items) {
  const line =
    `[${new Date().toISOString()}] ` +
    items.map(String).join(' ');

  console.log(line);
  state.logs.push(line);

  if (state.logs.length > 150) {
    state.logs.shift();
  }
}

function sleep(ms) {
  return new Promise(resolve =>
    setTimeout(resolve, ms)
  );
}

function normalize(value) {
  return String(value || '')
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '');
}

function updateMarket(market, values) {
  Object.assign(state.markets[market], values);
}

function rejectPending(reason) {
  for (const [id, item] of pending) {
    clearTimeout(item.timer);
    pending.delete(id);
    item.reject(new Error(reason));
  }
}

function request(payload, gen = generation) {
  return new Promise((resolve, reject) => {
    if (
      !ws ||
      ws.readyState !== WebSocket.OPEN ||
      gen !== generation
    ) {
      return reject(
        new Error('Deriv WebSocket not connected')
      );
    }

    const id = requestId++;
    const currentSocket = ws;

    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error('Deriv request timeout'));
    }, TIMEOUT);

    pending.set(id, {
      resolve,
      reject,
      timer
    });

    currentSocket.send(
      JSON.stringify({
        ...payload,
        req_id: id
      }),
      error => {
        if (error && pending.has(id)) {
          clearTimeout(timer);
          pending.delete(id);
          reject(error);
        }
      }
    );
  });
}

function reconnect() {
  if (reconnectTimer) return;

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, 15000);
}

function connect() {
  if (
    state.connecting ||
    (ws && (
      ws.readyState === WebSocket.OPEN ||
      ws.readyState === WebSocket.CONNECTING
    ))
  ) return;

  state.connecting = true;
  state.attempts++;

  const url =
    WS_URLS[endpointIndex % WS_URLS.length];

  state.endpoint = url;

  log('Connecting to Deriv:', url);

  let socket;

  try {
    socket = new WebSocket(url, {
      handshakeTimeout: TIMEOUT,
      perMessageDeflate: false
    });
  } catch (error) {
    state.connecting = false;
    state.lastError = error.message;
    endpointIndex++;
    reconnect();
    return;
  }

  ws = socket;
  const gen = ++generation;

  socket.on('open', () => {
    if (ws !== socket) return;

    state.connected = true;
    state.connecting = false;
    state.lastError = null;

    log('DERIV WEBSOCKET CONNECTED');

    heartbeat = setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({
          ping: 1
        }));
      }
    }, 30000);

    initializeMarkets(gen).catch(error => {
      if (gen !== generation) return;

      state.lastError = error.message;
      log('INITIALIZATION ERROR:', error.message);

      socket.close();
    });
  });

  socket.on('message', raw => {
    if (ws !== socket) return;

    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (data.error) {
      state.lastError =
        data.error.message || 'Deriv API error';

      log(
        'DERIV ERROR:',
        data.error.code || '',
        state.lastError
      );
    }

    if (data.msg_type === 'tick' && data.tick) {
      const symbol =
        data.tick.symbol || data.echo_req?.ticks;

      const price = Number(data.tick.quote);

      if (Number.isFinite(price)) {
        const time = new Date(
          Number(data.tick.epoch) * 1000
        ).toISOString();

        state.lastPriceAt = time;

        for (const market of MARKETS) {
          if (state.markets[market].symbol === symbol) {
            updateMarket(market, {
              price,
              priceAt: time
            });
          }
        }
      }
    }

    if (data.req_id && pending.has(data.req_id)) {
      const item = pending.get(data.req_id);

      clearTimeout(item.timer);
      pending.delete(data.req_id);

      if (data.error) {
        item.reject(
          new Error(data.error.message)
        );
      } else {
        item.resolve(data);
      }
    }
  });

  socket.on('error', error => {
    if (ws !== socket) return;

    state.lastError = error.message;
    log('WEBSOCKET ERROR:', error.message);
  });

  socket.on('close', (code, reason) => {
    if (ws !== socket) return;

    state.connected = false;
    state.connecting = false;
    state.activeSymbolsLoaded = false;

    clearInterval(heartbeat);
    clearTimeout(scanTimer);

    heartbeat = null;
    scanTimer = null;

    activeSymbols = [];
    subscriptions.clear();

    rejectPending('WebSocket disconnected');

    ws = null;
    endpointIndex++;

    log(
      'WEBSOCKET CLOSED:',
      code,
      reason.toString()
    );

    reconnect();
  });
}

const ALIASES = {
  XAUUSD: ['GOLD', 'XAUUSD'],
  XAGUSD: ['SILVER', 'XAGUSD'],
  US100: ['US100', 'NASDAQ100', 'USTECH100'],
  US500: ['US500', 'SP500', 'USA500'],
  US30: ['US30', 'DJ30', 'WALLSTREET30']
};

function resolveSymbol(market) {
  const wanted = normalize(market);

  const targets = [
    wanted,
    ...(ALIASES[wanted] || [])
  ].map(normalize);

  for (const item of activeSymbols) {
    const names = [
      item.symbol,
      item.underlying_symbol,
      item.display_name,
      item.underlying_symbol_name
    ].map(normalize);

    if (targets.some(t => names.includes(t))) {
      return item;
    }
  }

  return null;
}

function symbolCode(item) {
  return item?.symbol ||
    item?.underlying_symbol ||
    null;
}

async function initializeMarkets(gen) {
  log('Loading Deriv market symbols...');

  const response = await request({
    active_symbols: 'brief'
  }, gen);

  if (gen !== generation) return;

  activeSymbols = response.active_symbols || [];

  if (!activeSymbols.length) {
    throw new Error('No active symbols returned');
  }

  state.activeSymbolsLoaded = true;

  log(
    'ACTIVE SYMBOLS LOADED:',
    activeSymbols.length
  );

  const uniqueSymbols = new Set();

  for (const market of MARKETS) {
    const found = resolveSymbol(market);
    const symbol = symbolCode(found);

    updateMarket(market, {
      symbol,
      status: symbol ? 'Ready' : 'Unavailable',
      error: symbol
        ? null
        : 'Symbol not found in Deriv API'
    });

    if (symbol) {
      uniqueSymbols.add(symbol);
      log('SYMBOL:', market, '=>', symbol);
    }
  }

  for (const symbol of uniqueSymbols) {
    if (gen !== generation) return;

    try {
      await request({
        ticks: symbol,
        subscribe: 1
      }, gen);

      subscriptions.add(symbol);

      log('LIVE PRICE SUBSCRIBED:', symbol);
    } catch (error) {
      log(
        'SUBSCRIPTION ERROR:',
        symbol,
        error.message
      );
    }

    await sleep(100);
  }

  runScan();
}

async function getCandles(
  symbol,
  granularity,
  count,
  gen
) {
  const response = await request({
    ticks_history: symbol,
    style: 'candles',
    granularity,
    count,
    end: 'latest'
  }, gen);

  if (!Array.isArray(response.candles)) {
    throw new Error(
      `No candles for ${symbol} / ${granularity}`
    );
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
      [
        c.time,
        c.open,
        c.high,
        c.low,
        c.close
      ].every(Number.isFinite)
    )
    .sort((a, b) => a.time - b.time);
}

function completedCandles(candles, seconds) {
  const now = Math.floor(Date.now() / 1000);

  return candles.filter(
    c => c.time + seconds <= now
  );
}

function aggregate12H(hourly) {
  const buckets = new Map();

  for (const candle of hourly) {
    const time =
      Math.floor(candle.time / 43200) * 43200;

    if (!buckets.has(time)) {
      buckets.set(time, {
        ...candle,
        time,
        lastTime: candle.time,
        count: 1
      });
    } else {
      const b = buckets.get(time);

      b.high = Math.max(b.high, candle.high);
      b.low = Math.min(b.low, candle.low);

      if (candle.time >= b.lastTime) {
        b.close = candle.close;
        b.lastTime = candle.time;
      }

      b.count++;
    }
  }

  const now = Math.floor(Date.now() / 1000);

  return [...buckets.values()]
    .filter(c =>
      c.time + 43200 <= now &&
      c.count === 12
    )
    .sort((a, b) => a.time - b.time);
}

function midpoint(candles, period, index) {
  if (index < period - 1) return null;

  const section = candles.slice(
    index - period + 1,
    index + 1
  );

  const high = Math.max(
    ...section.map(c => c.high)
  );

  const low = Math.min(
    ...section.map(c => c.low)
  );

  return (high + low) / 2;
}

function ichimokuDirection(candles) {
  if (candles.length < 80) {
    return 'INSUFFICIENT DATA';
  }

  const i = candles.length - 1;

  const tenkan = midpoint(candles, 9, i);
  const kijun = midpoint(candles, 26, i);

  const spanA = (tenkan + kijun) / 2;
  const spanB = midpoint(candles, 52, i);

  const close = candles[i].close;

  if (
    [tenkan, kijun, spanA, spanB]
      .some(v => v === null)
  ) {
    return 'INSUFFICIENT DATA';
  }

  const top = Math.max(spanA, spanB);
  const bottom = Math.min(spanA, spanB);

  if (close > top && tenkan > kijun) {
    return 'BULLISH';
  }

  if (close < bottom && tenkan < kijun) {
    return 'BEARISH';
  }

  return 'NEUTRAL';
}

function findFractals(candles, type) {
  const result = [];

  for (
    let i = 2;
    i < candles.length - 2;
    i++
  ) {
    const value = type === 'high'
      ? candles[i].high
      : candles[i].low;

    const left = candles.slice(i - 2, i);
    const right = candles.slice(i + 1, i + 3);

    const confirmed = type === 'high'
      ? [...left, ...right].every(
          c => value > c.high
        )
      : [...left, ...right].every(
          c => value < c.low
        );

    if (confirmed) {
      result.push({
        time: candles[i].time,
        value
      });
    }
  }

  return result;
}

function lineValue(a, b, time) {
  if (!a || !b || a.time === b.time) {
    return null;
  }

  return a.value +
    ((b.value - a.value) /
      (b.time - a.time)) *
    (time - a.time);
}

function fractalBreakRetest(market, candles) {
  if (candles.length < 20) {
    return {
      status: 'INSUFFICIENT DATA',
      direction: null
    };
  }

  const highs = findFractals(candles, 'high');
  const lows = findFractals(candles, 'low');

  if (highs.length < 2 || lows.length < 2) {
    return {
      status: 'Waiting for fractals',
      direction: null
    };
  }

  const last = candles.at(-1);
  const prev = candles.at(-2);

  const resistanceNow = lineValue(
    highs.at(-2),
    highs.at(-1),
    last.time
  );

  const resistancePrev = lineValue(
    highs.at(-2),
    highs.at(-1),
    prev.time
  );

  const supportNow = lineValue(
    lows.at(-2),
    lows.at(-1),
    last.time
  );

  const supportPrev = lineValue(
    lows.at(-2),
    lows.at(-1),
    prev.time
  );

  const key = normalize(market);
  const remembered = breakoutMemory.get(key);

  if (
    prev.close <= resistancePrev &&
    last.close > resistanceNow
  ) {
    breakoutMemory.set(key, {
      direction: 'BUY',
      time: last.time,
      line: resistanceNow
    });

    return {
      status: 'BUY breakout; waiting retest',
      direction: null
    };
  }

  if (
    prev.close >= supportPrev &&
    last.close < supportNow
  ) {
    breakoutMemory.set(key, {
      direction: 'SELL',
      time: last.time,
      line: supportNow
    });

    return {
      status: 'SELL breakout; waiting retest',
      direction: null
    };
  }

  if (!remembered) {
    return {
      status: 'Waiting for breakout',
      direction: null
    };
  }

  if (last.time <= remembered.time) {
    return {
      status: 'Waiting for next candle',
      direction: null
    };
  }

  const tolerance = Math.max(
    Math.abs(remembered.line) * 0.0005,
    0.00001
  );

  const touched =
    last.low <= remembered.line + tolerance &&
    last.high >= remembered.line - tolerance;

  if (
    remembered.direction === 'BUY' &&
    touched &&
    last.close > remembered.line &&
    last.close > last.open
  ) {
    breakoutMemory.delete(key);

    return {
      status: 'BUY retest confirmed',
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
      status: 'SELL retest confirmed',
      direction: 'SELL'
    };
  }

  return {
    status: 'Waiting for retest',
    direction: null
  };
}

function confirm5M(candles, direction) {
  if (candles.length < 3) return false;

  const last = candles.at(-1);
  const prev = candles.at(-2);

  if (direction === 'BUY') {
    return (
      last.close > last.open &&
      last.close > prev.high
    );
  }

  if (direction === 'SELL') {
    return (
      last.close < last.open &&
      last.close < prev.low
    );
  }

  return false;
}

function tradeLevels(candles, direction) {
  const recent = candles.slice(-12);

  if (recent.length < 3) return null;

  const entry = candles.at(-1).close;

  if (direction === 'BUY') {
    const stopLoss = Math.min(
      ...recent.map(c => c.low)
    );

    if (stopLoss >= entry) return null;

    return {
      entry,
      stopLoss,
      takeProfit:
        entry + (entry - stopLoss) * RR
    };
  }

  const stopLoss = Math.max(
    ...recent.map(c => c.high)
  );

  if (stopLoss <= entry) return null;

  return {
    entry,
    stopLoss,
    takeProfit:
      entry - (stopLoss - entry) * RR
  };
}

async function sendTelegram(message, market) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;

  if (!token || !chatId) return;

  const now = Date.now();
  const previous = telegramCooldown.get(market) || 0;

  if (now - previous < 600000) return;

  try {
    const response = await fetch(
      `https://api.telegram.org/bot${token}/sendMessage`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          chat_id: chatId,
          text: message
        }),
        signal: AbortSignal.timeout(15000)
      }
    );

    const result = await response.json();

    if (!response.ok || !result.ok) {
      throw new Error(
        result.description || 'Telegram failed'
      );
    }

    telegramCooldown.set(market, now);
    log('TELEGRAM SIGNAL SENT:', market);

  } catch (error) {
    log('TELEGRAM ERROR:', error.message);
  }
}

async function scanMarket(market, gen) {
  const resolved = resolveSymbol(market);
  const symbol = symbolCode(resolved);

  if (!symbol) return;

  try {
    const hourlyRaw = await getCandles(
      symbol, 3600, 1800, gen
    );

    await sleep(150);

    const m15Raw = await getCandles(
      symbol, 900, 180, gen
    );

    await sleep(150);

    const m5Raw = await getCandles(
      symbol, 300, 180, gen
    );

    if (gen !== generation) return;

    const h1 = completedCandles(
      hourlyRaw, 3600
    );

    const h12 = aggregate12H(h1);

    const m15 = completedCandles(
      m15Raw, 900
    );

    const m5 = completedCandles(
      m5Raw, 300
    );

    const direction12H =
      ichimokuDirection(h12);

    const direction1H =
      ichimokuDirection(h1);

    const fractal =
      fractalBreakRetest(market, m15);

    state.counts.checked++;

    updateMarket(market, {
      status: 'Scanned',
      direction12H,
      direction1H,
      fractal15M: fractal.status,
      retest15M: fractal.direction || 'Waiting',
      confirm5M: 'Waiting',
      error: null
    });

    let direction = null;

    if (
      direction12H === 'BULLISH' &&
      direction1H === 'BULLISH'
    ) {
      direction = 'BUY';
    }

    if (
      direction12H === 'BEARISH' &&
      direction1H === 'BEARISH'
    ) {
      direction = 'SELL';
    }

    if (
      !direction ||
      fractal.direction !== direction
    ) return;

    if (!confirm5M(m5, direction)) {
      updateMarket(market, {
        confirm5M: 'Waiting for confirmation'
      });

      return;
    }

    const levels = tradeLevels(m5, direction);

    if (!levels) return;

    const candleTime = m5.at(-1).time;

    const id =
      `${normalize(market)}-${direction}-${candleTime}`;

    if (state.signals.some(s => s.id === id)) {
      return;
    }

    const signal = {
      id,
      market,
      symbol,
      direction,
      timeframe: '12H + 1H + 15M + 5M',
      ...levels,
      riskReward: '1:2',
      createdAt: new Date().toISOString()
    };

    state.signals.unshift(signal);
    state.signals = state.signals.slice(0, 100);

    state.counts.signals++;

    updateMarket(market, {
      status: 'SIGNAL',
      confirm5M: `${direction} confirmed`,
      ...levels
    });

    log(
      'NEW SIGNAL:',
      market,
      direction,
      'ENTRY:', levels.entry,
      'SL:', levels.stopLoss,
      'TP:', levels.takeProfit
    );

    await sendTelegram(
      [
        `DERIV SIGNAL — ${market}`,
        `Direction: ${direction}`,
        `12H Ichimoku: ${direction12H}`,
        `1H Ichimoku: ${direction1H}`,
        '15M Fractal: Breakout + Retest',
        '5M: Confirmed',
        `Entry: ${levels.entry}`,
        `Stop Loss: ${levels.stopLoss}`,
        `Take Profit: ${levels.takeProfit}`,
        'Risk Reward: 1:2',
        'Signal only — no trade placed'
      ].join('\n'),
      market
    );

  } catch (error) {
    if (gen !== generation) return;

    state.counts.errors++;

    updateMarket(market, {
      status: 'Scan error',
      error: error.message
    });

    log(
      'SCAN ERROR:',
      market,
      error.message
    );
  }
}

async function runScan() {
  if (state.scanning) {
    scanAgain = true;
    return;
  }

  if (
    !state.connected ||
    !state.activeSymbolsLoaded
  ) return;

  state.scanning = true;
  const gen = generation;

  clearTimeout(scanTimer);
  scanTimer = null;

  log('SCAN START:', MARKETS.length, 'markets');

  try {
    for (const market of MARKETS) {
      if (
        !state.connected ||
        gen !== generation
      ) break;

      await scanMarket(market, gen);
      await sleep(200);
    }
  } finally {
    state.scanning = false;
    state.lastScan = new Date().toISOString();

    log('SCAN FINISHED');

    if (
      state.connected &&
      state.activeSymbolsLoaded
    ) {
      const delay = scanAgain
        ? 1000
        : SCAN_INTERVAL;

      scanAgain = false;

      scanTimer = setTimeout(
        runScan,
        delay
      );
    }
  }
}

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    connected: state.connected,
    activeSymbolsLoaded:
      state.activeSymbolsLoaded,
    attempts: state.attempts,
    lastError: state.lastError,
    lastPriceAt: state.lastPriceAt
  });
});

app.get('/api/status', (req, res) => {
  res.json({
    ...state,
    markets: Object.values(state.markets),
    signals: state.signals.slice(0, 50),
    logs: state.logs.slice(-80),
    subscribedSymbols: subscriptions.size
  });
});

app.get('/api/symbols', (req, res) => {
  res.json({
    activeSymbolsCount: activeSymbols.length,
    markets: MARKETS.map(market => ({
      market,
      symbol: symbolCode(resolveSymbol(market))
    }))
  });
});

app.post('/api/scan', (req, res) => {
  if (
    !state.connected ||
    !state.activeSymbolsLoaded
  ) {
    return res.status(503).json({
      ok: false,
      error: 'Deriv not connected'
    });
  }

  runScan();

  res.json({
    ok: true,
    message: 'Scan requested'
  });
});

app.get('/', (req, res) => {
  res.type('html').send(`
<!DOCTYPE html>
<html>
<head>
<meta name="viewport"
content="width=device-width,initial-scale=1">
<title>Deriv Trading Scanner</title>
<style>
body {
  background:#101820;
  color:white;
  font-family:Arial;
  margin:15px;
}
h2 {color:#35d0e2}
.status {
  background:#20303c;
  padding:12px;
  border-radius:8px;
  margin:10px 0;
}
button {
  background:#16899b;
  color:white;
  border:0;
  padding:12px;
  border-radius:6px;
}
.wrap {overflow-x:auto}
table {
  border-collapse:collapse;
  min-width:1100px;
  width:100%;
}
th,td {
  border:1px solid #40505c;
  padding:8px;
  font-size:12px;
}
th {background:#243746}
pre {
  background:#080e14;
  padding:12px;
  white-space:pre-wrap;
  overflow-wrap:anywhere;
}
</style>
</head>
<body>
<h2>Deriv Ichimoku + Williams Fractal</h2>
<p>Live Market Scanner — Signal Only</p>
<div id="status" class="status">
Connecting...
</div>
<button onclick="scanNow()">Scan Now</button>
<h3>Market Analysis</h3>
<div class="wrap">
<table>
<thead>
<tr>
<th>Market</th>
<th>Live Price</th>
<th>Status</th>
<th>12H</th>
<th>1H</th>
<th>15M Fractal</th>
<th>Retest</th>
<th>5M</th>
<th>Entry</th>
<th>SL</th>
<th>TP</th>
<th>Error</th>
</tr>
</thead>
<tbody id="markets"></tbody>
</table>
</div>
<h3>Recent Signals</h3>
<div id="signals"></div>
<h3>Server Logs</h3>
<pre id="logs"></pre>

<script>
function esc(v) {
  return String(v == null ? '' : v)
    .replace(/[&<>"']/g, c => ({
      '&':'&amp;',
      '<':'&lt;',
      '>':'&gt;',
      '"':'&quot;',
      "'":'&#39;'
    }[c]));
}

function price(v) {
  return v == null
    ? '—'
    : String(Number(Number(v).toPrecision(8)));
}

async function refresh() {
  try {
    const response = await fetch('/api/status');
    const data = await response.json();

    document.getElementById('status').textContent =
      'Connection: ' +
      (data.connected ? 'CONNECTED' : 'DISCONNECTED') +
      ' | Symbols: ' + data.subscribedSymbols +
      ' | Scanning: ' + data.scanning +
      ' | Last price: ' + (data.lastPriceAt || 'None') +
      ' | Last error: ' + (data.lastError || 'None');

    document.getElementById('markets').innerHTML =
      data.markets.map(m => {
        const cells = [
          m.market,
          price(m.price),
          m.status,
          m.direction12H,
          m.direction1H,
          m.fractal15M,
          m.retest15M,
          m.confirm5M,
          price(m.entry),
          price(m.stopLoss),
          price(m.takeProfit),
          m.error || ''
        ];

        return '<tr>' +
          cells.map(v => '<td>' + esc(v) + '</td>').join('') +
          '</tr>';
      }).join('');

    document.getElementById('signals').innerHTML =
      data.signals.length
        ? data.signals.map(s =>
          '<div class="status">' +
          esc(s.market) + ' ' +
          esc(s.direction) +
          ' | Entry: ' + esc(price(s.entry)) +
          ' | SL: ' + esc(price(s.stopLoss)) +
          ' | TP: ' + esc(price(s.takeProfit)) +
          '</div>'
        ).join('')
        : 'No confirmed signals yet';

    document.getElementById('logs').textContent =
      data.logs.join('\\n');

  } catch (error) {
    document.getElementById('status').textContent =
      'Dashboard error: ' + error.message;
  }
}

async function scanNow() {
  try {
    const response = await fetch('/api/scan', {
      method:'POST'
    });
    const result = await response.json();

    if (!response.ok) {
      alert(result.error || 'Scan failed');
    }

    refresh();
  } catch (error) {
    alert(error.message);
  }
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
  `);
});

const server = app.listen(
  PORT,
  '0.0.0.0',
  () => {
    log('SERVER STARTED ON PORT:', PORT);
    log('CONFIGURED MARKETS:', MARKETS.length);
    log('SIGNAL-ONLY MODE ENABLED');
    connect();
  }
);

process.on('SIGTERM', () => {
  clearInterval(heartbeat);
  clearTimeout(scanTimer);
  clearTimeout(reconnectTimer);

  if (ws) ws.close();

  server.close(() => process.exit(0));
});

process.on('unhandledRejection', error => {
  log('UNHANDLED REJECTION:', error.message);
});
