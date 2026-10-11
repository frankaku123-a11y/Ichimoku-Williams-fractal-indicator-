
'use strict';

const express = require('express');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const APP_ID = process.env.DERIV_APP_ID || '1089';
const RR = 2;
const SCAN_INTERVAL = 300000;
const FRACTAL_PERIOD = 2;
const ICHIMOKU = { tenkan: 9, kijun: 26, spanB: 52, displacement: 26 };
const MAX_CANDLES = 1500;

const WS_URL =
  `wss://ws.derivws.com/websockets/v3?app_id=${encodeURIComponent(APP_ID)}`;

const MARKETS = [
  { key: 'EUR/USD', aliases: ['EUR/USD', 'EURUSD'] },
  { key: 'GBP/USD', aliases: ['GBP/USD', 'GBPUSD'] },
  { key: 'USD/JPY', aliases: ['USD/JPY', 'USDJPY'] },
  { key: 'GBP/JPY', aliases: ['GBP/JPY', 'GBPJPY'] },
  { key: 'EUR/JPY', aliases: ['EUR/JPY', 'EURJPY'] },
  { key: 'AUD/USD', aliases: ['AUD/USD', 'AUDUSD'] },
  { key: 'USD/CAD', aliases: ['USD/CAD', 'USDCAD'] },
  { key: 'USD/CHF', aliases: ['USD/CHF', 'USDCHF'] },
  { key: 'AUD/JPY', aliases: ['AUD/JPY', 'AUDJPY'] },
  { key: 'NZD/USD', aliases: ['NZD/USD', 'NZDUSD'] },
  { key: 'EUR/GBP', aliases: ['EUR/GBP', 'EURGBP'] },
  { key: 'EUR/AUD', aliases: ['EUR/AUD', 'EURAUD'] },
  { key: 'EUR/CAD', aliases: ['EUR/CAD', 'EURCAD'] },
  { key: 'EUR/CHF', aliases: ['EUR/CHF', 'EURCHF'] },
  { key: 'GBP/AUD', aliases: ['GBP/AUD', 'GBPAUD'] },
  { key: 'GBP/CAD', aliases: ['GBP/CAD', 'GBPCAD'] },
  { key: 'GBP/CHF', aliases: ['GBP/CHF', 'GBPCHF'] },
  { key: 'AUD/CAD', aliases: ['AUD/CAD', 'AUDCAD'] },
  { key: 'AUD/CHF', aliases: ['AUD/CHF', 'AUDCHF'] },
  { key: 'NZD/JPY', aliases: ['NZD/JPY', 'NZDJPY'] },
  { key: 'USD/SGD', aliases: ['USD/SGD', 'USDSGD'] },
  { key: 'USD/HKD', aliases: ['USD/HKD', 'USDHKD'] },
  { key: 'USD/NOK', aliases: ['USD/NOK', 'USDNOK'] },
  { key: 'USD/SEK', aliases: ['USD/SEK', 'USDSEK'] },
  { key: 'USD/PLN', aliases: ['USD/PLN', 'USDPLN'] },
  { key: 'EUR/NZD', aliases: ['EUR/NZD', 'EURNZD'] },
  { key: 'GBP/NZD', aliases: ['GBP/NZD', 'GBPNZD'] },
  { key: 'CAD/JPY', aliases: ['CAD/JPY', 'CADJPY'] },
  { key: 'CHF/JPY', aliases: ['CHF/JPY', 'CHFJPY'] },
  { key: 'NZD/CAD', aliases: ['NZD/CAD', 'NZDCAD'] },
  { key: 'XAU/USD', aliases: ['XAU/USD', 'XAUUSD', 'GOLD/USD', 'GOLD'] },
  { key: 'US 100', aliases: ['US 100', 'US100', 'NASDAQ 100', 'NASDAQ'] },
  { key: 'US 500', aliases: ['US 500', 'US500', 'S&P 500', 'SP500'] },
  { key: 'US 30', aliases: ['US 30', 'US30', 'DOW JONES', 'DOW'] }
];

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const state = {
  connected: false,
  scanning: false,
  lastScan: null,
  lastError: null,
  symbols: {},
  results: {}
};

for (const m of MARKETS) {
  state.results[m.key] = {
    market: m.key,
    signal: 'WAIT',
    status: 'Starting',
    reason: 'Waiting for Deriv connection'
  };
}

let socket;
let reqId = 1;
let reconnectTimer;
let scanTimer;
let pingTimer;
let scanning = false;
let reconnectAttempts = 0;

const pending = new Map();
const lastAlerts = new Map();

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function norm(value) {
  return String(value || '')
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '')
    .replace(/^FRX/, '');
}

function send(payload) {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    return false;
  }

  socket.send(JSON.stringify(payload));
  return true;
}

function request(payload, timeout = 30000) {
  return new Promise((resolve, reject) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      reject(new Error('Deriv WebSocket is not connected'));
      return;
    }

    const id = reqId++;

    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`Request timed out: ${payload.ticks_history || 'Deriv API'}`));
    }, timeout);

    pending.set(id, { resolve, reject, timer });

    if (!send({ ...payload, req_id: id })) {
      clearTimeout(timer);
      pending.delete(id);
      reject(new Error('Failed to send Deriv request'));
    }
  });
}

function resolveSymbols(items) {
  const resolved = {};

  for (const market of MARKETS) {
    const aliases = market.aliases.map(norm);

    const found = items.find(item => {
      const display = norm(item.display_name || item.name);
      const symbol = norm(item.symbol || item.underlying_symbol);

      return aliases.includes(display) || aliases.includes(symbol);
    });

    if (found) {
      resolved[market.key] =
        found.symbol || found.underlying_symbol;

      log('SYMBOL FOUND:', market.key, '=>', resolved[market.key]);
    } else {
      log('SYMBOL UNAVAILABLE:', market.key);
    }
  }

  state.symbols = resolved;

  for (const market of MARKETS) {
    const symbol = resolved[market.key];

    state.results[market.key] = {
      ...state.results[market.key],
      symbol: symbol || null,
      status: symbol ? 'Ready' : 'Symbol unavailable',
      reason: symbol
        ? 'Waiting for a confirmed setup'
        : 'No matching symbol found in Deriv active symbols'
    };
  }

  log(`Resolved ${Object.keys(resolved).length}/${MARKETS.length} markets`);
}

/* Convert Deriv candles into a consistent format. */
function parseCandles(response) {
  const source = response.candles ||
    response.history?.candles || [];

  return source.map(c => ({
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

async function getCandles(symbol, granularity, count = 220) {
  const response = await request({
    ticks_history: symbol,
    adjust_start_time: 1,
    count,
    end: 'latest',
    granularity,
    style: 'candles'
  });

  return parseCandles(response);
}

/* Build 12-hour candles from hourly candles. */
function aggregate(candles, seconds) {
  const groups = new Map();

  for (const c of candles) {
    const time = Math.floor(c.time / seconds) * seconds;

    if (!groups.has(time)) {
      groups.set(time, {
        time,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close
      });
    }

    const g = groups.get(time);
    g.high = Math.max(g.high, c.high);
    g.low = Math.min(g.low, c.low);
    g.close = c.close;
  }

  return [...groups.values()].sort((a, b) => a.time - b.time);
}

function midpoint(candles, index, period) {
  if (index < period - 1) return null;

  let high = -Infinity;
  let low = Infinity;

  for (let i = index - period + 1; i <= index; i++) {
    high = Math.max(high, candles[i].high);
    low = Math.min(low, candles[i].low);
  }

  return (high + low) / 2;
}

/*
 * Standard Ichimoku:
 * Tenkan = 9-period midpoint
 * Kijun = 26-period midpoint
 * Senkou A = midpoint of Tenkan and Kijun
 * Senkou B = 52-period midpoint
 *
 * The cloud is plotted 26 candles forward. Therefore, the cloud
 * visible at the current candle is calculated from index - 26.
 *
 * Uses the last CLOSED candle, not the currently forming candle.
 */
function ichimokuDirection(candles) {
  const i = candles.length - 2;
  const { tenkan, kijun, spanB, displacement } = ICHIMOKU;

  if (i < spanB - 1 + displacement) {
    return {
      direction: 'WAIT',
      reason: 'Not enough candles for displaced Ichimoku Cloud'
    };
  }

  const tenkanNow = midpoint(candles, i, tenkan);
  const kijunNow = midpoint(candles, i, kijun);

  const cloudSourceIndex = i - displacement;
  const tenkanSource = midpoint(candles, cloudSourceIndex, tenkan);
  const kijunSource = midpoint(candles, cloudSourceIndex, kijun);
  const spanA = (tenkanSource + kijunSource) / 2;
  const spanBValue = midpoint(candles, cloudSourceIndex, spanB);

  if (
    [tenkanNow, kijunNow, spanA, spanBValue].some(
      value => value === null || !Number.isFinite(value)
    )
  ) {
    return { direction: 'WAIT', reason: 'Ichimoku values unavailable' };
  }

  const cloudTop = Math.max(spanA, spanBValue);
  const cloudBottom = Math.min(spanA, spanBValue);
  const close = candles[i].close;

  if (close > cloudTop && tenkanNow > kijunNow && spanA > spanBValue) {
    return {
      direction: 'BUY',
      reason: 'Close above cloud; Tenkan above Kijun; bullish cloud',
      close,
      tenkan: tenkanNow,
      kijun: kijunNow,
      cloudTop,
      cloudBottom
    };
  }

  if (close < cloudBottom && tenkanNow < kijunNow && spanA < spanBValue) {
    return {
      direction: 'SELL',
      reason: 'Close below cloud; Tenkan below Kijun; bearish cloud',
      close,
      tenkan: tenkanNow,
      kijun: kijunNow,
      cloudTop,
      cloudBottom
    };
  }

  return {
    direction: 'WAIT',
    reason: 'Price or Ichimoku lines do not fully confirm direction',
    close,
    tenkan: tenkanNow,
    kijun: kijunNow,
    cloudTop,
    cloudBottom
  };
}

/* Williams Fractals. A pivot is only returned once right-side candles
   have formed, so an unconfirmed fractal is never used. */
function fractals(candles, lastClosedIndex, period = FRACTAL_PERIOD) {
  const highs = [];
  const lows = [];

  for (
    let i = period;
    i <= lastClosedIndex - period;
    i++
  ) {
    let isHigh = true;
    let isLow = true;

    for (let j = 1; j <= period; j++) {
      if (
        candles[i].high <= candles[i - j].high ||
        candles[i].high <= candles[i + j].high
      ) {
        isHigh = false;
      }

      if (
        candles[i].low >= candles[i - j].low ||
        candles[i].low >= candles[i + j].low
      ) {
        isLow = false;
      }
    }

    if (isHigh) {
      highs.push({ index: i, price: candles[i].high });
    }

    if (isLow) {
      lows.push({ index: i, price: candles[i].low });
    }
  }

  return { highs, lows };
}

function trendLine(a, b, index) {
  if (!a || !b || a.index === b.index) return null;

  return a.price +
    ((b.price - a.price) / (b.index - a.index)) *
    (index - a.index);
}

/*
 * Looks for a confirmed 15M line break followed by a LATER retest.
 * The latest closed 15M candle must be the retest candle.
 */
function checkBreakRetest(candles) {
  const last = candles.length - 2;

  if (last < 20) {
    return {
      direction: 'WAIT',
      reason: 'Not enough closed 15M candles'
    };
  }

  const { highs, lows } = fractals(candles, last);
  const earliestBreak = Math.max(10, last - 12);

  function searchLine(pivots, side) {
    if (pivots.length < 2) return null;

    // Try recent pivot pairs first.
    for (let bIndex = pivots.length - 1; bIndex >= 1; bIndex--) {
      for (let aIndex = bIndex - 1; aIndex >= 0; aIndex--) {
        const a = pivots[aIndex];
        const b = pivots[bIndex];

        // Require a descending line for sell or ascending line for buy.
        if (side === 'SELL' && b.price >= a.price) continue;
        if (side === 'BUY' && b.price <= a.price) continue;

        let breakIndex = -1;

        for (let j = Math.max(earliestBreak, b.index + 1); j <= last - 1; j++) {
          const previousLine = trendLine(a, b, j - 1);
          const currentLine = trendLine(a, b, j);

          if (previousLine === null || currentLine === null) continue;

          if (side === 'SELL') {
            if (
              candles[j - 1].close >= previousLine &&
              candles[j].close < currentLine
            ) {
              breakIndex = j;
            }
          } else {
            if (
              candles[j - 1].close <= previousLine &&
              candles[j].close > currentLine
            ) {
              breakIndex = j;
            }
          }
        }

        if (breakIndex < 0 || breakIndex >= last) continue;

        const level = trendLine(a, b, last);
        const latest = candles[last];

        if (side === 'SELL') {
          if (latest.high >= level && latest.close < level) {
            return {
              direction: 'SELL',
              level,
              breakIndex,
              reason: '15M close below fractal line, followed by bearish retest'
            };
          }
        } else {
          if (latest.low <= level && latest.close > level) {
            return {
              direction: 'BUY',
              level,
              breakIndex,
              reason: '15M close above fractal line, followed by bullish retest'
            };
          }
        }
      }
    }

    return null;
  }

  const sell = searchLine(highs, 'SELL');
  if (sell) return sell;

  const buy = searchLine(lows, 'BUY');
  if (buy) return buy;

  return {
    direction: 'WAIT',
    reason: 'Waiting for a confirmed 15M break and subsequent retest'
  };
}

function confirm5M(candles, side) {
  const lastIndex = candles.length - 2;

  if (lastIndex < 2) return false;

  const last = candles[lastIndex];
  const prev = candles[lastIndex - 1];

  if (side === 'BUY') {
    return last.close > last.open && last.close > prev.high;
  }

  if (side === 'SELL') {
    return last.close < last.open && last.close < prev.low;
  }

  return false;
}

function calculateTrade(candles, side) {
  const lastIndex = candles.length - 2;
  const entry = candles[lastIndex].close;
  const recent = candles.slice(Math.max(0, lastIndex - 14), lastIndex);

  if (recent.length < 5) return null;

  const low = Math.min(...recent.map(c => c.low));
  const high = Math.max(...recent.map(c => c.high));
  const buffer = Math.max(Math.abs(entry) * 0.0002, (high - low) * 0.1);

  let sl;
  let tp;

  if (side === 'BUY') {
    sl = low - buffer;
    if (sl >= entry) return null;
    tp = entry + (entry - sl) * RR;
  } else {
    sl = high + buffer;
    if (sl <= entry) return null;
    tp = entry - (sl - entry) * RR;
  }

  return { entry, sl, tp };
}

async function telegram(message) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;

  if (!token || !chatId) {
    log('Telegram skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing');
    return;
  }

  try {
    const response = await fetch(
      `https://api.telegram.org/bot${token}/sendMessage`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId, text: message })
      }
    );

    const result = await response.json();

    if (!result.ok) {
      log('Telegram API error:', result.description);
    } else {
      log('Telegram alert sent');
    }
  } catch (error) {
    log('Telegram error:', error.message);
  }
}

async function scanMarket(market) {
  const symbol = state.symbols[market.key];

  if (!symbol) {
    return {
      market: market.key,
      signal: 'WAIT',
      status: 'Symbol unavailable',
      reason: 'No matching symbol found in Deriv'
    };
  }

  try {
    const [h1, m15, m5] = await Promise.all([
      getCandles(symbol, 3600, MAX_CANDLES),
      getCandles(symbol, 900, 220),
      getCandles(symbol, 300, 220)
    ]);

    if (h1.length < 100 || m15.length < 25 || m5.length < 20) {
      return {
        market: market.key,
        symbol,
        signal: 'WAIT',
        status: 'Insufficient data',
        reason: `Candle history too short: H1=${h1.length}, M15=${m15.length}, M5=${m5.length}`
      };
    }

    const h12 = aggregate(h1, 43200);

    // Do not include a potentially incomplete latest 12H candle.
    const d12Result = ichimokuDirection(h12);
    const d1Result = ichimokuDirection(h1);
    const d12 = d12Result.direction;
    const d1 = d1Result.direction;

    const setup = checkBreakRetest(m15);

    let signal = 'WAIT';
    let entry = null;
    let sl = null;
    let tp = null;
    let reason = 'Waiting for timeframe confirmation';

    if (d12 === 'WAIT') {
      reason = `12H Ichimoku: ${d12Result.reason}`;
    } else if (d1 === 'WAIT') {
      reason = `1H Ichimoku: ${d1Result.reason}`;
    } else if (d12 !== d1) {
      reason = '1H Ichimoku direction does not match 12H';
    } else if (setup.direction !== d12) {
      reason = setup.reason;
    } else if (!confirm5M(m5, d12)) {
      reason = '15M retest found; waiting for 5M confirmation';
    } else {
      const trade = calculateTrade(m5, d12);

      if (!trade) {
        reason = 'Unable to calculate a valid swing-based stop-loss';
      } else {
        ({ entry, sl, tp } = trade);
        signal = d12;
        reason = '12H + 1H Ichimoku, 15M fractal break/retest, 5M confirmation';
      }
    }

    const result = {
      market: market.key,
      symbol,
      signal,
      status: signal,
      direction12H: d12,
      direction1H: d1,
      direction15M: setup.direction,
      entry,
      sl,
      tp,
      reason,
      scannedAt: new Date().toISOString()
    };

    if (signal === 'BUY' || signal === 'SELL') {
      const key = `${market.key}:${signal}`;
      const previousAlert = lastAlerts.get(key) || 0;

      if (Date.now() - previousAlert > 1800000) {
        await telegram(
          `DERIV ${signal} SIGNAL\n` +
          `Market: ${market.key}\n` +
          `Symbol: ${symbol}\n` +
          `Entry: ${entry}\nSL: ${sl}\nTP: ${tp}\n` +
          `Risk/Reward: 1:${RR}\n` +
          `12H Ichimoku: ${d12}\n` +
          `1H Ichimoku: ${d1}\n` +
          `15M Fractal: ${setup.direction}\n` +
          `${reason}\n` +
          'Signal only — no automatic trade executed.'
        );

        lastAlerts.set(key, Date.now());
      }
    }

    return result;
  } catch (error) {
    log(`SCAN ERROR ${market.key}:`, error.message);

    return {
      market: market.key,
      symbol,
      signal: 'WAIT',
      status: 'ERROR',
      reason: error.message
    };
  }
}

async function runScan() {
  if (scanning || !state.connected) {
    if (!state.connected) log('Scan skipped: Deriv disconnected');
    return;
  }

  scanning = true;
  state.scanning = true;
  log('========== SCAN START ==========');

  let completed = 0;

  try {
    for (const market of MARKETS) {
      if (!state.connected) break;

      state.results[market.key] = await scanMarket(market);
      completed++;

      const result = state.results[market.key];

      log(
        `${market.key}: ${result.signal || 'WAIT'} | ` +
        `${result.reason || result.status}`
      );

      // Pause between markets to reduce request bursts.
      await new Promise(resolve => setTimeout(resolve, 700));
    }

    state.lastScan = new Date().toISOString();
    state.lastError = null;
    log(`========== SCAN FINISHED: ${completed}/${MARKETS.length} ==========`);
  } catch (error) {
    state.lastError = error.message;
    log('SCAN FAILED:', error.message);
  } finally {
    scanning = false;
    state.scanning = false;
  }
}

function connect() {
  clearTimeout(reconnectTimer);

  if (pingTimer) clearInterval(pingTimer);

  if (socket) {
    try {
      socket.terminate();
    } catch (_) {}
  }

  reconnectAttempts++;
  log(`Connecting to Deriv (attempt ${reconnectAttempts}): ${WS_URL}`);

  socket = new WebSocket(WS_URL, { handshakeTimeout: 20000 });

  socket.on('open', async () => {
    state.connected = true;
    state.lastError = null;
    reconnectAttempts = 0;

    log('SUCCESS: Connected to Deriv WebSocket');

    pingTimer = setInterval(() => {
      send({ ping: 1 });
    }, 45000);

    try {
      const response = await request({
        active_symbols: 'brief',
        product_type: 'basic'
      });

      if (response.active_symbols) {
        resolveSymbols(response.active_symbols);
      } else {
        throw new Error('Deriv returned no active_symbols list');
      }

      if (!scanTimer) {
        scanTimer = setInterval(runScan, SCAN_INTERVAL);
      }

      await runScan();
    } catch (error) {
      state.lastError = error.message;
      log('STARTUP / SYMBOL DISCOVERY ERROR:', error.message);
    }
  });

  socket.on('message', raw => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch (_) {
      log('Ignored invalid WebSocket JSON');
      return;
    }

    if (data.req_id && pending.has(data.req_id)) {
      const item = pending.get(data.req_id);
      clearTimeout(item.timer);
      pending.delete(data.req_id);

      if (data.error) {
        item.reject(new Error(
          `${data.error.code || 'DerivError'}: ${data.error.message || 'API error'}`
        ));
      } else {
        item.resolve(data);
      }

      return;
    }

    if (data.error) {
      state.lastError = data.error.message;
      log('DERIV API ERROR:', data.error.message);
    }
  });

  socket.on('error', error => {
    state.lastError = error.message;
    log('WEBSOCKET ERROR:', error.message);
  });

  socket.on('close', (code, reason) => {
    state.connected = false;

    if (pingTimer) clearInterval(pingTimer);
    pingTimer = null;

    state.lastError = `WebSocket closed: ${code} ${reason.toString()}`;
    log(state.lastError);

    for (const [id, item] of pending) {
      clearTimeout(item.timer);
      item.reject(new Error('Deriv connection closed'));
      pending.delete(id);
    }

    reconnectTimer = setTimeout(connect, 5000);
  });
}

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    connected: state.connected,
    scanning: state.scanning,
    lastScan: state.lastScan,
    lastError: state.lastError,
    resolved: Object.keys(state.symbols).length,
    totalMarkets: MARKETS.length
  });
});

app.get('/api/status', (req, res) => {
  res.json({
    connected: state.connected,
    scanning: state.scanning,
    lastScan: state.lastScan,
    lastError: state.lastError,
    totalMarkets: MARKETS.length,
    resolved: Object.keys(state.symbols).length,
    results: state.results
  });
});

app.get('/api/symbols', (req, res) => {
  res.json({
    symbols: state.symbols,
    resolved: Object.keys(state.symbols).length,
    totalMarkets: MARKETS.length
  });
});

app.post('/api/scan', (req, res) => {
  if (!state.connected) {
    return res.status(503).json({
      ok: false,
      error: 'Deriv is not connected'
    });
  }

  runScan();
  res.json({ ok: true, message: 'Scan requested' });
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
  log(`SERVER STARTED on port ${PORT}`);
  log(`Configured markets: ${MARKETS.length}`);
  log('Strategy: 12H Ichimoku -> 1H Ichimoku -> 15M fractal break/retest -> 5M confirmation');
  log('Mode: SIGNAL ONLY; no automatic trades');
  connect();
});

process.on('unhandledRejection', error => {
  log('UNHANDLED REJECTION:', error?.message || error);
});

process.on('uncaughtException', error => {
  log('UNCAUGHT EXCEPTION:', error.message);
});
