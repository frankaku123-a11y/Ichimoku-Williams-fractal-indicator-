'use strict';

const express = require('express');
const WebSocket = require('ws');
const path = require('path');

const PORT = Number(process.env.PORT || 3000);
const DERIV_APP_ID = process.env.DERIV_APP_ID || '1089'; // Set your own Deriv app ID for production.
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
const SCAN_INTERVAL_MS = Math.max(60000, Number(process.env.SCAN_INTERVAL_MS || 60000));
const RISK_REWARD = Math.max(1, Number(process.env.RISK_REWARD || 2));
const FRACTAL_N = 2;
const WS_URL = `wss://ws.derivws.com/websockets/v3?app_id=${encodeURIComponent(DERIV_APP_ID)}`;

// The first 10 are the pairs from the previous bot. The next 9 forex pairs,
// gold, and three US indices make 23 requested markets in total.
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
  { key: 'XAU/USD', type: 'metal', names: ['XAU/USD', 'XAUUSD', 'GOLD', 'Gold'] },
  { key: 'US 100', type: 'index', names: ['US Tech 100', 'US 100', 'NASDAQ 100', 'Nasdaq 100', 'USTEC', 'US100'] },
  { key: 'US 500', type: 'index', names: ['US 500', 'US SP 500', 'S&P 500', 'US500', 'SPX500'] },
  { key: 'US 30', type: 'index', names: ['Wall Street 30', 'US 30', 'Dow Jones 30', 'US30', 'WS30'] }
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
  results: MARKETS.map(m => ({ market: m.key, type: m.type, symbol: null, status: 'Waiting for Deriv connection', signal: 'WAIT' }))
};

let socket;
let requestId = 1;
let reconnectTimer;
let scanTimer;
let scanRunning = false;
let activeSymbols = [];
const pending = new Map();
const candleCache = new Map();
const lastAlertByMarket = new Map();

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function norm(s) { return String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, ''); }
function medianRange(c) { return (Number(c.high) + Number(c.low)) / 2; }
function roundPrice(value, digits = 5) { return Number(Number(value).toFixed(digits)); }
function digitsFor(market, price) {
  if (market === 'XAU/USD') return 2;
  if (market.endsWith('/JPY')) return 3;
  if (market.startsWith('US ')) return 2;
  return price < 10 ? 5 : 3;
}
function safeSend(obj) {
  if (socket && socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(obj));
}
function derivRequest(payload, timeoutMs = 25000) {
  return new Promise((resolve, reject) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) return reject(new Error('Deriv WebSocket is not connected'));
    const req_id = requestId++;
    const timer = setTimeout(() => { pending.delete(req_id); reject(new Error(`Deriv request timed out (${payload.ticks_history || payload.active_symbols || 'request'})`)); }, timeoutMs);
    pending.set(req_id, { resolve, reject, timer });
    safeSend({ ...payload, req_id });
  });
}
function connectDeriv() {
  clearTimeout(reconnectTimer);
  try { if (socket) socket.terminate(); } catch (_) {}
  socket = new WebSocket(WS_URL);
  socket.on('open', async () => {
    state.connected = true;
    state.lastError = null;
    try {
      const response = await derivRequest({ active_symbols: 'brief', product_type: 'basic' });
      activeSymbols = response.active_symbols || [];
      resolveSymbols();
      if (!scanTimer) scanTimer = setInterval(runScan, SCAN_INTERVAL_MS);
      runScan();
    } catch (err) {
      state.lastError = err.message;
      state.connected = false;
    }
  });
  socket.on('message', raw => {
    let data;
    try { data = JSON.parse(raw.toString()); } catch (_) { return; }
    const item = data.req_id ? pending.get(data.req_id) : null;
    if (item) {
      clearTimeout(item.timer);
      pending.delete(data.req_id);
      if (data.error) item.reject(new Error(data.error.message || 'Deriv API error'));
      else item.resolve(data);
    }
    if (data.error && !item) state.lastError = data.error.message || 'Deriv API error';
  });
  socket.on('close', () => {
    state.connected = false;
    for (const [id, p] of pending) { clearTimeout(p.timer); p.reject(new Error('Deriv connection closed')); pending.delete(id); }
    reconnectTimer = setTimeout(connectDeriv, 5000);
  });
  socket.on('error', err => {
  state.lastError = err.message;
  console.error('Deriv WebSocket error:', err.message);
});
}

function resolveSymbols() {
  const symbols = activeSymbols.map(s => ({ raw: s, display: s.display_name || s.underlying_symbol_name || s.symbol || '', symbol: s.symbol || s.underlying_symbol || '' }));
  for (const market of MARKETS) {
    const aliases = market.names.map(norm);
    let match = symbols.find(s => aliases.includes(norm(s.display)) || aliases.includes(norm(s.symbol)));
    // Deriv forex symbols commonly use frxEURUSD-style codes.
    if (!match && market.type === 'forex') {
      const compact = norm(market.key);
      match = symbols.find(s => norm(s.symbol).endsWith(compact) || norm(s.display).endsWith(compact));
    }
    if (!match && market.type === 'index') {
      match = symbols.find(s => aliases.some(a => norm(s.display).includes(a) || norm(s.symbol).includes(a)));
    }
    if (!match && market.type === 'metal') {
      match = symbols.find(s => /GOLD|XAUUSD|XAU\/USD/i.test(`${s.display} ${s.symbol}`));
    }
    market.symbol = match ? match.symbol : null;
    market.resolvedName = match ? match.display : null;
  }
  state.symbolsResolved = MARKETS.filter(m => m.symbol).length;
  state.results = MARKETS.map(m => ({ market: m.key, type: m.type, symbol: m.symbol, resolvedName: m.resolvedName || null, status: m.symbol ? 'Ready' : 'Symbol not found in Deriv active_symbols', signal: 'WAIT' }));
}

function toCandles(response) {
  if (response.candles) return response.candles.map(c => ({ epoch: Number(c.epoch), open: Number(c.open), high: Number(c.high), low: Number(c.low), close: Number(c.close) })).sort((a,b) => a.epoch-b.epoch);
  if (response.history && response.history.prices && response.history.times) {
    return response.history.times.map((t, i) => ({ epoch: Number(t), open: Number(response.history.prices[i]), high: Number(response.history.prices[i]), low: Number(response.history.prices[i]), close: Number(response.history.prices[i]) }));
  }
  return [];
}
async function fetchCandles(symbol, granularity, count) {
  const response = await derivRequest({ ticks_history: symbol, style: 'candles', granularity, count, end: 'latest' });
  return toCandles(response);
}
function aggregateCandles(candles, blockSeconds) {
  const buckets = new Map();
  for (const c of candles) {
    const start = Math.floor(c.epoch / blockSeconds) * blockSeconds;
    const b = buckets.get(start);
    if (!b) buckets.set(start, { epoch: start, open: c.open, high: c.high, low: c.low, close: c.close });
    else { b.high = Math.max(b.high, c.high); b.low = Math.min(b.low, c.low); b.close = c.close; }
  }
  return [...buckets.values()].sort((a,b) => a.epoch-b.epoch);
}
function donchian(candles, endIndex, length) {
  if (endIndex - length + 1 < 0) return null;
  let hi = -Infinity, lo = Infinity;
  for (let i = endIndex - length + 1; i <= endIndex; i++) { hi = Math.max(hi, candles[i].high); lo = Math.min(lo, candles[i].low); }
  return (hi + lo) / 2;
}
function ichimoku(candles, lengths) {
  const i = candles.length - 2; // last fully closed candle; latest candle may still be forming
  if (i < Math.max(lengths.conversion, lengths.base, lengths.spanB) - 1 || i < 1) return null;
  const conv = donchian(candles, i, lengths.conversion);
  const base = donchian(candles, i, lengths.base);
  const spanA = (conv + base) / 2;
  const spanB = donchian(candles, i, lengths.spanB);
  const close = candles[i].close;
  const top = Math.max(spanA, spanB), bottom = Math.min(spanA, spanB);
  const prevConv = donchian(candles, i - 1, lengths.conversion);
  const prevBase = donchian(candles, i - 1, lengths.base);
  return { close, conv, base, spanA, spanB, top, bottom, bullish: close > top && conv >= base, bearish: close < bottom && conv <= base, crossUp: prevConv <= prevBase && conv > base, crossDown: prevConv >= prevBase && conv < base, inside: close >= bottom && close <= top, candle: candles[i] };
}
function findFractals(candles, n = 2) {
  const highs = [], lows = [];
  // Exclude the most recent still-forming candle; each fractal requires n closed bars on its right.
  const lastClosed = candles.length - 2;
  for (let i = n; i <= lastClosed - n; i++) {
    let isHigh = true, isLow = true;
    for (let j = 1; j <= n; j++) {
      if (candles[i-j].high >= candles[i].high || candles[i+j].high >= candles[i].high) isHigh = false;
      if (candles[i-j].low <= candles[i].low || candles[i+j].low <= candles[i].low) isLow = false;
    }
    if (isHigh) highs.push({ index: i, epoch: candles[i].epoch, price: candles[i].high });
    if (isLow) lows.push({ index: i, epoch: candles[i].epoch, price: candles[i].low });
  }
  return { highs, lows };
}
function lineAt(p1, p2, index) {
  if (!p1 || !p2 || p2.index === p1.index) return null;
  return p1.price + (p2.price - p1.price) * ((index - p1.index) / (p2.index - p1.index));
}
function assessFractalBreak(candles) {
  const fr = findFractals(candles, FRACTAL_N);
  const i = candles.length - 2;
  if (i < 10) return { direction: null, reason: 'Waiting for candles' };
  const close = candles[i].close;
  const prevClose = candles[i-1].close;
  const lastHighs = fr.highs.filter(p => p.index < i).slice(-2);
  const lastLows = fr.lows.filter(p => p.index < i).slice(-2);
  let direction = null, line = null, anchor1 = null, anchor2 = null;
  if (lastHighs.length === 2) {
    line = lineAt(lastHighs[0], lastHighs[1], i);
    const prevLine = lineAt(lastHighs[0], lastHighs[1], i - 1);
    if (line != null && prevLine != null && prevClose >= prevLine && close < line) { direction = 'SELL'; anchor1 = lastHighs[0]; anchor2 = lastHighs[1]; }
  }
  if (!direction && lastLows.length === 2) {
    line = lineAt(lastLows[0], lastLows[1], i);
    const prevLine = lineAt(lastLows[0], lastLows[1], i - 1);
    if (line != null && prevLine != null && prevClose <= prevLine && close > line) { direction = 'BUY'; anchor1 = lastLows[0]; anchor2 = lastLows[1]; }
  }
  // Retest/rejection check in the last four closed 15m candles. This is a practical
  // first version; verify its behavior on demo before relying on alerts.
  if (!direction) {
    for (let k = Math.max(2, i - 4); k <= i; k++) {
      const c = candles[k];
      const highs = fr.highs.filter(p => p.index < k).slice(-2);
      const lows = fr.lows.filter(p => p.index < k).slice(-2);
      if (highs.length === 2) {
        const lv = lineAt(highs[0], highs[1], k);
        if (lv != null && c.high >= lv && c.close < lv && c.close < c.open) return { direction: 'SELL', reason: 'Fractal line retest/rejection', line: lv, candle: candles[i] };
      }
      if (lows.length === 2) {
        const lv = lineAt(lows[0], lows[1], k);
        if (lv != null && c.low <= lv && c.close > lv && c.close > c.open) return { direction: 'BUY', reason: 'Fractal line retest/rejection', line: lv, candle: candles[i] };
      }
    }
  }
  return direction ? { direction, reason: 'Closed candle broke fractal trend line', line, anchor1, anchor2, candle: candles[i] } : { direction: null, reason: 'No confirmed fractal break/retest' };
}
function getFiveMinuteConfirmation(candles, direction) {
  if (candles.length < 4) return false;
  const i = candles.length - 2;
  const c = candles[i], prev = candles[i-1];
  return direction === 'BUY' ? c.close > c.open && c.close > prev.close : c.close < c.open && c.close < prev.close;
}
function get1HStructure(candles) {
  const i = candles.length - 2;
  if (i < 10) return 'NEUTRAL';
  const closes = candles.slice(Math.max(0, i-5), i+1).map(c => c.close);
  const ups = closes.every((v, k) => k === 0 || v >= closes[k-1]);
  const downs = closes.every((v, k) => k === 0 || v <= closes[k-1]);
  if (ups) return 'BULLISH';
  if (downs) return 'BEARISH';
  return candles[i].close > candles[i-5].close ? 'BULLISH' : candles[i].close < candles[i-5].close ? 'BEARISH' : 'NEUTRAL';
}
async function sendTelegram(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  // Built-in fetch is available in Node 18+.
  try {
    const response = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text, disable_web_page_preview: true }) });
    if (!response.ok) throw new Error(`Telegram HTTP ${response.status}`);
  } catch (err) { state.lastError = `Telegram: ${err.message}`; }
}
function buildAlert(result) {
  return `DERIV ${result.signal} SIGNAL\nMarket: ${result.market}\nSymbol: ${result.symbol}\nDirection: ${result.signal}\nEntry: ${result.entry}\nStop Loss: ${result.stopLoss}\nTake Profit: ${result.takeProfit}\nRisk:Reward: 1:${RISK_REWARD}\n12H Ichimoku: ${result.ichimoku12H}\n1H Structure: ${result.structure1H}\n15M Fractal: ${result.fractalReason}\n5M Confirmation: confirmed\nSignal-only alert; verify spread and market availability.`;
}
async function scanMarket(market) {
  if (!market.symbol) return { market: market.key, type: market.type, symbol: null, status: 'Symbol not found in Deriv active_symbols', signal: 'WAIT' };
  const cacheKey = market.symbol;
  let cache = candleCache.get(cacheKey) || { h1: [], m15: [], m5: [], fetchedAt: {} };
  const now = Date.now();
  // Load H1 once per hour; it is also aggregated into UTC-aligned 12H candles.
  if (!cache.h1.length || now - (cache.fetchedAt.h1 || 0) > 55 * 60 * 1000) {
    cache.h1 = await fetchCandles(market.symbol, 3600, 1500);
    cache.fetchedAt.h1 = now;
    await sleep(180);
  }
  if (!cache.m15.length || now - (cache.fetchedAt.m15 || 0) > 14 * 60 * 1000) {
    cache.m15 = await fetchCandles(market.symbol, 900, 220);
    cache.fetchedAt.m15 = now;
    await sleep(180);
  }
  if (!cache.m5.length || now - (cache.fetchedAt.m5 || 0) > 4 * 60 * 1000) {
    cache.m5 = await fetchCandles(market.symbol, 300, 220);
    cache.fetchedAt.m5 = now;
  }
  candleCache.set(cacheKey, cache);

  const h12 = aggregateCandles(cache.h1, 43200);
  const ichi12 = ichimoku(h12, { conversion: 20, base: 60, spanB: 120 });
  const ichi1 = ichimoku(cache.h1, { conversion: 8, base: 24, spanB: 48 });
  const structure = get1HStructure(cache.h1);
  const fractal = assessFractalBreak(cache.m15);
  const price = cache.m5.length ? cache.m5[cache.m5.length - 2].close : null;
  const digits = digitsFor(market.key, price || 1);

  let signal = 'WAIT', status = 'Waiting for complete setup', entry = null, stopLoss = null, takeProfit = null;
  let reason = 'Need 12H trend, 1H structure, 15M fractal event, and 5M confirmation';
  if (ichi12 && ichi1 && fractal.direction && !ichi12.inside && !ichi1.inside) {
    const trendAllows = fractal.direction === 'BUY'
      ? ichi12.bullish && ichi1.bullish && structure !== 'BEARISH'
      : ichi12.bearish && ichi1.bearish && structure !== 'BULLISH';
    const confirmed5m = getFiveMinuteConfirmation(cache.m5, fractal.direction);
    if (trendAllows && confirmed5m) {
      const c = cache.m5[cache.m5.length - 2];
      const recent = cache.m5.slice(-15, -1);
      const atrLike = recent.length ? recent.reduce((sum, x) => sum + (x.high - x.low), 0) / recent.length : c.high - c.low;
      const risk = Math.max(atrLike, Math.abs(c.close - c.open), Math.abs(c.close) * 0.0001);
      signal = fractal.direction;
      entry = roundPrice(c.close, digits);
      stopLoss = roundPrice(signal === 'BUY' ? entry - risk : entry + risk, digits);
      takeProfit = roundPrice(signal === 'BUY' ? entry + risk * RISK_REWARD : entry - risk * RISK_REWARD, digits);
      status = 'Signal conditions met';
      reason = '12H Ichimoku + 1H structure + 15M fractal + 5M confirmation';
    } else if (!trendAllows) reason = 'Fractal event found, but higher-timeframe trend/structure does not agree';
    else reason = 'Fractal event found; waiting for 5M candle confirmation';
  } else if (!ichi12 || !ichi1) reason = 'Not enough candles yet for Ichimoku calculations';
  else if (ichi12.inside || ichi1.inside) reason = 'Price is inside an Ichimoku cloud; trend unclear';

  const result = {
    market: market.key, type: market.type, symbol: market.symbol, resolvedName: market.resolvedName || null,
    status, signal, entry, stopLoss, takeProfit, price: price == null ? null : roundPrice(price, digits),
    ichimoku12H: !ichi12 ? 'NOT READY' : ichi12.bullish ? 'BULLISH' : ichi12.bearish ? 'BEARISH' : 'NEUTRAL',
    ichimoku1H: !ichi1 ? 'NOT READY' : ichi1.bullish ? 'BULLISH' : ichi1.bearish ? 'BEARISH' : 'NEUTRAL',
    structure1H: structure, fractalReason: fractal.reason, reason, updatedAt: new Date().toISOString()
  };
  if ((signal === 'BUY' || signal === 'SELL') && lastAlertByMarket.get(market.key) !== `${signal}:${cache.m5[cache.m5.length - 2].epoch}`) {
    lastAlertByMarket.set(market.key, `${signal}:${cache.m5[cache.m5.length - 2].epoch}`);
    await sendTelegram(buildAlert(result));
  }
  return result;
}
async function runScan() {
  if (scanRunning || !state.connected) return;
  scanRunning = true;
  try {
    // Sequential requests are intentionally paced to reduce API/socket pressure.
    for (const market of MARKETS) {
      try {
        const result = await scanMarket(market);
        const index = state.results.findIndex(r => r.market === market.key);
        if (index >= 0) state.results[index] = result;
      } catch (err) {
        const index = state.results.findIndex(r => r.market === market.key);
        const failure = { market: market.key, type: market.type, symbol: market.symbol || null, status: 'Data request failed', signal: 'WAIT', reason: err.message, error: err.message, updatedAt: new Date().toISOString() };
        if (index >= 0) state.results[index] = failure;
        state.lastError = `${market.key}: ${err.message}`;
        await sleep(500);
      }
      await sleep(150);
    }
    state.lastScan = new Date().toISOString();
  } finally { scanRunning = false; }
}

app.get('/api/status', (_req, res) => res.json({ ...state, scanIntervalMs: SCAN_INTERVAL_MS, riskReward: RISK_REWARD }));
app.post('/api/scan', (_req, res) => {
  if (!state.connected) return res.status(503).json({ ok: false, error: 'Deriv is not connected yet.' });
  runScan();
  res.json({ ok: true, message: 'Scan started.' });
});
app.get('/api/symbols', (_req, res) => res.json({ resolved: state.symbolsResolved, requested: MARKETS.length, symbols: MARKETS.map(m => ({ market: m.key, type: m.type, symbol: m.symbol || null, resolvedName: m.resolvedName || null })) }));
app.get('/health', (_req, res) => res.json({ ok: true, connected: state.connected, lastScan: state.lastScan }));
app.listen(PORT, () => console.log(`Ichimoku + Williams Fractals bot listening on ${PORT}`));
connectDeriv();
