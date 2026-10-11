
'use strict';

const express = require('express');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const APP_ID = process.env.DERIV_APP_ID || '1089';
const RR = 2;
const SCAN_INTERVAL = 300000;

const WS_URL =
  `wss://ws.derivws.com/websockets/v3?app_id=${encodeURIComponent(APP_ID)}`;

const MARKETS = [
  // Forex — 30 pairs
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

  // Gold
  {
    key: 'XAU/USD',
    aliases: ['XAU/USD', 'XAUUSD', 'GOLD/USD', 'GOLD']
  },

  // US stock indices
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
    status: 'Waiting for connection',
    reason: 'Waiting for market data'
  };
}

let socket;
let reqId = 1;
let reconnectTimer;
let scanTimer;
let pingTimer;
let scanning = false;
const pending = new Map();
const lastAlerts = new Map();

function norm(s) {
  return String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function send(payload) {
  if (!socket || socket.readyState !== WebSocket.OPEN) return false;
  socket.send(JSON.stringify(payload));
  return true;
}

function request(payload, timeout = 25000) {
  return new Promise((resolve, reject) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return reject(new Error('Deriv is not connected'));
    }

    const id = reqId++;
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error('Deriv request timed out'));
    }, timeout);

    pending.set(id, { resolve, reject, timer });

    if (!send({ ...payload, req_id: id })) {
      clearTimeout(timer);
      pending.delete(id);
      reject(new Error('Could not send request'));
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
      resolved[market.key] = found.symbol || found.underlying_symbol;
    } else {
      console.log('Symbol unavailable:', market.key);
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
        : 'Not matched in Deriv active symbols'
    };
  }

  console.log(`Resolved ${Object.keys(resolved).length}/${MARKETS.length} markets`);
}

function parseCandles(response) {
  const candles = response.candles ||
    response.history?.candles || [];

  return candles.map(c => ({
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

async function getCandles(symbol, granularity, count) {
  const result = await request({
    ticks_history: symbol,
    adjust_start_time: 1,
    count,
    end: 'latest',
    granularity,
    style: 'candles'
  }, 30000);

  return parseCandles(result);
}

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

function range(candles, i, n) {
  if (i < n - 1) return null;
  const slice = candles.slice(i - n + 1, i + 1);
  return {
    high: Math.max(...slice.map(c => c.high)),
    low: Math.min(...slice.map(c => c.low))
  };
}

function direction(candles, tenkanN, kijunN, spanN) {
  const i = candles.length - 2;
  if (i < spanN - 1) return 'WAIT';

  const a = range(candles, i, tenkanN);
  const b = range(candles, i, kijunN);
  const c = range(candles, i, spanN);
  if (!a || !b || !c) return 'WAIT';

  const tenkan = (a.high + a.low) / 2;
  const kijun = (b.high + b.low) / 2;
  const spanA = (tenkan + kijun) / 2;
  const spanB = (c.high + c.low) / 2;
  const top = Math.max(spanA, spanB);
  const bottom = Math.min(spanA, spanB);
  const close = candles[i].close;

  if (close > top && tenkan > kijun) return 'BUY';
  if (close < bottom && tenkan < kijun) return 'SELL';
  return 'WAIT';
}

function fractals(candles, period = 2) {
  const highs = [];
  const lows = [];

  for (let i = period; i < candles.length - period; i++) {
    let high = true;
    let low = true;

    for (let j = 1; j <= period; j++) {
      if (
        candles[i].high <= candles[i-j].high ||
        candles[i].high <= candles[i+j].high
      ) high = false;

      if (
        candles[i].low >= candles[i-j].low ||
        candles[i].low >= candles[i+j].low
      ) low = false;
    }

    if (high) highs.push({ index: i, price: candles[i].high });
    if (low) lows.push({ index: i, price: candles[i].low });
  }

  return { highs, lows };
}

function trendLine(a, b, i) {
  if (!a || !b || a.index === b.index) return null;
  return a.price +
    ((b.price - a.price) / (b.index - a.index)) * (i - a.index);
}

function checkBreak(candles) {
  if (candles.length < 10) {
    return { direction: 'WAIT', reason: 'Not enough 15M candles' };
  }

  const { highs, lows } = fractals(candles);
  const i = candles.length - 2;
  const prev = candles[i - 1];
  const last = candles[i];

  const hs = highs.filter(x => x.index < i).slice(-2);
  const ls = lows.filter(x => x.index < i).slice(-2);

  if (hs.length === 2) {
    const before = trendLine(hs[0], hs[1], i - 1);
    const now = trendLine(hs[0], hs[1], i);

    if (before !== null && now !== null &&
        prev.close >= before && last.close < now) {
      return {
        direction: 'SELL',
        level: now,
        reason: '15M candle closed below fractal trend line'
      };
    }
  }

  if (ls.length === 2) {
    const before = trendLine(ls[0], ls[1], i - 1);
    const now = trendLine(ls[0], ls[1], i);

    if (before !== null && now !== null &&
        prev.close <= before && last.close > now) {
      return {
        direction: 'BUY',
        level: now,
        reason: '15M candle closed above fractal trend line'
      };
    }
  }

  return { direction: 'WAIT', reason: 'No confirmed 15M break' };
}

function confirm5M(candles, side) {
  if (candles.length < 4) return false;

  const last = candles[candles.length - 2];
  const prev = candles[candles.length - 3];

  if (side === 'BUY') {
    return last.close > last.open && last.close > prev.high;
  }

  if (side === 'SELL') {
    return last.close < last.open && last.close < prev.low;
  }

  return false;
}

async function telegram(message) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return;

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
    if (!result.ok) console.error('Telegram error:', result.description);
  } catch (e) {
    console.error('Telegram error:', e.message);
  }
}

async function scanMarket(market) {
  const symbol = state.symbols[market.key];

  if (!symbol) {
    return {
      market: market.key,
      signal: 'WAIT',
      status: 'Symbol unavailable',
      reason: 'No matching Deriv symbol'
    };
  }

  try {
    const [h1, m15, m5] = await Promise.all([
      getCandles(symbol, 3600, 1500),
      getCandles(symbol, 900, 220),
      getCandles(symbol, 300, 220)
    ]);

    if (h1.length < 150 || m15.length < 10 || m5.length < 10) {
      return {
        market: market.key,
        symbol,
        signal: 'WAIT',
        status: 'WAIT',
        reason: 'Insufficient candle history'
      };
    }

    const h12 = aggregate(h1, 43200);
    const d12 = direction(h12, 20, 60, 120);
    const d1 = direction(h1, 8, 24, 48);
    const br = checkBreak(m15);

    let signal = 'WAIT';
    let entry = null;
    let sl = null;
    let tp = null;
    let reason = 'Waiting for timeframe confirmation';

    if (d12 === 'WAIT') {
      reason = '12H direction not confirmed';
    } else if (d1 !== d12) {
      reason = '1H direction does not match 12H';
    } else if (br.direction !== d12) {
      reason = 'Waiting for matching 15M fractal break';
    } else {
      const c15 = m15[m15.length - 2];
      const c5 = m5[m5.length - 2];
      const level = br.level;

      const retest = d12 === 'BUY'
        ? c15.low <= level && c15.close > level
        : c15.high >= level && c15.close < level;

      if (!retest) {
        reason = 'Waiting for 15M retest';
      } else if (!confirm5M(m5, d12)) {
        reason = 'Waiting for 5M confirmation';
      } else {
        entry = c5.close;
        const recent = m5.slice(-16, -2);
        const lo = Math.min(...recent.map(c => c.low));
        const hi = Math.max(...recent.map(c => c.high));
        const buffer = Math.max(Math.abs(entry) * 0.0002, (hi - lo) * 0.1);

        if (d12 === 'BUY') {
          sl = lo - buffer;
          tp = entry + (entry - sl) * RR;
        } else {
          sl = hi + buffer;
          tp = entry - (sl - entry) * RR;
        }

        signal = d12;
        reason = '12H + 1H + 15M break/retest + 5M confirmed';
      }
    }

    const result = {
      market: market.key,
      symbol,
      signal,
      status: signal,
      direction12H: d12,
      direction1H: d1,
      direction15M: br.direction,
      entry,
      sl,
      tp,
      reason,
      scannedAt: new Date().toISOString()
    };

    if (signal === 'BUY' || signal === 'SELL') {
      const alertKey = `${market.key}:${signal}`;
      const previous = lastAlerts.get(alertKey) || 0;

      if (Date.now() - previous > 1800000) {
        await telegram(
          `DERIV ${signal} SIGNAL\n` +
          `Market: ${market.key}\n` +
          `Symbol: ${symbol}\n` +
          `Entry: ${entry}\nSL: ${sl}\nTP: ${tp}\n` +
          `Risk/Reward: 1:${RR}\n` +
          `12H: ${d12} | 1H: ${d1} | 15M: ${br.direction}\n` +
          `${reason}\nSignal only — no automatic trade executed.`
        );
        lastAlerts.set(alertKey, Date.now());
      }
    }

    return result;
  } catch (e) {
    console.error(`Scan error ${market.key}:`, e.message);
    return {
      market: market.key,
      symbol,
      signal: 'WAIT',
      status: 'ERROR',
      reason: e.message
    };
  }
}

async function runScan() {
  if (scanning || !state.connected) return;

  scanning = true;
  state.scanning = true;

  try {
    for (const market of MARKETS) {
      if (!state.connected) break;

      state.results[market.key] = await scanMarket(market);

      // Space out markets to reduce request bursts.
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    state.lastScan = new Date().toISOString();
  } catch (e) {
    state.lastError = e.message;
    console.error('Scan error:', e.message);
  } finally {
    scanning = false;
    state.scanning = false;
  }
}

function connect() {
  clearTimeout(reconnectTimer);

  if (pingTimer) clearInterval(pingTimer);

  if (socket) {
    try { socket.terminate(); } catch (_) {}
  }

  console.log('Connecting to Deriv:', WS_URL);
  socket = new WebSocket(WS_URL, { handshakeTimeout: 20000 });

  socket.on('open', async () => {
    state.connected = true;
    state.lastError = null;
    console.log('Connected to Deriv');

    pingTimer = setInterval(() => send({ ping: 1 }), 45000);

    try {
      const result = await request({
        active_symbols: 'brief',
        product_type: 'basic'
      });

      resolveSymbols(result.active_symbols || []);

      if (!scanTimer) scanTimer = setInterval(runScan, SCAN_INTERVAL);
      runScan();
    } catch (e) {
      state.lastError = e.message;
      console.error('Symbol discovery failed:', e.message);
    }
  });

  socket.on('message', raw => {
    let data;
    try { data = JSON.parse(raw.toString()); } catch (_) { return; }

    if (data.req_id && pending.has(data.req_id)) {
      const item = pending.get(data.req_id);
      clearTimeout(item.timer);
      pending.delete(data.req_id);

      if (data.error) {
        item.reject(new Error(data.error.message || 'Deriv API error'));
      } else {
        item.resolve(data);
      }
      return;
    }

    if (data.error) {
      state.lastError = data.error.message;
      console.error('Deriv error:', state.lastError);
    }
  });

  socket.on('error', e => {
    state.lastError = e.message;
    console.error('WebSocket error:', e.message);
  });

  socket.on('close', (code, reason) => {
    state.connected = false;
    if (pingTimer) clearInterval(pingTimer);
    pingTimer = null;

    state.lastError = `WebSocket closed: ${code} ${reason || ''}`;
    console.error(state.lastError);

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

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Configured markets: ${MARKETS.length}`);
  connect();
});
