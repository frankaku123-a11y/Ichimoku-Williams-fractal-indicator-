# Deriv Ichimoku + Williams Fractals Signal Bot

Separate Node.js/Express project for Deriv public market data. It does **not** place trades.

## Requested markets

- 19 forex pairs: EUR/USD, GBP/USD, USD/JPY, GBP/JPY, EUR/JPY, AUD/USD, USD/CAD, USD/CHF, AUD/JPY, NZD/USD, EUR/GBP, EUR/AUD, GBP/AUD, GBP/CAD, AUD/CAD, NZD/JPY, CAD/JPY, CHF/JPY, EUR/CHF
- Gold: XAU/USD
- Indices: US 100, US 500, US 30

Deriv symbol names can differ by product/account/region. The app calls `active_symbols` and attempts to match names. Any unresolved instrument stays marked as missing; do not assume a requested market is available.

## Run locally

1. Install Node.js 18 or later.
2. In this folder run `npm install`.
3. Copy `.env.example` to `.env` and set values as needed (or add environment variables directly).
4. Run `npm start`.
5. Open `http://localhost:3000`.

## Railway

1. Create a new GitHub repository and upload `server.js`, `package.json`, `README.md`, `.env.example`, and the `public` folder.
2. In Railway, create a new project from that repository.
3. Set variables under Railway Variables: `DERIV_APP_ID`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, optionally `SCAN_INTERVAL_MS` and `RISK_REWARD`.
4. Generate a public domain and open it.

Never commit real Telegram tokens or other secrets to GitHub.

## Environment variables

- `PORT` — set automatically by Railway, default 3000.
- `DERIV_APP_ID` — Deriv application ID; defaults to public example ID 1089. Use your own app ID for a persistent deployment.
- `TELEGRAM_BOT_TOKEN` — optional; if empty, Telegram alerts are disabled.
- `TELEGRAM_CHAT_ID` — optional; if empty, Telegram alerts are disabled.
- `SCAN_INTERVAL_MS` — minimum 60000, default 60000.
- `RISK_REWARD` — default 2 (target 1:2).

## Strategy outline

- 12H Ichimoku lengths 20/60/120, derived by aggregating hourly candles into UTC-aligned 12H candles.
- 1H Ichimoku lengths 8/24/48 plus a simple structure bias.
- 15M Williams Fractals with `n = 2`, requiring two closed candles to the right before confirming a fractal.
- A 15M trend-line break/retest/rejection assessment and a 5M candle confirmation.
- Entry at the latest closed 5M candle close; initial stop distance is based on recent 5M average range; target defaults to 2R.

## Important limitations before live use

This is a starter implementation and must be tested on a demo account. The original Pine scripts include chart display/table details that do not all translate into trading rules. The Ichimoku trend calculation is implemented from its numeric components, but the displaced cloud / Chikou presentation is not an exact full reproduction of TradingView's visual behavior. The retest detector is a first-pass rule-based approximation. Stop-loss sizing is a simple recent-range estimate, not broker-validated risk sizing. No trade execution is included.

Deriv may reject or not offer some requested symbols, and its public API may impose rate/connection limits. The dashboard shows missing symbols and request errors. Verify candle availability, exact instrument names, signal behavior, and market hours before relying on alerts.
