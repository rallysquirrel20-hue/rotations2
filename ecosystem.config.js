const path = require('path');

// Signal Pipeline Processes
// All three use exchange_calendars (NYSE) to determine trading days.
// Calendars auto-refresh on Jan 1 each year.
//
// rot-universes  — Runs build_universes.py at 5pm ET on the last trading day
//                  of each quarter (Mar 31, Jun 30, Sep 30, Dec 31 2026).
//                  Builds: top 500 stocks by dollar volume, top 50 ETFs,
//                  thematic universes (beta, momentum, risk-adj momentum,
//                  dividends, size, volume leaders/losers), GICS sector/industry
//                  mappings. Industries filtered to top 25% by dollar volume
//                  per quarter (INDUSTRY_TOP_PCT=0.25, min 3 tickers).
//                  Checks hourly.
//
// rot-signals    — Runs build_signals.py then build_baskets.py at 5pm ET on
//                  every trading day (~251/year). build_signals computes
//                  individual ticker signals (numba-accelerated). build_baskets
//                  computes basket OHLC, breadth, breakout%, correlation%,
//                  and contributions. Daily runs use incremental append (not
//                  full rebuild) and only process current-quarter industries
//                  (~38 baskets vs ~70 on --force). Checks every 5 min.
//
// rot-live       — Runs live_updates.py every 5 min between 9:30am–4:00pm ET
//                  on trading days. Writes full signal rows (not just OHLC) to
//                  live_signals_500.parquet and live basket OHLC for current-
//                  quarter baskets to live_basket_signals_500.parquet.
//
// Data flows:  build_universes → build_signals → build_baskets (daily caches)
//              live_updates reads those caches + Databento for intraday signals

module.exports = {
  apps: [
    {
      name: "rotations-frontend",
      cwd: path.join(__dirname, "app", "frontend"),
      script: path.join(__dirname, "app", "frontend", "node_modules", "vite", "bin", "vite.js"),
      args: "dev",
      env: {
        NODE_ENV: "development",
      },
    },
    {
      name: "rotations-backend",
      cwd: path.join(__dirname, "app", "backend"),
      interpreter: path.join(__dirname, "app", "backend", "venv", "Scripts", "python.exe"),
      script: "main.py",
      env: {
        PYTHONPATH: path.join(__dirname, "app", "backend"),
      },
    },
    {
      name: "rot-universes",
      cwd: path.join(__dirname, "signals"),
      script: "loop_universes.py",
      interpreter: path.join(process.env.LOCALAPPDATA, "Python", "pythoncore-3.14-64", "python.exe"),
      autorestart: true,
      restart_delay: 60000,
      max_restarts: 10,
      min_uptime: 300000,
      kill_timeout: 10000,
    },
    {
      name: "rot-signals",
      cwd: path.join(__dirname, "signals"),
      script: "loop_signals.py",
      interpreter: path.join(process.env.LOCALAPPDATA, "Python", "pythoncore-3.14-64", "python.exe"),
      autorestart: true,
      restart_delay: 60000,
      max_restarts: 10,
      min_uptime: 300000,
      kill_timeout: 10000,
    },
    {
      name: "rot-live",
      cwd: path.join(__dirname, "signals"),
      script: "loop_live.py",
      interpreter: path.join(process.env.LOCALAPPDATA, "Python", "pythoncore-3.14-64", "python.exe"),
      autorestart: true,
      restart_delay: 30000,
      max_restarts: 50,
      min_uptime: 60000,
      kill_timeout: 10000,
    },
  ],
};
