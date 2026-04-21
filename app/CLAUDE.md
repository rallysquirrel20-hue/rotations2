# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Full-stack financial analysis dashboard for visualizing stock rotation signals, basket composition, correlations, and cumulative returns. Consumes pre-computed signal data from the `rotations_signals` project and adds real-time intraday capabilities via Databento.

## Tech Stack

- **Backend:** FastAPI (Python), Pandas, NumPy, Databento API
- **Frontend:** React 18 + TypeScript, Vite, lightweight-charts (TradingView), Axios
- **Data:** Parquet/JSON caches from `rotations_signals`, Databento for live data

## Commands

All commands are run from the repository root.

```bash
# Setup & Sync (Clone or Refresh - Warning: deletes local changes)
rm -rf ./rotations_app && git clone https://github.com/rallysquirrel20-hue/rotations_app.git

# Backend — activate venv then start server
cd backend
python -m venv venv                # first time only
source venv/bin/activate           # Linux/macOS
# .\venv\Scripts\Activate.ps1     # Windows (PowerShell)
pip install -r requirements.txt    # or: pip install fastapi uvicorn pandas numpy databento python-dotenv pyarrow
python main.py                     # starts uvicorn on http://0.0.0.0:8000

# Frontend — install deps then start dev server
cd frontend
npm install                        # first time only
npm run dev                        # Dev server on http://localhost:5173
npm run build                      # tsc -b && vite build
npm run lint                       # eslint .
```

## Environment Variables

The backend auto-loads `.env` from the local `backend/` directory first, falling back to `~/Documents/Repositories/.env`. Place the file in either location — no manual copying needed.

- `DATABENTO_API_KEY` — Required for live/intraday data
- `DATABENTO_DATASET` — Default: `EQUS.MINI`
- `DATABENTO_STYPE_IN` — Default: `raw_symbol`
- `DATABENTO_LOOKBACK_DAYS` — Default: 90 (intraday history depth)
- `DATABENTO_SYMBOL_CHUNK` — Default: 200 (symbols per API call)
- `INTRADAY_RTH_ONLY` — Default: True (filter to 09:30–15:59 NY)
- `FORCE_REBUILD_INTRADAY_CACHE` — Default: False
- `PYTHON_OUTPUTS_DIR` — Base path for cached data (default: `~/Documents/Python_Outputs`)

## Architecture

### Backend (`backend/`)

**`main.py`** — FastAPI server with REST endpoints and WebSocket streaming.

API endpoints:
- `GET /api/baskets` — Lists all baskets (Themes, Sectors, Industries)
- Optional query params: `universe_start` / `universe_end` (quarter keys like `2026 Q1`) widen industry inclusion to any active quarter in the selected window; default is current quarter only.
- `GET /api/baskets/returns` — Cross-basket, single-basket, or analogs returns. Query params: `mode` (`period` for ranked bar chart, `daily` for single basket day-by-day, `analogs` for regime analog matching), `start`/`end` (date range), `group` (`all`/`themes`/`sectors`/`industries`), `basket` (slug, daily mode), `threshold` (min similarity, analogs mode). The `analogs` mode returns: multi-timeframe returns, cross-basket correlation, `forward_series`, aggregate stats, per-basket `ranks`, and `basket_count`.
- `GET /api/baskets/{name}` — Basket OHLCV data, signals, correlation, weighted tickers
- `GET /api/baskets/{name}/summary` — Open signals, 21-day correlation matrix, 1-year cumulative returns
- `GET /api/baskets/{name}/contributions` — Per-constituent return contribution data for a date range
- `GET /api/tickers` — Lists all 500 tickers
- `GET /api/tickers/{ticker}` — Daily OHLCV with optional live Databento merge
- `WebSocket /ws/live/{ticker}` — Real-time 1-minute bars from Databento Live API

Data sources (read from `PYTHON_OUTPUTS_DIR`):
- `Data_Storage/signals_cache_500.parquet` — Individual ticker signals
- `Data_Storage/basket_equity_cache/{slug}_equity_ohlc.parquet` — Basket OHLC
- `Data_Storage/basket_signals_cache/{slug}_basket_signals.parquet` — Basket signals
- `Data_Storage/correlation_cache/within_osc_500.parquet` — Pre-computed correlations
- `Data_Storage/gics_mappings_500.json` — Sector/industry ticker mappings
- `Data_Storage/top500stocks.json` — Quarterly universe

**`signals_engine.py`** — Pure Python signal calculation engine. Provides `_build_signals_from_df()` which runs the same 3-phase rotation algorithm used in `rotations_signals`:

1. **Phase 1 — Trend & Pivots**: RV with 10-day EMA, support/resistance pivots scaled by `sqrt(252/21)`
2. **Phase 2 — Ranges & Targets**: EMA-smoothed up/down ranges, upper/lower price targets
3. **Phase 3 — Entry/Exit & Stats**: 6 signal types (`Up_Rot`, `Down_Rot`, `Breakout`, `Breakdown`, `BTFD`, `STFR`) with `RollingStatsAccumulator` for rolling win rate, EV, MFE/MAE

Called by `main.py` to recompute signals when merging live Databento bars with historical cached data.

### Frontend (`frontend/`)

**`App.tsx`** — Main orchestration component. Manages view switching (Themes/Sectors/Industries/Tickers), date range filtering, and WebSocket lifecycle. Uses `window.location.hostname` for dynamic API host detection (enables mobile-to-PC access). Header buttons: **Intrabasket**, **Cross-Basket**, **Backtest**, **Analogs**, **Export** (5 total, mutually exclusive panels).

**`TVChart.tsx`** — Multi-pane chart using `lightweight-charts`. Synchronized crosshairs and time scales across panes:
- **Price pane**: Candlesticks + resistance pivots (pink) + support pivots (blue) + upper/lower targets
- **Volume pane**: Histogram (tickers only)
- **Breadth pane**: Uptrend_Pct line (baskets only)
- **Breakout pane**: Breakout_Pct line (baskets only)
- **Correlation pane**: Correlation_Pct line (baskets only)
- **RV pane**: Annualized realized volatility (RV_EMA * sqrt(252) * 100), gold line (`#b58900`). Shown for both tickers and baskets.

Panes are drag-resizable (min 40px, default 80px). Supports live WebSocket updates, date range navigation, and chart export. Header toggle order: Pivots, Targets, then for tickers: RV%, Volume; for baskets: Breadth%, Breakout%, Correlation%, RV%, Constituents.

**`BasketSummary.tsx`** — Two analysis modes activated by separate header buttons:
- **Intrabasket Analysis** — Per-basket analysis. When a basket is selected in the sidebar, it auto-targets that basket. When a ticker is selected, shows a searchable basket picker (grouped by Themes/Sectors/Industries) to choose which basket to analyze. Tabs:
  - **Signals tabs** (LT Trend / ST Trend / BTFD-STFR, open and closed): Sortable tables of signals with performance metrics
  - **Correlation tab**: Canvas-rendered heatmap of 21-day correlation matrix with date picker
  - **Returns tab**: Canvas-rendered cumulative returns line chart with quarter/year presets and hover interaction
  - **Contribution tab**: Canvas-rendered per-constituent return contribution chart via `GET /api/baskets/{name}/contributions`
- **Cross-Basket Analysis** — Cross-basket comparison via `BasketReturnsChart` component. Modes: `cross` (ranked bar chart, filterable by ALL/T/S/I) and `daily` (single basket day-by-day via searchable dropdown). Date presets, live intraday overlay, canvas PNG export.

**`AnalogsPanel.tsx`** — Self-contained analogs analysis panel activated by the **Analogs** header button. Matches the current market environment fingerprint against historical periods using Spearman rank correlation across multiple factors. Uses condition-based query builder to find historical matches. Three tabs:
  - **Summary**: Ranking table showing raw value → rank transformation per factor per basket (the fingerprint). Sortable columns (1D/1W/1M/1Q/1Y/3Y/5Y returns + Breadth% + BO% + Corr% + RV%). Click any cell to add a query condition. Blue→purple→pink color gradient by rank.
  - **Forward**: 3-pane layout. Left sidebar: match date picker (scrollable list of matching dates with avg +1Q return). Center: canvas cumulative forward returns chart per basket with horizon presets (1M/1Q/1Y) and log scale toggle. Right panel: sortable Basket/Chg legend with hover highlighting. Y-axis on right side.
  - **Aggregate**: 3-pane layout. Left sidebar: sortable basket picker (Basket/Avg columns) with horizon presets (1M/1Q/1Y). Center: canvas mean forward return path with ±1σ shaded band; hovering a match in the right panel overlays that individual path. Right panel: sortable Date/Chg columns showing each match's return for the selected basket.

Backend horizons: analogs mode returns `1M/1Q/6M/1Y` forward point returns and up to 252-day forward series. Query mode returns `1W/1M/1Q/6M/1Y` forward returns and 252-day forward series.

### Styling

Solarized Light color scheme. Monospace font (Fira Code / Cascadia Code / Consolas). Flexbox layout with 300px sidebar. No border-radius or box-shadows (terminal aesthetic).

## Data Flow

```
rotations_signals pipeline (offline)
  → Parquet/JSON caches in Python_Outputs/
    → backend/main.py serves via REST API
      → frontend App.tsx fetches and renders
        → TVChart.tsx (lightweight-charts)
        → BasketSummary.tsx (signals/correlation/returns/contribution/basket_returns)

Databento Live API (real-time)
  → backend/main.py WebSocket proxy
    → frontend TVChart.tsx live updates

Live basket closes (intraday overlay)
  → LIVE_BASKET_SIGNALS_FILE parquet (written by live-signals process)
    → GET /api/baskets/returns appends today's close to each basket
      → BasketReturnsChart renders live bar as final data point
```

## Debug Scripts

- `check_data.py` — Inspect cached data files
- `check_pivots.py` — Validate pivot calculations
