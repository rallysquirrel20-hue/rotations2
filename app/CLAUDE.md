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
- `GET /api/baskets/returns` — Cross-basket or single-basket returns. Query params: `mode` (`period` for ranked bar chart across all baskets, `daily` for a single basket's day-by-day return series), `start`/`end` (date range), `group` (`all`/`themes`/`sectors`/`industries`, period mode only), `basket` (basket slug, daily mode only). Both modes append today's live close from `LIVE_BASKET_SIGNALS_FILE` before computing returns.
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

**`App.tsx`** — Main orchestration component. Manages view switching (Themes/Sectors/Industries/Tickers), date range filtering, and WebSocket lifecycle. Uses `window.location.hostname` for dynamic API host detection (enables mobile-to-PC access).

**`TVChart.tsx`** — Multi-pane chart using `lightweight-charts`. Synchronized crosshairs and time scales across panes:
- **Price pane**: Candlesticks + resistance pivots (pink) + support pivots (blue) + upper/lower targets
- **Volume pane**: Histogram
- **Breadth pane**: Uptrend_Pct line
- **Breakout pane**: Breakout_Pct line
- **Correlation pane**: Correlation_Pct line

Panes are drag-resizable (min 40px, default 80px). Supports live WebSocket updates, date range navigation, and chart export.

**`BasketSummary.tsx`** — Two analysis modes activated by separate header buttons:
- **Intrabasket Analysis** — Per-basket analysis. When a basket is selected in the sidebar, it auto-targets that basket. When a ticker is selected, shows a searchable basket picker (grouped by Themes/Sectors/Industries) to choose which basket to analyze. Tabs:
  - **Signals tabs** (LT Trend / ST Trend / BTFD-STFR, open and closed): Sortable tables of signals with performance metrics
  - **Correlation tab**: Canvas-rendered heatmap of 21-day correlation matrix with date picker
  - **Returns tab**: Canvas-rendered cumulative returns line chart with quarter/year presets and hover interaction
  - **Contribution tab**: Canvas-rendered per-constituent return contribution chart via `GET /api/baskets/{name}/contributions`
- **Cross-Basket Analysis** — Cross-basket comparison via `BasketReturnsChart` component:
  - **Cross mode**: Ranked bar chart comparing period returns across all baskets, filterable by group (ALL/T/S/I)
  - **Single mode**: Daily return bar chart for one basket selected via searchable dropdown
  - Date presets (1D, 1W, 1M, 3M, 6M, YTD, 1Y, 3Y, 5Y, ALL), live intraday overlay, canvas PNG export

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
