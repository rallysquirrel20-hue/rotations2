# Monorepo Integration Map

> Auto-generated 2026-03-16. Last updated 2026-03-16. Tracks all communication points between signals/ and app/backend/.

## Signal Refresh Entry Point

### `signals/live_loop.py` — PM2-managed 15-minute refresh driver (added 2026-03-16)

- **Role**: Thin scheduler — the sole purpose is to invoke `signals/rotations.py` on a recurring interval. It does not read or write any data files itself.
- **Mechanism**: Calls `runpy.run_path("signals/rotations.py", run_name="__main__")` inside an infinite `while True` loop with `time.sleep(900)` (15 minutes) between iterations. Each call gets a fresh module namespace.
- **PM2 entry**: Registered as the `live-signals` app in `ecosystem.config.js`. Starting `pm2 start ecosystem.config.js` launches this loop as a managed background process.
- **Cache refresh cadence**: Because `live_loop.py` drives `rotations.py`, all signal caches in `~/Documents/Python_Outputs/Data_Storage/` (`signals_500.parquet`, basket parquets, `live_signals_500.parquet`, `live_basket_signals_500.parquet`, thematic universe JSONs, etc.) are refreshed on this 15-minute cycle during market hours.
- **Market-hours gate**: `rotations.py`'s `_get_live_update_gate()` function no-ops outside Mon–Fri 09:25–16:15 ET, so the loop runs continuously but full I/O is suppressed outside trading hours. Cache guards (`is_*_current()`) additionally skip re-building already-current data within a run.
- **Error isolation**: Exceptions from `rotations.py` are caught by a `try/except` in `live_loop.py` with `traceback.print_exc()`; the loop continues regardless, so a single failed run does not kill the PM2 process.

---

## Shared Data Directory

`~/Documents/Python_Outputs/Data_Storage/`

- **Producer**: `signals/rotations.py`
- **Consumer**: `app/backend/main.py`, `app/backend/signals_engine.py`, `app/backend/verify_backtest.py`

---

## Integration Points

### `signals_500.parquet`
- **Written by**: `load_or_build_signals()` / `_incremental_update_signals()` — `signals/rotations.py:2452` / `signals/rotations.py:2218`
  - Parquet write at line 2609 (full build) and line 4690 (live append)
- **Read by**: `_compute_live_breadth()`, `get_basket_summary()`, `get_basket_correlation()`, `get_ticker_data()`, `get_ticker_signals()` — `app/backend/main.py:326`, `930`, `1355`, `817`, `727`
  - `get_basket_summary()` reads `BTFD_Triggered` and `STFR_Triggered` columns (added to `cols_needed` at line 925)
  - `get_ticker_signals()` reads `Close` and `Volume` columns (line 723)
  - `get_ticker_data()` reads all columns via unfiltered `read_parquet` (line 817)
  - `_compute_live_breadth()` reads `Ticker`, `Date`, `Close`, `Trend`, `Resistance_Pivot`, `Support_Pivot`, `Upper_Target`, `Lower_Target`, `Is_Breakout_Sequence` (lines 324-326)
  - `get_basket_correlation()` reads `Ticker`, `Date`, `Close` (line 1355)
  - **Frontend** (`TVChart.tsx`, `BacktestPanel.tsx`): reads `RV_EMA` from `chart_data` returned by `/api/tickers/{ticker}`; annualizes as `RV_EMA * sqrt(252) * 100` for display as a Realized Volatility indicator pane (percentage)
- **Also read by**: `app/backend/verify_backtest.py` — loads `Ticker`, `Date`, `Close`, `Is_{signal}`, and per-signal trade columns (`{sig}_Entry_Price`, `{sig}_Exit_Date`, `{sig}_Exit_Price`, `{sig}_Final_Change`, `{sig}_MFE`, `{sig}_MAE`) for independent backtest verification
- **Dtype note**: `Close`, `Volume`, and most numeric columns are stored as `float32`. FastAPI's JSON encoder cannot serialize `numpy.float32` values — consumers must cast to native Python `float`/`int` before returning JSON responses.
- **Columns written**: `Date`, `Ticker`, `Open`, `High`, `Low`, `Close`, `Volume`, `RV`, `RV_EMA`, `Trend` (float32: 1.0/0.0/NaN), `Resistance_Pivot`, `Support_Pivot`, `Rotation_Open`, `Up_Range`, `Down_Range`, `Up_Range_EMA`, `Down_Range_EMA`, `Upper_Target`, `Lower_Target`, `Is_Up_Rotation`, `Is_Down_Rotation`, `Is_Breakout`, `Is_Breakdown`, `Is_BTFD`, `Is_STFR`, `Is_Breakout_Sequence`, `Rotation_ID` (int32), `BTFD_Triggered`, `STFR_Triggered`, and for each of 6 signals (`Up_Rot`, `Down_Rot`, `Breakout`, `Breakdown`, `BTFD`, `STFR`): `{sig}_Entry_Price`, `{sig}_Change`, `{sig}_Exit_Date`, `{sig}_Exit_Price`, `{sig}_Final_Change`, `{sig}_MFE`, `{sig}_MAE`, `{sig}_Win_Rate`, `{sig}_Avg_Winner`, `{sig}_Avg_Loser`, `{sig}_Avg_Winner_Bars`, `{sig}_Avg_Loser_Bars`, `{sig}_Avg_MFE`, `{sig}_Avg_MAE`, `{sig}_Historical_EV`, `{sig}_Std_Dev`, `{sig}_Risk_Adj_EV`, `{sig}_EV_Last_3`, `{sig}_Risk_Adj_EV_Last_3`, `{sig}_Count`. Plus `Source='norgate'` on disk.
- **Status**: OK

### `top500stocks.json`
- **Written by**: `load_or_build_universe()` — `signals/rotations.py:370` (write at line 382)
- **Read by**: `list_tickers()`, `list_tickers_by_quarter()` — `app/backend/main.py:601`, `616`
- **Schema**: `{"YYYY Qn": ["TICKER", ...], ...}`
- **Status**: OK

### `top500stocks.pkl`
- **Written by**: NOBODY — `signals/rotations.py` does NOT write this file
- **Read by**: NOBODY — pkl reference removed from `app/backend/signals_engine.py`
- **Status**: RESOLVED — No longer needed. `signals_engine.py` now uses `top500stocks.json`.

### `gics_mappings_500.json`
- **Written by**: `load_or_build_gics_mappings()` — `signals/rotations.py:1178` (write at line 1196)
- **Read by**: `get_latest_universe_tickers()`, `_get_universe_history()` — `app/backend/main.py:120`, `159`
- **Schema**: `{"ticker_sector": {...}, "ticker_subindustry": {...}, "sector_u": {"Sector": {"YYYY Qn": [...]}}, "industry_u": {...}}`
- **Keys read by consumer**: `sector_u`, `industry_u` sub-dicts
- **Also read by**: `app/backend/verify_backtest.py` — reads `sector_u` and `industry_u` to reconstruct quarterly basket membership for membership-filtering verification
- **Status**: OK

### Basket Signals Parquets (`*_basket_cache/`)
- **Written by**: `_finalize_basket_signals_output()` — `signals/rotations.py:3673`
  - Parquet write at line 3755, meta write at line 3762
  - Naming: `{slug}_{n}_of_500_signals.parquet` (thematic), `{slug}_of_500_signals.parquet` (sector/industry)
- **Read by**: `get_basket_data()` — `app/backend/main.py:544`; also `app/backend/verify_backtest.py` (basket-mode verification)
  - Naming glob: `{slug}_*_of_*_signals.parquet` (with `{slug}_of_*_signals.parquet` fallback) — `app/backend/main.py:92`
- **Columns**: Full signal schema (same as `signals_500.parquet`) plus `Uptrend_Pct`, `Downtrend_Pct`, `Breadth_EMA`, `Breakout_Pct`, `Breakdown_Pct`, `BO_Breadth_EMA`, `B_Trend`, `B_Resistance`, `B_Support`, `B_Up_Rot`, `B_Down_Rot`, `B_Rot_High`, `B_Rot_Low`, `B_Bull_Div`, `B_Bear_Div`, `BO_B_*` variants, `Correlation_Pct`, `Source='norgate'`
  - **Frontend** (`TVChart.tsx`, `BacktestPanel.tsx`): reads `RV_EMA` from `chart_data` returned by `/api/baskets/{basket_name}`; annualizes as `RV_EMA * sqrt(252) * 100` for display as a Realized Volatility indicator pane (percentage)
- **Status**: OK

### Basket Meta JSONs (`*_basket_cache/`)
- **Written by**: `_finalize_basket_signals_output()` — `signals/rotations.py:3673` (meta write at line 3762)
  - Naming: `*_signals_meta.json`
- **Read by**: `get_meta_file_tickers()` — `app/backend/main.py:144`
  - Naming glob: `{slug}_*_of_*_signals_meta.json` (with `{slug}_of_*_signals_meta.json` fallback) — `app/backend/main.py:104`
- **Schema**: `{schema_version, signal_logic_version, universe_logic_version, data_fingerprint, latest_source_date, last_cached_date, universe_signature, basket_type, state: {current_quarter, equity_prev_close, weights: {TICKER: float}}}`
- **Key read by consumer**: `state.weights`
- **Status**: OK

### Thematic Universe JSONs
- `beta_universes_500.json` — Written: `signals/rotations.py:545` (write at line 557). Read: `app/backend/main.py:131`, `168`. Schema: `{high: {quarter: [...]}, low: {quarter: [...]}}`. Status: OK
- `momentum_universes_500.json` — Written: `signals/rotations.py:662` (write at line 677). Schema: `{winners: ..., losers: ...}`. Status: OK
- `dividend_universes_500.json` — Written: `signals/rotations.py:1008` (write at line 1023). Schema: `{high_yield: ..., div_growth: ...}`. Status: OK
- `risk_adj_momentum_500.json` — Written: `signals/rotations.py:785` (write at line 798). Schema: `{"YYYY Qn": [...]}` (flat, no sub-key). Consumer reads with `key=None`. Status: OK

### `live_signals_500.parquet`
- **Written by**: `export_today_signals()` — `signals/rotations.py:4416`
  - First write path at line 4452 (fresh context), second at line 4486 (cached context)
- **Read by**: `_compute_live_breadth()`, `get_ticker_signals()`, `get_ticker_data()` — `app/backend/main.py:315`, `669`, `821`
- **Columns written**: `Date`, `Ticker`, `Open`, `High`, `Low`, `Close` (pure OHLC, no signal columns, no `Source`)
- **OHLC source**: `get_live_ohlc_bars()` — `signals/rotations.py:4201` — all four fields (Open, High, Low, Close) derived from Databento Historical `ohlcv-1m` aggregation since 9:30 ET. Close is the last 1-minute bar's close price. (Previously Close was overridden by an `mbp-1` live-feed mid-price; that feed was removed 2026-03-16.)
- **Status**: OK

### `live_basket_signals_500.parquet`
- **Written by**: `export_today_signals()` — `signals/rotations.py:5040`
- **Read by**: `get_basket_data()` — `app/backend/main.py:553`
  - Also read by basket list overlay at `app/backend/main.py:492`
- **Columns written**: `Date`, `BasketName`, `Open`, `High`, `Low`, `Close`
- **Consumer expects**: `BasketName` or `Basket` (tries both) — `app/backend/main.py:494`, `555`
- **Status**: OK

### `correlation_cache/`
- `basket_correlations_of_500.parquet` + `correlation_meta_500.json`
- **Written by**: NOBODY — `_save_corr_cache()` was removed when `rotations.py` was trimmed from ~8000 to ~5800 lines. Correlation is now computed inline by `_compute_within_basket_correlation()` (`signals/rotations.py:3566`) and stored directly as `Correlation_Pct` column in basket signals parquets.
- **Read by**: Nothing in `app/backend/main.py` or `app/backend/signals_engine.py`
- **Status**: STALE/REMOVED — These standalone files are no longer produced. Correlation data is now embedded in basket signals parquets.

---

## Backend Endpoint Notes

### `GET /api/basket_summary` — `cumulative_returns` series now includes `join_date` (2026-03-16)

**Data contract for `cumulative_returns`**:
```
{
  "dates": ["YYYY-MM-DD", ...],
  "series": [
    {
      "ticker": "AAPL",
      "values": [0.0123, null, ...],
      "join_date": "YYYY-MM-DD" | null
    },
    ...
  ]
}
```

**`join_date` semantics**: The most recent quarter-start date on which a ticker (re-)entered the basket — i.e., present in that quarter but absent in the prior quarter. Computed by `_get_ticker_join_dates()` (`app/backend/main.py:205`). `null` if no membership history is found.

**Producer paths** (both now populate `join_date`):
- **Contributions path** (primary): `app/backend/main.py:~1300` — calls `_get_ticker_join_dates(basket_name, sorted(ret_pivot.columns))` and attaches `join_date` to every series entry. Previously always `None`.
- **Fallback close-price path**: `app/backend/main.py:~1308` — also calls `_get_ticker_join_dates(basket_name, tickers)` and uses `jd` to rebase returns from the join date onward.

**Consumer** (`app/frontend/src/components/BasketSummary.tsx`):
- `CumulativeReturnsData` interface (`line 35`): `series` typed as `{ ticker: string; values: (number | null)[]; join_date?: string | null }[]`
- Returns tab legend (`line 601`): renders `s.join_date.slice(2)` (YY-MM-DD display) in a `.path-legend-col.date` span when `join_date` is present
- Returns tab sort (`line 455`): supports sorting series by `join_date` when `retSortCol === 'date'`

---

### `POST /api/backtest` — `basket_tickers` mode: quarterly membership filtering (2026-03-16)

**Previous behavior**: When `mode=basket_tickers`, the endpoint called `get_latest_universe_tickers()` to fetch only the current quarter's ticker list, then loaded signals for those tickers and built trades.

**Current behavior** (multi-step):
1. Calls `_get_universe_history()` to fetch the full quarterly membership history (all quarters the basket has ever existed).
2. Computes the union of all tickers across all quarters — loads signals for every ticker that was ever in the basket.
3. Finds entry signals across that full union set (unchanged signal logic).
4. **Step 5b (new)**: Filters the raw entry list by quarterly membership before building trades. For each candidate entry, `_quarter_str_to_date()` derives the quarter's start/end dates from the entry date, then checks whether the ticker was a member of the basket during that specific quarter. Entries where the ticker was NOT in the basket that quarter are dropped.
5. Only the membership-filtered entries proceed to trade construction.

**Why it matters**: Previously, a ticker added in Q1-2026 could generate backfilled entry signals from 2023 data and those trades would be included in the backtest. The new filter ensures historical trades only reflect actual basket membership at the time of entry.

**Functions involved** (`app/backend/main.py`):
- `_get_universe_history()` — replaces `get_latest_universe_tickers()` for history fetch
- `_quarter_str_to_date()` — converts `"YYYY Qn"` strings to `(start_date, end_date)` tuples for membership window checks

**No data-contract change**: No new files read or written; purely a logic change within the endpoint handler.

---

### `POST /api/backtest` — `benchmarks_only` fast-path and parallel benchmark calls (2026-03-16)

**Request body change** — new field in `BacktestRequest` (`app/backend/main.py:1589`):
```
benchmarks_only: bool = False   (default: False — backward compatible)
```

Full `BacktestRequest` schema (lines 1579–1590):
```
{
  target: str,
  target_type: str,               // "basket" | "basket_tickers" | "ticker"
  entry_signal: str,              // "Up_Rot" | "Down_Rot" | "Breakout" | "Breakdown" | "BTFD" | "STFR"
  filters: BacktestFilter[],     // default []
  start_date: str | null,
  end_date: str | null,
  position_size: float,           // default 1.0
  initial_equity: float,          // default 100000
  max_leverage: float,            // default 2.5
  benchmarks_only: bool,          // NEW — default False
  include_positions: bool,        // default False
}
```

**Response differences when `benchmarks_only=true`** (`app/backend/main.py:2168–2175`):

| Field | `benchmarks_only=false` (normal) | `benchmarks_only=true` (fast path) |
|---|---|---|
| `trades` | Full trade list | `[]` (empty) |
| `trade_paths` | Daily return paths per trade | `[]` (empty) |
| `equity_curve.filtered` | Filtered equity values | Same as `unfiltered` (no regime filtering applied) |
| `equity_curve.unfiltered` | Unfiltered equity values | Unfiltered equity values (identical) |
| `equity_curve.dates` | Date array | Date array (identical) |
| `equity_curve.buy_hold` | Buy-and-hold values | Buy-and-hold values (identical) |
| `stats.filtered` | Stats on filtered trades | Same as `unfiltered` stats |
| `stats.unfiltered` | Stats on all trades | Stats on all trades (identical) |
| `date_range` | `{min, max}` | `{min, max}` (identical) |
| `daily_positions` | Positions dict or null | **Not present** |
| `skipped_entries` | Skipped list or null | **Not present** |
| `blew_up` | Error string if blown up | Error string if blown up (same logic) |

**`daily_positions` response field** (returned when `include_positions: true`):
```
{
  "daily_positions": {               // sparse dict — only days with open positions
    "<date_index>": {                // key is integer index into equity_curve.dates
      "exposure_pct": float,         // total exposure as % of equity
      "equity": float,               // equity value at close
      "positions": [
        {
          "trade_idx": int,          // index into the trades array
          "ticker": str,
          "entry_date": str,         // "YYYY-MM-DD"
          "alloc": float,            // dollar allocation to this position
          "weight": float,           // position weight as fraction of equity
          "daily_return": float,     // single-day return for this position
          "contribution": float      // dollar P&L contribution for the day
        },
        ...
      ]
    },
    ...
  }
}
```

**`skipped_entries` response field** (returned when `include_positions: true`):
```
{
  "skipped_entries": [
    {
      "ticker": str,
      "entry_date": str,            // "YYYY-MM-DD"
      "entry_price": float,
      "reason": str,                // e.g. "max_leverage exceeded"
      "exposure_at_skip": float,    // exposure % when entry was skipped
      "equity_at_skip": float       // equity value when entry was skipped
    },
    ...
  ]
}
```

Both fields are `null` when `include_positions` is `false` (the default) or when `benchmarks_only` is `true`.

**Performance fast path** (lines 1865, 1880, 1951):
- Trade path computation (step 6b) is skipped entirely — the loop over trades to build daily return paths never executes.
- Equity curve computation (step 7) uses a simplified algorithm (lines 1951–1998): single equity track, no separate filtered vs. unfiltered curve, no mark-to-market on idle days. `filtered` and `unfiltered` are set to the same `eq_all_vals` array. `stats.filtered` uses `stats_unfiltered`.

**Frontend parallel call pattern** (`app/frontend/src/components/BacktestPanel.tsx:333–361`):
- On submit, `BacktestPanel` fires **7 parallel POST requests**:
  1. **Main call**: Full request body with user-selected `entry_signal`, all `filters`, `include_positions: true`, `benchmarks_only` omitted (defaults false).
  2. **6 benchmark calls**: One per signal in `ENTRY_SIGNALS = ['Up_Rot', 'Down_Rot', 'Breakout', 'Breakdown', 'BTFD', 'STFR']` (line 82). Each uses `benchmarks_only: true`, empty `filters: []`, same `target`, `target_type`, `start_date`, `end_date`, `initial_equity`, `position_size`, `max_leverage`.
- Benchmark responses: frontend only reads `r.data.equity_curve.unfiltered` (line 350) and stores it as `benchmarks[sig]: number[]` for overlay plotting.
- All 7 calls use `Promise.all` (line 355); benchmark failures are caught individually and logged, not fatal.

**Impact**: The endpoint contract is backward-compatible. Omitting `benchmarks_only` preserves the original behavior. The fast path reduces per-request cost for benchmark overlays by ~60-70% (no trade paths, simplified equity loop).

**Verification consumer** (`app/backend/verify_backtest.py`):
- Standalone script (no imports from `main.py`) that POSTs to `/api/backtest` with `benchmarks_only: false`, `include_positions: true` (via `call_api()`) and independently re-derives trades, equity curve, and stats from the same raw parquet files.
- Compares local vs API results across 4 dimensions: signal entry/exit correctness, leverage/skip parity, basket membership filtering, and equity curve accuracy.
- Reads: `signals_500.parquet`, basket parquets (via glob), `gics_mappings_500.json`, and thematic universe JSONs.
- Invoked via CLI: `python verify_backtest.py --run-defaults` or with `--target`/`--signal` flags.

---

### `POST /api/backtest/multi` — multi-leg basket backtest (2026-03-16)

**Request model** — `MultiBacktestRequest` (`app/backend/main.py:1600`):
```
{
  legs: MultiBasketLeg[],     // at least 2
  start_date: str | null,
  end_date: str | null,
  initial_equity: float,      // default 100000
  max_leverage: float,        // default 2.5
}
```

**`MultiBasketLeg`** (`app/backend/main.py:1592`):
```
{
  target: str,                // basket name or ticker
  target_type: str,           // "basket" | "basket_tickers" | "ticker"
  entry_signal: str,          // signal from SIGNAL_TYPES
  allocation_pct: float,      // 0.0-1.0, fraction of total equity (all legs must sum to 1.0)
  position_size: float,       // default 0.25, per-leg (NOT shared across legs)
  filters: BacktestFilter[],  // default []
}
```

**Key design detail**: `position_size` is per-leg — each leg sizes positions as a fraction of its own allocated equity. Leverage checking is global across all legs (shared `max_leverage`).

**Response shape** (`app/backend/main.py:2852`):
```
{
  legs: [                         // one per input leg
    {
      target, target_type, entry_signal, allocation_pct, direction,
      trades: [...],              // per-leg trade list
      stats: { trades, win_rate, avg_winner, avg_loser, ev, profit_factor, max_dd, avg_bars }
    },
    ...
  ],
  combined: {
    equity_curve: {
      dates: ["YYYY-MM-DD", ...],
      combined: [float, ...],     // summed equity across all legs
      per_leg: [[float, ...], ...], // per-leg equity series
      buy_hold: [float, ...],     // quarterly-rebalanced buy-and-hold benchmark
    },
    stats: { ... },               // stats computed on all trades merged
  },
  date_range: { min, max },
  skipped_entries: [...] | null,  // includes leg_index and leg_target fields
  daily_positions: { ... } | null // same schema as single backtest, positions include leg_target field
}
```

**Buy-and-hold benchmark**: Quarterly-rebalanced combined buy-and-hold — each leg's B&H uses the basket/ticker Close series, rebalanced to `allocation_pct` target weights at the start of each calendar quarter (`app/backend/main.py:2799–2824`).

**Helper function** — `_build_leg_data()` (`app/backend/main.py:1625`):
- Extracts the shared trade-building logic (data loading, signal detection, membership filtering, trade construction, trade path computation) into a reusable function.
- Currently called only by `run_multi_backtest()` (line 2515). The single `POST /api/backtest` endpoint still has its own inline implementation — not yet refactored to call `_build_leg_data()`.
- Returns: `{ trades, trade_paths, ticker_closes, all_dates, direction, is_long, is_multi_ticker, quarter_membership, df, date_range }` or `None` if the filtered DataFrame is empty.

**Frontend consumer**: `app/frontend/src/components/MultiBacktestPanel.tsx` — dedicated panel that constructs a `MultiBacktestRequest` and renders per-leg trades/stats plus the combined equity curve.

---

## Frontend Component Integration Notes

### BacktestPanel — Overlay Toggle Props Removed (2026-03-16)
- **Previous behavior**: `App.tsx` passed overlay toggle props to `BacktestPanel` via its interface: `showPivots`, `showTargets`, `showVolume`, `showBreadth`, `showBreakout`, `showCorrelation`.
- **Current behavior**: All six props have been removed from `BacktestPanel`'s interface. `BacktestPanel` now manages its own toggle state internally. `App.tsx` no longer passes these props.
- **Impact**: No backend/data contract change. Pure frontend prop interface change.

### MultiBacktestPanel — Multi-leg backtest UI (2026-03-16)
- **Component**: `app/frontend/src/components/MultiBacktestPanel.tsx`
- **Backend endpoint**: `POST /api/backtest/multi` (see endpoint section above for full request/response schema)
- **Renders**: Per-leg trade tables and stats, combined equity curve chart with per-leg overlay and quarterly-rebalanced buy-and-hold, daily position snapshots across all legs.
- **Also referenced in**: `app/frontend/src/App.tsx` (imported and routed)

### BacktestPanel — Leverage Preset Buttons & Uniform Sizing (2026-03-16)
- **Added**: `LEV_PRESETS = [100, 110, 125, 150, 200, 250]` constant and a row of leverage preset buttons (`.backtest-pos-preset`) in the Position Sizing config section. Clicking a button sets `maxLeverage` state, which is sent as `max_leverage: maxLeverage / 100` to `POST /api/backtest` (unchanged contract).
- **CSS changes** (`index.css`): Added `.backtest-pos-preset` / `.backtest-pos-preset.wide` button styles, uniform `.control-btn` sizing (120px x 32px), `.sidebar-header` and `.main-header` both pinned to `height: 114px` for alignment, `.backtest-results-header` height fixed to 41px to match accordion row heights.
- **Impact**: No backend/data contract change. The `POST /api/backtest` request body already accepted `max_leverage`; preset buttons are a UI convenience only.

---

## Signal Logic Parity (`signals/rotations.py` vs `app/backend/signals_engine.py`)

Both implement `_build_signals_from_df`. Known differences:

| Aspect | `signals/rotations.py` | `app/backend/signals_engine.py` |
|---|---|---|
| Acceleration | Numba JIT | Pure Python |
| `Avg_Winner_Bars`/`Avg_Loser_Bars` | Present | Present |
| BTFD entry price | `open_ if open_ <= lower_target else lower_target` | Matches (gap fill + prev-day target) |
| STFR entry price | `open_ if open_ >= upper_target else upper_target` | Matches (gap fill + prev-day target) |
| `{sig}_Change` column | Written (live running P&L) — line 1906 | Not written |

---

## `Source` Column Contract
- `signals_500.parquet`: `'norgate'` for EOD bars, `'live'` for intraday appended rows
- Basket `*_signals.parquet`: `'norgate'`
- `live_signals_500.parquet`: no `Source` column
- `live_basket_signals_500.parquet`: no `Source` column
- On rebuild, producer strips `Source='live'` rows before merging new Norgate data

---

## Issues (Priority Order)

1. ~~**CRITICAL**: Basket file naming mismatch~~ — **RESOLVED** (2026-03-13). Legacy `*_basket.parquet` / `*_basket_meta.json` fallback globs fully removed from `main.py`. All four lookup functions (`_find_basket_parquet`, `_find_basket_meta`, `list_baskets`, `get_basket_breadth`) now exclusively use `*_signals.parquet` / `*_signals_meta.json` patterns.

2. ~~**MODERATE**: `top500stocks.pkl` not produced~~ — **RESOLVED** (2026-03-13). `signals_engine.py` no longer reads `.pkl`; uses `top500stocks.json` instead.

3. ~~**LOW**: `signals_engine.py` missing `Avg_Winner_Bars`/`Avg_Loser_Bars`~~ — **RESOLVED** (2026-03-13). `RollingStatsAccumulator` now tracks both columns.

4. ~~**LOW**: BTFD/STFR entry price logic differs~~ — **RESOLVED** (2026-03-13). `signals_engine.py` batch path now uses previous day's target with gap fill logic, matching `rotations.py`.
