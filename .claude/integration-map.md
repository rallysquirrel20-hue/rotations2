# Monorepo Integration Map

> Auto-generated 2026-03-16. Last updated 2026-03-18 (batch 5). Tracks all communication points between signals/ and app/backend/.

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
- **Written by**: `load_or_build_signals()` / `_incremental_update_signals()` — `signals/rotations.py:2462` / `signals/rotations.py:2228`
  - Parquet write at line 2619 (full build, `SIGNALS_CACHE_FILE`) and line 5091 (live append via `append_live_today_to_signals_parquet()` at line 5031)
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
- **Written by**: `load_or_build_universe()` — `signals/rotations.py:380` (write at line 392)
- **Read by**: `list_tickers()`, `list_tickers_by_quarter()` — `app/backend/main.py:601`, `616`
- **Schema**: `{"YYYY Qn": ["TICKER", ...], ...}`
- **Status**: OK

### `top500stocks.pkl`
- **Written by**: NOBODY — `signals/rotations.py` does NOT write this file
- **Read by**: NOBODY — pkl reference removed from `app/backend/signals_engine.py`
- **Status**: RESOLVED — No longer needed. `signals_engine.py` now uses `top500stocks.json`.

### `gics_mappings_500.json`
- **Written by**: `load_or_build_gics_mappings()` — `signals/rotations.py:1188` (write at line 1206)
- **Read by**: `get_latest_universe_tickers()`, `_get_universe_history()` — `app/backend/main.py:120`, `159`
- **Schema**: `{"ticker_sector": {...}, "ticker_subindustry": {...}, "sector_u": {"Sector": {"YYYY Qn": [...]}}, "industry_u": {...}}`
- **Keys read by consumer**: `sector_u`, `industry_u` sub-dicts
- **Also read by**: `app/backend/verify_backtest.py` — reads `sector_u` and `industry_u` to reconstruct quarterly basket membership for membership-filtering verification
- **Status**: OK

### Basket Signals Parquets (`*_basket_cache/`)
- **Written by**: `_finalize_basket_signals_output()` — `signals/rotations.py:3860`
  - Parquet write and meta write within that function
  - Naming: `{slug}_{n}_of_500_signals.parquet` (thematic), `{slug}_of_500_signals.parquet` (sector/industry)
- **Read by**: `get_basket_data()` — `app/backend/main.py:708`; also `app/backend/verify_backtest.py` (basket-mode verification)
  - Naming glob: `{slug}_*_of_*_signals.parquet` (with `{slug}_of_*_signals.parquet` fallback) — `app/backend/main.py:92`
  - `get_basket_returns()` reads `Date`, `Close` columns — `app/backend/main.py:554` (slug discovery), `573` (date range scan), `619`/`673` (period & daily return computation)
- **Columns**: Full signal schema (same as `signals_500.parquet`) plus `Uptrend_Pct`, `Downtrend_Pct`, `Breadth_EMA`, `Breakout_Pct`, `Breakdown_Pct`, `BO_Breadth_EMA`, `B_Trend`, `B_Resistance`, `B_Support`, `B_Up_Rot`, `B_Down_Rot`, `B_Rot_High`, `B_Rot_Low`, `B_Bull_Div`, `B_Bear_Div`, `BO_B_*` variants, `Correlation_Pct`, `Source='norgate'`
  - **Frontend** (`TVChart.tsx`, `BacktestPanel.tsx`): reads `RV_EMA` from `chart_data` returned by `/api/baskets/{basket_name}`; annualizes as `RV_EMA * sqrt(252) * 100` for display as a Realized Volatility indicator pane (percentage)
  - **`Correlation_Pct` implementation note** (updated 2026-03-17): Computed by `_compute_within_basket_correlation()` (`signals/rotations.py:3710`) using z-score variance decomposition: `avg_pairwise_corr = (n * Var(EW z-portfolio) - 1) / (n - 1)`. This replaced an earlier per-date `.corr()` approach. Values may differ from the old implementation by ~0.1 mean / ~8 max at quarter boundaries (where basket membership changes cause different ticker subsets). All frontend consumers verified working correctly.
- **Status**: OK

### Basket Meta JSONs (`*_basket_cache/`)
- **Written by**: `_finalize_basket_signals_output()` — `signals/rotations.py:3860` (meta write within that function)
  - Naming: `*_signals_meta.json`
- **Read by**: `get_meta_file_tickers()` — `app/backend/main.py:144`
  - Naming glob: `{slug}_*_of_*_signals_meta.json` (with `{slug}_of_*_signals_meta.json` fallback) — `app/backend/main.py:104`
- **Schema**: `{schema_version, signal_logic_version, universe_logic_version, data_fingerprint, latest_source_date, last_cached_date, universe_signature, basket_type, state: {current_quarter, equity_prev_close, weights: {TICKER: float}}}`
- **Key read by consumer**: `state.weights`
- **Status**: OK

### Basket Contributions Parquets (`*_basket_cache/`)
- **Written by**: `_finalize_basket_signals_output()` — `signals/rotations.py:3860`
  - Uses pre-computed `contrib_df` from `compute_equity_ohlc_cached()` when available (line 3976 area), otherwise falls back to `_compute_and_save_contributions()` (line 4120)
  - Naming: `{slug}_{n}_of_500_contributions.parquet` (thematic), `{slug}_of_500_contributions.parquet` (sector/industry)
- **Read by**: `_find_basket_contributions()`, `get_basket_weights_from_contributions()`, `get_basket_contributions()`, `get_basket_contributions_day()` — `app/backend/main.py:1549`, `242`, `1562`, `1652`
  - Naming glob: `{slug}_*_of_*_contributions.parquet` (with `{slug}_of_*_contributions.parquet` fallback) — `app/backend/main.py:1554`
  - `get_basket_summary()` cumulative returns path reads contributions to build per-ticker weighted return series — `app/backend/main.py:~1440`
  - `get_basket_weights_from_contributions()` reads latest `Weight_BOD` per ticker — `app/backend/main.py:242`
- **Columns**: `Date`, `Ticker`, `Weight_BOD`, `Daily_Return`, `Contribution`
- **Status**: OK — schema and file locations unchanged (2026-03-17)

### Thematic Universe JSONs
- `beta_universes_500.json` — Written: `signals/rotations.py:555` (write at line 567). Read: `app/backend/main.py:131`, `168`. Schema: `{high: {quarter: [...]}, low: {quarter: [...]}}`. Status: OK
- `momentum_universes_500.json` — Written: `signals/rotations.py:672` (write at line 687). Schema: `{winners: ..., losers: ...}`. Status: OK
- `dividend_universes_500.json` — Written: `signals/rotations.py:1018` (write at line 1033). Schema: `{high_yield: ..., div_growth: ...}`. Status: OK
- `risk_adj_momentum_500.json` — Written: `signals/rotations.py:795` (write at line 808). Schema: `{"YYYY Qn": [...]}` (flat, no sub-key). Consumer reads with `key=None`. Status: OK

### `live_signals_500.parquet`
- **Written by**: `export_today_signals()` — `signals/rotations.py:4817`
  - First write path at line 4854 (fresh context), second at line 4888 (cached context)
- **Read by**: `_compute_live_breadth()`, `get_ticker_signals()`, `get_ticker_data()` — `app/backend/main.py:315`, `669`, `821`
- **Columns written**: `Date`, `Ticker`, `Open`, `High`, `Low`, `Close` (pure OHLC, no signal columns, no `Source`)
- **OHLC source**: `get_live_ohlc_bars()` — `signals/rotations.py:4602` — all four fields (Open, High, Low, Close) derived from Databento Historical `ohlcv-1m` aggregation since 9:30 ET. Close is the last 1-minute bar's close price. (Previously Close was overridden by an `mbp-1` live-feed mid-price; that feed was removed 2026-03-16.)
- **Status**: OK

### `live_basket_signals_500.parquet`
- **Written by**: Live basket export — `signals/rotations.py:5364` (basket OHLC loop), write at line 5442
- **Read by**: `get_basket_data()` — `app/backend/main.py:717`
  - Also read by basket list overlay at `app/backend/main.py:492`
  - Also read by `get_basket_returns()` — `app/backend/main.py:588` — reads `BasketName`/`Basket`, `Date`, `Close` to overlay live intraday close on basket return calculations
- **Columns written**: `Date`, `BasketName`, `Open`, `High`, `Low`, `Close`
- **Consumer expects**: `BasketName` or `Basket` (tries both) — `app/backend/main.py:494`, `590`, `719`
- **Status**: OK

### `returns_matrix_500.parquet` + `returns_matrix_500.fingerprint` (added 2026-03-17)
- **Written by**: Basket processing loop preamble — `signals/rotations.py` (before basket loop, ~line 4320–4350 area)
  - Parquet write via `returns_matrix.to_parquet(_ret_matrix_path, engine='pyarrow')` at line 4344
  - Fingerprint write via `_ret_fp_path.write_text(_ret_fp_hash)` immediately after
  - Built from `all_signals_df` close-price pct_change pivoted to Date x Ticker matrix
  - Fingerprint: MD5 hash of `f"{all_signals_df.shape}_{all_signals_df['Date'].max()}_{sorted(all_signals_df['Ticker'].unique())}"`, stored in `.fingerprint` text file
  - On subsequent runs, fingerprint is checked first; if unchanged, the parquet is loaded from cache instead of rebuilt
- **Read by**: NOBODY in `app/backend/` — this file is only consumed within the batch pipeline (`signals/rotations.py`)
  - Passed as `returns_matrix` argument to `process_basket_signals()`, then to `compute_equity_ohlc_cached()` (line 4190) and `_compute_within_basket_correlation()` (line 3710) for each basket
  - Also drives OHLC returns matrices (`ohlc_ret_matrices` dict with `Open_Ret`, `High_Ret`, `Low_Ret`) built alongside `returns_matrix` and passed through the same call chain
- **Columns**: One column per ticker (ticker name as column header), DatetimeIndex as row index, values are daily close-to-close percentage returns
- **Purpose**: Pre-computing the full returns matrix once before the basket loop eliminates redundant per-basket pivot operations. The fingerprint cache avoids rebuilding on re-runs when the underlying signals data has not changed.
- **Status**: OK — pipeline-internal only, no cross-repo contract

### `correlation_cache/`
- `basket_correlations_of_500.parquet` + `correlation_meta_500.json`
- **Written by**: NOBODY — `_save_corr_cache()` was removed when `rotations.py` was trimmed from ~8000 to ~5800 lines. Correlation is now computed inline by `_compute_within_basket_correlation()` (`signals/rotations.py:3710`) and stored directly as `Correlation_Pct` column in basket signals parquets.
- **Read by**: Nothing in `app/backend/main.py` or `app/backend/signals_engine.py`
- **Status**: STALE/REMOVED — These standalone files are no longer produced. Correlation data is now embedded in basket signals parquets.

---

## Backend Endpoint Notes

### `GET /api/baskets/returns` — cross-basket and single-basket return data (added 2026-03-17, extended with `analogs` mode)

**Endpoint**: `app/backend/main.py:543` — `get_basket_returns()`

**Query parameters**:
- `mode`: `"period"` (default) — one return value per basket; `"daily"` — day-by-day return series for a single basket; `"analogs"` — regime analog search (new, added HEAD commit)
- `start`, `end`: Date range (strings, `YYYY-MM-DD`). Defaults to trailing 1Y from the latest available date in period mode.
- `basket`: Basket slug (required for `mode=daily`)
- `group`: `"all"` (default), `"themes"`, `"sectors"`, `"industries"` — filters baskets in period and analogs modes
- `top_n`: int (default 10) — number of analog windows to return (analogs mode only)
- `threshold`: float (default 0.0) — minimum similarity score for returned analogs (analogs mode only). When > 0, analogs with `similarity < threshold` are filtered out after selection.

**Data sources consumed**:
1. **Basket parquets** (`*_of_*_signals.parquet`) from all `BASKET_CACHE_FOLDERS` — used in all modes:
   - Slug discovery via glob `*_of_*_signals.parquet` across all folders
   - Global date range computation (reads `Date` column only)
   - Period mode: reads `Date`, `Close` — computes `(last_close / first_close) - 1` per basket
   - Daily mode: reads `Date`, `Close` — computes `Close.pct_change()` for a single basket
   - **Analogs mode** (new): reads `Date`, `Close`, `Uptrend_Pct`, `Breakout_Pct`, `Correlation_Pct`, `RV_EMA` — builds cross-basket rank fingerprints for Spearman correlation similarity search
2. **`live_basket_signals_500.parquet`** — reads `BasketName`/`Basket`, `Date`, `Close`. Builds a `live_closes` dict mapping slug to `(date, close)`. Live row is appended to each basket's close series in all modes.

**Analogs mode data contract** (updated 2026-03-17):
- Reads `['Date', 'Close', 'Uptrend_Pct', 'Breakout_Pct', 'Correlation_Pct', 'RV_EMA']` from every basket parquet
- Computes rolling Spearman correlation across 5+ metrics: returns rank, uptrend rank, breakout rank, correlation rank, volatility rank, plus multi-timeframe return ranks (1Q/1Y/3Y/5Y) and cross-basket correlation similarity
- `MULTI_TF = {"1Q": 63, "1Y": 252, "3Y": 756, "5Y": 1260}` — additional return timeframes computed via rolling windows on close prices
- `cross_corr_series` — average pairwise cross-basket correlation computed from daily returns; used as an additional similarity dimension
- Overall similarity is the mean of all active metric similarities (base 5 + active timeframe metrics + cross-basket correlation if available)
- Greedy top-N selection with overlap exclusion (excludes windows within W/2 of each other)
- Post-selection `threshold` filtering: analogs with `similarity < threshold` are dropped
- Forward returns at 1M (21d), 3M (63d), 6M (126d) horizons computed from the basket close series
- Forward series: daily cumulative forward returns per basket for up to 252 trading days past the analog window end
- Aggregate statistics (mean/median/min/max/std/count) at 1M/3M/6M horizons across all post-threshold analogs, with per-basket breakdown
- **Column contract**: All 4 columns (`Uptrend_Pct`, `Breakout_Pct`, `Correlation_Pct`, `RV_EMA`) must be present in basket signals parquets — already satisfied (written by `_finalize_basket_signals_output()`)

**Response shape** (`mode=period`):
```
{
  "baskets": [{"name": str, "group": "theme"|"sector"|"industry", "return": float}, ...],
  "date_range": {"min": "YYYY-MM-DD"|null, "max": "YYYY-MM-DD"|null},
  "actual_range": {"start": str, "end": str}
}
```

**Response shape** (`mode=daily`):
```
{
  "basket": str,
  "dates": ["YYYY-MM-DD", ...],
  "returns": [float, ...],
  "date_range": {"min": "YYYY-MM-DD"|null, "max": "YYYY-MM-DD"|null}
}
```

**Response shape** (`mode=analogs`, updated 2026-03-17):
```
{
  "current": {
    "start": "YYYY-MM-DD", "end": "YYYY-MM-DD",
    "returns": {slug: float|null, ...},
    "metrics": {
      "uptrend_pct": {slug: float|null, ...},
      "breakout_pct": {slug: float|null, ...},
      "correlation_pct": {slug: float|null, ...},
      "rv_ema": {slug: float|null, ...},
      "returns_1Q": {slug: float|null, ...},
      "returns_1Y": {slug: float|null, ...},
      "returns_3Y": {slug: float|null, ...},
      "returns_5Y": {slug: float|null, ...},
      "cross_basket_corr": float|null          // scalar, not per-basket
    }
  },
  "analogs": [
    {
      "start": "YYYY-MM-DD", "end": "YYYY-MM-DD",
      "similarity": float,
      "similarity_breakdown": {
        "returns": float, "breadth": float, "breakout": float,
        "correlation": float, "volatility": float,
        "ret_1Q": float, "ret_1Y": float, "ret_3Y": float, "ret_5Y": float,   // present when timeframe data available
        "cross_corr": float                                                      // present when cross-basket corr available
      },
      "returns": {slug: float|null, ...},
      "forward": {"1M": {slug: float|null, ...}|null, "3M": ..., "6M": ...},
      "forward_series": {
        "dates": ["YYYY-MM-DD", ...],          // up to 252 trading days
        "baskets": {slug: [float|null, ...], ...}  // daily cumulative returns per basket
      }
    },
    ...
  ],
  "aggregate": {
    "1M": {
      "mean": float, "median": float, "min": float, "max": float, "std": float, "count": int,
      "per_basket": {slug: {"mean": float, "median": float, "min": float, "max": float, "std": float, "count": int}, ...}
    } | null,
    "3M": ... | null,
    "6M": ... | null
  },
  "date_range": {"min": "YYYY-MM-DD"|null, "max": "YYYY-MM-DD"|null}
}
```

**Frontend consumers**:
- `mode=period` and `mode=daily`: `BasketReturnsChart` component in `app/frontend/src/components/BasketSummary.tsx` — renders cross-basket ranked bar chart (period mode) and single-basket daily return bar chart (daily mode) with date presets and live intraday overlay.
- `mode=analogs`: `AnalogsPanel` top-level component in `app/frontend/src/components/AnalogsPanel.tsx` — dedicated panel for regime analog search, rendering similarity results, forward series charts, and aggregate statistics. Also referenced in `app/frontend/src/App.tsx` (imported and routed) and styled in `app/frontend/src/index.css`.

---

### `GET /api/baskets/{basket_name}/candle-detail` — per-constituent day detail (documented 2026-03-17)

**Endpoint**: `app/backend/main.py:1904` — `get_basket_candle_detail()`

**Query parameters**: `date` (optional, `YYYY-MM-DD`) — defaults to latest date in contributions file

**Data source**: Basket contributions parquet (`{slug}_*_of_*_contributions.parquet`) via `_find_basket_contributions()`. Reads all columns: `Date`, `Ticker`, `Weight_BOD`, `Daily_Return`, `Contribution`.

**Response shape**:
```
{
  "date": "YYYY-MM-DD",
  "basket_return": float,
  "constituents": [
    {"ticker": str, "weight": float, "daily_return": float, "contribution": float},
    ...
  ]   // sorted by Contribution descending
}
```

**Status**: OK — reads the same contributions parquet schema already documented above.

---

### `GET /api/ticker-baskets/{ticker}` — baskets containing a ticker (documented 2026-03-17)

**Endpoint**: `app/backend/main.py:1950` — `get_ticker_baskets()`

**Data sources read**:
1. `gics_mappings_500.json` — reads `sector_u` and `industry_u` sub-dicts to find sector/industry baskets containing the ticker
2. Thematic universe JSONs (all 4: `beta_universes_500.json`, `momentum_universes_500.json`, `dividend_universes_500.json`, `risk_adj_momentum_500.json`) via `THEMATIC_CONFIG` — checks if ticker appears in any quarter
3. Basket signals parquets — only baskets that have a signals parquet (`_find_basket_parquet()`) are returned

**Response shape**: `["BasketName1", "BasketName2", ...]` — sorted list of basket slug strings

**Status**: OK — purely reads existing files, no new data contract.

---

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

**`join_date` semantics** (updated HEAD commit): The first-ever quarter-start date on which a ticker appeared in the basket. `_get_ticker_join_dates()` (`app/backend/main.py:205`) now sets `join_date` only on first appearance (using `if t not in join_dates`) rather than on re-entries. Previously it tracked the most-recent re-entry by resetting when the ticker disappeared from `prev_tickers`. `null` if no membership history is found.

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

### `POST /api/backtest` — `BacktestFilter` extended with `source` field; `benchmarks_only`/`include_positions` removed (updated HEAD commit)

**`BacktestFilter` model** (`app/backend/main.py:2155`) — now has a `source` field:
```
{
  metric: str,           // column name (e.g. "Uptrend_Pct", "Trend", "Is_Breakout_Sequence")
  condition: str,        // "gt" | "lt" | "increasing" | "decreasing" | "is_true" | "is_false"
  value: float | null,   // threshold (required for gt/lt; unused for increasing/decreasing/is_true/is_false)
  source: str,           // default "self" — basket slug name to load an external basket parquet as filter source
}
```

**`source` field behavior** (new in HEAD commit, `app/backend/main.py:2089–2118`):
- `source="self"` (default): filter metric is read from the target's own signals data (same parquet being backtested)
- `source="<basket_slug>"` (e.g. `"Information_Technology"`): the basket parquet for that slug is loaded via `_find_basket_parquet(flt.source)` and merged onto the target DataFrame with `merge_asof` on `Date`. The external metric column is renamed to `{metric}__{source}` to avoid collisions. This allows filtering ticker signals against basket-level indicators (e.g. only take BTFD signals in a sector when that sector's `Uptrend_Pct` > 60).
- Failed sources (parquet not found) are recorded in `failed_sources` and their filters are silently skipped (no exception thrown).
- **Columns read from external basket parquets**: `Date` plus whichever `metric` columns appear in filters with that `source` slug.

**Full `BacktestRequest` schema** (`app/backend/main.py:2161`):
```
{
  target: str,
  target_type: str,               // "basket" | "basket_tickers" | "ticker"
  entry_signal: str,              // "Up_Rot" | "Down_Rot" | "Breakout" | "Breakdown" | "BTFD" | "STFR" | "Buy_Hold"
  filters: BacktestFilter[],      // default []
  start_date: str | null,
  end_date: str | null,
  position_size: float,           // default 1.0
  initial_equity: float,          // default 100000
  max_leverage: float,            // default 2.5 — now multiplies position sizes (wanted = equity * pos_size * max_lev)
}
```

**Breaking change from prior version**: `benchmarks_only: bool` and `include_positions: bool` fields have been REMOVED from `BacktestRequest`. Any frontend code still sending these fields will have them silently ignored by Pydantic (extra fields are ignored by default in FastAPI). The fast-path equity curve behavior (`benchmarks_only=true`) and position snapshotting (`include_positions=true`) are no longer supported by this endpoint.

**Response shape** (updated 2026-03-18 — `daily_positions` added):
```
{
  trades: [...],
  trade_paths: [...],
  equity_curve: { dates: [...], filtered: [...], unfiltered: [...], buy_hold: [...] },
  stats: { filtered: {...}, unfiltered: {...} },
  date_range: { min: str|null, max: str|null },
  daily_positions: { [int_date_idx]: { exposure_pct, equity, positions: [{trade_idx, ticker, entry_date, alloc, weight, daily_return, contribution}] } } | null,
  blew_up: {...} | absent
}
```

**`daily_positions` details** (added 2026-03-18):
- Previously missing from the single-leg response; now built inside the equity curve loop from `open_filt` positions.
- Keyed by integer date index (matches `equity_curve.dates` array index, NOT date strings).
- Each entry contains `exposure_pct` (total exposure / equity), `equity` (filtered equity at that date), and `positions` array with per-position detail: `trade_idx`, `ticker`, `entry_date`, `alloc` (dollar allocation), `weight` (alloc / equity), `daily_return`, `contribution` (weight * daily_return).
- `null` when no positions exist (no trades taken).

**`Buy_Hold` signal support** (added 2026-03-18, `app/backend/main.py:2445` + `2188`):
- When `entry_signal == 'Buy_Hold'`, the endpoint delegates to `_build_buy_hold()` (single-leg) or the Buy_Hold branch inside `_build_leg_trades()` (multi-leg).
- Loads the Close series from the basket parquet (`_find_basket_parquet()`) or individual signals parquet, then builds a single trade spanning the full date range.
- Returns equity curve (scaled by `initial_equity`), stats (1 trade, max drawdown from cummax), and a single trade with MFE/MAE.
- **Not supported** for `target_type == 'basket_tickers'` — raises HTTP 400.
- `Buy_Hold` is in `SIGNAL_TYPES` and `BACKTEST_DIRECTION` (`'long'`) but intentionally NOT in `SIGNAL_IS_COL` — it has no `Is_*` column in the parquets.
- **Note**: `get_basket_summary()` iterates `SIGNAL_TYPES` and indexes `SIGNAL_IS_COL[st]` — `Buy_Hold` would cause a `KeyError` there if reached. Currently the iteration at line 1518 is only for building `cols_needed` and will fail on startup/first call. This is a latent bug introduced by adding `Buy_Hold` to `SIGNAL_TYPES` without guarding the summary code.

**Leverage now multiplies position sizes** (changed 2026-03-18, both endpoints):
- **Previous behavior**: `wanted = equity * pos_size`, leverage only acted as an exposure cap (`equity * max_lev`).
- **Current behavior**: `wanted = equity * pos_size * max_lev`, cap = `equity * max_lev`. This means leverage amplifies position sizing, not just limits exposure.
- Applies to both `POST /api/backtest` (line 2814: `wanted = eq_est * pos_size * max_lev`) and `POST /api/backtest/multi` (line 3139: same formula per leg, both allocated and standalone paths).

**`_build_leg_trades()` helper** (`app/backend/main.py:2188`): Function extracted to encapsulate trade-building logic (data loading, filter application, signal detection, membership filtering, trade construction, Buy_Hold branch). Called by `run_multi_backtest()` via the multi-backtest path. The single `POST /api/backtest` endpoint has its own inline implementation — not yet refactored to call `_build_leg_trades()`.

**Regime filter column contract**: When filters are applied, the following columns must be present in the respective parquet:
- `pct_metrics`: `Uptrend_Pct`, `Breakout_Pct`, `Correlation_Pct`, `RV_EMA`, `Breakdown_Pct`, `Downtrend_Pct` — all present in basket signals parquets (written by `_finalize_basket_signals_output()`)
- `bool_metrics`: `Is_Breakout_Sequence`, `Trend`, `BTFD_Triggered`, `STFR_Triggered` — present in both `signals_500.parquet` and basket signals parquets

**Verification consumer** (`app/backend/verify_backtest.py`):
- Standalone script that POSTs to `/api/backtest` and independently re-derives trades, equity curve, and stats from raw parquet files.
- Reads: `signals_500.parquet`, basket parquets (via glob), `gics_mappings_500.json`, and thematic universe JSONs.
- Invoked via CLI: `python verify_backtest.py --run-defaults` or with `--target`/`--signal` flags.

---

### `POST /api/backtest/multi` — multi-leg basket backtest (updated 2026-03-18)

**Request model** — `MultiBacktestRequest` (`app/backend/main.py:2180`):
```
{
  legs: MultiBacktestLeg[],   // at least 2 (renamed from MultiBasketLeg)
  start_date: str | null,
  end_date: str | null,
  initial_equity: float,      // default 100000
  max_leverage: float,        // default 2.5
}
```

**`MultiBacktestLeg`** (`app/backend/main.py:2172`) — renamed from `MultiBasketLeg`:
```
{
  target: str,                // basket name or ticker
  target_type: str,           // "basket" | "basket_tickers" | "ticker"
  entry_signal: str,          // signal from SIGNAL_TYPES (including "Buy_Hold")
  allocation_pct: float,      // 0.0-1.0, fraction of total equity (all legs must sum to 1.0)
  position_size: float,       // default 1.0 (was 0.25 before HEAD commit), per-leg
  filters: BacktestFilter[],  // default [] (BacktestFilter now supports source field)
}
```

**Breaking change from prior version**: `MultiBasketLeg` renamed to `MultiBacktestLeg`. `position_size` default changed from `0.25` to `1.0`. Frontend code sending `MultiBacktestRequest` bodies must use `MultiBacktestLeg` semantics (though field names are identical — the rename is internal to the Python model).

**Key design detail**: `position_size` is per-leg — each leg sizes positions as a fraction of its own allocated equity. Leverage multiplies position sizes: `wanted = equity * pos_size * max_lev`, capped at `equity * max_lev`.

**Response shape** (updated 2026-03-18 — `trade_paths` added per leg, `per_leg` standalone, `daily_positions` fixed):
```
{
  legs: [                         // one per input leg
    {
      target, target_type, entry_signal, allocation_pct, direction,
      trades: [...],              // per-leg trade list
      trade_paths: number[][],    // daily % return paths from entry to exit for each trade (added 2026-03-18)
      stats: { trades, win_rate, avg_winner, avg_loser, ev, profit_factor, max_dd, avg_bars }
    },
    ...
  ],
  combined: {
    equity_curve: {
      dates: ["YYYY-MM-DD", ...],
      combined: [float, ...],     // summed equity across all legs (uses allocated capital)
      per_leg: [[float, ...], ...], // per-leg STANDALONE equity series (starts at initial_equity, not initial * alloc_pct)
      buy_hold: [float, ...],     // quarterly-rebalanced buy-and-hold benchmark
    },
    stats: { ... },               // stats computed on all trades merged
  },
  date_range: { min, max },
  skipped_entries: [...] | null,  // includes leg_index and leg_target fields
  daily_positions: { [int_date_idx]: { exposure_pct, equity, positions: [...] } } | null
}
```

**`per_leg` standalone curves** (changed 2026-03-18):
- **Previous behavior**: `per_leg` showed allocated-capital curves (starting at `initial_equity * allocation_pct`).
- **Current behavior**: Two parallel simulations run per leg — an allocated path (for combining via sum into the `combined` curve) and a standalone path (starts at full `initial_equity`, runs its own equity/cash tracking). The `per_leg` array in the response uses the standalone curves for chart display. This means each leg's curve shows what performance would look like if that leg had the full portfolio.
- Implementation: `per_leg_equity` (allocated, for combined sum) vs `per_leg_standalone` (standalone, for `per_leg` response). See `app/backend/main.py:3058–3059`.

**`daily_positions` fix** (changed 2026-03-18):
- **Previous behavior**: Keyed by string date values (e.g. `"2024-01-15"`).
- **Current behavior**: Keyed by integer date index (matches `equity_curve.dates` array index), consistent with single-leg endpoint.
- Position entries now include full detail: `weight`, `daily_return`, `contribution`, `leg_target`, `entry_date`.
- Post-processing pass sets combined `equity` and total `exposure_pct` per date from the `combined_equity` array.

**`trade_paths` per leg** (added 2026-03-18):
- Each leg response now includes `trade_paths: number[][]` — daily percentage return paths from entry to exit for every trade in that leg.
- Computed from `ticker_closes` per-ticker close series: `path[i] = close[i] / entry_price - 1`.
- Enables the Path tab in `MultiBacktestPanel` for visualizing individual trade trajectories across legs.

**Buy-and-hold benchmark**: Single-leg B&H — uses the largest-allocation leg's basket/ticker Close series as the benchmark. The buy-and-hold tracks the first available close in the date range and scales proportionally. Falls back to the first leg if no clear largest-allocation leg exists.

**Helper function** — `_build_leg_trades()` (`app/backend/main.py:2188`) — replaces `_build_leg_data()`:
- Extracts trade-building logic (data loading, filter application — including external source merging — signal detection, membership filtering, trade construction, Buy_Hold branch) into a reusable function.
- Returns `(trades, df, ticker_closes, direction)`.
- Called by `run_multi_backtest()`. The single `POST /api/backtest` endpoint still has its own inline implementation.

**Frontend consumer**: `app/frontend/src/components/MultiBacktestPanel.tsx` — dedicated panel that constructs a `MultiBacktestRequest` and renders per-leg trades/stats, combined equity curve, and trade path visualization (Path tab).

---

## Frontend Component Integration Notes

### BacktestPanel — Overlay Toggle Props Removed (2026-03-16); equity curve percentage-based (2026-03-18)
- **Previous behavior**: `App.tsx` passed overlay toggle props to `BacktestPanel` via its interface: `showPivots`, `showTargets`, `showVolume`, `showBreadth`, `showBreakout`, `showCorrelation`. Also, on submit, `BacktestPanel` fired 7 parallel POST requests: 1 main + 6 benchmark calls with `benchmarks_only: true`.
- **Current behavior**: All six toggle props removed from `BacktestPanel`'s interface (internally managed). `benchmarks_only` and `include_positions` fields also removed from `BacktestRequest` model in the backend.
- **Equity curve percentage rebasing** (added 2026-03-18): All equity curve series (filtered, unfiltered, buy_hold, benchmarks) are rebased to 0% return from the visible window start. The `rebase()` function divides each value by the first visible value and subtracts 1, converting to percentage returns. Y-axis shows percentages with a breakeven line at 0%. This rebasing applies dynamically when scrolling/zooming or selecting timeframe presets.
- **Impact**: No backend/data contract change beyond what is noted in the `POST /api/backtest` section. Percentage display is purely a frontend presentation change.

### MultiBacktestPanel — Multi-leg backtest UI (2026-03-16, updated 2026-03-18)
- **Component**: `app/frontend/src/components/MultiBacktestPanel.tsx`
- **Backend endpoint**: `POST /api/backtest/multi` (see endpoint section above for full request/response schema)
- **Renders**: Per-leg trade tables and stats, combined equity curve chart with per-leg overlay and quarterly-rebalanced buy-and-hold, daily position snapshots across all legs.
- **Equity curve percentage rebasing** (added 2026-03-18): Same percentage-based rebasing as `BacktestPanel` — all series rebased to 0% return from visible window start.
- **Path tab** (added 2026-03-18): Trade paths chart visualizing daily percentage return trajectories from entry to exit for all trades across all legs. Includes a sortable legend with columns: Leg, Ticker, Date, Chg. Uses `trade_paths` from each leg in the response. Hovered paths are highlighted; legend entries are color-coded by leg.
- **Also referenced in**: `app/frontend/src/App.tsx` (imported and routed)

### BacktestPanel — Leverage Preset Buttons & Uniform Sizing (2026-03-16)
- **Added**: `LEV_PRESETS = [100, 110, 125, 150, 200, 250]` constant and a row of leverage preset buttons (`.backtest-pos-preset`) in the Position Sizing config section. Clicking a button sets `maxLeverage` state, which is sent as `max_leverage: maxLeverage / 100` to `POST /api/backtest` (unchanged contract).
- **CSS changes** (`index.css`): Added `.backtest-pos-preset` / `.backtest-pos-preset.wide` button styles, uniform `.control-btn` sizing (120px x 32px), `.sidebar-header` and `.main-header` both pinned to `height: 114px` for alignment, `.backtest-results-header` height fixed to 41px to match accordion row heights.
- **Impact**: No backend/data contract change. The `POST /api/backtest` request body already accepted `max_leverage`; preset buttons are a UI convenience only.

### AnalogsPanel — Regime analog search UI (2026-03-17)
- **Component**: `app/frontend/src/components/AnalogsPanel.tsx`
- **Backend endpoint**: `GET /api/baskets/returns?mode=analogs` (see endpoint section above for full request/response schema)
- **Previous consumer**: `BasketReturnsChart` component in `BasketSummary.tsx` handled all three modes (period/daily/analogs). The `mode=analogs` consumer has been extracted to a dedicated top-level component.
- **Renders**: Regime analog similarity results, forward series charts (daily cumulative returns per basket up to 252 days), similarity breakdown details, and aggregate statistics (mean/median/min/max/std at 1M/3M/6M horizons with per-basket breakdown).
- **Also referenced in**: `app/frontend/src/App.tsx` (imported and routed), `app/frontend/src/index.css` (styling)

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

## Internal Pipeline Notes (signals/rotations.py only — no backend impact)

### `compute_equity_ohlc_cached()` return signature change (2026-03-17)
- **Previous signature**: Returns `ohlc_df` (single DataFrame)
- **Current signature** (`signals/rotations.py:3595`): Returns `(ohlc_df, contrib_df)` tuple
  - `contrib_df` is a DataFrame with columns `[Date, Ticker, Weight_BOD, Daily_Return, Contribution]` when the equity cache is rebuilt, or `None` when the cache is fully current
  - The tuple is produced by calling `compute_equity_ohlc()` with `return_contributions=True`
- **Callers updated**:
  - Pre-build equity cache loop at line 2769: `eq, _ = compute_equity_ohlc_cached(...)` — discards contributions (pre-build only needs the OHLC cache to exist)
  - Main basket processing at line 4190: `ohlc_df, _contrib_df = compute_equity_ohlc_cached(...)` — passes `_contrib_df` through to `_finalize_basket_signals_output()` which saves it as the contributions parquet (avoiding redundant recomputation)
- **No backend impact**: The backend never calls `compute_equity_ohlc_cached()` directly; it reads the output parquet files.

### `FORCE_REBUILD_BASKET_SIGNALS` now forces equity OHLC rebuild (2026-03-17)
- **Location**: `signals/rotations.py` (constant near top), checked inside `compute_equity_ohlc_cached()` at line 3595
- **Previous behavior**: `FORCE_REBUILD_BASKET_SIGNALS = True` only forced basket signals parquets to be rebuilt
- **Current behavior**: `compute_equity_ohlc_cached()` now also checks `FORCE_REBUILD_BASKET_SIGNALS` (line 3591: `if FORCE_REBUILD_EQUITY_CACHE or FORCE_REBUILD_BASKET_SIGNALS`). When either is `True`, the equity OHLC cache is invalidated and rebuilt, which produces fresh `contrib_df` as a byproduct.
- **Rationale**: Since basket signals now depend on contributions data from the equity OHLC build, forcing basket signals rebuild should also force the upstream equity OHLC rebuild to ensure contributions are fresh.
- **No backend impact**: These are build-time constants in the batch pipeline.

---

## Issues (Priority Order)

1. ~~**CRITICAL**: Basket file naming mismatch~~ — **RESOLVED** (2026-03-13). Legacy `*_basket.parquet` / `*_basket_meta.json` fallback globs fully removed from `main.py`. All four lookup functions (`_find_basket_parquet`, `_find_basket_meta`, `list_baskets`, `get_basket_breadth`) now exclusively use `*_signals.parquet` / `*_signals_meta.json` patterns.

2. ~~**MODERATE**: `top500stocks.pkl` not produced~~ — **RESOLVED** (2026-03-13). `signals_engine.py` no longer reads `.pkl`; uses `top500stocks.json` instead.

3. ~~**LOW**: `signals_engine.py` missing `Avg_Winner_Bars`/`Avg_Loser_Bars`~~ — **RESOLVED** (2026-03-13). `RollingStatsAccumulator` now tracks both columns.

4. ~~**LOW**: BTFD/STFR entry price logic differs~~ — **RESOLVED** (2026-03-13). `signals_engine.py` batch path now uses previous day's target with gap fill logic, matching `rotations.py`.

5. **LOW**: `Buy_Hold` in `SIGNAL_TYPES` causes `KeyError` in `get_basket_summary()` (2026-03-18). The loop at `app/backend/main.py:1518` iterates `SIGNAL_TYPES` and indexes `SIGNAL_IS_COL[st]` for each — but `Buy_Hold` has no entry in `SIGNAL_IS_COL`. This will crash `GET /api/baskets/{name}/summary` on first call. Fix: either guard the loop with `if st in SIGNAL_IS_COL` or remove `Buy_Hold` from `SIGNAL_TYPES` and handle it as a standalone constant.
