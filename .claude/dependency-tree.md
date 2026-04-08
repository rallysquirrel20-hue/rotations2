# Dependency Tree

Updated: 2026-04-08 (Q2 correlation fix in _compute_within_basket_correlation)

## File Dependency Graph

```
config.py                    (foundation — imported by all others)
  ├── build_universes.py     (depends on config)
  ├── build_signals.py       (depends on config)
  ├── build_baskets.py       (depends on config + build_signals)
  └── live_updates.py        (depends on config + build_signals)

Loop schedulers (subprocess calls, no Python imports):
  ├── loop_universes.py  → build_universes.py
  ├── loop_signals.py    → build_signals.py + build_baskets.py
  └── loop_live.py       → live_updates.py
```

---

## signals/config.py

Shared configuration, constants, paths, and utility functions. Zero side effects at import.

### Constants (selected)
- `SIZE = 500`, `ETF_SIZE = 50`, `THEME_SIZE = 25`, `START_YEAR = 2000`
- `SIGNALS = ['Up_Rot', 'Down_Rot', 'Breakout', 'Breakdown', 'BTFD', 'STFR']`
- `RV_MULT`, `EMA_MULT`, `RV_EMA_ALPHA` — signal math constants
- `FORCE_REBUILD_EQUITY_CACHE`, `FORCE_REBUILD_BASKET_SIGNALS` — env-var flags

### Classes
| Line | Class | Purpose |
|------|-------|---------|
| 149 | `OutputPaths` | Dataclass: derives all sub-folder paths from `BASE_OUTPUT_FOLDER` |
| 223 | `WriteThroughPath` | Wraps local path + OneDrive mirror; auto-copies on every write |

### Functions
| Line | Function | Purpose |
|------|----------|---------|
| 87 | `_resolve_onedrive_output_folder()` | Finds OneDrive mirror path |
| 115 | `_mirror_to_onedrive(local_path)` | Copies file to OneDrive |
| 128 | `_needs_write_and_mirror(local_path)` | Returns (need_write, need_mirror) |
| 282 | `build_pdf(figures, path)` | Save matplotlib figures to PDF |
| 308 | `_timed_print(*args, **kwargs)` | Print with elapsed time prefix |
| 316 | `_install_timed_print()` | Replace builtins.print with timed version |
| 321 | `reset_cell_timer(cell_name)` | Reset timer for timed_print |
| 331 | `_quarter_end_from_key(key)` | `'2026 Q2'` → last calendar day of Q2 |
| 339 | `_quarter_start_from_key(key)` | `'2026 Q2'` → first calendar day of Q2 |
| 347 | `get_current_quarter_key()` | Returns e.g. `'2026 Q2'` |
| 356 | `_universe_to_json(universe)` | Serialize `{str: set}` to JSON |
| 361 | `_json_to_universe(text)` | Deserialize JSON to `{str: set}` |
| 366 | `_beta_universes_to_json(result)` | Serialize `(high, low)` tuple |
| 373 | `_json_to_beta_universes(text)` | Deserialize beta tuple |
| 380 | `_gics_to_json(result)` | Serialize GICS 4-tuple |
| 391 | `_json_to_gics(text)` | Deserialize GICS 4-tuple |
| 402 | `atomic_write_parquet(df, path)` | Atomic write via tmp+rename, then mirror |
| 418 | `atomic_write_json(path, data)` | Atomic JSON write via WriteThroughPath |
| 423 | `load_universe_from_disk()` | Load equity universe from CACHE_FILE |
| 437 | `load_etf_universe_from_disk()` | Load ETF universe from ETF_CACHE_FILE |
| 451 | `load_gics_from_disk()` | Load GICS mappings from GICS_CACHE_FILE |
| 465 | `load_thematic_universe_from_disk(cache_file, subkey)` | Load thematic universe JSON |

---

## signals/build_universes.py

Builds all quarterly universes and saves to JSON caches. Uses NYSE calendar (`exchange_calendars`) to detect quarter boundaries.

### Imports from config
`SIZE`, `ETF_SIZE`, `THEME_SIZE`, `DIV_THEME_SIZE`, `START_YEAR`, `LOOKBACK_DAYS`, `MOMENTUM_LOOKBACK_DAYS`, `MARKET_SYMBOL`, `SECTOR_LIST`, cache file paths, serializers, quarter helpers, timer utilities

### Functions
| Line | Function | Purpose |
|------|----------|---------|
| 47 | `_last_trading_day_of_quarter(quarter_key)` | NYSE calendar lookup |
| 54 | `_latest_norgate_date()` | Latest date in Norgate SPY data |
| 69 | `_needs_rebuild(cached, force)` | Check if universe needs rebuild |
| 90 | `_prev_qtr(key)` | Quarter key - 1 |
| 95 | `_next_qtr(key)` | Quarter key + 1 |
| 100 | `_offset_key(date)` | Date → quarter key |
| 107 | `_filter_delisted(tickers, quarter_key)` | Remove delisted tickers |
| 121 | `_fetch_prices(ticker)` | Norgate price fetch |
| 137 | `_parallel_fetch(symbols, worker)` | ThreadPoolExecutor wrapper |
| 196 | `_cached_build(cache_file, builder_fn, ...)` | Generic cache-or-build pattern |
| 215 | `_needs_rebuild_simple(cached)` | Simpler staleness check |
| 241 | `_build_core_universe()` | Build top-500 by dollar volume |
| 270 | `_build_or_load_core(force)` | Cache wrapper for core universe |
| 292 | `_build_etf_universe()` | Build top-50 ETFs |
| 316 | `_build_or_load_etf(force)` | Cache wrapper for ETFs |
| 326 | `_build_ticker_names(quarter_universe, etf_universe)` | Ticker → name mapping |
| 359 | `_build_beta(quarter_universe)` | High/low beta universes |
| 394 | `_build_momentum(quarter_universe)` | Winners/losers momentum |
| 410 | `_build_risk_adj_momentum(quarter_universe)` | Risk-adjusted momentum |
| 427 | `_build_dividends(quarter_universe)` | High yield / growth / with growth |
| 506 | `_build_size(quarter_universe)` | Market cap quintiles |
| 520 | `_build_volume_growth(quarter_universe)` | Volume growth leaders |
| 553 | `_build_gics(quarter_universe)` | Sector/industry classifications |
| 629 | `main()` | Entry point: builds all universes |

### Writes
`top500stocks.json`, `etf_universes_50.json`, `ticker_names.json`, `beta_universes_500.json`, `momentum_universes_500.json`, `risk_adj_momentum_500.json` (winners/losers), `dividend_universes_500.json`, `size_universes_500.json`, `volume_universes_500.json` (leaders/losers, YoY), `gics_mappings_500.json` (industries filtered to top 25% by dollar volume per quarter)

---

## signals/build_signals.py

Builds stock and ETF signal DataFrames from universe tickers using Numba-accelerated passes.

### Imports from config
`SIZE`, `ETF_SIZE`, `SIGNALS`, `RV_MULT`, `EMA_MULT`, `RV_EMA_ALPHA`, `MARKET_SYMBOL`, `INCREMENTAL_MAX_DAYS`, `SIGNALS_CACHE_FILE`, `ETF_SIGNALS_CACHE_FILE`, `load_universe_from_disk`, `load_etf_universe_from_disk`, `WriteThroughPath`, `atomic_write_parquet`, `_install_timed_print`, `reset_cell_timer`, `DATA_FOLDER`

### Functions
| Line | Function | Purpose |
|------|----------|---------|
| 30 | `_numba_passes_1_to_4(...)` | @njit: Trend/Pivots, Ranges/Targets, Signals, Regime |
| 196 | `_numba_pass5_signal(...)` | @njit: Trade tracking (entry/exit/MFE/MAE) |
| 301 | `_build_signals_from_df(df, ticker)` | Full signal build for one ticker's history |
| 420 | `_build_signals_next_row(prev_row, live_price, ...)` | Incremental one-bar signal update |
| 611 | `build_signals_for_ticker(ticker)` | Fetch Norgate + build signals |
| 623 | `_build_signals_append_ticker(ticker, cached_last_row)` | Incremental append |
| 705 | `_get_latest_norgate_date()` | Latest date in Norgate SPY |
| 753 | `_optimize_dtypes(df)` | Downcast float64 → float32 |
| 767 | `_save_signals(df, cache_file)` | Optimize + write parquet |
| 790 | `_cache_is_current(df)` | Check staleness |
| 823 | `_incremental_update(cached_df, universe, ...)` | Append new bars to cache |
| 910 | `_load_or_build(universe, cache_file, force)` | Load or full rebuild |
| 999 | `load_or_build_signals(quarter_universe)` | Public: equity signals |
| 1003 | `load_or_build_etf_signals(etf_universe)` | Public: ETF signals |
| 1007 | `main()` | Entry point |

### Writes
`signals_500.parquet`, `signals_etf_50.parquet`

### Exported (used by build_baskets + live_updates)
`_build_signals_from_df`, `_build_signals_next_row`, `_get_latest_norgate_date`

---

## signals/build_baskets.py

Builds basket signals (OHLC + signals + breadth + correlation) for all baskets.

### Imports from config
`SIZE`, `DATA_FOLDER`, cache file paths, `WriteThroughPath`, quarter helpers, loaders, timer utilities, `paths`

### Imports from build_signals
`_build_signals_from_df`, `_build_signals_next_row`

### Key Functions
| Line | Function | Purpose |
|------|----------|---------|
| 226 | `compute_breadth_from_trend(...)` | Uptrend_Pct per date |
| 251 | `compute_breadth_from_breakout(...)` | Breakout_Pct per date |
| 274 | `compute_breadth_pivots(ema_values)` | Signal detection on breadth EMA |
| 444 | `compute_equity_ohlc(...)` | Equal-weight basket OHLC with quarterly rebalance |
| 940 | `compute_equity_ohlc_cached(...)` | Cached wrapper |
| 1047 | `_compute_within_basket_correlation(...)` | 21-day rolling pairwise correlation. Sparsity gate checks `sub_ret` (warmup + current quarter), not current-quarter slice — otherwise new quarters produce 0 rows until ~14 trading days elapse. |
| 1196 | `_finalize_basket_signals_output(...)` | Merge OHLC + breadth + corr + contributions |
| 1332 | `_compute_and_save_contributions(...)` | Per-constituent return contributions |
| 1472 | `process_basket_signals(...)` | Main per-basket processor |
| 1641 | `_build_or_load_returns_matrix(all_signals_df)` | Date×Ticker return pivot |
| 1754 | `main()` | Entry point |

### Industry Filtering
- Daily incremental: only processes current-quarter industries (top 25% by dollar volume)
- `--force`: processes all historically qualifying industries (full history rebuild)
- `FORCE_REBUILD_BASKET_SIGNALS` defaults to `False`

### Writes
`{cache}/*_signals.parquet`, `{cache}/*_ohlc.parquet`, `{cache}/*_contributions.parquet`, `{cache}/*_meta.json`, `returns_matrix_500.parquet`

---

## signals/live_updates.py

Fetches live Databento data, computes intraday signals and basket OHLC.

### Imports from config
`SIZE`, `ETF_SIZE`, `SIGNALS`, `DATA_FOLDER`, `paths`, cache file paths, loaders, quarter helpers, `WriteThroughPath`, timer utilities, `LIVE_ROTATIONS_FOLDER`, `HOLDINGS_FOLDER`, `SECTOR_LIST`

### Imports from build_signals
`_build_signals_from_df`, `_build_signals_next_row`, `_get_latest_norgate_date`

### Key Functions
| Line | Function | Purpose |
|------|----------|---------|
| 81 | `get_realtime_prices(symbols)` | Databento live snapshot |
| 216 | `get_live_ohlc_bars(symbols)` | Databento OHLC bars |
| 336 | `_get_live_update_gate(all_signals_df)` | Determine if live update needed |
| 633 | `_get_live_update_context(quarter_universe, all_signals_df)` | Build context dict for live cycle |
| 706 | `export_today_signals(...)` | Compute full signal rows → `live_signals_500.parquet` |
| 850 | `export_today_etf_signals(...)` | Live ETF OHLC export |
| 897 | `append_live_today_to_etf_signals_parquet(...)` | Append live ETF rows |
| 1020 | `_write_live_basket_ohlc(live_ctx, all_basket_specs)` | Live basket OHLC bars |
| 1209 | `update_basket_parquets_with_live_ohlcv(live_ctx, all_basket_specs)` | Append live row to basket parquets |
| 1313 | `main()` | Entry point: full live update cycle |

### Writes
`live_signals_500.parquet` (full signal rows with Source='live'), `live_signals_etf_50.parquet`, `live_basket_signals_500.parquet`

---

## Loop Schedulers

All use `exchange_calendars` (NYSE) for trading day schedule. Run scripts via `subprocess.run()`.

| File | Script Called | Schedule |
|------|-------------|----------|
| `loop_universes.py` | `build_universes.py` | 5pm ET, last trading day of quarter |
| `loop_signals.py` | `build_signals.py` then `build_baskets.py` | 5pm ET, every trading day |
| `loop_live.py` | `live_updates.py` | Every 5 min, 9:30–4:00 ET, trading days |
