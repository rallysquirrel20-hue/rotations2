# Dependency Tree
Updated: 2026-03-25 (incremental — PDF/xlsx/TradingView exports removed from rotations.py; 5 new split-pipeline files added; export_live_basket_signals added)
Files scanned: 22
Functions indexed: ~320

---

## Cell Map — signals/rotations.py (6332 lines)

| Cell # | Title | Lines | Key Functions |
|--------|-------|-------|---------------|
| 0 | Imports & Dependencies | 1-2 | (imports only) |
| 1 | Configuration & Constants | 3-175 | `_resolve_onedrive_output_folder`, `_mirror_to_onedrive`, `_needs_write_and_mirror`, OutputPaths class, WriteThroughPath class |
| 2 | Utility Functions | 176-295 | `build_pdf`, `_timed_print`, `_install_timed_print`, `reset_cell_timer`, `_get_current_quarter_key` |
| 3 | Universe Construction | 296-1571 | `get_quarterly_vol`, `build_quarter_universe`, `is_universe_current`, `_universe_to_json`, `_json_to_universe`, `load_or_build_universe`, `get_universe`, `build_quarter_etf_universe`, `load_or_build_etf_universe`, `_build_ticker_names`, `_quarter_end_from_key`, `_quarter_start_from_key`, `_calc_beta_quarterly`, `_safe_calc_beta`, `build_quarter_beta_universes`, `is_beta_universes_current`, `_beta_universes_to_json`, `_json_to_beta_universes`, `load_or_build_beta_universes`, `_calc_momentum_quarterly`, `_safe_calc_momentum`, `build_quarter_momentum_universes`, `is_momentum_universes_current`, `load_or_build_momentum_universes`, `_calc_risk_adj_momentum_quarterly`, `_safe_calc_risk_adj_momentum`, `build_quarter_risk_adj_momentum`, `is_risk_adj_momentum_current`, `load_or_build_risk_adj_momentum`, `_calc_dividend_yield_quarterly`, `_safe_calc_dividend_yield`, `_calc_trailing_dividends_quarterly`, `_safe_calc_trailing_divs`, `build_quarter_dividend_universes`, `is_dividend_universes_current`, `load_or_build_dividend_universes`, `_calc_avg_dollar_volume_quarterly`, `_safe_calc_dollar_volume`, `build_quarter_size_universes`, `is_size_universe_current`, `load_or_build_size_universes`, `_calc_quarterly_dollar_volume`, `_safe_calc_quarterly_dollar_volume`, `build_quarter_volume_growth_universes`, `is_volume_growth_current`, `load_or_build_volume_growth_universes`, `_build_gics_mappings`, `_build_sector_universes`, `_build_industry_universes`, `_is_gics_current`, `_gics_to_json`, `_json_to_gics`, `load_or_build_gics_mappings` |
| 4 | Signal Cache | 1572-3308 | `calc_rolling_stats`, RollingStatsAccumulator class, `_numba_passes_1_to_4`, `_numba_pass5_signal`, `_build_signals_from_df`, `_build_signals_next_row`, `build_signals_for_ticker`, `_build_signals_append_ticker`, `_incremental_update_signals`, `_get_latest_norgate_date`, `_signals_cache_is_current`, `load_or_build_signals`, `_etf_signals_cache_is_current`, `_incremental_update_etf_signals`, `load_or_build_etf_signals` |
| 5 | Basket Processing | 3309-5380 | `_cache_slugify_label`, `_cache_build_quarter_lookup`, `_cache_find_active_quarter`, `_compute_equity_close_for_cache`, `_get_data_signature`, `_prebuild_equity_cache_from_signals`, `compute_breadth_pivots`, `compute_signal_trades`, `_build_quarter_lookup`, `_find_active_quarter`, `_build_membership_df`, `_vectorized_quarter_filter`, `compute_breadth_from_trend`, `compute_breadth_from_breakout`, `_build_quarter_weights`, `compute_equity_ohlc`, `_build_universe_signature`, `_equity_cache_paths`, `_load_equity_cache`, `_save_equity_cache`, `_build_equity_meta`, `_is_equity_cache_valid`, `_basket_cache_folder`, `_cache_file_stem`, `_basket_cache_paths`, `_find_basket_parquet`, `_find_basket_meta`, `_get_chart_schema_version_from_parquet`, `_build_basket_signals_meta`, `_is_basket_signals_cache_valid`, `compute_equity_ohlc_cached`, `compute_equity_curve`, `_fmt_price`, `_fmt_bars`, `_fmt_pct`, `_append_trade_rows`, `_compute_within_basket_correlation`, `_compute_within_basket_correlation_incremental`, `_augment_basket_signals_with_breadth`, `_finalize_basket_signals_output`, `_compute_and_save_contributions`, `_compute_and_save_contributions_incremental`, `_record_basket_timing`, `process_basket_signals`; pre-computation block: builds `returns_matrix` + `ohlc_ret_matrices`, basket loop |
| 6 | Live Intraday Data | 5381-6332 | `_load_env_file`, `get_realtime_prices`, `get_realtime_ohlcv`, `get_live_ohlc_bars`, `_get_latest_norgate_date_fallback`, `_extract_spy_trade_date_from_df`, `_get_spy_last_trade_date_databento`, `_get_live_update_gate`, `_is_market_open_via_spy_volume`, `_append_live_row`, `build_signals_for_ticker_live`, `_sort_signals_df`, `export_today_signals`, `append_live_today_to_signals_parquet`, `export_today_etf_signals`, `append_live_today_to_etf_signals_parquet`, `_get_latest_norgate_rows_by_ticker`, `_compute_live_basket_ohlc`, `_compute_live_basket_ohlcv`, `_get_live_update_context`, `export_live_basket_signals`, `update_basket_parquets_with_live_ohlcv` |

---

## File Summary — All Python Files

| File | Lines | Functions | Purpose |
|------|-------|-----------|---------|
| signals/rotations.py | 6332 | ~150 | Main pipeline: universe, signals, baskets, live exports |
| signals/foundation.py | 2027 | ~30 | Shared constants, paths, signal engine, basket cache helpers (imported by split-pipeline files) |
| signals/universe.py | 1210 | 44 | Phase 1: quarterly universe construction (extracted from rotations.py) |
| signals/tickersignals.py | 858 | 9 | Phase 2: daily ticker + ETF signal generation (extracted from rotations.py) |
| signals/basketsignals.py | 1523 | 20 | Phase 3: basket equity/signal processing (extracted from rotations.py) |
| signals/livesignals.py | 1120 | 17 | Phase 4: intraday live loop (extracted from rotations.py) |
| signals/rotations_old_outputs.py | 2107 | 35 | Extracted Group B report cells (Excel, correlations, charts, PDFs) |
| signals/databento_test.py | 624 | 16 | Databento API connectivity tests |
| app/backend/main.py | 4300 | 46 | FastAPI REST endpoints + WebSocket |
| signals/test_all_optimizations.py | 75 | 1 | Compares old vs new breadth/contributions/correlation values after rebuild |
| signals/test_correlation_optimization.py | 207 | 5 | Step-by-step test harness: backup, compare, check returns_matrix, test API, restore |
| app/backend/signals_engine.py | 534 | 2 | Live signal computation (parallel impl) |
| app/backend/audit_basket.py | 327 | 7 | Diagnostic: equity curve audit tool |
| app/backend/verify_backtest.py | 1016 | 22 | Standalone CLI backtest verification (replays trades/equity from raw data, compares vs API) |
| app/backend/check_data.py | 5 | 0 | Quick data inspection script |
| app/backend/check_pivots.py | 7 | 0 | Quick pivot inspection script |
| signals/live_loop.py | 39 | 1 | PM2 daemon: reruns rotations.py every 15 min via runpy |
| app/backend/strategy_scanner.py | 503 | 10 | Backtest strategy scanner (sweeps parameter combinations via API) |

---

## File Detail — signals/live_loop.py

**Purpose:** PM2-managed daemon that drives the continuous signal refresh cycle.

**Key constant:** `INTERVAL = 900` (15 minutes between runs)

**Named function (added in commit 02c4eb0):**
#### `log(msg)` (L18-20)
- Writes to `sys.stderr` (not stdout) so cycle banners are not swallowed by rotations.py's timing wrapper (`_timed_print` intercepts stdout)
- Called by: the `while True` loop for cycle-start and cycle-end banners

**How it works:**
1. Resolves the path to `signals/rotations.py` at startup using `Path(__file__).with_name("rotations.py")`
2. Increments `cycle` counter and logs cycle-start banner via `log()`
3. Calls `runpy.run_path(script, run_name="__main__")` — this executes the entire `rotations.py` pipeline in a fresh namespace on every iteration, equivalent to running it as a standalone script
4. Any exception is caught and printed via `traceback.print_exc()` so a crash in one iteration does not kill the loop
5. Logs cycle-end banner and sleep duration via `log()`, then sleeps `INTERVAL` seconds

**Relationship to rotations.py:**
- Invokes the complete rotations.py pipeline (all Cells 0-7) on each loop tick
- Cache guards inside rotations.py (`is_*_current()`, `_signals_cache_is_current`, `_is_equity_cache_valid`) skip expensive rebuilds when data is still fresh
- Cell 6's market-hours gate (`_get_live_update_gate`) no-ops live Databento calls outside Mon-Fri 09:25-16:15 ET

**PM2 integration:** Registered as the `live-signals` app in `ecosystem.config.js`

**Imports:** `sys`, `time`, `runpy`, `traceback`, `datetime.datetime`, `pathlib.Path`

**Called by:** PM2 process manager (via `ecosystem.config.js` `live-signals` entry) — not called by any other Python module

---

## Constants & Version Config

| Constant | Value | File | Line | Purpose |
|----------|-------|------|------|---------|
| `START_YEAR` | 2000 | signals/rotations.py | 31 | Minimum year for universe construction |
| `SIZE` | 500 | signals/rotations.py | 32 | Top N stocks by dollar volume |
| `EQUITY_CACHE_SCHEMA_VERSION` | 1 | signals/rotations.py | 97 | Bump to force equity cache rebuild |
| `EQUITY_SIGNAL_LOGIC_VERSION` | '2026-03-13-btfd-stfr-prev-trend' | signals/rotations.py | 98 | Bump on signal logic changes |
| `EQUITY_UNIVERSE_LOGIC_VERSION` | '2026-02-10-codex-1' | signals/rotations.py | 99 | Bump on universe logic changes |
| `FORCE_REBUILD_EQUITY_CACHE` | False | signals/rotations.py | 100 | Manual override |
| `BASKET_SIGNALS_CACHE_SCHEMA_VERSION` | 1 | signals/rotations.py | 101 | Bump to force basket signals rebuild |
| `FORCE_REBUILD_BASKET_SIGNALS` | False | signals/rotations.py | 102 | Manual override |
| `CHART_SCHEMA_VERSION` | 2 | signals/rotations.py | 103 | Bump to force chart PNG rebuild |
| `BENCHMARK_BASKETS` | 0 | signals/rotations.py | 104 | If > 0, process only this many baskets then stop (benchmarking shortcut) |
| `BENCHMARK_TIMING` | True | signals/rotations.py | 105 | If True, print per-step timing breakdown for each basket |
| `_basket_timing` | dict of lists | signals/rotations.py | 107-113 | Global accumulator for per-step timing data (written by `_record_basket_timing`) |
| `_basket_timing_names` | list | signals/rotations.py | 113 | Ordered basket names parallel to `_basket_timing` rows |
| `THEME_SIZE` | 25 | signals/rotations.py | 411 | Stocks per thematic basket |
| `DIV_THEME_SIZE` | 25 | signals/rotations.py | 416 | Stocks per dividend basket |
| `LOOKBACK_DAYS` | 252 | signals/rotations.py | 414 | Beta rolling window |
| `MOMENTUM_LOOKBACK_DAYS` | 252 | signals/rotations.py | 580 | Momentum rolling window |
| `INDUSTRY_MIN_STOCKS` | 10 | signals/rotations.py | 1062 | Min stocks for industry basket |
| `INCREMENTAL_MAX_DAYS` | 5 | signals/rotations.py | 2784 | Max staleness before full rebuild |
| `RV_MULT` | sqrt(252)/sqrt(21) | signals/rotations.py | 1604 | Realized volatility multiplier |
| `EMA_MULT` | 2.0/11.0 | signals/rotations.py | 1605 | Range EMA alpha |
| `RV_EMA_ALPHA` | 2.0/11.0 | signals/rotations.py | 1606 | RV EMA span=10 alpha |
| `SIGNALS` | ['Up_Rot','Down_Rot','Breakout','Breakdown','BTFD','STFR'] | signals/rotations.py | 1603 | Signal type list |
| `SIGNAL_TYPES` | ['Breakout','Breakdown','Up_Rot','Down_Rot','BTFD','STFR','Buy_Hold'] | app/backend/main.py | 1751 | Backtest signal type list (includes Buy_Hold) |
| `BACKTEST_DIRECTION` | {Up_Rot:'long', Down_Rot:'short', Breakout:'long', Breakdown:'short', BTFD:'long', STFR:'short', Buy_Hold:'long'} | app/backend/main.py | 1759 | Signal-to-direction map (includes Buy_Hold) |

---

## Parallel Implementations

These functions exist in BOTH signals/rotations.py AND app/backend/signals_engine.py with equivalent logic but different optimization levels:

| Function | rotations.py | signals_engine.py | Difference |
|----------|-------------|-------------------|------------|
| `_build_signals_from_df` | L2160-2299 (numba-accelerated) | L85-343 (pure Python) | rotations.py uses `@numba.njit` for passes 1-5; signals_engine.py uses Python loops with set-based tracking |
| `_build_signals_next_row` | L2299-2490 | L346-534 | Near-identical logic; both are Python; used for incremental 1-bar updates |
| `RollingStatsAccumulator` | L1630-1710 (class, deque-based) | L11-82 (class, list-based) | Same interface; rotations.py uses `collections.deque(maxlen=3)`, signals_engine.py uses `list` with `pop(0)` |
| `_build_leg_trades` | main.py L2518-2795 | — | Extracted trade-building logic; called only by `run_multi_backtest`; includes Buy_Hold early return; `run_backtest` still has its own inline trade-building logic; trade dicts include `entry_weight`/`exit_weight`/`contribution` fields |

Functions in main.py that DUPLICATE logic from rotations.py (not exact copies but same purpose):

| Function | main.py | rotations.py equivalent | Notes |
|----------|---------|------------------------|-------|
| `_find_basket_parquet` | L110-120 | L3510-3522 | Same glob logic, different folder source |
| `_find_basket_meta` | L122-132 | L3525-3539 | Same glob logic, different folder source |
| `_tally_breadth` | L279-324 | `compute_breadth_from_trend` L3704-3731 | Simplified live version, single-day |
| `_compute_live_breadth` | L327-377 | `_compute_within_basket_correlation` L3710-3800 | Live version includes correlation |
| `_quarter_str_to_date` | L197-203 | `_quarter_start_from_key` L427-432 | Same conversion, different name |

Functions in audit_basket.py that duplicate rotations.py logic:

| Function | audit_basket.py | rotations.py equivalent |
|----------|----------------|------------------------|
| `_quarter_end_from_key` | L29-33 | L419-424 |
| `_quarter_start_from_key` | L36-40 | L427-432 |
| `_build_quarter_lookup` | L52-57 | L2966-2975 |
| `_find_active_quarter` | L60-64 | L2978-2983 |
| `walk_equity` | L67-127 | `compute_equity_ohlc` L3799-4048 |

Functions in verify_backtest.py that duplicate logic from other files (independent re-implementation for verification):

| Function | verify_backtest.py | Equivalent in other file |
|----------|--------------------|--------------------------|
| `find_basket_parquet` | L107-117 | rotations.py `_find_basket_parquet` L4194-4209 |
| `quarter_str_to_date` | L140-146 | main.py `_quarter_str_to_date` L197-203 |
| `build_trades` | L243-317 | main.py `run_backtest` trade-building |
| `build_equity_curve` | L346-418 | main.py `run_backtest` equity replay |
| `compute_stats` | L425-470 | main.py `run_backtest` `compute_stats` |

---

## Data I/O — File Paths Referenced

### JSON Cache Files

| File Pattern | Producer | Consumer | Purpose |
|-------------|----------|----------|---------|
| `Data_Storage/top{SIZE}stocks.json` | `load_or_build_universe` | `load_or_build_universe`, main.py `list_tickers`, `list_baskets`, `get_basket_compositions` | Quarterly universe |
| `thematic_basket_cache/beta_universes_{SIZE}.json` | `load_or_build_beta_universes` | `load_or_build_beta_universes`, main.py THEMATIC_CONFIG | High/Low beta universes |
| `thematic_basket_cache/momentum_universes_{SIZE}.json` | `load_or_build_momentum_universes` | `load_or_build_momentum_universes`, main.py THEMATIC_CONFIG | Momentum winners/losers |
| `thematic_basket_cache/risk_adj_momentum_{SIZE}.json` | `load_or_build_risk_adj_momentum` | `load_or_build_risk_adj_momentum`, main.py THEMATIC_CONFIG | Risk-adj momentum |
| `thematic_basket_cache/dividend_universes_{SIZE}.json` | `load_or_build_dividend_universes` | `load_or_build_dividend_universes`, main.py THEMATIC_CONFIG | High yield / div growth |
| `Data_Storage/gics_mappings_{SIZE}.json` | `load_or_build_gics_mappings` | `load_or_build_gics_mappings`, main.py `get_basket_compositions`, `get_basket_breadth`, `get_latest_universe_tickers`, `get_ticker_baskets` | Sector/industry mappings |

### Parquet Cache Files

| File Pattern | Producer | Consumer | Purpose |
|-------------|----------|----------|---------|
| `Data_Storage/signals_{SIZE}.parquet` | `load_or_build_signals`, `_incremental_update_signals`, `append_live_today_to_signals_parquet` | `load_or_build_signals`, main.py (all signal endpoints), `_prebuild_equity_cache_from_signals` | Individual ticker signals |
| `Data_Storage/live_signals_{SIZE}.parquet` | `export_today_signals` | main.py `_read_live_parquet(LIVE_SIGNALS_FILE)` | Live intraday ticker OHLC |
| `Data_Storage/live_basket_signals_{SIZE}.parquet` | `export_live_basket_signals` | main.py `_read_live_parquet(LIVE_BASKET_SIGNALS_FILE)` | Live basket OHLC bars |
| `{type}_basket_cache/{slug}_*_of_{SIZE}_ohlc.parquet` | `_save_equity_cache` | `_load_equity_cache`, `_get_basket_ohlc_for_reports` | Basket equity OHLC curves |
| `{type}_basket_cache/{slug}_*_of_{SIZE}_ohlc_meta.json` | `_save_equity_cache` | `_load_equity_cache`, `_finalize_basket_signals_output` | Equity cache metadata + state |
| `{type}_basket_cache/{slug}_*_of_{SIZE}_signals.parquet` | `_finalize_basket_signals_output` | `process_basket_signals`, main.py `get_basket_data`, `get_basket_breadth`, `run_backtest` | Consolidated basket signals |
| `{type}_basket_cache/{slug}_*_of_{SIZE}_signals_meta.json` | `_finalize_basket_signals_output` | `process_basket_signals`, main.py `_find_basket_meta`, `get_meta_file_tickers` | Basket signals metadata |
| `{type}_basket_cache/{slug}_*_of_{SIZE}_contributions.parquet` | `_compute_and_save_contributions`, `_finalize_basket_signals_output` (pre-computed path) | main.py `get_basket_contributions`, `get_basket_candle_detail`, `get_basket_weights_from_contributions` | Per-constituent weights/returns |
| `Data_Storage/returns_matrix_500.parquet` | pre-computation block (L4294-4430) | `process_basket_signals` (via `returns_matrix` param) | Pre-computed Date x Ticker daily close returns pivot |
| `Data_Storage/returns_matrix_500.fingerprint` | pre-computation block (L4294-4430) | pre-computation block (cache validity check) | MD5 hash of data shape/date/tickers for cache invalidation |

### Excel/PDF/Text Output Files

| File Pattern | Producer | Purpose |
|-------------|----------|---------|
| `Baskets/Basket_Reports/annual_reports/{date}_annual_returns.pdf` | `export_annual_returns` | Annual returns PDF |
| `Baskets/Basket_Reports/annual_reports/{date}_annual_returns_by_year.pdf` | `export_annual_returns_by_year` | Per-year bar charts PDF |
| `Baskets/Basket_Reports/{stamp}_last_20_days_returns.pdf` | `export_last_20_days_returns` | 20-day returns PDF |
| `Baskets/Basket_Reports/{stamp}_last_20_days_returns_by_day.pdf` | `export_last_20_days_returns_by_day` | Per-day bar charts PDF |
| `Trading_View_Lists/Theme of Top {SIZE} {qtr}.txt` | `export_group_holdings` | TradingView watchlist |
| `Trading_View_Lists/Sector of Top {SIZE} {qtr}.txt` | `export_group_holdings` | TradingView watchlist |
| `Trading_View_Lists/Industry of Top {SIZE} {qtr}.txt` | `export_group_holdings` | TradingView watchlist |

---

## Function Dependency Tree

### signals/rotations.py — Cell 1: Configuration & Constants

#### `_resolve_onedrive_output_folder` (L38-60)
- **Called by:** module-level (L63)
- **Calls:** (stdlib only)
- **Data I/O:** probes OneDrive paths

#### `_mirror_to_onedrive` (L66-76)
- **Called by:** WriteThroughPath._copy
- **Calls:** shutil.copy2

#### `_needs_write_and_mirror` (L79-94)
- **Called by:** `export_annual_returns`, `export_annual_returns_by_year`, rotations_old_outputs.py report functions
- **Calls:** Path.exists, Path.relative_to

### signals/rotations.py — Cell 2: Utility Functions

#### `build_pdf` (L233-242)
- **Called by:** `export_annual_returns`, `export_annual_returns_by_year`, `export_last_20_days_returns`, `export_last_20_days_returns_by_day`, rotations_old_outputs.py `generate_summary_pdf`, `generate_basket_report_pdfs`
- **Calls:** PdfPages, WriteThroughPath.sync

#### `_timed_print` (L256-261)
- **Called by:** installed as builtins.print via `_install_timed_print`
- **Calls:** _ORIGINAL_PRINT

#### `_install_timed_print` (L264-266)
- **Called by:** module-level (L276)

#### `reset_cell_timer` (L269-273)
- **Called by:** module-level at start of each cell

#### `_get_current_quarter_key` (L280-283)
- **Called by:** `_cache_file_stem`, `_compute_within_basket_correlation`, `export_group_holdings`, `export_current_quarter_universe`
- **Calls:** (reads QUARTER_UNIVERSE global)

### signals/rotations.py — Cell 3: Universe Construction

#### `get_quarterly_vol` (L294-307)
- **Called by:** `build_quarter_universe` (via ThreadPoolExecutor.map)
- **Calls:** norgatedata.price_timeseries
- **DataFrame columns read:** Close, Volume
- **DataFrame columns created:** (returns list of tuples)

#### `build_quarter_universe` (L310-351)
- **Called by:** `load_or_build_universe`
- **Calls:** `get_quarterly_vol`, norgatedata.database_symbols, norgatedata.subtype1
- **DataFrame columns created:** Date, Ticker, Vol

#### `is_universe_current` (L354-357)
- **Called by:** `load_or_build_universe`

#### `_universe_to_json` / `_json_to_universe` (L360-367)
- **Called by:** `load_or_build_universe`

#### `load_or_build_universe` (L370-385)
- **Called by:** module-level (L397 -> QUARTER_UNIVERSE)
- **Calls:** `_json_to_universe`, `is_universe_current`, `build_quarter_universe`, `_universe_to_json`, WriteThroughPath
- **Data I/O:** reads/writes `Data_Storage/top{SIZE}stocks.json`

#### `get_universe` (L388-394)
- **Called by:** (available for external use, not internally called in current code)
- **Calls:** (reads QUARTER_UNIVERSE global)

#### `_quarter_end_from_key` (L419-424)
- **Called by:** `build_quarter_beta_universes`, `build_quarter_momentum_universes`, `build_quarter_risk_adj_momentum`, `build_quarter_dividend_universes`, `_build_sector_universes`, `_build_industry_universes`, `_build_quarter_weights`, `compute_equity_ohlc`, `_compute_and_save_contributions`
- **Parallel impl:** audit_basket.py L29-33

#### `_quarter_start_from_key` (L427-432)
- **Called by:** `_cache_build_quarter_lookup`, `_build_quarter_lookup`
- **Parallel impl:** audit_basket.py L36-40, main.py `_quarter_str_to_date` L197-203

#### `_calc_beta_quarterly` / `_safe_calc_beta` (L425-455)
- **Called by:** `build_quarter_beta_universes`
- **Calls:** norgatedata.price_timeseries

#### `build_quarter_beta_universes` (L458-523)
- **Called by:** `load_or_build_beta_universes`
- **Calls:** `_safe_calc_beta`, `_quarter_end_from_key`, norgatedata.price_timeseries

#### `is_beta_universes_current` / `_beta_universes_to_json` / `_json_to_beta_universes` (L526-542)
- **Called by:** `load_or_build_beta_universes`

#### `load_or_build_beta_universes` (L545-561)
- **Called by:** module-level (L565 -> BETA_UNIVERSE, LOW_BETA_UNIVERSE)
- **Data I/O:** reads/writes `thematic_basket_cache/beta_universes_{SIZE}.json`

#### `_calc_momentum_quarterly` / `_safe_calc_momentum` (L573-601)
- **Called by:** `build_quarter_momentum_universes`

#### `build_quarter_momentum_universes` (L604-652)
- **Called by:** `load_or_build_momentum_universes`
- **Calls:** `_safe_calc_momentum`, `_quarter_end_from_key`

#### `is_momentum_universes_current` (L655-659)
- **Called by:** `load_or_build_momentum_universes`

#### `load_or_build_momentum_universes` (L662-685)
- **Called by:** module-level (L689 -> MOMENTUM_UNIVERSE, MOMENTUM_LOSERS_UNIVERSE)
- **Data I/O:** reads/writes `thematic_basket_cache/momentum_universes_{SIZE}.json`

#### `_calc_risk_adj_momentum_quarterly` / `_safe_calc_risk_adj_momentum` (L699-730)
- **Called by:** `build_quarter_risk_adj_momentum`

#### `build_quarter_risk_adj_momentum` (L733-776)
- **Called by:** `load_or_build_risk_adj_momentum`

#### `is_risk_adj_momentum_current` (L779-782)
- **Called by:** `load_or_build_risk_adj_momentum`

#### `load_or_build_risk_adj_momentum` (L785-803)
- **Called by:** module-level (L807 -> RISK_ADJ_MOM_UNIVERSE)
- **Data I/O:** reads/writes `thematic_basket_cache/risk_adj_momentum_{SIZE}.json`

#### `_calc_dividend_yield_quarterly` / `_safe_calc_dividend_yield` (L812-865)
- **Called by:** `build_quarter_dividend_universes`

#### `_calc_trailing_dividends_quarterly` / `_safe_calc_trailing_divs` (L868-907)
- **Called by:** `build_quarter_dividend_universes`

#### `build_quarter_dividend_universes` (L910-998)
- **Called by:** `load_or_build_dividend_universes`
- **DataFrame columns created:** Date, Ticker, Yield, TrailingDivs, PriorDivs, Growth

#### `is_dividend_universes_current` (L1001-1005)
- **Called by:** `load_or_build_dividend_universes`

#### `load_or_build_dividend_universes` (L1008-1031)
- **Called by:** module-level (L1035 -> HIGH_YIELD_UNIVERSE, DIV_GROWTH_UNIVERSE)
- **Data I/O:** reads/writes `thematic_basket_cache/dividend_universes_{SIZE}.json`

#### `_build_gics_mappings` (L1058-1088)
- **Called by:** `load_or_build_gics_mappings`
- **Calls:** norgatedata.classification_at_level

#### `_build_sector_universes` (L1091-1109)
- **Called by:** `load_or_build_gics_mappings`

#### `_build_industry_universes` (L1112-1149)
- **Called by:** `load_or_build_gics_mappings`
- **Sets global:** INDUSTRY_LIST

#### `_is_gics_current` / `_gics_to_json` / `_json_to_gics` (L1152-1175)
- **Called by:** `load_or_build_gics_mappings`

#### `load_or_build_gics_mappings` (L1178-1198)
- **Called by:** module-level (L1202 -> TICKER_SECTOR, TICKER_SUBINDUSTRY, SECTOR_UNIVERSES, INDUSTRY_UNIVERSES)
- **Calls:** `_build_gics_mappings`, `_build_sector_universes`, `_build_industry_universes`
- **Data I/O:** reads/writes `Data_Storage/gics_mappings_{SIZE}.json`

### signals/rotations.py — Cell 4: Signal Cache

#### `calc_rolling_stats` (L1587-1630)
- **Called by:** `_append_trade_rows`
- **Returns dict keys:** Win_Rate, Avg_Winner, Avg_Loser, Avg_Winner_Bars, Avg_Loser_Bars, Avg_MFE, Avg_MAE, Historical_EV, Std_Dev, Risk_Adj_EV, EV_Last_3, Risk_Adj_EV_Last_3, Count

#### `_numba_passes_1_to_4` (L1710-1877, @numba.njit)
- **Called by:** `_build_signals_from_df`
- **Returns:** 22-element tuple of numpy arrays (trends, resistance, support, signals, etc.)

#### `_numba_pass5_signal` (L1877-2160, @numba.njit)
- **Called by:** `_build_signals_from_df`
- **Returns:** 20-element tuple (entry_price, change, exit data, 13 stats arrays)

#### `_build_signals_from_df` (L2160-2299)
- **Called by:** `build_signals_for_ticker`, `build_signals_for_ticker_live`, `process_basket_signals`, main.py `get_basket_data` (via signals_engine)
- **Calls:** `_numba_passes_1_to_4`, `_numba_pass5_signal`
- **DataFrame columns read:** Date, Open, High, Low, Close, Volume
- **DataFrame columns created:** RV, RV_EMA, Trend, Resistance_Pivot, Support_Pivot, Is_Up_Rotation, Is_Down_Rotation, Rotation_Open, Up_Range, Down_Range, Up_Range_EMA, Down_Range_EMA, Upper_Target, Lower_Target, Is_Breakout, Is_Breakdown, Is_BTFD, Is_STFR, BTFD_Target_Entry, STFR_Target_Entry, Rotation_ID, BTFD_Triggered, STFR_Triggered, Is_Breakout_Sequence, Ticker, {Sig}_Entry_Price, {Sig}_Change, {Sig}_Exit_Date, {Sig}_Exit_Price, {Sig}_Final_Change, {Sig}_MFE, {Sig}_MAE, {Sig}_Win_Rate, {Sig}_Avg_Winner, {Sig}_Avg_Loser, {Sig}_Avg_Winner_Bars, {Sig}_Avg_Loser_Bars, {Sig}_Avg_MFE, {Sig}_Avg_MAE, {Sig}_Historical_EV, {Sig}_Std_Dev, {Sig}_Risk_Adj_EV, {Sig}_EV_Last_3, {Sig}_Risk_Adj_EV_Last_3, {Sig}_Count
- **PARALLEL IMPL:** app/backend/signals_engine.py L85-343

#### `_build_signals_next_row` (L2299-2490)
- **Called by:** `_build_signals_append_ticker`, `export_today_signals`, `append_live_today_to_signals_parquet`, `process_basket_signals`, `update_basket_parquets_with_live_ohlcv`, main.py `list_live_signal_tickers`, `get_ticker_data`
- **Calls:** (pure computation)
- **PARALLEL IMPL:** app/backend/signals_engine.py L346-534

#### `build_signals_for_ticker` (L2490-2502)
- **Called by:** `_incremental_update_signals`, `load_or_build_signals`
- **Calls:** norgatedata.price_timeseries, `_build_signals_from_df`

#### `_build_signals_append_ticker` (L2502-2584)
- **Called by:** `_incremental_update_signals`
- **Calls:** norgatedata.price_timeseries, `_build_signals_next_row`

#### `_incremental_update_signals` (L2584-2769)
- **Called by:** `load_or_build_signals`
- **Calls:** `_build_signals_append_ticker`, `build_signals_for_ticker`, WriteThroughPath
- **Data I/O:** writes `signals_{SIZE}.parquet`
- **DataFrame columns modified:** Source (set to 'norgate'), Trend (normalized to float32), bool cols, float32 stats cols

#### `_get_latest_norgate_date` (L2769-2784)
- **Called by:** `_signals_cache_is_current`, `load_or_build_signals`, `_get_latest_norgate_date_fallback`, `_build_basket_annual_grid`, `export_annual_returns`, `export_annual_returns_by_year`
- **Calls:** norgatedata.price_timeseries (SPY)

#### `_signals_cache_is_current` (L2784-2822)
- **Called by:** `load_or_build_signals`
- **Calls:** `_get_latest_norgate_date`

#### `load_or_build_signals` (L2822-3004)
- **Called by:** module-level (L3007 -> all_signals_df)
- **Calls:** `_signals_cache_is_current`, `_incremental_update_signals`, `build_signals_for_ticker`, `_get_latest_norgate_date`
- **Data I/O:** reads/writes `Data_Storage/signals_{SIZE}.parquet`

### signals/rotations.py — Cell 5: Basket Processing

#### `_cache_slugify_label` (L3317-3321)
- **Called by:** `_prebuild_equity_cache_from_signals`

#### `_cache_build_quarter_lookup` / `_cache_find_active_quarter` (L3321-3341)
- **Called by:** `_compute_equity_close_for_cache`

#### `_compute_equity_close_for_cache` (L3341-3389)
- **Called by:** (not directly called in current code — legacy helper)
- **Calls:** `_cache_build_quarter_lookup`, `_cache_find_active_quarter`
- **DataFrame columns:** Date, Ticker, Close, Volume, Prev_Close, Ret

#### `_get_data_signature` (L3389-3414)
- **Called by:** `_prebuild_equity_cache_from_signals`, `compute_equity_ohlc_cached`, `process_basket_signals`
- **Caches result in:** `_DATA_SIGNATURE_CACHE` global

#### `_prebuild_equity_cache_from_signals` (L3414-3465)
- **Called by:** (invoked in basket processing section, pre-builds equity caches)
- **Calls:** `_get_data_signature`, `_cache_slugify_label`, `_load_equity_cache`, `_build_universe_signature`, `_is_equity_cache_valid`, `compute_equity_ohlc_cached`

#### `compute_breadth_pivots` (L3465-3583)
- **Called by:** `_finalize_basket_signals_output`
- **Returns DataFrame columns:** B_Trend, B_Resistance, B_Support, B_Up_Rot, B_Down_Rot, B_Rot_High, B_Rot_Low, B_Bull_Div, B_Bear_Div

#### `compute_signal_trades` (L3583-3650)
- **Called by:** rotations_old_outputs.py `plot_basket_charts`
- **Returns:** list of trade dicts (entry_date, exit_date, entry_price, exit_price, change, mfe, mae, bars)

#### `_build_quarter_lookup` / `_find_active_quarter` (L3650-3670)
- **Called by:** `_vectorized_quarter_filter`, `compute_breadth_from_trend`, `compute_breadth_from_breakout`, `_build_quarter_weights`, `compute_equity_ohlc`, `_compute_within_basket_correlation`, `_compute_and_save_contributions`, `_augment_basket_signals_with_breadth`
- **Parallel impl:** audit_basket.py L52-64

#### `_build_membership_df` (L3670-3679)
- **Called by:** `_vectorized_quarter_filter`, `_augment_basket_signals_with_breadth`
- **How it works:** flattens `universe_by_date` dict into a two-column DataFrame `(_q_key, Ticker)` for use as a hash-join table
- **Returns:** pd.DataFrame with columns `_q_key`, `Ticker`

#### `_vectorized_quarter_filter` (L3679-3704)
- **Called by:** `compute_breadth_from_trend`, `compute_breadth_from_breakout`
- **Calls:** `_build_membership_df` (if `membership_df` not provided)
- **How it works:** assigns each row its active quarter via `np.searchsorted` on quarter_ends, then inner-joins on `(_q_key, Ticker)` for O(N) filtering vs O(N*Q) date-by-date loop
- **DataFrame columns added:** `_q_key` (temporary, dropped after join)

#### `compute_breadth_from_trend` (L3704-3731)
- **Signature:** `compute_breadth_from_trend(all_df, universe_by_date, membership_df=None)` — `membership_df` optional for reuse across breadth calls
- **Called by:** `_augment_basket_signals_with_breadth`
- **Calls:** `_build_quarter_lookup`, `_vectorized_quarter_filter`
- **DataFrame columns read:** Date, Ticker, Trend
- **DataFrame columns created:** Date, Uptrend_Count, Downtrend_Count, Total_Stocks, Breadth_Ratio

#### `compute_breadth_from_breakout` (L3731-3754)
- **Signature:** `compute_breadth_from_breakout(all_df, universe_by_date, membership_df=None)` — `membership_df` optional for reuse
- **Called by:** `_augment_basket_signals_with_breadth`
- **Calls:** `_build_quarter_lookup`, `_vectorized_quarter_filter`
- **DataFrame columns read:** Date, Ticker, Is_Breakout_Sequence
- **DataFrame columns created:** Date, Breakout_Count, Breakdown_Count, BO_Total_Stocks, BO_Breadth_Ratio

#### `_build_quarter_weights` (L3754-3799)
- **Called by:** `compute_equity_ohlc`
- **Calls:** `_quarter_end_from_key`
- **How it works:** computes per-quarter initial dollar-volume weights by averaging Dollar_Vol per ticker per calendar quarter (via `groupby(['Ticker', pd.Grouper(freq='QE-DEC')])`), then normalizing within each quarter's universe using the PRIOR quarter's ranking date
- **Returns:** dict[quarter_key -> dict[ticker -> normalized_weight]]
- **Note:** `_compute_and_save_contributions` duplicates this weight-building logic internally (not yet calling this helper) to avoid dep on its result format

#### `compute_equity_ohlc` (L3799-4048)
- **Signature:** `compute_equity_ohlc(all_df, universe_by_date, start_after_date=None, initial_state=None, return_state=False, returns_matrix=None, ohlc_ret_matrices=None, return_contributions=False)`
- **Called by:** `compute_equity_ohlc_cached`, `compute_equity_curve`
- **Calls:** `_build_quarter_lookup`, `_build_quarter_weights`, `_find_active_quarter`
- **DataFrame columns read:** Date, Ticker, Open, High, Low, Close, Volume
- **DataFrame columns created (incremental path):** Ret, Open_Ret, High_Ret, Low_Ret; output: Date, Open, High, Low, Close
- **Two paths:**
  - **Fast vectorized path** (L3820-3928): when `returns_matrix` and `ohlc_ret_matrices` are provided and `start_after_date` is None; iterates per-quarter using pre-computed matrices; `cumprod` weight drift; optionally returns `contrib_df` as byproduct when `return_contributions=True`; also returns `last_state` for caching when `return_state=True`
  - **Incremental loop path** (L3930-4048): for appending a few new days when cache has partial data; date-by-date loop with `initial_state`

#### `_build_universe_signature` (L4048-4059)
- **Called by:** `_prebuild_equity_cache_from_signals`, `compute_equity_ohlc_cached`, `process_basket_signals`, `_finalize_basket_signals_output`

#### `_equity_cache_paths` / `_load_equity_cache` / `_save_equity_cache` (L4059-4111)
- **Called by:** `_prebuild_equity_cache_from_signals`, `compute_equity_ohlc_cached`, `_finalize_basket_signals_output`
- **Calls:** `_basket_cache_folder`, `_cache_file_stem`

#### `_build_equity_meta` / `_is_equity_cache_valid` (L4111-4157)
- **Called by:** `_prebuild_equity_cache_from_signals`, `compute_equity_ohlc_cached`
- **References constants:** EQUITY_CACHE_SCHEMA_VERSION, EQUITY_SIGNAL_LOGIC_VERSION, EQUITY_UNIVERSE_LOGIC_VERSION

#### `_basket_cache_folder` (L4157-4167)
- **Called by:** `_equity_cache_paths`, `_basket_cache_paths`, `_finalize_basket_signals_output`, `_compute_and_save_contributions`

#### `_cache_file_stem` (L4167-4180)
- **Called by:** `_equity_cache_paths`, `_basket_cache_paths`, `_finalize_basket_signals_output`, `_compute_and_save_contributions`
- **Calls:** `_get_current_quarter_key`

#### `_basket_cache_paths` (L4180-4194)
- **Called by:** (available for cache path resolution)

#### `_find_basket_parquet` (L4194-4209)
- **Called by:** `_get_chart_schema_version_from_parquet`, `process_basket_signals`, `update_basket_parquets_with_live_ohlcv`
- **Parallel impl:** main.py L92-102

#### `_find_basket_meta` (L4209-4226)
- **Called by:** `process_basket_signals`
- **Parallel impl:** main.py L104-114

#### `_get_chart_schema_version_from_parquet` (L4226-4239)
- **Called by:** rotations_old_outputs.py `plot_basket_charts`

#### `_build_basket_signals_meta` / `_is_basket_signals_cache_valid` (L4239-4279)
- **Called by:** `process_basket_signals`
- **References constants:** BASKET_SIGNALS_CACHE_SCHEMA_VERSION, EQUITY_SIGNAL_LOGIC_VERSION

#### `compute_equity_ohlc_cached` (L4279-4329)
- **Signature:** `compute_equity_ohlc_cached(all_df, universe_by_date, basket_name, slug, basket_type='sector', returns_matrix=None, ohlc_ret_matrices=None)`
- **Returns:** `(ohlc_df, contrib_df)` tuple; `contrib_df` is non-None only on full rebuild
- **Called by:** `_prebuild_equity_cache_from_signals`, `process_basket_signals`
- **Calls:** `_get_data_signature`, `_build_universe_signature`, `_load_equity_cache`, `_is_equity_cache_valid`, `compute_equity_ohlc` (with `return_contributions=True` on full rebuild), `_build_equity_meta`, `_save_equity_cache`
- **Key behavior:** `FORCE_REBUILD_BASKET_SIGNALS` (L4285) also triggers equity rebuild so `contrib_df` is always captured; incremental-append path uses the slow loop path (no matrix params) — `contrib_df` is None in that case

#### `compute_equity_curve` (L4329-4339)
- **Called by:** (compatibility helper, not directly called in current code)

#### `_fmt_price` / `_fmt_bars` / `_fmt_pct` (L4339-4351)
- **Called by:** `export_today_signals`, rotations_old_outputs.py

#### `_append_trade_rows` (L4351-4394)
- **Called by:** rotations_old_outputs.py `plot_basket_charts`
- **Calls:** `calc_rolling_stats`

#### `_compute_within_basket_correlation` (L4394-4485)
- **Signature:** `_compute_within_basket_correlation(universe_by_qtr, returns_matrix, window=21)`
- **Called by:** `_finalize_basket_signals_output` (full-recompute path when `incremental_dates` is None)
- **Calls:** `_build_quarter_lookup`
- **How it works:** numpy z-score variance decomposition per rolling window; for each date computes `avg_corr = (n * Var(EW z-portfolio) - 1) / (n - 1)` which is the exact simple average of all pairwise correlations, in O(n*w) per date vs O(n^2*w) for full .corr(); `window` warmup is included from the previous quarter's tail
- **DataFrame columns created:** Date, Correlation_Pct (scaled 0-100)
- **`returns_matrix` is now a required param** (no default); caller must pass the pre-computed pivot; function does NOT read `all_signals_df` directly

#### `_compute_within_basket_correlation_incremental` (L4487-4572)
- **Signature:** `_compute_within_basket_correlation_incremental(universe_by_qtr, returns_matrix, new_dates, window=21)`
- **Called by:** `_finalize_basket_signals_output` (incremental path when `incremental_dates` is set and `Correlation_Pct` already exists in `merged_all`)
- **Calls:** `_build_quarter_lookup`, `_find_active_quarter`
- **How it works:** same z-score variance decomposition as full version, but processes only the quarter(s) containing `new_dates`; groups new dates by active quarter to avoid redundant setup; for each date extracts a rolling window slice from `returns_matrix` ending at that date. 171x speedup over full version for 1-day append.
- **DataFrame columns created:** Date, Correlation_Pct (scaled 0-100) — only rows for `new_dates`; caller merges back into `merged_all`

#### `_augment_basket_signals_with_breadth` (L4575-4632)
- **Called by:** `process_basket_signals`
- **Calls:** `_build_quarter_lookup`, `_build_membership_df`, `compute_breadth_from_trend`, `compute_breadth_from_breakout`
- **How it works:** builds `membership_df` once via `_build_membership_df`, then passes it to both breadth calls to avoid re-building it twice; timing instrumentation via `BENCHMARK_TIMING` / `BENCHMARK_BASKETS`
- **DataFrame columns created/merged:** Uptrend_Pct, Downtrend_Pct, Breadth_EMA, Breakout_Pct, Breakdown_Pct, BO_Breadth_EMA

#### `_finalize_basket_signals_output` (L4632-4776)
- **Signature:** `_finalize_basket_signals_output(name, slug, hist_folder, merged_all, data_sig, universe_sig, universe_by_qtr, basket_type='sector', returns_matrix=None, contrib_df=None, incremental_dates=None)`
- **Called by:** `process_basket_signals`
- **Calls:** `compute_breadth_pivots`, `_compute_within_basket_correlation` OR `_compute_within_basket_correlation_incremental`, `_cache_file_stem`, `_basket_cache_folder`, `_equity_cache_paths`, `_compute_and_save_contributions` OR `_compute_and_save_contributions_incremental`, WriteThroughPath, pa.Table, pq.write_table
- **DataFrame columns added:** B_Trend, B_Resistance, B_Support, B_Up_Rot, B_Down_Rot, B_Rot_High, B_Rot_Low, B_Bull_Div, B_Bear_Div, BO_B_* (same prefixed), Correlation_Pct, Source
- **Data I/O:** writes `{type}_basket_cache/{stem}_signals.parquet`, `{stem}_signals_meta.json`
- **New `incremental_dates` param:** when set (list of new dates), routes correlation to `_compute_within_basket_correlation_incremental` (if `Correlation_Pct` already in `merged_all`) and contributions to `_compute_and_save_contributions_incremental` (when `contrib_df` is None); full-recompute paths unchanged when `incremental_dates` is None
- **Key behavior:** if `contrib_df` is provided (from `compute_equity_ohlc` vectorized path via `compute_equity_ohlc_cached`), saves it directly; otherwise falls back to incremental or full contributions recompute depending on `incremental_dates`

#### `_compute_and_save_contributions` (L4779-4917)
- **Signature:** `_compute_and_save_contributions(slug, basket_type, universe_by_qtr, returns_matrix=None)`
- **Called by:** `_finalize_basket_signals_output` (full-recompute fallback when `contrib_df` is None and `incremental_dates` is not set); also called by `_compute_and_save_contributions_incremental` as its own fallback
- **Calls:** `_build_quarter_lookup`, `_quarter_end_from_key`, `_basket_cache_folder`, `_cache_file_stem`, WriteThroughPath
- **How it works:** vectorized per-quarter with cumprod weight drift; accepts `returns_matrix` param to avoid rebuilding the pivot; if not provided, builds its own from `all_signals_df`
- **Data I/O:** writes `{type}_basket_cache/{stem}_contributions.parquet`
- **DataFrame columns created:** Date, Ticker, Weight_BOD, Daily_Return, Contribution

#### `_compute_and_save_contributions_incremental` (L4919-5083)
- **Signature:** `_compute_and_save_contributions_incremental(slug, basket_type, universe_by_qtr, returns_matrix, new_dates)`
- **Called by:** `_finalize_basket_signals_output` (when `incremental_dates` is set and `contrib_df` is None)
- **Calls:** `_build_quarter_lookup`, `_find_active_quarter`, `_quarter_end_from_key`, `_basket_cache_folder`, `_cache_file_stem`, WriteThroughPath; falls back to `_compute_and_save_contributions` if no existing parquet
- **How it works:** loads existing contributions parquet, determines active quarter for each new date, seeds BOD weights from last cached day's drifted weights, chains multi-day appends via `_carried_bod`, appends new rows, deduplicates, saves. 6.6x speedup over full version.
- **Data I/O:** reads and overwrites `{type}_basket_cache/{stem}_contributions.parquet`
- **DataFrame columns created:** Date, Ticker, Weight_BOD, Daily_Return, Contribution (same schema as full version)

#### `_record_basket_timing` (L5086-5095)
- **Called by:** `process_basket_signals`
- **How it works:** appends per-step timing values to the global `_basket_timing` dict and `_basket_timing_names` list; prints a one-line summary per basket when `BENCHMARK_TIMING` or `BENCHMARK_BASKETS > 0`

#### `process_basket_signals` (L5095-5397)
- **Signature:** `process_basket_signals(name, universe_by_qtr, charts_folder, basket_type='sector', returns_matrix=None, ohlc_ret_matrices=None)`
- **Called by:** module-level basket loop
- **Calls:** `_cache_slugify_label`, `_get_data_signature`, `_build_universe_signature`, `_find_basket_parquet`, `_find_basket_meta`, `_is_basket_signals_cache_valid`, `compute_equity_ohlc_cached`, `_build_signals_next_row`, `_augment_basket_signals_with_breadth`, `_build_signals_from_df`, `_finalize_basket_signals_output`, `_record_basket_timing`
- **Accepts:** `returns_matrix` and `ohlc_ret_matrices` params (passed through to `compute_equity_ohlc_cached` and `_finalize_basket_signals_output`)
- **Timing instrumentation:** when `BENCHMARK_TIMING=True` or `BENCHMARK_BASKETS>0`, records wall-clock per step and calls `_record_basket_timing` at end

#### Pre-computation block (module-level, after process_basket_signals definition)
- **Purpose:** builds shared `returns_matrix` (Date x Ticker pivot of daily close returns) and `ohlc_ret_matrices` (dict of Date x Ticker pivots for Open_Ret, High_Ret, Low_Ret) once before the basket loop; caches `returns_matrix` to `Data_Storage/returns_matrix_500.parquet` with MD5 fingerprint check; `BENCHMARK_BASKETS` constant limits the loop to N baskets when set > 0
- **Consumed by:** all `process_basket_signals` calls in the basket loop
- **Data I/O:** reads/writes `Data_Storage/returns_matrix_500.parquet`, `Data_Storage/returns_matrix_500.fingerprint`

### signals/rotations.py — Cell 6: Live Intraday Data

#### `_load_env_file` (L5397-5429)
- **Called by:** module-level (L4466)

#### `get_realtime_prices` (L5429-5491)
- **Called by:** (available for external use)
- **Calls:** db.Live

#### `get_realtime_ohlcv` (L5491-5564)
- **Called by:** (available for external use)
- **Calls:** db.Live

#### `get_live_ohlc_bars` (L5564-5614)
- **Called by:** `export_today_signals`, `_get_live_update_context`
- **Calls:** db.Historical

#### `_get_latest_norgate_date_fallback` (L5614-5626)
- **Called by:** `_get_live_update_gate`
- **Calls:** `_get_latest_norgate_date`

#### `_extract_spy_trade_date_from_df` (L5626-5646)
- **Called by:** `_get_spy_last_trade_date_databento`

#### `_get_spy_last_trade_date_databento` (L5646-5675)
- **Called by:** `_get_live_update_gate`
- **Calls:** db.Historical, `_extract_spy_trade_date_from_df`

#### `_get_live_update_gate` (L5675-5716)
- **Called by:** `_is_market_open_via_spy_volume`, `export_today_signals`, `append_live_today_to_signals_parquet`, `_get_live_update_context`
- **Calls:** `_get_latest_norgate_date_fallback`, `_get_spy_last_trade_date_databento`

#### `_is_market_open_via_spy_volume` (L5716-5722)
- **Called by:** (compatibility wrapper)
- **Calls:** `_get_live_update_gate`

#### `_append_live_row` (L5722-5743)
- **Called by:** `build_signals_for_ticker_live`

#### `build_signals_for_ticker_live` (L5743-5759)
- **Called by:** (available for external use)
- **Calls:** `_append_live_row`, `_build_signals_from_df`

#### `_sort_signals_df` (L5759-5779)
- **Called by:** `export_today_signals`, rotations_old_outputs.py

#### `export_today_signals` (L5779-5862)
- **Called by:** module-level
- **Calls:** `_get_live_update_gate`, `get_live_ohlc_bars`, `_get_latest_norgate_rows_by_ticker`, `_build_signals_next_row`, `_fmt_price`, `_fmt_bars`, `_sort_signals_df`, WriteThroughPath
- **Data I/O:** writes `Data_Storage/live_signals_{SIZE}.parquet` (xlsx output removed)

#### `append_live_today_to_signals_parquet` (L5862-5926)
- **Called by:** (available for manual invocation)
- **Calls:** `_get_live_update_gate`, `_get_live_update_context`, `_build_signals_next_row`
- **Data I/O:** reads/writes `Data_Storage/signals_{SIZE}.parquet`

#### `export_today_etf_signals` (L5926-5964)
- **Called by:** module-level
- **Calls:** `_get_live_update_gate`, `get_live_ohlc_bars`, `_build_signals_next_row`, `_sort_signals_df`
- **Data I/O:** writes `Data_Storage/live_signals_etf_{ETF_SIZE}.parquet`

#### `append_live_today_to_etf_signals_parquet` (L5964-6042)
- **Called by:** module-level
- **Calls:** `_get_live_update_gate`, `_build_signals_next_row`
- **Data I/O:** reads/writes `Data_Storage/signals_etf_{ETF_SIZE}.parquet`

#### `_get_latest_norgate_rows_by_ticker` (L6042-6056)
- **Called by:** `export_today_signals`, `export_live_basket_signals`, `_get_live_update_context`
- **Reads:** all_signals_df global

#### `_compute_live_basket_ohlc` (L6056-6092)
- **Called by:** `export_live_basket_signals`
- **Signature:** `(universe_by_qtr, live_ohlc_map, last_rows, current_key, prev_basket_close)`

#### `_compute_live_basket_ohlcv` (L6092-6147)
- **Called by:** `update_basket_parquets_with_live_ohlcv`

#### `_get_live_update_context` (L6147-6196)
- **Called by:** `export_live_basket_signals`, `append_live_today_to_signals_parquet`, `append_live_today_to_etf_signals_parquet`, module-level
- **Calls:** `_get_live_update_gate`, `get_live_ohlc_bars`, `_get_latest_norgate_rows_by_ticker`

#### `export_live_basket_signals` (L6196-6235) — NEW
- **Called by:** module-level
- **Signature:** `export_live_basket_signals(live_ctx=None)`
- **Calls:** `_get_live_update_context`, `_find_basket_parquet`, `_compute_live_basket_ohlc`
- **How it works:** iterates all entries in `BASKET_RESULTS`; for each basket reads its last parquet Close as `prev_basket_close`; calls `_compute_live_basket_ohlc`; collects OHLC bars into a DataFrame and writes `Data_Storage/live_basket_signals_{SIZE}.parquet`
- **Data I/O:** writes `Data_Storage/live_basket_signals_{SIZE}.parquet`

#### `update_basket_parquets_with_live_ohlcv` (L6235-6322)
- **Called by:** (available for manual invocation)
- **Calls:** `_find_basket_parquet`, `_compute_live_basket_ohlcv`, `_build_signals_next_row`

### signals/rotations.py — Cell 7: Holdings Exports

> **Note:** Cell 7 (TradingView list exports) was removed from rotations.py in this update. `export_group_holdings` and `export_current_quarter_universe` no longer exist in the main pipeline.

---

### signals/rotations_old_outputs.py (2107 lines)

This file imports everything from rotations.py via `from rotations import *` plus specific private names. It contains the Group B report cells (9-15).

#### `_get_ticker_theme` (L122-124)
- **Called by:** `export_today_signals` (signals/rotations.py), signal filtering section
- **Reads:** `_thematic_universes` local list

#### `_basket_tsi` (L190-196)
- **Called by:** basket signal CSV export section

#### `_corr_asof_date` (L446)
- **Called by:** `build_correlation_reports`

#### `_corr_cache_signature` (L455)
- **Called by:** `build_correlation_reports`

#### `_load_corr_cache` / `_save_corr_cache` (L474-508)
- **Called by:** `build_correlation_reports`
- **Data I/O:** reads/writes `correlation_cache/` JSON files

#### `_quarter_key_from_date` / `_fallback_latest_quarter_key` (L513-527)
- **Called by:** `build_correlation_reports`

#### `_window_corr_matrix` (L530-538)
- **Called by:** `build_correlation_reports`

#### `_corr_pairs` (L541-558)
- **Called by:** `build_correlation_reports`

#### `_render_corr_heatmap` / `_render_pairs_table` (L561-600)
- **Called by:** `build_correlation_reports`

#### `_build_stock_returns_matrix` (L604-614)
- **Called by:** `build_correlation_reports`

#### `_build_basket_returns_matrix` (L617-634)
- **Called by:** `build_correlation_reports`

#### `_mean_offdiag` (L636-645)
- **Called by:** `_rolling_avg_pairwise_corr_series`

#### `_rolling_avg_pairwise_corr_series` (L648-667)
- **Called by:** `_update_rolling_osc_incremental`

#### `_series_last_date` (L670-676)
- **Called by:** `_update_rolling_osc_incremental`, `_update_within_osc_map_incremental`

#### `_update_rolling_osc_incremental` (L679-712)
- **Called by:** `_update_within_osc_map_incremental`

#### `_update_within_osc_map_incremental` (L715-740)
- **Called by:** `build_correlation_reports`

#### `_plot_corr_oscillator` / `_plot_single_corr_oscillator` (L743-805)
- **Called by:** `build_correlation_reports`

#### `build_correlation_reports` (L808-1067)
- **Called by:** module-level cell execution
- **Data I/O:** reads/writes correlation cache parquet/JSON, generates PDF

#### `export_basket_excel_reports` (L1069-1173)
- **Called by:** module-level cell execution
- **Data I/O:** writes Excel files

#### `plot_one_year_breadth_and_equity` (L1176-1241)
- **Called by:** `plot_basket_charts`

#### `_slugify_label` / `_make_fmt` (L1243-1254)
- **Called by:** `plot_basket_charts`

#### `plot_basket_charts` (L1257-1605)
- **Called by:** module-level chart generation cell
- **Calls:** `compute_signal_trades`, `_append_trade_rows`, `_get_chart_schema_version_from_parquet`, `plot_one_year_breadth_and_equity`

#### `_find_latest_file` / `_date_label_from_file` / `_find_basket_chart_path` (L1607-1642)
- **Called by:** `generate_summary_pdf`, `generate_basket_report_pdfs`

#### `_embed_image_page` (L1645-1691)
- **Called by:** `generate_summary_pdf`, `generate_basket_report_pdfs`

#### `_render_df_table_pages` (L1693-1785)
- **Called by:** `generate_summary_pdf`, `generate_basket_report_pdfs`

#### `_render_ytd_rebase_page` (L1787-1835)
- **Called by:** `generate_summary_pdf`

#### `generate_summary_pdf` (L1837-2001)
- **Called by:** module-level cell execution
- **Calls:** `_find_latest_file`, `_date_label_from_file`, `_embed_image_page`, `_render_df_table_pages`, `_render_ytd_rebase_page`, `_build_basket_annual_grid`, `_build_basket_daily_grid_last20`, `_render_return_bar_charts`

#### `generate_basket_report_pdfs` (L2003-2177)
- **Called by:** module-level cell execution
- **Calls:** `_find_basket_chart_path`, `_embed_image_page`, `_render_df_table_pages`

---

### signals/databento_test.py (624 lines)

#### `_load_env` (L39) — loads .env for DATABENTO_API_KEY
#### `_result_line` / `_decode` / `_instrument_id` / `_ts_event_ns` / `_ts_et` / `_ohlcv_prices` / `_scale_df_prices` (L92-165) — test helpers
#### `_live_run` (L168-208) — shared live subscription runner
#### `print_available_schemas` (L211-239) — prints Databento schemas
#### `test1_historical_ohlcv_1d` (L241-282) — historical daily OHLC test
#### `test2_historical_ohlcv_1m_aggregate` (L284-349) — historical 1m aggregate test
#### `test3_live_ohlcv_1d` (L351-438) — live daily OHLC test
#### `test4_live_mbp1_replay` (L440-524) — live MBP replay test
#### `test5_live_mbp1_snapshot` (L526-590) — live MBP snapshot test
#### `main` (L592-624) — test runner

---

### app/backend/main.py (3963 lines)

#### `_read_live_parquet` (L80-91)
- **Called by:** `_compute_live_breadth`, `get_basket_breadth`, `get_basket_returns`, `get_basket_data`, `list_live_signal_tickers`, `get_ticker_signals`, `get_ticker_data`, `get_basket_summary`

#### `_live_is_current` (L93-108)
- **Called by:** `_compute_live_breadth`, `get_basket_breadth`, `get_basket_returns`, `get_basket_data`, `list_live_signal_tickers`, `get_ticker_signals`, `get_ticker_data`, `get_basket_summary`
- **Purpose:** Returns True if the live parquet date is strictly newer than Norgate max date; prevents stale live overlay after market close when Norgate has already updated with end-of-day data
- **Calls:** pd.to_datetime
#### `_find_basket_parquet` (L110-120)
- **Called by:** `get_basket_returns`, `get_basket_data`, `get_ticker_baskets`, `get_date_range`, `run_backtest`, `_build_leg_trades`, `run_benchmarks`
- **PARALLEL IMPL:** signals/rotations.py L3510-3522

#### `_find_basket_meta` (L122-132)
- **Called by:** `get_meta_file_tickers`
- **PARALLEL IMPL:** signals/rotations.py L3525-3539

#### `clean_data_for_json` (L135-136)
- **Called by:** `get_basket_data`, `get_ticker_data`

#### `get_latest_universe_tickers` (L138-160)
- **Called by:** `_compute_live_breadth`, `get_basket_breadth`, `get_basket_data`, `get_basket_summary`, `get_basket_correlation`, `get_ticker_baskets`, `run_backtest`, `_build_leg_trades`, `run_benchmarks`
- **Data I/O:** reads `gics_mappings_{SIZE}.json`, thematic JSON files

#### `get_meta_file_tickers` (L162-174)
- **Called by:** `get_basket_summary`, `get_basket_correlation`, `run_backtest`, `_build_leg_trades`, `run_benchmarks`

#### `_get_universe_history` (L177-194)
- **Called by:** `_get_universe_tickers_for_range`, `_get_ticker_join_dates`, `_get_tickers_for_date`, `get_basket_summary`, `run_backtest`, `_build_leg_trades`, `run_benchmarks`
- **Data I/O:** reads `gics_mappings_{SIZE}.json`, thematic JSON files

#### `_quarter_str_to_date` (L197-203)
- **Called by:** `_get_universe_tickers_for_range`, `_get_ticker_join_dates`, `_get_tickers_for_date`, `get_basket_summary`, `run_backtest`, `_build_leg_trades`, `run_benchmarks`
- **PARALLEL IMPL:** signals/rotations.py `_quarter_start_from_key` L531-539

#### `_get_universe_tickers_for_range` (L206-220)
- **Called by:** `run_backtest`, `_build_leg_trades`, `run_benchmarks`

#### `_get_ticker_join_dates` (L223-235)
- **Called by:** `get_basket_summary`

#### `_get_tickers_for_date` (L238-256)
- **Called by:** `get_basket_correlation`

#### `get_basket_weights_from_contributions` (L260-277)
- **Called by:** `get_basket_data`
- **Data I/O:** reads contributions parquet via `_find_basket_contributions`

#### `_tally_breadth` (L279-324)
- **Called by:** `_compute_live_breadth`, `get_basket_breadth`
- **PARALLEL IMPL (simplified):** signals/rotations.py `compute_breadth_from_trend`

#### `_compute_live_breadth` (L327-377)
- **Called by:** `get_basket_data`
- **Calls:** `get_latest_universe_tickers`, `_read_live_parquet`, `_live_is_current`, `_tally_breadth`
- **Data I/O:** reads `live_signals_{SIZE}.parquet`, `signals_{SIZE}.parquet`

#### `read_root` (L384) — GET /
#### `list_baskets` (L387-402) — GET /api/baskets
#### `get_basket_compositions` (L405-428) — GET /api/baskets/compositions
#### `get_basket_breadth` (L431-566) — GET /api/baskets/breadth
- **Calls:** `_tally_breadth`, `_read_live_parquet`, `_live_is_current`, `get_latest_universe_tickers`

#### `get_basket_returns` (L568-1428) — GET /api/baskets/returns
- **Calls:** `_find_basket_parquet`, `_read_live_parquet`, `_live_is_current`
- **Nested functions:**
  - `_categorize` (L583-589) — classifies slug as "theme", "sector", or "industry"
  - `rank_vec(v, descending=False)` — rank values 1..B, NaN gets rank B; `descending=True` makes rank 1 = highest (analogs/query mode inner helper)
  - `rank_matrix(mat, descending=False)` — vectorized rank_vec across all rolling windows (analogs mode inner helper)
  - `spearman_vec` — batch Spearman rho from rank diff squares (analogs mode inner helper)
- **Query params:** `start`, `end` (date range), `mode` ("period", "daily", "analogs", or "query"), `basket` (slug, daily mode), `group` ("all"/"themes"/"sectors"/"industries"), `top_n` (int, analogs mode), `threshold` (float, analogs mode — minimum similarity score), `conditions` (JSON string, query mode — list of condition objects)
- **Data I/O:** reads basket signal parquets (columns: Date, Close), `LIVE_BASKET_SIGNALS_FILE` (live intraday overlay); analogs mode also reads Uptrend_Pct, Breakout_Pct, Correlation_Pct, RV_EMA
- **Constants:** `THEMATIC_CONFIG`, `BASKET_CACHE_FOLDERS`, `MULTI_TF` (local: {"1D":1, "1W":5, "1M":21, "1Q":63, "1Y":252, "3Y":756, "5Y":1260}), `AGG_HORIZONS` (local: {"1M":21, "3M":63, "6M":126})
- **Key behaviors:**
  - `mode=daily`: returns daily pct_change return series for a single basket; appends live close if available
  - `mode=period` (default): returns one period return per basket; defaults to 1Y range if no dates specified; filterable by group
  - `mode=analogs`: finds top-N historical windows with highest cross-basket regime similarity using Spearman rank correlation across 5+ metrics; greedy top-N with overlap exclusion of W/2 days; expanded features:
    - **Multi-timeframe return fingerprints** (`MULTI_TF`): Spearman rho for 1D/1W/1M/1Q/1Y/3Y/5Y return snapshots, averaged into similarity score alongside breadth/breakout/correlation/volatility
    - **Cross-basket rolling correlation**: pairwise close correlation across all baskets, included in similarity breakdown and current metrics
    - **Forward series per analog**: daily cumulative return series per basket for up to 252 days after each analog window end
    - **Threshold filtering**: `threshold` param filters analogs by minimum similarity score before aggregation
    - **Aggregate stats**: mean/median/min/max/std/count of forward returns at 1M/3M/6M horizons, overall and per-basket
  - `mode=query`: condition-based historical date search across baskets; accepts `conditions` JSON param (list of {basket, metric, operator, value}); supports per-basket or group-wide (`*sectors`, `*themes`, `*industries`) conditions; operators: `positive`, `negative`, `above`, `below`, `top_n`, `bottom_n`; metrics: `return_1D/1W/1M/1Q/1Y`, `uptrend_pct`, `breakout_pct`, `correlation_pct`, `rv_ema`; pre-computes rank matrices; deduplicates matches within 5 days; returns up to 100 matches with forward returns at 1W/1M/3M/6M horizons plus daily forward series (126 days); aggregate stats per horizon per basket

#### `get_basket_data` (L1429-1484) — GET /api/baskets/{basket_name}
- **Calls:** `_find_basket_parquet`, `_read_live_parquet`, `_live_is_current`, `signals_engine._build_signals_from_df`, `_compute_live_breadth`, `get_basket_weights_from_contributions`, `get_latest_universe_tickers`, `clean_data_for_json`

#### `list_tickers` (L1486-1499) — GET /api/tickers
#### `list_tickers_by_quarter` (L1501-1512) — GET /api/tickers/quarters
#### `list_live_signal_tickers` (L1515-1601) — GET /api/live-signals
- **Calls:** `signals_engine._build_signals_next_row`, `_read_live_parquet`, `_live_is_current`

#### `get_ticker_signals` (L1603-1697) — GET /api/ticker-signals
- **Calls:** `_read_live_parquet`, `_live_is_current`

#### `get_ticker_data` (L1699-1760) — GET /api/tickers/{ticker}
- **Calls:** `signals_engine._build_signals_next_row`, `_read_live_parquet`, `_live_is_current`

#### `safe_float` / `safe_int` (L1762-1771) — utility formatters

#### `get_basket_summary` (L1773-2227) — GET /api/baskets/{basket_name}/summary
- **Calls:** `get_latest_universe_tickers`, `get_meta_file_tickers`, `_get_universe_history`, `_quarter_str_to_date`, `_read_live_parquet`, `_live_is_current`, `_find_basket_contributions`, `_get_ticker_join_dates`, `safe_float`, `safe_int`

#### `get_basket_correlation` (L2229-2275) — GET /api/baskets/{basket_name}/correlation
- **Calls:** `_get_tickers_for_date`, `get_latest_universe_tickers`, `get_meta_file_tickers`

#### `_find_basket_contributions` (L2278-2288)
- **Called by:** `get_basket_contributions`, `get_basket_candle_detail`, `get_basket_summary`, `get_basket_weights_from_contributions`

#### `get_basket_contributions` (L2291-2376) — GET /api/baskets/{basket_name}/contributions

#### `get_basket_candle_detail` (L2379-2454) — GET /api/baskets/{basket_name}/candle-detail

#### `get_ticker_baskets` (L2456-2484) — GET /api/ticker-baskets/{ticker}

#### `BacktestFilter` (L2487-2491) — Pydantic model
- **Fields:** `metric`, `condition`, `value`, `source`

#### `BacktestRequest` (L2493-2501) — Pydantic model
- **Fields:** `target`, `target_type`, `entry_signal`, `filters`, `start_date`, `end_date`, `position_size` (default 1.0), `max_leverage` (default 2.5)
- **Removed:** `initial_equity` (no longer user-configurable; hardcoded to 1.0 internally for percentage-based equity)

#### `MultiBacktestLeg` (L2503-2509) — Pydantic model
- **Fields:** `target`, `target_type`, `entry_signal`, `allocation_pct` (0-1 fraction), `position_size` (default 1.0), `filters`

#### `MultiBacktestRequest` (L2511-2517) — Pydantic model
- **Fields:** `legs`, `start_date`, `end_date`, `max_leverage` (default 2.5), `equity_only` (bool, default False)
- **Removed:** `initial_equity` (no longer user-configurable; hardcoded to 1.0 internally for percentage-based equity)

#### `BenchmarkRequest` (L3961-3968) — Pydantic model (NEW 2026-03-23)
- **Fields:** `target`, `target_type`, `position_size` (default 1.0), `max_leverage` (default 2.5), `start_date`, `end_date`, `signals` (default: all 6 from SIGNAL_IS_COL)
- **Used by:** `run_benchmarks` endpoint

#### `run_benchmarks` (L3971-4228) — POST /api/backtest/benchmarks (NEW 2026-03-23)
- **Purpose:** Compute benchmark equity curves + stats for multiple signals in a single request. Loads parquet ONCE, loops over signals — eliminates redundant I/O and GIL contention from parallel /api/backtest/multi calls.
- **Calls:** `_find_basket_parquet`, `_get_universe_history`, `_get_universe_tickers_for_range`, `_quarter_str_to_date`, `get_latest_universe_tickers`, `get_meta_file_tickers`, `safe_float`
- **Reads:** `INDIVIDUAL_SIGNALS_FILE`, basket parquets, `ETF_SIGNALS_FILE`, universe JSON files
- **Returns:** `{ dates[], benchmarks: { signal: equity_curve[] }, stats: { signal: { portfolio, trade } }, timings }`
- **Key behaviors:**
  - Collects ALL columns needed across all signals in one parquet read
  - Builds ticker_closes once, shared across all signal loops
  - Uses default exit path only (pre-computed trade columns), no custom exits/stops/filters
  - Tracks `_was_taken` for trade stats; computes portfolio stats from equity curve
  - Includes per-signal timing breakdown
- **Frontend caller:** BacktestPanel.tsx `runBacktest` (single batch request replaces 6 parallel /api/backtest/multi calls)

#### `_build_leg_trades` (L2518-2795)
- **Purpose:** extracted from `run_backtest` — builds the trades list + ticker_closes dict for a single backtest leg
- **Called by:** `run_multi_backtest`
- **Calls:** `_find_basket_parquet`, `get_latest_universe_tickers`, `get_meta_file_tickers`, `_get_universe_history`, `_get_universe_tickers_for_range`, `_quarter_str_to_date`, `safe_float`
- **Parameters:** `target`, `target_type`, `entry_signal`, `filters`, `start_date`, `end_date`
- **Returns:** `(trades, df, ticker_closes, direction)` tuple
- **Key behaviors:**
  - Early return for Buy_Hold signal: builds single trade from Close series (basket or ticker); not supported for basket_tickers; trade includes `entry_weight/exit_weight/contribution` fields
  - Loads data for basket/basket_tickers/ticker target types
  - Applies regime filters (above/below/increasing/decreasing/equals_true/equals_false) from pre-computed parquet values
  - Merges external basket filter sources via `pd.merge_asof`
  - Skips open trades (no exit_date/exit_price)
  - Trade dicts include `entry_weight`, `exit_weight`, `contribution` fields (initialized to None, populated by equity engine)

#### `get_date_range` (L2798-2814) — GET /api/date-range/{target_type}/{target}
- **Calls:** `_find_basket_parquet`
- **Data I/O:** reads `signals_500.parquet` (columns: Ticker, Date), basket parquets (column: Date)

#### `_build_buy_hold` (L2816-2879)
- **Purpose:** builds a complete buy-and-hold backtest result from the Close series
- **Called by:** `run_backtest` (early return when `entry_signal == 'Buy_Hold'`)
- **Calls:** `_find_basket_parquet`
- **Parameters:** `target`, `target_type`, `start_date`, `end_date`
- **Returns:** full backtest response dict (single trade, equity curve as ratio from 1.0, trade_paths, stats)
- **Key behaviors:**
  - Supports basket (reads basket parquet) and ticker (reads INDIVIDUAL_SIGNALS_FILE) target types
  - Rejects basket_tickers mode with HTTP 400
  - Equity curve is `close / first_close` (percentage-based, no initial_equity)
  - Trade includes `entry_weight: 1.0`, `exit_weight: 1.0`, `contribution: total_return`
  - Stats: single trade, max_dd from cummax drawdown

#### `run_backtest` (L2882-3393) — POST /api/backtest
- **Calls:** `_build_buy_hold` (early return for Buy_Hold), `_find_basket_parquet`, `get_latest_universe_tickers`, `get_meta_file_tickers`, `_quarter_str_to_date`, `_get_universe_history`, `_get_universe_tickers_for_range`, `safe_float`
- **Nested functions:**
  - `mtm_equity` (L3170) — mark-to-market equity computation for open positions
  - `compute_stats` (L3332) — trade statistics: filters by `_was_taken` flag to separate trades_met_criteria (total signals) vs trades_taken (positions actually opened); reports `trades_met_criteria`, `trades_taken`, `trades_skipped`; win rate/EV/PF computed from taken trades only; avg_bars from taken trades only
- **Data I/O:** reads `signals_500.parquet`, basket parquets, thematic/gics JSON
- **Constants:** `SIGNAL_IS_COL`, `BACKTEST_DIRECTION`
- **Key behaviors:**
  - Early return for Buy_Hold: delegates to `_build_buy_hold()` helper
  - Vectorized trade building from pre-computed arrays — no iterrows
  - Buy-and-hold curve aligned to equity curve dates, ratio from 1.0
  - Initial equity = 1.0 (percentage-based); leverage multiplies position sizes: `wanted = equity * pos_size * max_lev`; exposure capped by `equity * max_lev`
  - Per-trade `entry_weight` = alloc/equity at entry; `exit_weight` = exit_val/equity at exit; `contribution` = entry_weight * return; `_was_taken` = True set when position is actually opened (leverage room available)
  - Daily position snapshots keyed by date index with full position detail (entry_weight, weight, daily_return, contribution)
  - After stats computation, `_was_taken` internal field is cleaned up (popped) before returning trades to client
- **Frontend callers:** BacktestPanel.tsx `runBacktest` now calls POST `/api/backtest/multi` for all backtests (single-leg wrapped as 1-leg multi)

#### `run_multi_backtest` (L3395-3894) — POST /api/backtest/multi
- **Calls:** `_build_leg_trades`, `_find_basket_parquet`
- **Nested function:** `compute_stats` — NEW split structure: returns `{'portfolio': {...}, 'trade': {...}}` dict
  - `portfolio` stats: `strategy_return`, `cagr`, `volatility`, `max_dd`, `sharpe`, `sortino`, `contribution?`, `allocation?`
  - `trade` stats: `trades_met_criteria`, `trades_taken`, `trades_skipped`, `win_rate`, `avg_winner`, `avg_loser`, `ev`, `profit_factor`, `avg_time_winner`, `avg_time_loser`
- **Key behaviors:**
  - Initial equity = 1.0 (percentage-based, no initial_equity parameter)
  - Per-leg equity curves run two parallel simulations: allocated (for combined sum, using `leg_initial = initial * alloc_frac`) and standalone (for per-leg display, using full `initial`)
  - Leverage multiplies position sizes: `wanted = equity * pos_size * max_lev`; max exposure per leg: `equity * max_lev`
  - Per-trade `entry_weight`/`exit_weight`/`contribution` computed relative to combined equity (not per-leg)
  - Daily positions keyed by date index with full position detail (entry_weight, weight, daily_return, contribution, leg_target)
  - Each leg response includes `trade_paths` (daily cumulative return per trade)
  - Combined equity curve is sum of allocated per-leg curves
  - Buy-hold curve derived from first basket leg (falls back to first leg overall)
  - Inter-leg correlation matrix from daily returns of standalone equity curves
  - Per-leg contribution = alloc_frac * leg_return

#### `get_signals_log` (L4864-5074) — GET /api/signals/log
- **Params:** `universe` (stocks|etfs|baskets), `period` (1d|1w|1m|3m|6m|1y)
- **Reads:** `signals_500.parquet` (stocks), `signals_etf_50.parquet` (ETFs), `*_of_*_signals.parquet` from basket cache folders (baskets)
- **Also reads:** `top500stocks.json`, `etf_universes_50.json` (universe filtering), `gics_mappings_500.json` (sector/industry), `ticker_names.json` (names), `live_signals_500.parquet`, `live_signals_etf_50.parquet`, `live_basket_signals_500.parquet` (live overlay)
- **Calls:** `_read_live_parquet`, `_live_is_current`, `safe_float`
- **Signal columns:** `Is_Breakout`, `Is_Breakdown`, `Is_Up_Rotation`, `Is_Down_Rotation`, `Is_BTFD`, `Is_STFR`, `{sig}_Entry_Price`, `{sig}_Exit_Date`, `{sig}_Exit_Price`, `{sig}_MFE`, `{sig}_MAE`
- **Open/closed detection:** checks latest row per ticker for NaT exit_date (not firing row)
- **Live overlay:** updates exit price, pct_chg, MAE, MFE for open trades
- **Note:** fully vectorized (no iterrows), numpy types converted for JSON serialization
- **Frontend consumer:** `SignalsPanel.tsx`

#### `uvicorn.run` (L5082) — entry point

---

### app/backend/signals_engine.py (534 lines)

#### `_build_signals_from_df` (L85-343)
- **Called by:** main.py `get_basket_data`
- **PARALLEL IMPL:** signals/rotations.py L1804-1940 (numba version)
- **Pure Python implementation of passes 1-5; same signal detection logic but uses set-based btfd_rotations/stfr_rotations instead of numba boolean arrays**

#### `_build_signals_next_row` (L346-534)
- **Called by:** main.py `list_live_signal_tickers`, `get_ticker_data`
- **PARALLEL IMPL:** signals/rotations.py L2299-2490
- **Near-identical logic; key for incremental live bar computation**

---

### app/backend/audit_basket.py (327 lines)

#### `_quarter_end_from_key` (L29-33) — DUPLICATE of signals/rotations.py L419-424
#### `_quarter_start_from_key` (L36-40) — DUPLICATE of signals/rotations.py L427-432
#### `_prev_quarter_key` (L43-49) — unique to audit
#### `_build_quarter_lookup` (L52-57) — DUPLICATE of signals/rotations.py L2966-2975
#### `_find_active_quarter` (L60-64) — DUPLICATE of signals/rotations.py L2978-2983
#### `walk_equity` (L67-127) — simplified version of signals/rotations.py `compute_equity_ohlc`
#### `main` (L132-327) — diagnostic script entry point
- **Data I/O:** reads `signals_500.parquet`, `momentum_universes_500.json`, `Momentum_Leaders_equity_ohlc.parquet`

---

### app/backend/verify_backtest.py (1016 lines)

Standalone CLI script — independent re-implementation of backtest logic for verification.
No imports from main.py or rotations.py; reads the same parquet/JSON caches directly.

**Data-loading utilities:**
#### `safe_float` (L92-104) — safe numeric conversion with rounding
#### `find_basket_parquet` (L107-117) — glob basket parquet across cache folders; DUPLICATE of rotations.py `_find_basket_parquet` L3510-3522
#### `get_universe_history` (L120-137) — load quarterly universe from gics_mappings or thematic JSON
#### `quarter_str_to_date` (L140-146) — convert '2025 Q4' to Timestamp; DUPLICATE of main.py `_quarter_str_to_date` L179-185
#### `build_quarter_membership` (L149-159) — build sorted (start, end, ticker_set) tuples for membership filtering
#### `load_data` (L162-236) — load raw parquet data for ticker/basket/basket_tickers modes
- **Data I/O:** reads `signals_500.parquet`, `{type}_basket_cache/{slug}_*_signals.parquet`, `gics_mappings_500.json`, thematic JSON files

**Trade builder:**
#### `build_trades` (L243-317) — replay signal entries with membership filtering; mirrors main.py `run_backtest` trade-building logic
- **Calls:** `safe_float`

**Equity engine:**
#### `mtm_equity` (L324-343) — mark-to-market equity calculation for open positions
#### `build_equity_curve` (L346-418) — replay position sizing and daily MTM equity; mirrors main.py run_backtest
- **Calls:** `mtm_equity`

**Stats:**
#### `compute_stats` (L425-470) — compute backtest statistics (win rate, EV, profit factor, max drawdown); mirrors main.py `run_backtest` nested `compute_stats`

**API caller:**
#### `call_api` (L477-496) — POST to /api/backtest and return JSON response
- **Calls:** requests.post

**Comparison engine:**
#### `compare_trades` (L503-561) — Test 1: match local vs API trades by (ticker, entry_date, exit_date)
#### `compare_equity` (L564-613) — Test 4: day-by-day equity curve comparison with tolerance
#### `compare_skipped` (L616-638) — Test 2: verify same entries skipped due to leverage limits
#### `compare_membership` (L641-677) — Test 3a: verify all trades respect basket membership at entry date
#### `verify_no_dropped_entries` (L680-725) — Test 3b: verify no valid signal entries incorrectly excluded
#### `compare_stats` (L728-750) — compare computed stats against API stats

**Report & test runner:**
#### `report` (L757-763) — print coloured PASS/FAIL line
#### `run_test` (L766-867) — run full verification for a single configuration
- **Calls:** `load_data`, `build_trades`, `build_equity_curve`, `compute_stats`, `call_api`, `compare_trades`, `compare_skipped`, `compare_membership`, `verify_no_dropped_entries`, `compare_equity`, `compare_stats`, `report`
#### `detect_basket_slug` (L874-908) — find an available basket slug with parquet + universe history
- **Calls:** `find_basket_parquet`, `get_universe_history`
#### `run_defaults` (L911-968) — run default test suite across 4 configurations
- **Calls:** `detect_basket_slug`, `run_test`
#### `main` (L975-1015) — CLI entry point (argparse)
- **Calls:** `run_defaults`, `run_test`

---

### app/backend/check_data.py (5 lines) — trivial HTTP check script
### app/backend/check_pivots.py (7 lines) — trivial HTTP check script

---

### app/backend/strategy_scanner.py (503 lines)

Standalone CLI script — sweeps backtest parameter combinations by calling the running backend API and collects results into CSV + JSON.

**No imports from main.py or rotations.py.** Communicates exclusively via HTTP to `localhost:8000`.

**Configuration constants (editable):**
- `API_BASE` = `http://localhost:8000`
- `START_DATE`, `END_DATE` — date range for all backtests
- `SIGNALS` — which entry signals to sweep (default: ["Breakout"])
- `TARGET_TYPES` — ["ticker"] or ["basket", "basket_tickers"]
- `SCAN_SECTORS`, `SCAN_THEMES`, `SCAN_INDUSTRIES` — which basket groups to include
- `TICKERS` — individual tickers to test (default: ["NVDA"])
- `POSITION_SIZES` — 1% to 100% sweep
- `MAX_LEVERAGES` — leverage multiplier sweep
- `FILTER_PRESETS` — dict of named regime filter combinations (uptrend, breakout, trend, vol, etc.)
- `ACTIVE_FILTERS` — which presets to actually run
- `MULTI_LEG_TEMPLATES` — optional multi-leg portfolio templates
- `DELAY_BETWEEN_CALLS` = 0.05s

#### `fetch_baskets` (L128-140)
- **Calls:** GET `/api/baskets`
- **Returns:** list of (group, basket_name) tuples filtered by SCAN_* flags

#### `run_single_backtest` (L143-164)
- **Calls:** POST `/api/backtest`
- **Returns:** (result_json, error) tuple

#### `run_multi_backtest` (L167-180)
- **Calls:** POST `/api/backtest/multi`
- **Returns:** (result_json, error) tuple

#### `extract_stats` (L183-212)
- **Purpose:** pull flat stats dict from single-backtest response; computes CAGR from equity curve
- **Returns dict keys:** `trades_met_criteria`, `trades_taken`, `trades_skipped`, `trades`, `win_rate`, `avg_winner`, `avg_loser`, `ev`, `profit_factor`, `max_dd`, `avg_bars`, `final_equity`, `cagr`

#### `extract_multi_stats` (L215-236)
- **Purpose:** pull combined stats from multi-backtest response
- **Returns dict keys:** `strategy_return`, `cagr`, `volatility`, `max_dd`, `sharpe`, `sortino`, `trades_taken`, `win_rate`, `ev`, `profit_factor`, `final_equity`

#### `build_combinations` (L239-281)
- **Purpose:** generate all (target, signal, filter_name, pos_size, leverage) combos from config
- **Returns:** list of combo dicts

#### `main` (L284-500)
- **Purpose:** CLI entry point; connects to backend, sweeps single-leg combos, runs multi-leg templates, saves CSV + JSON, prints summary with top/bottom strategies by EV
- **Calls:** `fetch_baskets`, `build_combinations`, `run_single_backtest`, `extract_stats`, `run_multi_backtest`, `extract_multi_stats`
- **Data I/O:** writes `strategy_scan_results.csv`, `strategy_scan_results_full.json`
- **CLI args:** `--dry-run`, `--limit`, `--output`
- **Key behaviors:**
  - Filter validation: flags filters that had zero effect on trade count
  - Prints position sizing, CAGR, MaxDD, final equity per combo
  - Top-10 and bottom-5 ranking by EV (min 5 trades)

**Imports:** `itertools`, `json`, `time`, `csv`, `sys`, `argparse`, `datetime`, `pathlib.Path`, `requests`

**Called by:** manual invocation (`python strategy_scanner.py`); requires backend running

---

### signals/test_all_optimizations.py (75 lines)

Standalone comparison script — run after a pipeline rebuild to verify that vectorized breadth/correlation/contributions values match the pre-optimization outputs stored in a backup directory.

**No imports from rotations.py.** Reads parquets directly.

#### `compare_column` (L17-32)
- **Called by:** module-level loop over SLUGS
- **Purpose:** compares a single column between old and new DataFrames on their common dates; prints mean/max abs diff and correlation

**Module-level:** iterates over 3 basket slugs (High_Beta, Low_Beta, Momentum_Leaders), reading both backup and current parquets; checks breadth columns + Correlation_Pct; also inspects contribution parquets for weight sanity (daily weight sums ≈ 1.0)

---

### signals/test_correlation_optimization.py (207 lines)

Step-by-step test harness for verifying the correlation vectorization. Invoked as a CLI with a step argument (`step1` through `step5`).

**No imports from rotations.py.** Reads/writes parquets directly.

#### `step1_backup` (L29-52)
- **Purpose:** backs up current basket parquets and saves old Correlation_Pct values to `_correlation_test_backup/old_correlation_pct.parquet`
- **Data I/O:** reads thematic basket parquets; writes backup parquets + `old_correlation_pct.parquet`

#### `step2_compare` (L55-107)
- **Purpose:** loads new basket parquets and compares Correlation_Pct against the backed-up values; reports mean/max abs diff, correlation, and % within ±2/±5 tolerance; shows worst 5 mismatches
- **Data I/O:** reads `old_correlation_pct.parquet`, current basket parquets

#### `step3_check_returns_matrix` (L110-124)
- **Purpose:** verifies `returns_matrix_500.parquet` and fingerprint file exist and prints shape/size/date range

#### `step4_test_backend` (L127-166)
- **Purpose:** hits 3 live backend endpoints (`/api/baskets/breadth`, `/api/baskets/High Beta/correlation`, `/api/baskets/High Beta/summary`) to confirm correlation plumbing works end-to-end; requires running backend

#### `step5_restore` (L169-177)
- **Purpose:** restores original basket parquets from backup directory (non-destructive — leaves backup files in place)

---

## Global Variables Set at Module Level (signals/rotations.py)

These are computed during cell execution and used by downstream cells:

| Variable | Set At | Set By | Used By |
|----------|--------|--------|---------|
| `QUARTER_UNIVERSE` | L407 | `load_or_build_universe()` | Nearly everything |
| `BETA_UNIVERSE` | L681 | `load_or_build_beta_universes()` | basket processing, exports |
| `LOW_BETA_UNIVERSE` | L681 | `load_or_build_beta_universes()` | basket processing, exports |
| `MOMENTUM_UNIVERSE` | L803 | `load_or_build_momentum_universes()` | basket processing, exports |
| `MOMENTUM_LOSERS_UNIVERSE` | L803 | `load_or_build_momentum_universes()` | basket processing, exports |
| `RISK_ADJ_MOM_UNIVERSE` | L929 | `load_or_build_risk_adj_momentum()` | basket processing, exports |
| `HIGH_YIELD_UNIVERSE` | L1168 | `load_or_build_dividend_universes()` | basket processing, exports |
| `DIV_GROWTH_UNIVERSE` | L1168 | `load_or_build_dividend_universes()` | basket processing, exports |
| `TICKER_SECTOR` | L1568 | `load_or_build_gics_mappings()` | exports, signal filtering |
| `TICKER_SUBINDUSTRY` | L1568 | `load_or_build_gics_mappings()` | exports, signal filtering |
| `SECTOR_UNIVERSES` | L1568 | `load_or_build_gics_mappings()` | basket processing |
| `INDUSTRY_UNIVERSES` | L1568 | `load_or_build_gics_mappings()` | basket processing |
| `INDUSTRY_LIST` | L1478/1570 | `_build_industry_universes()` | basket processing |
| `all_signals_df` | L3007 | `load_or_build_signals()` | basket processing, breadth, correlation, live exports |
| `BASKET_RESULTS` | module-level basket loop | `process_basket_signals()` loop | `update_basket_parquets_with_live_ohlcv`, rotations_old_outputs.py |
| `_live_ctx_for_reports` | L6327 | `_get_live_update_context()` | exports, rotations_old_outputs.py |

---

---

### signals/foundation.py (2027 lines)

**Purpose:** Shared infrastructure module imported by all four split-pipeline files (`universe.py`, `tickersignals.py`, `basketsignals.py`, `livesignals.py`). Contains no top-level side-effects; safe to import without hitting Norgate or building caches.

**Sections:**
1. Imports
2. Constants (`START_YEAR`, `SIZE`, `BASE_OUTPUT_FOLDER`, version constants, timing globals)
3. OneDrive / output path helpers (`_resolve_onedrive_output_folder`, `_mirror_to_onedrive`, `_needs_write_and_mirror`, `OutputPaths`, `WriteThroughPath`)
4. Utility functions (`build_pdf`, `_timed_print`, `_install_timed_print`, `reset_cell_timer`)
5. Serialisation helpers (`_universe_to_json`, `_json_to_universe`, `_beta_universes_to_json`, `_json_to_beta_universes`, `_gics_to_json`, `_json_to_gics`, `is_universe_current`)
6. Cache loader functions — load from pre-built JSON caches without rebuilding:
   - `get_current_quarter_key(quarter_universe)` — takes universe dict, returns most recent key
   - `_quarter_end_from_key` / `_quarter_start_from_key` (L396-412)
   - `get_universe(date, quarter_universe)` — prev-quarter look-ahead-safe universe (L412)
   - `load_universe_from_cache()` / `load_etf_universe_from_cache()` / `load_beta_universes_from_cache()` / `load_momentum_universes_from_cache()` / `load_risk_adj_momentum_from_cache()` / `load_dividend_universes_from_cache()` / `load_size_universes_from_cache()` / `load_volume_growth_universes_from_cache()` / `load_gics_mappings_from_cache()` / `load_ticker_names_from_cache()` (L427-503)
   - `load_all_universes()` — loads all caches and returns a named dict (L505-536)
   - `build_all_basket_specs(universes)` — returns list of `(name, universe_dict, charts_folder, basket_type)` tuples for the standard 27-basket run (L539-555)
7. Signal Computation Engine — identical numba-accelerated implementation:
   - `calc_rolling_stats` (L567)
   - `RollingStatsAccumulator` class (L607)
   - `_numba_passes_1_to_4` (L682, @numba.njit)
   - `_numba_pass5_signal` (L849, @numba.njit)
   - `_build_signals_from_df` (L1132)
   - `_build_signals_next_row` (L1271)
   - `_get_latest_norgate_date` (L1462)
8. Basket cache helpers — shared by `basketsignals.py` and `livesignals.py`:
   - `_cache_slugify_label` (L1484)
   - `_get_data_signature` / `reset_data_signature_cache` (L1488-1519)
   - `compute_breadth_pivots` (L1519)
   - `_build_quarter_lookup` / `_find_active_quarter` / `_build_membership_df` / `_vectorized_quarter_filter` (L1637-1691)
   - `compute_breadth_from_trend` / `compute_breadth_from_breakout` (L1691-1741)
   - `_build_quarter_weights` (L1741)
   - `_build_universe_signature` (L1786)
   - `_equity_cache_paths` / `_load_equity_cache` / `_save_equity_cache` / `_build_equity_meta` / `_is_equity_cache_valid` (L1797-1895)
   - `_basket_cache_folder` / `_cache_file_stem` / `_basket_cache_paths` / `_find_basket_parquet` / `_find_basket_meta` / `_get_chart_schema_version_from_parquet` / `_build_basket_signals_meta` / `_is_basket_signals_cache_valid` (L1895-1994)

**Key difference from rotations.py:** `get_current_quarter_key` takes the universe dict as a parameter (not a global). All cache-loader functions raise `FileNotFoundError` if the JSON cache is missing — callers must run the appropriate phase first.

**Imported by:** `universe.py`, `tickersignals.py`, `basketsignals.py`, `livesignals.py`

---

### signals/universe.py (1210 lines)

**Purpose:** Phase 1 of the split pipeline. Builds and caches all quarterly universes (top-500, ETF, beta, momentum, risk-adj momentum, dividends, size, volume growth, GICS). Writes JSON caches consumed by Phases 2-4 via `foundation.load_all_universes()`.

**Run schedule:** Quarterly or on demand (Norgate universe changes).

**Key public functions:**

| Function | Line | What it does |
|----------|------|-------------|
| `get_quarterly_vol` | L60 | Per-ticker dollar-volume calculation (ThreadPoolExecutor worker) |
| `build_quarter_universe` | L76 | Builds top-500 universe dict from Norgate |
| `load_or_build_universe` | L120 | Cache-aware wrapper; writes `top500stocks.json` |
| `build_quarter_etf_universe` | L149 | Builds ETF universe dict |
| `load_or_build_etf_universe` | L185 | Cache-aware wrapper; writes `etf_universes_50.json` |
| `build_quarter_beta_universes` | L275 | High/Low beta top-25 per quarter |
| `load_or_build_beta_universes` | L350 | Cache-aware; writes `beta_universes_500.json` |
| `build_quarter_momentum_universes` | L402 | Momentum winners/losers |
| `load_or_build_momentum_universes` | L460 | Cache-aware; writes `momentum_universes_500.json` |
| `build_quarter_risk_adj_momentum` | L522 | Risk-adjusted momentum universe |
| `load_or_build_risk_adj_momentum` | L574 | Cache-aware; writes `risk_adj_momentum_500.json` |
| `build_quarter_dividend_universes` | L695 | High yield / dividend growth universes |
| `load_or_build_dividend_universes` | L803 | Cache-aware; writes `dividend_universes_500.json` |
| `build_quarter_size_universes` | L864 | Size (market-cap proxy) universe |
| `load_or_build_size_universes` | L916 | Cache-aware |
| `build_quarter_volume_growth_universes` | L969 | Volume-growth universe |
| `load_or_build_volume_growth_universes` | L1038 | Cache-aware |
| `load_or_build_gics_mappings` | L1164 | Sector/industry GICS mappings; writes `gics_mappings_500.json` |

**Imports from foundation.py:** `OutputPaths`, `WriteThroughPath`, `_universe_to_json`, `_json_to_universe`, `_beta_universes_to_json`, `_json_to_beta_universes`, `_gics_to_json`, `_json_to_gics`, `is_universe_current`, `_quarter_end_from_key`, `SIZE`, `BASE_OUTPUT_FOLDER` and path constants.

**Module-level execution:** calls all `load_or_build_*` functions in sequence, writes all JSON caches, then exits.

---

### signals/tickersignals.py (858 lines)

**Purpose:** Phase 2 of the split pipeline. Builds and incrementally updates the individual ticker signal parquet (`signals_500.parquet`) and ETF signal parquet (`signals_etf_50.parquet`). Requires universe JSON caches from Phase 1.

**Run schedule:** Daily after Norgate data update.

**Key public functions:**

| Function | Line | What it does |
|----------|------|-------------|
| `build_signals_for_ticker` | L46 | Full signal rebuild for one ticker via `_build_signals_from_df` |
| `_build_signals_append_ticker` | L58 | Incremental 1-2 bar append via `_build_signals_next_row` |
| `_incremental_update_signals` | L140 | Incremental update driver: appends new days to cached parquet |
| `_signals_cache_is_current` | L320 | Checks parquet freshness vs latest Norgate date |
| `load_or_build_signals` | L358 | Cache-aware wrapper; writes `signals_500.parquet` |
| `_etf_signals_cache_is_current` | L539 | ETF-specific cache check |
| `_incremental_update_etf_signals` | L560 | Incremental ETF signal update |
| `load_or_build_etf_signals` | L685 | Cache-aware wrapper; writes `signals_etf_50.parquet` |

**Imports from foundation.py:** `_build_signals_from_df`, `_build_signals_next_row`, `_numba_passes_1_to_4`, `_numba_pass5_signal`, `RollingStatsAccumulator`, `_get_latest_norgate_date`, `WriteThroughPath`, `SIZE`, path constants, `load_universe_from_cache`, `load_etf_universe_from_cache`.

**Module-level execution:** loads universes via `foundation.load_all_universes()`, calls `load_or_build_signals()` then `load_or_build_etf_signals()`.

---

### signals/basketsignals.py (1523 lines)

**Purpose:** Phase 3 of the split pipeline. Processes all ~27 baskets: builds equity OHLC curves, computes breadth/correlation/contributions, writes basket parquets. Requires signal parquets from Phase 2.

**Run schedule:** Daily after Phase 2 completes.

**Key public functions:**

| Function | Line | What it does |
|----------|------|-------------|
| `compute_signal_trades` | L99 | Extracts closed trade list from basket signals parquet |
| `compute_equity_ohlc` | L163 | Vectorized basket equity OHLC construction (fast + incremental paths) |
| `compute_equity_ohlc_cached` | L412 | Cache-aware wrapper returning `(ohlc_df, contrib_df)` |
| `compute_equity_curve` | L462 | Compatibility shim |
| `_compute_within_basket_correlation` | L527 | Full correlation rebuild (z-score variance decomposition) |
| `_compute_within_basket_correlation_incremental` | L620 | Incremental correlation for new dates only |
| `_augment_basket_signals_with_breadth` | L708 | Merges breadth columns into basket signals df |
| `_finalize_basket_signals_output` | L765 | Writes `_signals.parquet` + `_signals_meta.json`; handles incremental path |
| `_compute_and_save_contributions` | L912 | Full contributions rebuild |
| `_compute_and_save_contributions_incremental` | L1052 | Incremental contributions for new dates only |
| `_record_basket_timing` | L1219 | Per-step timing accumulator |
| `process_basket_signals` | L1228 | Top-level per-basket driver: cache check, equity, breadth, correlation, contributions |

**Imports from foundation.py:** all basket cache helpers, `_build_signals_from_df`, `_build_signals_next_row`, `compute_breadth_pivots`, `_build_quarter_lookup`, `_find_active_quarter`, `_build_membership_df`, `_vectorized_quarter_filter`, `compute_breadth_from_trend`, `compute_breadth_from_breakout`, `_build_quarter_weights`, `_get_data_signature`, `_build_universe_signature`, all equity/basket cache path helpers, `WriteThroughPath`, `SIZE`, path constants, `load_all_universes`, `build_all_basket_specs`.

**Module-level execution:** loads universes + signals, builds `returns_matrix` + `ohlc_ret_matrices`, runs `process_basket_signals` for each basket in `build_all_basket_specs()`.

---

### signals/livesignals.py (1120 lines)

**Purpose:** Phase 4 of the split pipeline. Intraday live loop: fetches Databento OHLC, computes live signal rows, appends `Source='live'` rows to all parquets. Runs approximately every 75 seconds during market hours.

**Run schedule:** Every ~75s (market hours Mon-Fri 09:25-16:15 ET).

**Key public functions:**

| Function | Line | What it does |
|----------|------|-------------|
| `_load_env_file` | L53 | Loads `.env` for Databento credentials |
| `get_realtime_prices` | L88 | Databento Live snapshot prices |
| `get_realtime_ohlcv` | L150 | Databento Live OHLCV |
| `get_live_ohlc_bars` | L223 | Databento Historical 1m OHLC aggregated to 1d |
| `_get_live_update_gate` | L347 | Market-hours gate: returns context dict or None if outside hours |
| `build_signals_for_ticker_live` | L419 | Appends live bar + computes signal row via `_build_signals_next_row` |
| `export_today_signals` | L460 | Writes live ticker signals parquet (`live_signals_500.parquet`) |
| `append_live_today_to_signals_parquet` | L543 | Idempotent append of live rows to `signals_500.parquet` |
| `export_today_etf_signals` | L611 | Writes live ETF signals parquet |
| `append_live_today_to_etf_signals_parquet` | L649 | Idempotent append of live rows to `signals_etf_50.parquet` |
| `export_live_basket_signals` | L896 | Computes live basket OHLC for all baskets; writes `live_basket_signals_500.parquet` |
| `update_basket_parquets_with_live_ohlcv` | L935 | Appends live OHLCV row to each basket's consolidated parquet (Option B path) |
| `_load_universes_and_baskets` | L1030 | Helper: loads all universes + builds basket specs for the live loop |

**Imports from foundation.py:** `_build_signals_from_df`, `_build_signals_next_row`, `_get_latest_norgate_date`, `_find_basket_parquet`, `_compute_live_basket_ohlc` (re-implemented locally), `WriteThroughPath`, `SIZE`, path constants, `load_all_universes`, `build_all_basket_specs`.

**Module-level execution:** calls `_load_env_file()`, loads universes, runs the live update sequence (gate check → fetch → export_today_signals → export_today_etf_signals → append parquets → export_live_basket_signals).

---

## Cross-File Import Dependencies

```
signals/foundation.py
  └── standalone (no imports from other signals/ files)

signals/universe.py
  └── from foundation import * (constants, paths, serialisation helpers)

signals/tickersignals.py
  └── from foundation import * (signal engine, constants, paths)

signals/basketsignals.py
  └── from foundation import * (basket cache helpers, signal engine, constants)

signals/livesignals.py
  └── from foundation import * (signal engine, basket helpers, constants)

signals/rotations_old_outputs.py
  └── from rotations import * (all public names)
  └── from rotations import (specific private names — see file header)

app/backend/main.py
  └── import signals_engine  (local module)
  └── reads shared parquet/JSON files produced by signals/rotations.py

app/backend/signals_engine.py
  └── standalone (numpy, pandas only)

app/backend/audit_basket.py
  └── standalone (reads parquet/JSON files)

app/backend/strategy_scanner.py
  └── standalone (HTTP client only; calls running backend at localhost:8000 via requests)

signals/test_all_optimizations.py
  └── standalone (reads parquets from Python_Outputs directly)

signals/test_correlation_optimization.py
  └── standalone (reads parquets from Python_Outputs directly; step4 hits localhost:8000)

app/frontend/src/components/BacktestPanel.tsx
  └── imports: react, RangeScrollbar (local)
  └── consumes: app/backend/main.py POST /api/backtest/multi, GET /api/baskets, GET /api/tickers, GET /api/date-range

app/frontend/src/components/AnalogsPanel.tsx
  └── imports: react, axios
  └── consumes: app/backend/main.py GET /api/baskets/returns?mode=analogs, GET /api/baskets

app/frontend/src/components/MultiBacktestPanel.tsx (DEFUNCT — no longer imported by App.tsx; functionality merged into BacktestPanel.tsx)
  └── imports: react, axios, lightweight-charts
  └── consumes: app/backend/main.py POST /api/backtest/multi, GET /api/baskets, GET /api/tickers

app/frontend/src/index.css
  └── standalone (global stylesheet, no imports)
```

---

## app/frontend/src/components/BacktestPanel.tsx (1857 lines)

**COMPLETE REWRITE:** Unified single-leg and multi-leg backtest into a single component. All backtests now use POST `/api/backtest/multi` endpoint (single-leg is sent as 1-leg multi). The old `MultiBacktestPanel.tsx` is no longer needed; `BacktestPanel` handles both modes.

### Interfaces

#### BacktestFilter (L6-11)
- **Fields:** `metric`, `condition`, `value`, `source`

#### LegConfig (L13-20) — NEW (replaces separate single/multi config)
- **Fields:** `target`, `targetType` ('basket' | 'basket_tickers' | 'ticker'), `entrySignal`, `allocationPct`, `positionSize`, `filters`

#### PortfolioStats (L22-31) — NEW (replaces flat Stats)
- **Fields:** `strategy_return`, `cagr`, `volatility`, `max_dd`, `sharpe`, `sortino`, `contribution?`, `allocation?`

#### TradeStats (L33-44) — NEW
- **Fields:** `trades_met_criteria`, `trades_taken`, `trades_skipped`, `win_rate`, `avg_winner`, `avg_loser`, `ev`, `profit_factor`, `avg_time_winner`, `avg_time_loser`

#### Stats (L46-49) — CHANGED (now split into portfolio + trade sections)
- **Fields:** `portfolio: PortfolioStats`, `trade: TradeStats`

#### Trade (L51-66)
- **Fields:** `ticker?`, `entry_date`, `exit_date`, `entry_price`, `exit_price`, `change`, `mfe`, `mae`, `bars_held`, `regime_pass`, `skipped?`, `entry_weight`, `exit_weight`, `contribution`
- **New fields:** `entry_weight: number | null`, `exit_weight: number | null`, `contribution: number | null`

#### DailyPosition (L68-78)
- **Fields:** `trade_idx`, `ticker?`, `entry_date`, `leg_target`, `alloc`, `entry_weight`, `weight`, `daily_return`, `contribution`
- **New field:** `leg_target: string` — identifies which leg the position belongs to
- **New field:** `entry_weight: number` — weight at entry time

#### DailySnapshot (L80-84)
- **Fields:** `exposure_pct`, `equity`, `positions: DailyPosition[]`

#### LegResult (L86-95) — NEW
- **Fields:** `target`, `target_type`, `entry_signal`, `allocation_pct`, `direction`, `trades`, `trade_paths`, `stats`

#### MultiBacktestResult (L97-112) — NEW (replaces old BacktestResult)
- **Fields:** `legs: LegResult[]`, `combined: { equity_curve, stats }`, `date_range`, `skipped_entries?`, `daily_positions?`, `leg_correlations?`
- **equity_curve:** `{ dates, combined, per_leg, buy_hold }` — all percentage-based (no initial_equity scaling)

#### BacktestPanelProps (L114-119) — SIMPLIFIED
- **Fields:** `apiBase`, `target?`, `targetType?` ('basket' | 'ticker'), `exportTrigger?`
- **Removed:** `allBaskets` (now fetched internally)

### Constants

#### ENTRY_SIGNALS (L123)
- **Value**: `['Up_Rot', 'Down_Rot', 'Breakout', 'Breakdown', 'BTFD', 'STFR', 'Buy_Hold']`

#### EXIT_MAP (L124-129)
- **Value**: `{ Up_Rot: 'Down_Rot', Down_Rot: 'Up_Rot', Breakout: 'Breakdown', Breakdown: 'Breakout', BTFD: 'Breakdown', STFR: 'Breakout', Buy_Hold: 'End of Period' }`

#### LEG_COLORS (L130)
- **Value**: `['#1565C0', '#C2185B', '#2E7D32', '#E65100', '#6A1B9A', '#00838F']`
- **Used by**: Multi-leg equity curves, stats headers, leg cards

#### BENCHMARK_COLORS (L136-143)
- **Value**: `{ Breakout: '#1565C0', Up_Rot: '#42A5F5', BTFD: '#90CAF9', Breakdown: '#C2185B', Down_Rot: '#F06292', STFR: '#F8BBD0' }`

#### POS_PRESETS (L133) / LEV_PRESETS (L134)
- **Values**: `[1, 5, 10, 25, 50, 100]` / `[100, 110, 125, 150, 200, 250]`

#### defaultLeg() (L164-166)
- **Returns**: `{ target: '', targetType: 'basket_tickers', entrySignal: 'Breakout', allocationPct: 100, positionSize: 25, filters: [] }`

### Key State

#### legs (L209-216)
- **Type**: `LegConfig[]` — array of leg configurations (1 for single-leg, up to 6 for multi-leg)
- **Initialized from**: props `target`/`targetType` if provided

#### benchmarks (L855) / benchmarkStats (L856) / showBenchmark (L857)
- **Type**: `Record<string, number[]>` / `Record<string, Stats>` / `Record<string, boolean>`
- **Set by**: `runBacktest` — single POST `/api/backtest/benchmarks` batch request (replaced 6 parallel calls as of 2026-03-23)

#### statsSortRow / statsSortAsc (L866-867)
- **Type**: `string | null` / `boolean` — sort state for unified stats table

#### showConstituents (L319) / eqHoverIdx (L347) / eqPinnedIdx (L348)
- **Type**: boolean / number | null / number | null — constituents overlay state

#### constSortCol (L349)
- **Type**: `'ticker' | 'leg_target' | 'entry_date' | 'entry_weight' | 'daily_return' | 'weight' | 'contribution'`
- **Expanded from**: old version to include `leg_target` and `entry_weight` columns

### API Call Pattern

#### runBacktest (L896-992)
- **Main backtest**: POST `/api/backtest/multi` — single-leg wrapped as 1-leg multi with `allocation_pct: 1.0`
- **Benchmarks (single-leg only)**: POST `/api/backtest/benchmarks` — single batch request returns all 6 signal equity curves + stats
- **Benchmark calls**: 6 parallel `/api/backtest/multi` calls with each signal (single-leg only)
- **Position sizing**: `leg.positionSize / 100` sent to backend
- **Max leverage**: `maxLeverage / 100` sent to backend
- **Backend endpoint**: POST `/api/backtest/multi` (`run_multi_backtest` in main.py L3078)

### Constituents Overlay (equity tab)

#### useEffect: hover, pin, escape handlers (L868-904)
- **Guards**: canvas exists, result exists, resultTab === 'equity', showConstituents === true
- **mousemove**: converts mouse X to date index via eqScaleRef, updates eqHoverIdx
- **click**: toggles eqPinnedIdx on/off at clicked date index
- **keydown Escape**: clears both eqPinnedIdx and eqHoverIdx
- **mouseleave**: clears eqHoverIdx when not pinned

#### Crosshair line + floating overlay (L1248-1328)
- Renders vertical crosshair `<div>` at computed X position
- Renders `.candle-detail-overlay` panel showing daily position snapshot
- Panel columns: Ticker, Leg (multi-leg only), Entry, Ent.Wt, Return, Cur.Wt, Contrib
- Footer row: Total exposure_pct, total contribution

### Result Tabs

- `equity` — Canvas-drawn equity curves showing percentage returns (rebased to 0%); toggle buttons for Combined (or Equity for single), per-leg visibility, 6 benchmarks (single-leg only), Buy Hold, Constituents; strategy toggle buttons use `bt-strat-btn` class; zoom/pan via wheel/drag; log scale toggle
- `stats` (L1342-1441) — Split portfolio + trade stats tables; per-leg + combined (multi-leg); includes CAGR, Sharpe, Sortino, Volatility, inter-leg correlations; trades taken/skipped breakdown
- `distribution` — KDE curve canvas of change/MFE/MAE with per-leg filtering
- `path` — Trade path overlay canvas with multi-leg support; sortable legend (Leg, Ticker, Date, Chg)
- `trades` (L1540-1616) — Sortable trade table with per-leg filter; shows entry_weight, exit_weight, contribution columns

### Configuration Mode (L1624-1857)

#### renderLegCard (L1626-1784)
- Renders a single leg configuration card with: searchable target picker, basket signal/constituent toggle, entry signal select, position size presets, allocation % (multi-leg), regime filters
- Cards arranged in `.multi-leg-grid` layout (2 columns for multi-leg, single-card for single-leg)

#### Portfolio Settings card (L1791-1844)
- Max Leverage presets, date range inputs, Run Backtest button
- Always first in the grid, same size as leg cards

#### Removed: Equity $ input field
- `initial_equity` is no longer user-configurable (uses default 1.0 for percentage-based equity)

#### Removed: Separate MultiBacktestPanel component
- BacktestPanel now handles both single and multi-leg modes natively

---

## app/frontend/src/components/AnalogsPanel.tsx (935 lines)

Self-contained React component for the Analogs feature — finds historical cross-basket regime analogs and displays similarity analysis, forward return projections, and aggregate statistics.

### Props

#### AnalogsPanelProps
- **Fields:** `apiBase` (string), `exportTrigger?` (number), `allBaskets?` (BasketsData)

### Types

#### AnalogItem — single analog window
- **Fields:** `start`, `end`, `similarity`, `similarity_breakdown` (Record), `returns` (Record), `forward` (Record of horizon->per-basket returns), `forward_series` ({dates, baskets})

#### AggHorizon — aggregate forward stats at one horizon
- **Fields:** `mean`, `median`, `min`, `max`, `std`, `count`, `per_basket` (Record)

#### AnalogsResponse — full API response shape
- **Fields:** `current`, `analogs`, `aggregate`, `date_range`, `message?`

### Tabs (AnalogTab union type)

- **summary** — Current regime snapshot: per-basket returns, breadth, breakout, correlation, volatility metrics
- **analogs** — Ranked list of top-N analog windows with similarity scores and per-metric breakdowns
- **comparison** — Side-by-side comparison of current period vs selected analog, with per-basket return deltas
- **forward** — Forward cumulative return series chart per analog (canvas-drawn, up to 252 days)
- **aggregate** — Aggregate forward return stats (1M/3M/6M horizons): mean, median, min, max, std across all analogs, overall and per-basket

### Key State
- `tab` (AnalogTab) — active tab
- `data` (AnalogsResponse | null) — API response
- `loading`, `error` — request state
- `window` (days preset via PRESETS), `topN`, `threshold`, `group` (GroupFilter)

### API Integration
- **Endpoint:** GET `/api/baskets/returns?mode=analogs&start=...&end=...&top_n=...&group=...&threshold=...`
- **Basket list:** GET `/api/baskets` (for basket names/groups)
- **Imported by:** App.tsx (L6)
- **Rendered when:** `showAnalogs` state is true in App.tsx

---

## app/frontend/src/components/MultiBacktestPanel.tsx (DEFUNCT)

**No longer imported by App.tsx.** All functionality merged into BacktestPanel.tsx, which now handles both single-leg and multi-leg modes.

The file still exists on disk but is dead code.

---

## app/frontend/src/index.css

### .backtest-stats-table (L1195-1199)
- **Purpose**: Stats comparison table in the Stats tab
- **Properties**: font-size 11px, border-collapse, nowrap
- **Used by**: BacktestPanel.tsx Stats tab (L1342)

### .backtest-stats-th (L1201-1211)
- **Purpose**: Header cells in stats table
- **Properties**: font-size 10px, bold, uppercase, right-aligned (first-child left-aligned)
- **Used by**: BacktestPanel.tsx Stats tab

### .backtest-stats-td (L1213-1226)
- **Purpose**: Data cells in stats table, with `.label` variant for row labels
- **Properties**: right-aligned, `.label` variant is left-aligned, bold, uppercase
- **Used by**: BacktestPanel.tsx Stats tab

### .backtest-preset-label
- **Purpose**: Label text ("Size:", "Lev:") preceding preset button rows in backtest config
- **Properties**: font-size 10px, bold, uppercase, fixed width 28px
- **Used by**: BacktestPanel.tsx config UI

### .backtest-pos-preset.wide
- **Purpose**: Wider variant of preset buttons for text labels (e.g., "Basket Signal", "Constituent Tickers")
- **Properties**: width auto, padding 3px 12px (overrides default 40px fixed width)
- **Used by**: BacktestPanel.tsx Trade Source toggle

### .backtest-results-header
- **Purpose**: Results header bar with title and control buttons
- **Properties**: height 42px with box-sizing border-box — matches accordion-header row height
- **Used by**: BacktestPanel.tsx results view header

### .summary-panel > .summary-tabs height fix
- **Purpose**: Constrains summary tabs bar to fixed 42px height, matching other header rows
- **Properties**: height 42px, box-sizing border-box, centered alignment, compact padding
- **Used by**: BasketSummary.tsx — tabs bar (Signals / Correlation / Returns / Contribution)

### .bt-search-* classes
- **Purpose**: Searchable target picker combo box for backtest panels
- **Includes**: `.bt-search-wrap`, `.bt-search-input`, `.bt-search-dropdown`, `.bt-search-item`, `.bt-search-group`
- **Used by**: BacktestPanel.tsx target selectors

### .backtest-sizing-row / .backtest-sizing-field
- **Purpose**: Equal-width input layout for position size, leverage, date fields
- **Used by**: BacktestPanel.tsx config section

### .multi-leg-grid / .multi-leg-card / .multi-leg-add
- **Purpose**: Multi-leg grid layout (2 columns, up to 6 legs) with per-leg cards and add button
- **Used by**: BacktestPanel.tsx leg configuration area (both single and multi-leg modes)

### .single-leg-card
- **Purpose**: Single-leg card layout variant
- **Used by**: BacktestPanel.tsx (single-leg mode)

### .bt-strat-btn
- **Purpose**: Equal-width strategy toggle buttons in backtest results
- **Used by**: BacktestPanel.tsx equity tab toggle row (benchmarks, per-leg, buy hold, constituents)

### .path-legend-header / .path-legend-row / .path-legend-col
- **Purpose**: Trade paths legend with Leg, Ticker, Date, Chg columns
- **Used by**: BacktestPanel.tsx path tab

### .backtest-path-legend
- **Width**: 240px (default), 320px (multi-leg via inline style)
- **Used by**: BacktestPanel.tsx path tab

### .candle-detail-row .const-entry (L1008)
- **Purpose**: Entry date column in constituents overlay
- **Properties**: flex: 0 0 80px, text-align: right
- **Used by**: TVChart.tsx candle detail overlay, BacktestPanel.tsx constituents overlay
- **Format**: Entry dates shown as `MM-DD-YYYY` (e.g., `03-18-2026`) via `date.slice(5) + '-' + date.slice(0, 4)`

### Removed: max-width 700px from .backtest-config
- **Previously**: constrained config panel width
- **Now**: removed to allow full-width layout

### Removed: .backtest-stats-sidebar
- **Previously**: sidebar panel showing stats next to equity chart
- **Replaced by**: Stats tab with `.backtest-stats-table` layout
