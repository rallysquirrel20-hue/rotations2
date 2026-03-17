# Dependency Tree
Updated: 2026-03-17
Files scanned: 13
Functions indexed: 211

---

## Cell Map — signals/rotations.py (6192 lines)

| Cell # | Title | Lines | Key Functions |
|--------|-------|-------|---------------|
| 0 | Imports & Dependencies | 1-2 | (imports only) |
| 1 | Configuration & Constants | 3-165 | `_resolve_onedrive_output_folder`, `_mirror_to_onedrive`, `_needs_write_and_mirror`, OutputPaths class, WriteThroughPath class |
| 2 | Utility Functions | 166-285 | `build_pdf`, `_timed_print`, `_install_timed_print`, `reset_cell_timer`, `_get_current_quarter_key` |
| 3 | Universe Construction | 286-1205 | `get_quarterly_vol`, `build_quarter_universe`, `is_universe_current`, `_universe_to_json`, `_json_to_universe`, `load_or_build_universe`, `get_universe`, `_quarter_end_from_key`, `_quarter_start_from_key`, `_calc_beta_quarterly`, `_safe_calc_beta`, `build_quarter_beta_universes`, `is_beta_universes_current`, `_beta_universes_to_json`, `_json_to_beta_universes`, `load_or_build_beta_universes`, `_calc_momentum_quarterly`, `_safe_calc_momentum`, `build_quarter_momentum_universes`, `is_momentum_universes_current`, `load_or_build_momentum_universes`, `_calc_risk_adj_momentum_quarterly`, `_safe_calc_risk_adj_momentum`, `build_quarter_risk_adj_momentum`, `is_risk_adj_momentum_current`, `load_or_build_risk_adj_momentum`, `_calc_dividend_yield_quarterly`, `_safe_calc_dividend_yield`, `_calc_trailing_dividends_quarterly`, `_safe_calc_trailing_divs`, `build_quarter_dividend_universes`, `is_dividend_universes_current`, `load_or_build_dividend_universes`, `_build_gics_mappings`, `_build_sector_universes`, `_build_industry_universes`, `_is_gics_current`, `_gics_to_json`, `_json_to_gics`, `load_or_build_gics_mappings` |
| 4 | Signal Cache | 1206-2617 | `calc_rolling_stats`, RollingStatsAccumulator class, `_numba_passes_1_to_4`, `_numba_pass5_signal`, `_build_signals_from_df`, `_build_signals_next_row`, `build_signals_for_ticker`, `_build_signals_append_ticker`, `_incremental_update_signals`, `_get_latest_norgate_date`, `_signals_cache_is_current`, `load_or_build_signals` |
| 5 | Basket Processing | 2618-4405 | `_cache_slugify_label`, `_cache_build_quarter_lookup`, `_cache_find_active_quarter`, `_compute_equity_close_for_cache`, `_get_data_signature`, `_prebuild_equity_cache_from_signals`, `compute_breadth_pivots`, `compute_signal_trades`, `_build_quarter_lookup`, `_find_active_quarter`, `_vectorized_quarter_filter`, `compute_breadth_from_trend`, `compute_breadth_from_breakout`, `_build_quarter_weights`, `compute_equity_ohlc`, `_build_universe_signature`, `_equity_cache_paths`, `_load_equity_cache`, `_save_equity_cache`, `_build_equity_meta`, `_is_equity_cache_valid`, `_basket_cache_folder`, `_cache_file_stem`, `_basket_cache_paths`, `_find_basket_parquet`, `_find_basket_meta`, `_get_chart_schema_version_from_parquet`, `_build_basket_signals_meta`, `_is_basket_signals_cache_valid`, `compute_equity_ohlc_cached`, `compute_equity_curve`, `_fmt_price`, `_fmt_bars`, `_fmt_pct`, `_append_trade_rows`, `_compute_within_basket_correlation`, `_augment_basket_signals_with_breadth`, `_finalize_basket_signals_output`, `_compute_and_save_contributions`, `_record_basket_timing`, `process_basket_signals`; pre-computation block (L4280-4362): builds `returns_matrix` + `ohlc_ret_matrices`, basket loop |
| 6 | Live Intraday Data | 4406-6112 | `_load_env_file`, `get_realtime_prices`, `get_realtime_ohlcv`, `get_live_ohlc_bars`, `_get_latest_norgate_date_fallback`, `_extract_spy_trade_date_from_df`, `_get_spy_last_trade_date_databento`, `_get_live_update_gate`, `_is_market_open_via_spy_volume`, `_append_live_row`, `build_signals_for_ticker_live`, `_sort_signals_df`, `export_today_signals`, `append_live_today_to_signals_parquet`, `_get_basket_ohlc_for_reports`, `_compute_annual_returns_for_basket`, `_build_group_annual_return_grid`, `_compute_daily_returns_for_basket`, `_get_latest_norgate_rows_by_ticker`, `_compute_live_basket_return`, `_compute_live_basket_ohlc`, `_compute_live_basket_ohlcv`, `_get_live_update_context`, `_build_group_daily_return_grid`, `_render_return_table_pages`, `_render_return_bar_charts`, `_get_all_basket_specs_for_reports`, `_build_basket_annual_grid`, `_build_basket_daily_grid_last20`, `export_annual_returns`, `export_last_20_days_returns`, `_render_year_basket_bar_charts`, `_render_day_basket_bar_charts`, `export_annual_returns_by_year`, `export_last_20_days_returns_by_day`, `update_basket_parquets_with_live_ohlcv` |
| 7 | Holdings Exports (TradingView lists) | 6113-6192 | `export_group_holdings`, `export_current_quarter_universe` |

---

## File Summary — All Python Files

| File | Lines | Functions | Purpose |
|------|-------|-----------|---------|
| signals/rotations.py | 6192 | 139 | Main pipeline: universe, signals, baskets, live, reports |
| signals/rotations_old_outputs.py | 2177 | 35 | Extracted Group B report cells (Excel, correlations, charts, PDFs) |
| signals/databento_test.py | 624 | 16 | Databento API connectivity tests |
| app/backend/main.py | 2243 | 37 | FastAPI REST endpoints + WebSocket |
| app/backend/signals_engine.py | 534 | 2 | Live signal computation (parallel impl) |
| app/backend/audit_basket.py | 327 | 7 | Diagnostic: equity curve audit tool |
| app/backend/verify_backtest.py | 1016 | 22 | Standalone CLI backtest verification (replays trades/equity from raw data, compares vs API) |
| app/backend/check_data.py | 5 | 0 | Quick data inspection script |
| app/backend/check_pivots.py | 7 | 0 | Quick pivot inspection script |
| signals/live_loop.py | 24 | 0 | PM2 daemon: reruns rotations.py every 15 min via runpy |

---

## File Detail — signals/live_loop.py

**Purpose:** PM2-managed daemon that drives the continuous signal refresh cycle. Has no named functions — execution is a single top-level `while True` loop.

**Key constant:** `INTERVAL = 900` (15 minutes between runs)

**How it works:**
1. Resolves the path to `signals/rotations.py` at startup using `Path(__file__).with_name("rotations.py")`
2. Calls `runpy.run_path(script, run_name="__main__")` — this executes the entire `rotations.py` pipeline in a fresh namespace on every iteration, equivalent to running it as a standalone script
3. Any exception is caught and printed via `traceback.print_exc()` so a crash in one iteration does not kill the loop
4. Sleeps `INTERVAL` seconds, then repeats

**Relationship to rotations.py:**
- Invokes the complete rotations.py pipeline (all Cells 0-7) on each loop tick
- Cache guards inside rotations.py (`is_*_current()`, `_signals_cache_is_current`, `_is_equity_cache_valid`) skip expensive rebuilds when data is still fresh
- Cell 6's market-hours gate (`_get_live_update_gate`) no-ops live Databento calls outside Mon-Fri 09:25-16:15 ET

**PM2 integration:** Registered as the `live-signals` app in `ecosystem.config.js`

**Imports:** `time`, `runpy`, `traceback`, `pathlib.Path`

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
| `THEME_SIZE` | 25 | signals/rotations.py | 411 | Stocks per thematic basket |
| `DIV_THEME_SIZE` | 25 | signals/rotations.py | 416 | Stocks per dividend basket |
| `LOOKBACK_DAYS` | 252 | signals/rotations.py | 414 | Beta rolling window |
| `MOMENTUM_LOOKBACK_DAYS` | 252 | signals/rotations.py | 580 | Momentum rolling window |
| `INDUSTRY_MIN_STOCKS` | 10 | signals/rotations.py | 1062 | Min stocks for industry basket |
| `INCREMENTAL_MAX_DAYS` | 5 | signals/rotations.py | 2410 | Max staleness before full rebuild |
| `RV_MULT` | sqrt(252)/sqrt(21) | signals/rotations.py | 1226 | Realized volatility multiplier |
| `EMA_MULT` | 2.0/11.0 | signals/rotations.py | 1227 | Range EMA alpha |
| `RV_EMA_ALPHA` | 2.0/11.0 | signals/rotations.py | 1228 | RV EMA span=10 alpha |
| `SIGNALS` | ['Up_Rot','Down_Rot','Breakout','Breakdown','BTFD','STFR'] | signals/rotations.py | 1225 | Signal type list |

---

## Parallel Implementations

These functions exist in BOTH signals/rotations.py AND app/backend/signals_engine.py with equivalent logic but different optimization levels:

| Function | rotations.py | signals_engine.py | Difference |
|----------|-------------|-------------------|------------|
| `_build_signals_from_df` | L1804-1940 (numba-accelerated) | L85-343 (pure Python) | rotations.py uses `@numba.njit` for passes 1-5; signals_engine.py uses Python loops with set-based tracking |
| `_build_signals_next_row` | L1943-2131 | L346-534 | Near-identical logic; both are Python; used for incremental 1-bar updates |
| `RollingStatsAccumulator` | L1274-1351 (class, deque-based) | L11-82 (class, list-based) | Same interface; rotations.py uses `collections.deque(maxlen=3)`, signals_engine.py uses `list` with `pop(0)` |

Functions in main.py that DUPLICATE logic from rotations.py (not exact copies but same purpose):

| Function | main.py | rotations.py equivalent | Notes |
|----------|---------|------------------------|-------|
| `_find_basket_parquet` | L92-102 | L3500-3512 | Same glob logic, different folder source |
| `_find_basket_meta` | L104-114 | L3515-3529 | Same glob logic, different folder source |
| `_tally_breadth` | L261-306 | `compute_breadth_from_trend` L3010-3034 | Simplified live version, single-day |
| `_compute_live_breadth` | L309-359 | `_compute_within_basket_correlation` L3700-3790 | Live version includes correlation |
| `_quarter_str_to_date` | L179-185 | `_quarter_start_from_key` L427-432 | Same conversion, different name |

Functions in audit_basket.py that duplicate rotations.py logic:

| Function | audit_basket.py | rotations.py equivalent |
|----------|----------------|------------------------|
| `_quarter_end_from_key` | L29-33 | L419-424 |
| `_quarter_start_from_key` | L36-40 | L427-432 |
| `_build_quarter_lookup` | L52-57 | L2966-2975 |
| `_find_active_quarter` | L60-64 | L2978-2983 |
| `walk_equity` | L67-127 | `compute_equity_ohlc` L3105-3351 |

Functions in verify_backtest.py that duplicate logic from other files (independent re-implementation for verification):

| Function | verify_backtest.py | Equivalent in other file |
|----------|--------------------|--------------------------|
| `find_basket_parquet` | L107-117 | rotations.py `_find_basket_parquet` L3500-3512 |
| `quarter_str_to_date` | L140-146 | main.py `_quarter_str_to_date` L179-185 |
| `build_trades` | L243-317 | main.py `run_backtest` trade-building |
| `build_equity_curve` | L346-418 | main.py `run_backtest` equity replay |
| `compute_stats` | L425-470 | main.py `run_backtest` `compute_stats` (L2125-2160) |

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
| `Data_Storage/live_basket_signals_{SIZE}.parquet` | `_build_group_daily_return_grid` | main.py `_read_live_parquet(LIVE_BASKET_SIGNALS_FILE)` | Live basket OHLC bars |
| `{type}_basket_cache/{slug}_*_of_{SIZE}_ohlc.parquet` | `_save_equity_cache` | `_load_equity_cache`, `_get_basket_ohlc_for_reports` | Basket equity OHLC curves |
| `{type}_basket_cache/{slug}_*_of_{SIZE}_ohlc_meta.json` | `_save_equity_cache` | `_load_equity_cache`, `_finalize_basket_signals_output` | Equity cache metadata + state |
| `{type}_basket_cache/{slug}_*_of_{SIZE}_signals.parquet` | `_finalize_basket_signals_output` | `process_basket_signals`, main.py `get_basket_data`, `get_basket_breadth`, `run_backtest` | Consolidated basket signals |
| `{type}_basket_cache/{slug}_*_of_{SIZE}_signals_meta.json` | `_finalize_basket_signals_output` | `process_basket_signals`, main.py `_find_basket_meta`, `get_meta_file_tickers` | Basket signals metadata |
| `{type}_basket_cache/{slug}_*_of_{SIZE}_contributions.parquet` | `_compute_and_save_contributions`, `_finalize_basket_signals_output` (pre-computed path) | main.py `get_basket_contributions`, `get_basket_candle_detail`, `get_basket_weights_from_contributions` | Per-constituent weights/returns |
| `Data_Storage/returns_matrix_500.parquet` | pre-computation block (L4280-4333) | `process_basket_signals` (via `returns_matrix` param) | Pre-computed Date x Ticker daily close returns pivot |
| `Data_Storage/returns_matrix_500.fingerprint` | pre-computation block (L4280-4333) | pre-computation block (cache validity check) | MD5 hash of data shape/date/tickers for cache invalidation |

### Excel/PDF/Text Output Files

| File Pattern | Producer | Purpose |
|-------------|----------|---------|
| `Live_Rotations/{date}_{time}_Live_Signals_for_top_{SIZE}.xlsx` | `export_today_signals` | Live signal export spreadsheet |
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
- **Parallel impl:** audit_basket.py L36-40, main.py `_quarter_str_to_date` L179-185

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

#### `calc_rolling_stats` (L1231-1268)
- **Called by:** `_append_trade_rows`
- **Returns dict keys:** Win_Rate, Avg_Winner, Avg_Loser, Avg_Winner_Bars, Avg_Loser_Bars, Avg_MFE, Avg_MAE, Historical_EV, Std_Dev, Risk_Adj_EV, EV_Last_3, Risk_Adj_EV_Last_3, Count

#### `_numba_passes_1_to_4` (L1354-1517, @numba.njit)
- **Called by:** `_build_signals_from_df`
- **Returns:** 22-element tuple of numpy arrays (trends, resistance, support, signals, etc.)

#### `_numba_pass5_signal` (L1521-1801, @numba.njit)
- **Called by:** `_build_signals_from_df`
- **Returns:** 20-element tuple (entry_price, change, exit data, 13 stats arrays)

#### `_build_signals_from_df` (L1804-1940)
- **Called by:** `build_signals_for_ticker`, `build_signals_for_ticker_live`, `process_basket_signals`, main.py `get_basket_data` (via signals_engine)
- **Calls:** `_numba_passes_1_to_4`, `_numba_pass5_signal`
- **DataFrame columns read:** Date, Open, High, Low, Close, Volume
- **DataFrame columns created:** RV, RV_EMA, Trend, Resistance_Pivot, Support_Pivot, Is_Up_Rotation, Is_Down_Rotation, Rotation_Open, Up_Range, Down_Range, Up_Range_EMA, Down_Range_EMA, Upper_Target, Lower_Target, Is_Breakout, Is_Breakdown, Is_BTFD, Is_STFR, BTFD_Target_Entry, STFR_Target_Entry, Rotation_ID, BTFD_Triggered, STFR_Triggered, Is_Breakout_Sequence, Ticker, {Sig}_Entry_Price, {Sig}_Change, {Sig}_Exit_Date, {Sig}_Exit_Price, {Sig}_Final_Change, {Sig}_MFE, {Sig}_MAE, {Sig}_Win_Rate, {Sig}_Avg_Winner, {Sig}_Avg_Loser, {Sig}_Avg_Winner_Bars, {Sig}_Avg_Loser_Bars, {Sig}_Avg_MFE, {Sig}_Avg_MAE, {Sig}_Historical_EV, {Sig}_Std_Dev, {Sig}_Risk_Adj_EV, {Sig}_EV_Last_3, {Sig}_Risk_Adj_EV_Last_3, {Sig}_Count
- **PARALLEL IMPL:** app/backend/signals_engine.py L85-343

#### `_build_signals_next_row` (L1943-2131)
- **Called by:** `_build_signals_append_ticker`, `export_today_signals`, `append_live_today_to_signals_parquet`, `process_basket_signals`, `update_basket_parquets_with_live_ohlcv`, main.py `list_live_signal_tickers`, `get_ticker_data`
- **Calls:** (pure computation)
- **PARALLEL IMPL:** app/backend/signals_engine.py L346-534

#### `build_signals_for_ticker` (L2134-2143)
- **Called by:** `_incremental_update_signals`, `load_or_build_signals`
- **Calls:** norgatedata.price_timeseries, `_build_signals_from_df`

#### `_build_signals_append_ticker` (L2146-2225)
- **Called by:** `_incremental_update_signals`
- **Calls:** norgatedata.price_timeseries, `_build_signals_next_row`

#### `_incremental_update_signals` (L2228-2405)
- **Called by:** `load_or_build_signals`
- **Calls:** `_build_signals_append_ticker`, `build_signals_for_ticker`, WriteThroughPath
- **Data I/O:** writes `signals_{SIZE}.parquet`
- **DataFrame columns modified:** Source (set to 'norgate'), Trend (normalized to float32), bool cols, float32 stats cols

#### `_get_latest_norgate_date` (L2413-2425)
- **Called by:** `_signals_cache_is_current`, `load_or_build_signals`, `_get_latest_norgate_date_fallback`, `_build_basket_annual_grid`, `export_annual_returns`, `export_annual_returns_by_year`
- **Calls:** norgatedata.price_timeseries (SPY)

#### `_signals_cache_is_current` (L2428-2459)
- **Called by:** `load_or_build_signals`
- **Calls:** `_get_latest_norgate_date`

#### `load_or_build_signals` (L2462-2613)
- **Called by:** module-level (L2616 -> all_signals_df)
- **Calls:** `_signals_cache_is_current`, `_incremental_update_signals`, `build_signals_for_ticker`, `_get_latest_norgate_date`
- **Data I/O:** reads/writes `Data_Storage/signals_{SIZE}.parquet`

### signals/rotations.py — Cell 5: Basket Processing

#### `_cache_slugify_label` (L2636-2637)
- **Called by:** `_prebuild_equity_cache_from_signals`

#### `_cache_build_quarter_lookup` / `_cache_find_active_quarter` (L2640-2657)
- **Called by:** `_compute_equity_close_for_cache`

#### `_compute_equity_close_for_cache` (L2660-2702)
- **Called by:** (not directly called in current code — legacy helper)
- **Calls:** `_cache_build_quarter_lookup`, `_cache_find_active_quarter`
- **DataFrame columns:** Date, Ticker, Close, Volume, Prev_Close, Ret

#### `_get_data_signature` (L2708-2730)
- **Called by:** `_prebuild_equity_cache_from_signals`, `compute_equity_ohlc_cached`, `process_basket_signals`
- **Caches result in:** `_DATA_SIGNATURE_CACHE` global

#### `_prebuild_equity_cache_from_signals` (L2733-2779)
- **Called by:** (invoked in basket processing section, pre-builds equity caches)
- **Calls:** `_get_data_signature`, `_cache_slugify_label`, `_load_equity_cache`, `_build_universe_signature`, `_is_equity_cache_valid`, `compute_equity_ohlc_cached`

#### `compute_breadth_pivots` (L2781-2896)
- **Called by:** `_finalize_basket_signals_output`
- **Returns DataFrame columns:** B_Trend, B_Resistance, B_Support, B_Up_Rot, B_Down_Rot, B_Rot_High, B_Rot_Low, B_Bull_Div, B_Bear_Div

#### `compute_signal_trades` (L2899-2960)
- **Called by:** rotations_old_outputs.py `plot_basket_charts`
- **Returns:** list of trade dicts (entry_date, exit_date, entry_price, exit_price, change, mfe, mae, bars)

#### `_build_quarter_lookup` / `_find_active_quarter` (L2966-2983)
- **Called by:** `_vectorized_quarter_filter`, `compute_breadth_from_trend`, `compute_breadth_from_breakout`, `_build_quarter_weights`, `compute_equity_ohlc`, `_compute_within_basket_correlation`, `_compute_and_save_contributions`
- **Parallel impl:** audit_basket.py L52-64

#### `_vectorized_quarter_filter` (L2986-3007) — NEW
- **Called by:** `compute_breadth_from_trend`, `compute_breadth_from_breakout`
- **Calls:** `_build_quarter_lookup` (indirectly, receives quarter_labels/quarter_ends)
- **How it works:** assigns each row its active quarter via `np.searchsorted` on quarter_ends, builds a flat membership DataFrame from universe_by_date, then inner-joins on `(_q_key, Ticker)` for hash-based filtering
- **DataFrame columns added:** `_q_key` (temporary, used for join)

#### `compute_breadth_from_trend` (L3010-3034)
- **Called by:** `_augment_basket_signals_with_breadth`
- **Calls:** `_build_quarter_lookup`, `_vectorized_quarter_filter`
- **DataFrame columns read:** Date, Ticker, Trend
- **DataFrame columns created:** Date, Uptrend_Count, Downtrend_Count, Total_Stocks, Breadth_Ratio

#### `compute_breadth_from_breakout` (L3037-3057)
- **Called by:** `_augment_basket_signals_with_breadth`
- **Calls:** `_build_quarter_lookup`, `_vectorized_quarter_filter`
- **DataFrame columns read:** Date, Ticker, Is_Breakout_Sequence
- **DataFrame columns created:** Date, Breakout_Count, Breakdown_Count, BO_Total_Stocks, BO_Breadth_Ratio

#### `_build_quarter_weights` (L3060-3102) — NEW
- **Called by:** `compute_equity_ohlc`, `_compute_and_save_contributions` (indirectly, duplicated logic in _compute_and_save_contributions)
- **Calls:** `_quarter_end_from_key`
- **How it works:** computes per-quarter initial dollar-volume weights by averaging Dollar_Vol per ticker per calendar quarter, then normalizing within each quarter's universe using the PRIOR quarter's ranking date
- **Returns:** dict[quarter_key -> dict[ticker -> weight]]

#### `compute_equity_ohlc` (L3105-3351)
- **Signature:** `compute_equity_ohlc(all_df, universe_by_date, start_after_date=None, initial_state=None, return_state=False, returns_matrix=None, ohlc_ret_matrices=None, return_contributions=False)`
- **Called by:** `compute_equity_ohlc_cached`, `compute_equity_curve`
- **Calls:** `_build_quarter_lookup`, `_build_quarter_weights`, `_find_active_quarter`, `_quarter_end_from_key`
- **DataFrame columns read:** Date, Ticker, Open, High, Low, Close, Volume
- **DataFrame columns created:** Ret, Open_Ret, High_Ret, Low_Ret, Dollar_Vol; output: Date, Open, High, Low, Close
- **Two paths:**
  - **Fast vectorized path** (L3126-3234): when `returns_matrix` and `ohlc_ret_matrices` are provided and `start_after_date` is None; uses per-quarter cumprod on pre-computed matrices; optionally returns `contrib_df` as byproduct when `return_contributions=True`
  - **Incremental loop path** (L3236-3351): for appending a few new days (used when cache has partial data)

#### `_build_universe_signature` (L3354-3362)
- **Called by:** `_prebuild_equity_cache_from_signals`, `compute_equity_ohlc_cached`, `process_basket_signals`, `_finalize_basket_signals_output`

#### `_equity_cache_paths` / `_load_equity_cache` / `_save_equity_cache` (L3365-3414)
- **Called by:** `_prebuild_equity_cache_from_signals`, `compute_equity_ohlc_cached`, `_finalize_basket_signals_output`
- **Calls:** `_basket_cache_folder`, `_cache_file_stem`

#### `_build_equity_meta` / `_is_equity_cache_valid` (L3417-3460)
- **Called by:** `_prebuild_equity_cache_from_signals`, `compute_equity_ohlc_cached`
- **References constants:** EQUITY_CACHE_SCHEMA_VERSION, EQUITY_SIGNAL_LOGIC_VERSION, EQUITY_UNIVERSE_LOGIC_VERSION

#### `_basket_cache_folder` (L3463-3470)
- **Called by:** `_equity_cache_paths`, `_basket_cache_paths`, `_finalize_basket_signals_output`, `_compute_and_save_contributions`

#### `_cache_file_stem` (L3473-3483)
- **Called by:** `_equity_cache_paths`, `_basket_cache_paths`, `_finalize_basket_signals_output`, `_compute_and_save_contributions`
- **Calls:** `_get_current_quarter_key`

#### `_basket_cache_paths` (L3486-3497)
- **Called by:** (available for cache path resolution)

#### `_find_basket_parquet` (L3500-3512)
- **Called by:** `_get_chart_schema_version_from_parquet`, `process_basket_signals`, `_get_basket_ohlc_for_reports`, `_build_group_daily_return_grid`, `update_basket_parquets_with_live_ohlcv`
- **Parallel impl:** main.py L92-102

#### `_find_basket_meta` (L3515-3529)
- **Called by:** `process_basket_signals`
- **Parallel impl:** main.py L104-114

#### `_get_chart_schema_version_from_parquet` (L3532-3541)
- **Called by:** rotations_old_outputs.py `plot_basket_charts`

#### `_build_basket_signals_meta` / `_is_basket_signals_cache_valid` (L3545-3582)
- **Called by:** `process_basket_signals`
- **References constants:** BASKET_SIGNALS_CACHE_SCHEMA_VERSION, EQUITY_SIGNAL_LOGIC_VERSION

#### `compute_equity_ohlc_cached` (L3585-3632)
- **Signature:** `compute_equity_ohlc_cached(all_df, universe_by_date, basket_name, slug, basket_type='sector', returns_matrix=None, ohlc_ret_matrices=None)`
- **Returns:** `(ohlc_df, contrib_df)` tuple; `contrib_df` is non-None only on full rebuild
- **Called by:** `_prebuild_equity_cache_from_signals`, `process_basket_signals`
- **Calls:** `_get_data_signature`, `_build_universe_signature`, `_load_equity_cache`, `_is_equity_cache_valid`, `compute_equity_ohlc` (with `return_contributions=True` on full rebuild), `_build_equity_meta`, `_save_equity_cache`
- **Key behavior:** `FORCE_REBUILD_BASKET_SIGNALS` now also triggers equity rebuild (L3591)

#### `compute_equity_curve` (L3635-3642)
- **Called by:** (compatibility helper, not directly called in current code)

#### `_fmt_price` / `_fmt_bars` / `_fmt_pct` (L3645-3654)
- **Called by:** `export_today_signals`, rotations_old_outputs.py

#### `_append_trade_rows` (L3657-3697)
- **Called by:** rotations_old_outputs.py `plot_basket_charts`
- **Calls:** `calc_rolling_stats`

#### `_compute_within_basket_correlation` (L3700-3790)
- **Signature:** `_compute_within_basket_correlation(universe_by_qtr, returns_matrix, window=21)`
- **Called by:** `_finalize_basket_signals_output`
- **Calls:** `_build_quarter_lookup`
- **How it works:** numpy z-score variance decomposition per rolling window; for each date computes `avg_corr = (n * Var(EW z-portfolio) - 1) / (n - 1)` which is the exact simple average of all pairwise correlations, in O(n*w) per date vs O(n^2*w) for full .corr()
- **DataFrame columns created:** Date, Correlation_Pct (scaled 0-100)
- **Accepts:** `returns_matrix` param (pre-computed Date x Ticker pivot of daily returns); no longer reads all_signals_df directly

#### `_augment_basket_signals_with_breadth` (L3793-3843)
- **Called by:** `process_basket_signals`
- **Calls:** `compute_breadth_from_trend`, `compute_breadth_from_breakout`
- **DataFrame columns created/merged:** Uptrend_Pct, Downtrend_Pct, Breadth_EMA, Breakout_Pct, Breakdown_Pct, BO_Breadth_EMA

#### `_finalize_basket_signals_output` (L3846-3972)
- **Signature:** `_finalize_basket_signals_output(name, slug, hist_folder, merged_all, data_sig, universe_sig, universe_by_qtr, basket_type='sector', returns_matrix=None, contrib_df=None)`
- **Called by:** `process_basket_signals`
- **Calls:** `compute_breadth_pivots`, `_compute_within_basket_correlation`, `_cache_file_stem`, `_basket_cache_folder`, `_equity_cache_paths`, `_compute_and_save_contributions`, WriteThroughPath, pa.Table, pq.write_table
- **DataFrame columns added:** B_Trend, B_Resistance, B_Support, B_Up_Rot, B_Down_Rot, B_Rot_High, B_Rot_Low, B_Bull_Div, B_Bear_Div, BO_B_* (same prefixed), Correlation_Pct, Source
- **Data I/O:** writes `{type}_basket_cache/{stem}_signals.parquet`, `{stem}_signals_meta.json`
- **Key behavior:** if `contrib_df` is provided (from equity OHLC vectorized path), saves it directly instead of recomputing; otherwise falls back to `_compute_and_save_contributions`

#### `_compute_and_save_contributions` (L3975-4112)
- **Signature:** `_compute_and_save_contributions(slug, basket_type, universe_by_qtr, returns_matrix=None)`
- **Called by:** `_finalize_basket_signals_output` (fallback when `contrib_df` is None)
- **Calls:** `_build_quarter_lookup`, `_quarter_end_from_key`, `_basket_cache_folder`, `_cache_file_stem`, WriteThroughPath
- **How it works:** vectorized per-quarter with cumprod weight drift; accepts `returns_matrix` param to avoid rebuilding the pivot; if not provided, builds its own from `all_signals_df`
- **Data I/O:** writes `{type}_basket_cache/{stem}_contributions.parquet`
- **DataFrame columns created:** Date, Ticker, Weight_BOD, Daily_Return, Contribution

#### `_record_basket_timing` (L4115-4121)
- **Called by:** `process_basket_signals`
- **Calls:** (writes to `_basket_timing` and `_basket_timing_names` globals)

#### `process_basket_signals` (L4124-4278)
- **Signature:** `process_basket_signals(name, universe_by_qtr, charts_folder, basket_type='sector', returns_matrix=None, ohlc_ret_matrices=None)`
- **Called by:** module-level basket loop (L4350)
- **Calls:** `_cache_slugify_label`, `_get_data_signature`, `_build_universe_signature`, `_find_basket_parquet`, `_find_basket_meta`, `_is_basket_signals_cache_valid`, `compute_equity_ohlc_cached`, `_build_signals_next_row`, `_augment_basket_signals_with_breadth`, `_build_signals_from_df`, `_finalize_basket_signals_output`, `_record_basket_timing`
- **Accepts:** `returns_matrix` and `ohlc_ret_matrices` params (passed through to `compute_equity_ohlc_cached` and `_finalize_basket_signals_output`)

#### Pre-computation block (L4280-4362, module-level)
- **Purpose:** builds shared `returns_matrix` (Date x Ticker pivot of daily close returns) and `ohlc_ret_matrices` (dict of Date x Ticker pivots for Open_Ret, High_Ret, Low_Ret) once before the basket loop; caches `returns_matrix` to `Data_Storage/returns_matrix_500.parquet` with fingerprint check
- **Consumed by:** all `process_basket_signals` calls in the basket loop (L4350-4361)
- **Data I/O:** reads/writes `Data_Storage/returns_matrix_500.parquet`, `Data_Storage/returns_matrix_500.fingerprint`

### signals/rotations.py — Cell 6: Live Intraday Data

#### `_load_env_file` (L4421-4451)
- **Called by:** module-level (L4453)

#### `get_realtime_prices` (L4453-4512)
- **Called by:** (available for external use)
- **Calls:** db.Live

#### `get_realtime_ohlcv` (L4515-4585)
- **Called by:** (available for external use)
- **Calls:** db.Live

#### `get_live_ohlc_bars` (L4588-4635)
- **Called by:** `export_today_signals`, `_get_live_update_context`
- **Calls:** db.Historical

#### `_get_latest_norgate_date_fallback` (L4638-4647)
- **Called by:** `_get_live_update_gate`
- **Calls:** `_get_latest_norgate_date`

#### `_extract_spy_trade_date_from_df` (L4650-4667)
- **Called by:** `_get_spy_last_trade_date_databento`

#### `_get_spy_last_trade_date_databento` (L4670-4696)
- **Called by:** `_get_live_update_gate`
- **Calls:** db.Historical, `_extract_spy_trade_date_from_df`

#### `_get_live_update_gate` (L4699-4737)
- **Called by:** `_is_market_open_via_spy_volume`, `export_today_signals`, `append_live_today_to_signals_parquet`, `_get_live_update_context`
- **Calls:** `_get_latest_norgate_date_fallback`, `_get_spy_last_trade_date_databento`

#### `_is_market_open_via_spy_volume` (L4740-4743)
- **Called by:** (compatibility wrapper)
- **Calls:** `_get_live_update_gate`

#### `_append_live_row` (L4746-4764)
- **Called by:** `build_signals_for_ticker_live`

#### `build_signals_for_ticker_live` (L4767-4777)
- **Called by:** (available for external use)
- **Calls:** `_append_live_row`, `_build_signals_from_df`

#### `_sort_signals_df` (L4783-4800)
- **Called by:** `export_today_signals`, rotations_old_outputs.py

#### `export_today_signals` (L4803-5014)
- **Called by:** module-level (L6105)
- **Calls:** `_get_live_update_gate`, `get_live_ohlc_bars`, `_get_latest_norgate_rows_by_ticker`, `_build_signals_next_row`, `_fmt_price`, `_fmt_bars`, `_sort_signals_df`, WriteThroughPath
- **Data I/O:** writes `Live_Rotations/{date}_{time}_Live_Signals_for_top_{SIZE}.xlsx`, `Data_Storage/live_signals_{SIZE}.parquet`

#### `append_live_today_to_signals_parquet` (L5017-5078)
- **Called by:** (available for manual invocation)
- **Calls:** `_get_live_update_gate`, `_get_live_update_context`, `_build_signals_next_row`
- **Data I/O:** reads/writes `Data_Storage/signals_{SIZE}.parquet`

#### `_get_basket_ohlc_for_reports` (L5081-5115)
- **Called by:** `_compute_annual_returns_for_basket`, `_compute_daily_returns_for_basket`
- **Calls:** `_find_basket_parquet`
- **Data I/O:** reads basket parquet files

#### `_compute_annual_returns_for_basket` (L5118-5147)
- **Called by:** `_build_group_annual_return_grid`
- **Calls:** `_get_basket_ohlc_for_reports`, `_compute_live_basket_return`

#### `_build_group_annual_return_grid` (L5150-5170)
- **Called by:** `_build_basket_annual_grid`
- **Calls:** `_compute_annual_returns_for_basket`

#### `_compute_daily_returns_for_basket` (L5173-5185)
- **Called by:** `_build_group_daily_return_grid`
- **Calls:** `_get_basket_ohlc_for_reports`

#### `_get_latest_norgate_rows_by_ticker` (L5188-5198)
- **Called by:** `export_today_signals`, `_get_live_update_context`, `append_live_today_to_signals_parquet`
- **Reads:** all_signals_df global

#### `_compute_live_basket_return` (L5201-5234)
- **Called by:** `_compute_annual_returns_for_basket`, `_build_basket_daily_grid_last20`, `_build_group_daily_return_grid`

#### `_compute_live_basket_ohlc` (L5237-5270)
- **Called by:** `_build_group_daily_return_grid`

#### `_compute_live_basket_ohlcv` (L5273-5325)
- **Called by:** `update_basket_parquets_with_live_ohlcv`

#### `_get_live_update_context` (L5328-5374)
- **Called by:** `_build_basket_annual_grid`, `_build_basket_daily_grid_last20`, `_build_group_daily_return_grid`, `append_live_today_to_signals_parquet`
- **Calls:** `_get_live_update_gate`, `get_live_ohlc_bars`, `_get_latest_norgate_rows_by_ticker`

#### `_build_group_daily_return_grid` (L5377-5437)
- **Called by:** `_build_basket_daily_grid_last20`
- **Calls:** `_compute_daily_returns_for_basket`, `_compute_live_basket_return`, `_compute_live_basket_ohlc`, `_find_basket_parquet`
- **Data I/O:** writes `Data_Storage/live_basket_signals_{SIZE}.parquet`

#### `_render_return_table_pages` (L5440-5570)
- **Called by:** rotations_old_outputs.py

#### `_render_return_bar_charts` (L5573-5683)
- **Called by:** `export_annual_returns`, `export_last_20_days_returns`, rotations_old_outputs.py

#### `_get_all_basket_specs_for_reports` (L5686-5698)
- **Called by:** `_build_basket_annual_grid`, `_build_basket_daily_grid_last20`

#### `_build_basket_annual_grid` (L5701-5714)
- **Called by:** `export_annual_returns`, `export_annual_returns_by_year`, rotations_old_outputs.py
- **Calls:** `_get_latest_norgate_date`, `_get_live_update_context`, `_get_all_basket_specs_for_reports`, `_build_group_annual_return_grid`

#### `_build_basket_daily_grid_last20` (L5717-5744)
- **Called by:** `export_last_20_days_returns`, `export_last_20_days_returns_by_day`, rotations_old_outputs.py
- **Calls:** `_get_all_basket_specs_for_reports`, `_get_live_update_context`, `_build_group_daily_return_grid`, `_compute_live_basket_return`

#### `export_annual_returns` (L5747-5786)
- **Called by:** module-level (L6106)
- **Calls:** `_get_latest_norgate_date`, `_needs_write_and_mirror`, `_build_basket_annual_grid`, `_render_return_bar_charts`, `build_pdf`

#### `export_last_20_days_returns` (L5789-5812)
- **Called by:** module-level (L6108)
- **Calls:** `_build_basket_daily_grid_last20`, `_render_return_bar_charts`, `build_pdf`

#### `_render_year_basket_bar_charts` (L5815-5879)
- **Called by:** `export_annual_returns_by_year`

#### `_render_day_basket_bar_charts` (L5882-5946)
- **Called by:** `export_last_20_days_returns_by_day`

#### `export_annual_returns_by_year` (L5949-5986)
- **Called by:** module-level (L6107)
- **Calls:** `_get_latest_norgate_date`, `_needs_write_and_mirror`, `_build_basket_annual_grid`, `_render_year_basket_bar_charts`, `build_pdf`

#### `export_last_20_days_returns_by_day` (L5989-6009)
- **Called by:** module-level (L6109)
- **Calls:** `_build_basket_daily_grid_last20`, `_render_day_basket_bar_charts`, `build_pdf`

#### `update_basket_parquets_with_live_ohlcv` (L6012-6096)
- **Called by:** (disabled in current code, L6110 comment)
- **Calls:** `_find_basket_parquet`, `_compute_live_basket_ohlcv`, `_build_signals_next_row`

### signals/rotations.py — Cell 7: Holdings Exports

#### `export_group_holdings` (L6121-6175)
- **Called by:** (available for manual invocation)
- **Calls:** `_get_current_quarter_key`, WriteThroughPath
- **Data I/O:** writes TradingView .txt files

#### `export_current_quarter_universe` (L6178-6192)
- **Called by:** (available for manual invocation)
- **Calls:** `_get_current_quarter_key`

---

### signals/rotations_old_outputs.py (2177 lines)

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

### app/backend/main.py (2243 lines)

#### `_read_live_parquet` (L80-90)
- **Called by:** `_compute_live_breadth`, `get_basket_breadth`, `get_basket_returns`, `get_basket_data`, `list_live_signal_tickers`, `get_ticker_signals`, `get_ticker_data`, `get_basket_summary`

#### `_find_basket_parquet` (L92-102)
- **Called by:** `get_basket_returns`, `get_basket_data`, `get_ticker_baskets`, `get_date_range`, `run_backtest`
- **PARALLEL IMPL:** signals/rotations.py L3500-3512

#### `_find_basket_meta` (L104-114)
- **Called by:** `get_meta_file_tickers`
- **PARALLEL IMPL:** signals/rotations.py L3515-3529

#### `clean_data_for_json` (L117-118)
- **Called by:** `get_basket_data`, `get_ticker_data`

#### `get_latest_universe_tickers` (L120-142)
- **Called by:** `_compute_live_breadth`, `get_basket_breadth`, `get_basket_data`, `get_basket_summary`, `get_basket_correlation`, `get_ticker_baskets`, `run_backtest`
- **Data I/O:** reads `gics_mappings_{SIZE}.json`, thematic JSON files

#### `get_meta_file_tickers` (L144-156)
- **Called by:** `get_basket_summary`, `get_basket_correlation`, `run_backtest`

#### `_get_universe_history` (L159-176)
- **Called by:** `_get_universe_tickers_for_range`, `_get_ticker_join_dates`, `_get_tickers_for_date`, `get_basket_summary`
- **Data I/O:** reads `gics_mappings_{SIZE}.json`, thematic JSON files

#### `_quarter_str_to_date` (L179-185)
- **Called by:** `_get_universe_tickers_for_range`, `_get_ticker_join_dates`, `_get_tickers_for_date`, `get_basket_summary`, `run_backtest`
- **PARALLEL IMPL:** signals/rotations.py `_quarter_start_from_key` L427-432

#### `_get_universe_tickers_for_range` (L188-202)
- **Called by:** (available for API use)

#### `_get_ticker_join_dates` (L205-217)
- **Called by:** `get_basket_summary`

#### `_get_tickers_for_date` (L220-238)
- **Called by:** `get_basket_correlation`

#### `get_basket_weights_from_contributions` (L242-259)
- **Called by:** `get_basket_data`
- **Data I/O:** reads contributions parquet via `_find_basket_contributions`

#### `_tally_breadth` (L261-306)
- **Called by:** `_compute_live_breadth`, `get_basket_breadth`
- **PARALLEL IMPL (simplified):** signals/rotations.py `compute_breadth_from_trend`

#### `_compute_live_breadth` (L309-359)
- **Called by:** `get_basket_data`
- **Calls:** `get_latest_universe_tickers`, `_read_live_parquet`, `_tally_breadth`
- **Data I/O:** reads `live_signals_{SIZE}.parquet`, `signals_{SIZE}.parquet`

#### `read_root` (L363) — GET /
#### `list_baskets` (L366-381) — GET /api/baskets
#### `get_basket_compositions` (L384-407) — GET /api/baskets/compositions
#### `get_basket_breadth` (L410-541) — GET /api/baskets/breadth
- **Calls:** `_tally_breadth`, `_read_live_parquet`, `get_latest_universe_tickers`

#### `get_basket_returns` (L544-705) — GET /api/baskets/returns
- **Calls:** `_find_basket_parquet`, `_read_live_parquet`
- **Nested function:** `_categorize` (L558-564) — classifies slug as "theme", "sector", or "industry"
- **Query params:** `start`, `end` (date range), `mode` ("period" or "daily"), `basket` (slug, required for daily mode), `group` ("all"/"themes"/"sectors"/"industries", period mode filter)
- **Data I/O:** reads basket signal parquets (columns: Date, Close), `LIVE_BASKET_SIGNALS_FILE` (live intraday overlay)
- **Constants:** `THEMATIC_CONFIG`, `BASKET_CACHE_FOLDERS`
- **Key behaviors:**
  - `mode=daily`: returns daily pct_change return series for a single basket; appends live close if available
  - `mode=period` (default): returns one period return per basket; defaults to 1Y range if no dates specified; filterable by group

#### `get_basket_data` (L708-762) — GET /api/baskets/{basket_name}
- **Calls:** `_find_basket_parquet`, `_read_live_parquet`, `signals_engine._build_signals_from_df`, `_compute_live_breadth`, `get_basket_weights_from_contributions`, `get_latest_universe_tickers`, `clean_data_for_json`

#### `list_tickers` (L765-777) — GET /api/tickers
#### `list_tickers_by_quarter` (L780-791) — GET /api/tickers/quarters
#### `list_live_signal_tickers` (L794-879) — GET /api/live-signals
- **Calls:** `signals_engine._build_signals_next_row`, `_read_live_parquet`

#### `get_ticker_signals` (L882-975) — GET /api/ticker-signals
- **Calls:** `_read_live_parquet`

#### `get_ticker_data` (L978-1033) — GET /api/tickers/{ticker}
- **Calls:** `signals_engine._build_signals_next_row`, `_read_live_parquet`

#### `safe_float` / `safe_int` (L1035-1044) — utility formatters

#### `get_basket_summary` (L1047-1498) — GET /api/baskets/{basket_name}/summary
- **Calls:** `get_latest_universe_tickers`, `get_meta_file_tickers`, `_get_universe_history`, `_quarter_str_to_date`, `_read_live_parquet`, `_find_basket_contributions`, `_get_ticker_join_dates`, `safe_float`, `safe_int`

#### `get_basket_correlation` (L1501-1547) — GET /api/baskets/{basket_name}/correlation
- **Calls:** `_get_tickers_for_date`, `get_latest_universe_tickers`, `get_meta_file_tickers`

#### `_find_basket_contributions` (L1549-1560)
- **Called by:** `get_basket_contributions`, `get_basket_candle_detail`, `get_basket_summary`, `get_basket_weights_from_contributions`

#### `get_basket_contributions` (L1563-1648) — GET /api/baskets/{basket_name}/contributions

#### `get_basket_candle_detail` (L1651-1694) — GET /api/baskets/{basket_name}/candle-detail

#### `get_ticker_baskets` (L1697-1725) — GET /api/ticker-baskets/{ticker}

#### `BacktestFilter` (L1727-1731) — Pydantic model
- **Fields:** `metric`, `condition`, `value`, `source`

#### `BacktestRequest` (L1733-1743) — Pydantic model
- **Fields:** `target`, `target_type`, `entry_signal`, `filters`, `start_date`, `end_date`, `position_size`, `initial_equity`, `max_leverage`, `benchmarks_only` (default False), `include_positions` (default False)

#### `get_date_range` (L1745-1760) — GET /api/date-range/{target_type}/{target}
- **Calls:** `_find_basket_parquet`
- **Data I/O:** reads `signals_500.parquet` (columns: Ticker, Date), basket parquets (column: Date)

#### `run_backtest` (L1763-2175) — POST /api/backtest
- **Calls:** `_find_basket_parquet`, `get_latest_universe_tickers`, `get_meta_file_tickers`, `_quarter_str_to_date`, `safe_float`
- **Nested functions:**
  - `mtm_equity` (L2016-2035) — mark-to-market equity computation
  - `compute_stats` (L2125-2160) — trade statistics (win rate, EV, PF, max DD, avg bars); filters out `skipped` trades before computing
- **Data I/O:** reads `signals_500.parquet`, basket parquets, thematic/gics JSON
- **Constants:** `SIGNAL_IS_COL` (L1023-1027), `BACKTEST_DIRECTION` (L1028-1032)
- **Key behaviors:**
  - Vectorized trade building from pre-computed arrays — no iterrows
  - Buy-and-hold curve aligned to equity curve dates
  - Skipped entry detection: when `alloc_f <= 0` in filtered equity path, records skip reason
  - Daily position snapshots when `include_positions=True`
- **Frontend callers:** BacktestPanel.tsx `runBacktest` (fires main + 6 benchmark calls in parallel)

#### `uvicorn.run` (L2240-2243) — entry point

---

### app/backend/signals_engine.py (534 lines)

#### `_build_signals_from_df` (L85-343)
- **Called by:** main.py `get_basket_data` L736
- **PARALLEL IMPL:** signals/rotations.py L1804-1940 (numba version)
- **Pure Python implementation of passes 1-5; same signal detection logic but uses set-based btfd_rotations/stfr_rotations instead of numba boolean arrays**

#### `_build_signals_next_row` (L346-534)
- **Called by:** main.py `list_live_signal_tickers` L862, `get_ticker_data` L1004
- **PARALLEL IMPL:** signals/rotations.py L1943-2131
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
#### `find_basket_parquet` (L107-117) — glob basket parquet across cache folders; DUPLICATE of rotations.py `_find_basket_parquet` L3500-3512
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
#### `build_equity_curve` (L346-418) — replay position sizing and daily MTM equity; mirrors main.py L1944-2050
- **Calls:** `mtm_equity`

**Stats:**
#### `compute_stats` (L425-470) — compute backtest statistics (win rate, EV, profit factor, max drawdown); mirrors main.py L2132-2167

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

## Global Variables Set at Module Level (signals/rotations.py)

These are computed during cell execution and used by downstream cells:

| Variable | Set At | Set By | Used By |
|----------|--------|--------|---------|
| `QUARTER_UNIVERSE` | L407 | `load_or_build_universe()` | Nearly everything |
| `BETA_UNIVERSE` | L575 | `load_or_build_beta_universes()` | basket processing, exports |
| `LOW_BETA_UNIVERSE` | L575 | `load_or_build_beta_universes()` | basket processing, exports |
| `MOMENTUM_UNIVERSE` | L699 | `load_or_build_momentum_universes()` | basket processing, exports |
| `MOMENTUM_LOSERS_UNIVERSE` | L699 | `load_or_build_momentum_universes()` | basket processing, exports |
| `RISK_ADJ_MOM_UNIVERSE` | L817 | `load_or_build_risk_adj_momentum()` | basket processing, exports |
| `HIGH_YIELD_UNIVERSE` | L1045 | `load_or_build_dividend_universes()` | basket processing, exports |
| `DIV_GROWTH_UNIVERSE` | L1045 | `load_or_build_dividend_universes()` | basket processing, exports |
| `TICKER_SECTOR` | L1212 | `load_or_build_gics_mappings()` | exports, signal filtering |
| `TICKER_SUBINDUSTRY` | L1212 | `load_or_build_gics_mappings()` | exports, signal filtering |
| `SECTOR_UNIVERSES` | L1212 | `load_or_build_gics_mappings()` | basket processing |
| `INDUSTRY_UNIVERSES` | L1212 | `load_or_build_gics_mappings()` | basket processing |
| `INDUSTRY_LIST` | L1063/1196 | `_build_industry_universes()` | basket processing |
| `all_signals_df` | L2626 | `load_or_build_signals()` | basket processing, breadth, correlation, live exports |
| `BASKET_RESULTS` | L4335-4362 | `process_basket_signals()` loop | `update_basket_parquets_with_live_ohlcv`, rotations_old_outputs.py |
| `_live_ctx_for_reports` | L6118 | `_get_live_update_context()` | exports, rotations_old_outputs.py |

---

## Cross-File Import Dependencies

```
signals/rotations_old_outputs.py
  └── from rotations import * (all public names)
  └── from rotations import (24 specific private names)

app/backend/main.py
  └── import signals_engine  (local module)
  └── reads shared parquet/JSON files produced by signals/rotations.py

app/backend/signals_engine.py
  └── standalone (numpy, pandas only)

app/backend/audit_basket.py
  └── standalone (reads parquet/JSON files)

app/frontend/src/components/BacktestPanel.tsx
  └── imports: react, axios, lightweight-charts, RangeScrollbar (local)
  └── consumes: app/backend/main.py REST endpoints (backtest API)

app/frontend/src/components/MultiBacktestPanel.tsx
  └── imports: react, axios, lightweight-charts (likely)
  └── consumes: app/backend/main.py POST /api/backtest/multi endpoint

app/frontend/src/index.css
  └── standalone (global stylesheet, no imports)
```

---

## app/frontend/src/components/BacktestPanel.tsx (1898 lines)

### Interfaces

#### BacktestFilter (L6-11)
- **Fields:** `metric`, `condition`, `value`, `source`

#### Trade (L13-25)
- **Fields:** `ticker?`, `entry_date`, `exit_date`, `entry_price`, `exit_price`, `change`, `mfe`, `mae`, `bars_held`, `regime_pass`, `skipped?`
- **New field:** `skipped?: boolean` — set by backend when trade was skipped due to leverage limit

#### Stats (L27-36)
- **Fields:** `trades`, `win_rate`, `avg_winner`, `avg_loser`, `ev`, `profit_factor`, `max_dd`, `avg_bars`

#### DailyPosition (L38-46) — NEW
- **Fields:** `trade_idx`, `ticker?`, `entry_date`, `alloc`, `weight`, `daily_return`, `contribution`
- **Consumed from:** `run_backtest` response `daily_positions[idx].positions`

#### DailySnapshot (L48-52) — NEW
- **Fields:** `exposure_pct`, `equity`, `positions: DailyPosition[]`
- **Consumed from:** `run_backtest` response `daily_positions` dict values

#### SkippedEntry (L54-61) — NEW
- **Fields:** `ticker?`, `entry_date`, `entry_price`, `reason`, `exposure_at_skip`, `equity_at_skip`
- **Consumed from:** `run_backtest` response `skipped_entries` array

#### BacktestResult (L63-72)
- **Fields:** `trades`, `trade_paths`, `equity_curve`, `stats`, `date_range`, `blew_up?`, `daily_positions?`, `skipped_entries?`
- **New fields:** `daily_positions?: Record<number, DailySnapshot>`, `skipped_entries?: SkippedEntry[]`

### Constants

#### BENCHMARK_COLORS (L103-110)
- **Value**: `{ Breakout: '#1565C0', Up_Rot: '#42A5F5', BTFD: '#90CAF9', Breakdown: '#C2185B', Down_Rot: '#F06292', STFR: '#F8BBD0' }`
- **Used by**:
  - Equity curve canvas drawing (L586) — benchmark line colors
  - Equity curve legend (L605) — benchmark legend color
  - Equity tab toggle buttons (L1589-1599) — button active background colors

#### LEV_PRESETS (L92)
- **Value**: `[100, 110, 125, 150, 200, 250]`
- **Used by**: BacktestPanel config UI — renders preset buttons for `maxLeverage` state
- **CSS classes**: `.backtest-pos-presets` (container), `.backtest-pos-preset` (buttons), `.backtest-preset-label` (label "Lev:")

#### POS_PRESETS (L91)
- **Value**: `[1, 5, 10, 25, 50, 100]`
- **Used by**: BacktestPanel config UI — renders preset buttons for `positionSize` state
- **CSS classes**: `.backtest-pos-presets` (container), `.backtest-pos-preset` (buttons), `.backtest-preset-label` (label "Size:")

### Key State

#### benchmarks (L172)
- **Type**: `Record<string, number[]>` — maps signal name to unfiltered equity curve
- **Set by**: `runBacktest` — 6 parallel `benchmarks_only: true` API calls (L348-351)
- **Used by**: `eqWindowed` memo (L486-487), equity canvas drawing (L584-587), equity legend (L601-606), toggle buttons (L1587-1600)

#### showBenchmark (L173)
- **Type**: `Record<string, boolean>` — per-signal visibility toggle
- **Used by**: equity canvas y-range (L522-523), equity line drawing (L585), legend (L602), toggle buttons (L1589-1599)

#### showConstituents (L212) — NEW
- **Type**: `boolean` — toggles constituent overlay on equity tab
- **Set by**: Constituents toggle button (L1607-1610)
- **Used by**: equity canvas useEffect for hover/pin/escape handlers (L706-749), crosshair line + floating overlay panel rendering (L1620-1675)

#### eqHoverIdx (L213) — NEW
- **Type**: `number | null` — equity curve date index under mouse pointer
- **Set by**: mouse move handler in constituents overlay useEffect (L711-718)
- **Used by**: crosshair line rendering, daily position snapshot lookup (L1621-1628)

#### eqPinnedIdx (L214) — NEW
- **Type**: `number | null` — clicked/pinned equity curve date index (overrides hover)
- **Set by**: click handler (L721-731), Escape key clears (L733-734)
- **Used by**: crosshair line color (blue when pinned), overlay `pointerEvents` (auto when pinned), PINNED label (L1642-1644)

#### eqScaleRef (L215) — NEW
- **Type**: `Ref<{ padLeft, plotW, n, startIdx }>` — cached equity chart scale parameters
- **Set by**: equity canvas drawing useEffect (L530)
- **Used by**: hover/click handlers to convert mouse X to date index (L715-717, L725-727)

#### eqDidDragRef (L216) — NEW
- **Type**: `Ref<boolean>` — true when mouse was dragged (suppresses click-to-pin)
- **Set by**: mouseDown resets to false (L662), mouseMove sets to true (L669)
- **Used by**: click handler guards pin action (L722)

### API Call Pattern

#### runBacktest (L308-371)
- **Fires 7 parallel requests**: 1 main backtest + 6 benchmark calls (one per ENTRY_SIGNAL)
- Main call: full body with `entry_signal`, `filters`, `include_positions: true`
- Benchmark calls: stripped body with `benchmarks_only: true`, `filters: []`, each signal
- Uses `benchmarkGenRef` (L174) to discard stale results from superseded runs
- **Backend endpoint**: POST `/api/backtest` (`run_backtest` in main.py L1611)

### Constituents Overlay (equity tab) — NEW

#### useEffect: hover, pin, escape handlers (L706-749)
- **Guards**: canvas exists, result exists, resultTab === 'equity', showConstituents === true
- **mousemove** (L711-718): converts mouse X to date index via eqScaleRef, updates eqHoverIdx; suppressed when dragging or pinned
- **click** (L721-731): toggles eqPinnedIdx on/off at clicked date index; suppressed if eqDidDragRef is true (drag)
- **keydown Escape** (L733-734): clears both eqPinnedIdx and eqHoverIdx
- **mouseleave** (L737): clears eqHoverIdx when not pinned

#### Crosshair line + floating overlay (L1620-1675)
- Renders vertical crosshair `<div>` at computed X position (blue when pinned, gray when hovering)
- Renders `.candle-detail-overlay` panel showing daily position snapshot from `result.daily_positions[activeIdx]`
- Panel columns: Ticker (or Entry date), Weight, Return, Contribution
- Footer row: Total exposure_pct, total contribution
- Pin/unpin hint text shown in title

### Result Tabs

- `equity` — Canvas-drawn equity curves with toggle buttons for Filtered, 6 benchmarks (long: Breakout/Up_Rot/BTFD, short: Breakdown/Down_Rot/STFR), Buy & Hold, Constituents; crosshair overlay when Constituents enabled
- `stats` (L1831-1851) — Table comparing Filtered vs All stats (trades, win rate, avg winner/loser, EV, PF, max DD, avg bars). Uses `.backtest-stats-table` / `.backtest-stats-th` / `.backtest-stats-td` CSS classes
- `distribution` — Histogram canvas of change/MFE/MAE
- `chart` — Lightweight Charts OHLC with entry/exit markers; skips rendering trade lines for `t.skipped` trades (L1217); renders skipped entry markers as gray arrows with 'skip' text (L1229-1238)
- `path` — Trade path overlay canvas
- `trades` — Sortable trade table; skipped rows styled with gray background, '--' placeholders, and 'SKIP' badge (L1866-1886)

### UI Sections

#### Trade Source toggle
- Uses `.backtest-pos-preset.wide` class for wider "Basket Signal" / "Constituent Tickers" toggle buttons
- Controls `useConstituents` state

#### Position Size presets
- Uses `.backtest-preset-label` for "Size:" label
- Uses `.backtest-pos-preset` for each POS_PRESETS button

#### Max Leverage presets
- Uses `.backtest-preset-label` for "Lev:" label
- Uses `.backtest-pos-preset` for each LEV_PRESETS button

---

## app/frontend/src/components/MultiBacktestPanel.tsx (950 lines)

Multi-basket backtest UI — configures and displays results for multi-leg backtests with per-leg capital pools.

### Key Features
- **Leg configuration:** add/remove legs, each with target basket, signal, allocation %, position size, optional filters
- **Equity curve:** canvas-drawn combined + per-leg equity curves with zoom/pan and constituents overlay
- **Stats table:** combined and per-leg statistics (trades, win rate, EV, PF, max DD, avg bars)
- **Trades table:** all trades across legs with leg identifier column

### API Integration
- **Endpoint:** POST `/api/backtest/multi` (REMOVED — `run_multi_backtest` no longer exists in main.py)
- **Request model:** `MultiBacktestRequest` (REMOVED from main.py)
- **Response:** per-leg results + combined equity curve + combined stats

---

## app/frontend/src/index.css

### .backtest-stats-table (L1195-1199)
- **Purpose**: Stats comparison table in the Stats tab
- **Properties**: font-size 11px, border-collapse, nowrap
- **Used by**: BacktestPanel.tsx Stats tab (L1833)

### .backtest-stats-th (L1201-1211)
- **Purpose**: Header cells in stats table (columns: empty, Filtered, All)
- **Properties**: font-size 10px, bold, uppercase, right-aligned (first-child left-aligned)
- **Used by**: BacktestPanel.tsx Stats tab (L1836-1838)

### .backtest-stats-td (L1213-1226)
- **Purpose**: Data cells in stats table, with `.label` variant for row labels
- **Properties**: right-aligned, `.label` variant is left-aligned, bold, uppercase
- **Used by**: BacktestPanel.tsx Stats tab (L1844-1846)

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
- **Properties**: height 41px with box-sizing border-box — matches accordion-header row height
- **Used by**: BacktestPanel.tsx results view header

### .summary-panel > .summary-tabs height fix
- **Purpose**: Constrains summary tabs bar to fixed 41px height, matching other header rows
- **Properties**: height 41px, box-sizing border-box, centered alignment, compact padding
- **Used by**: BasketSummary.tsx — tabs bar (Signals / Correlation / Returns / Contribution)

### Removed: .backtest-stats-sidebar
- **Previously**: sidebar panel showing stats next to equity chart
- **Replaced by**: Stats tab with `.backtest-stats-table` layout
