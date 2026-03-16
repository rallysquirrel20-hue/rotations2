# Dependency Tree
Updated: 2026-03-16
Files scanned: 10
Functions indexed: 153

---

## Cell Map

| File | Cell | Title | Lines |
|------|------|-------|-------|
| rotations.py | 0 | Imports & Dependencies | 1-26 |
| rotations.py | 1 | Configuration & Constants | 27-104 |
| rotations.py | 2 | Output Path Layout | 105-165 |
| rotations.py | 3 | Utility Functions | 166-285 |
| rotations.py | 4 | Universe Construction | 286-1205 |
| rotations.py | 5 | Signal Cache | 1206-2617 |
| rotations.py | 6 | Basket Processing | 2618-4018 |
| rotations.py | 7 | Live Intraday Data | 4019-5725 |
| rotations.py | 8 | Holdings Exports (TradingView lists) [Group B] | 5726-5805 |
| rotations_old_outputs.py | — | Import Header (from rotations import *) | 1-56 |
| rotations_old_outputs.py | 9 | Signal Universe Filtering [Group B] | 57-94 |
| rotations_old_outputs.py | 10 | Daily Signal Exports (Excel) [Group B] | 95-419 |
| rotations_old_outputs.py | 11 | Correlation Cache [Group B] | 420-1060 |
| rotations_old_outputs.py | 12 | Per-Basket Excel Reports [Group B] | 1061-1167 |
| rotations_old_outputs.py | 13 | PNG Chart Generation [Group B] | 1168-1586 |
| rotations_old_outputs.py | 14 | Comprehensive Summary Report (PDF) [Group B] | 1587-1992 |
| rotations_old_outputs.py | 15 | Per-Basket Report (PDF) [Group B] | 1993-2177 |

---

## rotations.py

### _resolve_onedrive_output_folder
- **Defined**: rotations.py:38-60
- **Cell**: 1 — Configuration & Constants
- **Called by**:
  - rotations.py:63 (module-level, assigns ONEDRIVE_OUTPUT_FOLDER)
- **Calls**: none (uses os.getenv, Path)
- **Reads**: none
- **Writes**: none
- **Columns created**: none
- **Constants**: BASE_OUTPUT_FOLDER

### _mirror_to_onedrive
- **Defined**: rotations.py:66-76
- **Cell**: 1 — Configuration & Constants
- **Called by**:
  - rotations.py:205 (WriteThroughPath._copy)
- **Calls**: shutil.copy2
- **Reads**: none
- **Writes**: mirrors local file to ONEDRIVE_OUTPUT_FOLDER path
- **Constants**: ONEDRIVE_OUTPUT_FOLDER, BASE_OUTPUT_FOLDER

### _needs_write_and_mirror
- **Defined**: rotations.py:79-94
- **Cell**: 1 — Configuration & Constants
- **Called by**:
  - rotations_old_outputs.py (generate_basket_report_pdfs)
- **Calls**: Path.exists
- **Reads**: none
- **Writes**: none (returns tuple of booleans)
- **Constants**: ONEDRIVE_OUTPUT_FOLDER, BASE_OUTPUT_FOLDER

### OutputPaths.__post_init__
- **Defined**: rotations.py:120-135
- **Cell**: 2 — Output Path Layout
- **Called by**:
  - rotations.py:147 (module-level, via OutputPaths dataclass)
- **Calls**: self._mkdirs
- **Writes**: creates all output subdirectories

### OutputPaths._mkdirs
- **Defined**: rotations.py:137-144
- **Cell**: 2 — Output Path Layout
- **Called by**:
  - rotations.py:135 (OutputPaths.__post_init__)
- **Calls**: Path.mkdir

### WriteThroughPath.__init__
- **Defined**: rotations.py:183-192
- **Cell**: 3 — Utility Functions
- **Called by**: everywhere WriteThroughPath(path) is called
- **Constants**: ONEDRIVE_OUTPUT_FOLDER, BASE_OUTPUT_FOLDER

### WriteThroughPath._copy
- **Defined**: rotations.py:194-200
- **Cell**: 3 — Utility Functions
- **Called by**:
  - rotations.py:205 (write_bytes)
  - rotations.py:210 (write_text)
  - rotations.py:217 (open context manager)
  - rotations.py:221 (sync)
- **Calls**: shutil.copy2

### WriteThroughPath.write_bytes
- **Defined**: rotations.py:202-205
- **Cell**: 3 — Utility Functions
- **Called by**: none directly (API method)
- **Calls**: self._copy

### WriteThroughPath.write_text
- **Defined**: rotations.py:207-210
- **Cell**: 3 — Utility Functions
- **Called by**:
  - rotations.py:382 (load_or_build_universe)
  - rotations.py:557 (load_or_build_beta_universes)
  - rotations.py:677 (load_or_build_momentum_universes)
  - rotations.py:798 (load_or_build_risk_adj_momentum)
  - rotations.py:1023 (load_or_build_dividend_universes)
  - rotations.py:1196 (load_or_build_gics_mappings)
  - rotations.py:5787 (export_group_holdings)
  - rotations.py:5803 (export_current_quarter_universe)
  - rotations_old_outputs.py:492 (_save_corr_cache)
- **Calls**: self._copy

### WriteThroughPath.open
- **Defined**: rotations.py:213-217
- **Cell**: 3 — Utility Functions
- **Calls**: self._copy

### WriteThroughPath.sync
- **Defined**: rotations.py:219-221
- **Cell**: 3 — Utility Functions
- **Called by**:
  - rotations.py:242 (build_pdf)
  - rotations.py:2387 (_incremental_update_signals)
  - rotations.py:3802 (_finalize_basket_signals_output)
  - rotations.py:3808 (_finalize_basket_signals_output)
  - rotations.py:4614 (export_today_signals, xlsx)
  - rotations.py:4625 (export_today_signals, parquet)
  - rotations_old_outputs.py (_save_corr_cache)
  - rotations_old_outputs.py (export_basket_excel_reports)
  - rotations_old_outputs.py (generate_basket_report_pdfs)
- **Calls**: self._copy

### WriteThroughPath.__fspath__
- **Defined**: rotations.py:223-224
- **Cell**: 3 — Utility Functions

### WriteThroughPath.__str__
- **Defined**: rotations.py:226-227
- **Cell**: 3 — Utility Functions

### build_pdf
- **Defined**: rotations.py:233-242
- **Cell**: 3 — Utility Functions
- **Called by**:
  - rotations.py:5398 (export_annual_returns)
  - rotations.py:5424 (export_last_20_days_returns)
  - rotations.py:5598 (export_annual_returns_by_year)
  - rotations.py:5621 (export_last_20_days_returns_by_day)
  - rotations_old_outputs.py (generate_summary_pdf)
- **Calls**: PdfPages, plt.close, WriteThroughPath.sync
- **Writes**: PDF to path

### _timed_print
- **Defined**: rotations.py:256-261
- **Cell**: 3 — Utility Functions
- **Called by**: replaces builtins.print globally
- **Constants**: _CELL_TIMER_START, _ORIGINAL_PRINT

### _install_timed_print
- **Defined**: rotations.py:264-266
- **Cell**: 3 — Utility Functions
- **Called by**:
  - rotations.py:276 (module-level)
- **Calls**: (sets builtins.print = _timed_print)

### reset_cell_timer
- **Defined**: rotations.py:269-273
- **Cell**: 3 — Utility Functions
- **Called by**:
  - rotations.py:277 (module-level, "Utility Functions")
  - rotations.py:291 (module-level, "Universe Construction")
  - rotations.py:1212 (module-level, "Signal Cache")
  - rotations.py:2622 (module-level, "Basket Processing")
  - rotations.py:4022 (module-level, "Live Intraday Signal Exports")
  - rotations.py:5730 (module-level, "Holdings Exports")
  - rotations_old_outputs.py:62 (module-level, "Signal Universe Filtering")
  - rotations_old_outputs.py:99 (module-level, "Daily Signal Exports")
  - rotations_old_outputs.py:424 (module-level, "Correlation Cache")
  - rotations_old_outputs.py:1065 (module-level, "Per-Basket Excel Reports")
  - rotations_old_outputs.py:1172 (module-level, "PNG Chart Generation")
  - rotations_old_outputs.py:1591 (module-level, "Comprehensive Summary Report")
  - rotations_old_outputs.py:1997 (module-level, "Per-Basket Report PDFs")

### _get_current_quarter_key
- **Defined**: rotations.py:280-283
- **Cell**: 3 — Utility Functions
- **Called by**:
  - rotations.py:3346 (_cache_file_stem)
  - rotations.py:3621 (_compute_within_basket_correlation)
  - rotations.py:5735 (export_group_holdings)
  - rotations.py:5792 (export_current_quarter_universe)
  - rotations_old_outputs.py (module-level, Daily Signal Exports cell)
  - rotations_old_outputs.py:446 (_corr_asof_date)
- **Calls**: sorted(QUARTER_UNIVERSE.keys())
- **Constants**: QUARTER_UNIVERSE

---

### get_quarterly_vol
- **Defined**: rotations.py:294-307
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:337 (build_quarter_universe, via ThreadPoolExecutor.map)
- **Calls**: norgatedata.price_timeseries
- **Columns read**: Close, Volume
- **Constants**: START_YEAR

### build_quarter_universe
- **Defined**: rotations.py:310-351
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:381 (load_or_build_universe)
- **Calls**: norgatedata.database_symbols, norgatedata.subtype1, get_quarterly_vol (ThreadPoolExecutor), pd.DataFrame
- **Constants**: SIZE

### is_universe_current
- **Defined**: rotations.py:354-357
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:374 (load_or_build_universe)

### _universe_to_json
- **Defined**: rotations.py:360-362
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:382 (load_or_build_universe)

### _json_to_universe
- **Defined**: rotations.py:365-367
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:373 (load_or_build_universe)

### load_or_build_universe
- **Defined**: rotations.py:370-385
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:397 (module-level, assigns QUARTER_UNIVERSE)
- **Calls**: _json_to_universe, is_universe_current, build_quarter_universe, WriteThroughPath.write_text, _universe_to_json
- **Reads**: CACHE_FILE (top500stocks.json) — JSON
- **Writes**: CACHE_FILE (top500stocks.json) — JSON

### get_universe
- **Defined**: rotations.py:388-394
- **Cell**: 4 — Universe Construction
- **Called by**: none found in scope (utility for external callers)
- **Constants**: QUARTER_UNIVERSE

### _quarter_end_from_key
- **Defined**: rotations.py:409-414
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:511 (build_quarter_beta_universes)
  - rotations.py:643 (build_quarter_momentum_universes)
  - rotations.py:768 (build_quarter_risk_adj_momentum)
  - rotations.py:966 (build_quarter_dividend_universes)
  - rotations.py:3103 (compute_equity_ohlc)
- **Parallel impl**: audit_basket.py:29 has identical function

### _quarter_start_from_key
- **Defined**: rotations.py:417-422
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:2635 (_cache_build_quarter_lookup)
  - rotations.py:2959 (_build_quarter_lookup)
- **Parallel impl**: audit_basket.py:36 has identical function

### _calc_beta_quarterly
- **Defined**: rotations.py:425-447
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:452 (_safe_calc_beta)
- **Calls**: norgatedata.price_timeseries
- **Columns read**: Close
- **Constants**: LOOKBACK_DAYS

### _safe_calc_beta
- **Defined**: rotations.py:450-455
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:485 (build_quarter_beta_universes, via ThreadPoolExecutor)
- **Calls**: _calc_beta_quarterly

### build_quarter_beta_universes
- **Defined**: rotations.py:458-523
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:556 (load_or_build_beta_universes)
- **Calls**: _safe_calc_beta (ThreadPoolExecutor), _quarter_end_from_key, norgatedata.price_timeseries
- **Constants**: QUARTER_UNIVERSE, THEME_SIZE, LOOKBACK_DAYS, MARKET_SYMBOL

### is_beta_universes_current
- **Defined**: rotations.py:526-530
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:549 (load_or_build_beta_universes)

### _beta_universes_to_json
- **Defined**: rotations.py:533-536
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:557 (load_or_build_beta_universes)

### _json_to_beta_universes
- **Defined**: rotations.py:539-542
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:548 (load_or_build_beta_universes)

### load_or_build_beta_universes
- **Defined**: rotations.py:545-561
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:565 (module-level, assigns BETA_UNIVERSE, LOW_BETA_UNIVERSE)
- **Calls**: _json_to_beta_universes, is_beta_universes_current, build_quarter_beta_universes, WriteThroughPath.write_text, _beta_universes_to_json
- **Reads**: BETA_CACHE_FILE (beta_universes_500.json) — JSON
- **Writes**: BETA_CACHE_FILE (beta_universes_500.json) — JSON

### _calc_momentum_quarterly
- **Defined**: rotations.py:573-589
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:594 (_safe_calc_momentum)
- **Calls**: norgatedata.price_timeseries
- **Columns read**: Close
- **Constants**: MOMENTUM_LOOKBACK_DAYS, START_YEAR

### _safe_calc_momentum
- **Defined**: rotations.py:592-601
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:614 (build_quarter_momentum_universes, via ThreadPoolExecutor)
- **Calls**: _calc_momentum_quarterly

### build_quarter_momentum_universes
- **Defined**: rotations.py:604-652
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:675 (load_or_build_momentum_universes)
- **Calls**: _safe_calc_momentum (ThreadPoolExecutor), _quarter_end_from_key
- **Constants**: QUARTER_UNIVERSE, THEME_SIZE

### is_momentum_universes_current
- **Defined**: rotations.py:655-659
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:668 (load_or_build_momentum_universes)

### load_or_build_momentum_universes
- **Defined**: rotations.py:662-685
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:689 (module-level, assigns MOMENTUM_UNIVERSE, MOMENTUM_LOSERS_UNIVERSE)
- **Calls**: is_momentum_universes_current, build_quarter_momentum_universes, WriteThroughPath.write_text
- **Reads**: MOMENTUM_CACHE_FILE (momentum_universes_500.json) — JSON
- **Writes**: MOMENTUM_CACHE_FILE (momentum_universes_500.json) — JSON

### _calc_risk_adj_momentum_quarterly
- **Defined**: rotations.py:699-718
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:722 (_safe_calc_risk_adj_momentum)
- **Calls**: norgatedata.price_timeseries
- **Columns read**: Close
- **Constants**: MOMENTUM_LOOKBACK_DAYS

### _safe_calc_risk_adj_momentum
- **Defined**: rotations.py:721-730
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:742 (build_quarter_risk_adj_momentum, via ThreadPoolExecutor)
- **Calls**: _calc_risk_adj_momentum_quarterly

### build_quarter_risk_adj_momentum
- **Defined**: rotations.py:733-776
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:797 (load_or_build_risk_adj_momentum)
- **Calls**: _safe_calc_risk_adj_momentum (ThreadPoolExecutor), _quarter_end_from_key
- **Constants**: QUARTER_UNIVERSE, THEME_SIZE

### is_risk_adj_momentum_current
- **Defined**: rotations.py:779-782
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:790 (load_or_build_risk_adj_momentum)

### load_or_build_risk_adj_momentum
- **Defined**: rotations.py:785-803
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:807 (module-level, assigns RISK_ADJ_MOM_UNIVERSE)
- **Calls**: is_risk_adj_momentum_current, build_quarter_risk_adj_momentum, WriteThroughPath.write_text
- **Reads**: RISK_ADJ_MOM_CACHE_FILE (risk_adj_momentum_500.json) — JSON
- **Writes**: RISK_ADJ_MOM_CACHE_FILE (risk_adj_momentum_500.json) — JSON

### _calc_dividend_yield_quarterly
- **Defined**: rotations.py:812-853
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:857 (_safe_calc_dividend_yield)
- **Calls**: norgatedata.dividend_yield_timeseries, pd.DataFrame

### _safe_calc_dividend_yield
- **Defined**: rotations.py:856-865
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:922 (build_quarter_dividend_universes, via ThreadPoolExecutor)
- **Calls**: _calc_dividend_yield_quarterly

### _calc_trailing_dividends_quarterly
- **Defined**: rotations.py:868-895
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:898 (_safe_calc_trailing_divs)
- **Calls**: norgatedata.price_timeseries
- **Columns read**: Dividend

### _safe_calc_trailing_divs
- **Defined**: rotations.py:898-907
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:936 (build_quarter_dividend_universes, via ThreadPoolExecutor)
- **Calls**: _calc_trailing_dividends_quarterly

### build_quarter_dividend_universes
- **Defined**: rotations.py:910-998
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:1021 (load_or_build_dividend_universes)
- **Calls**: _safe_calc_dividend_yield (ThreadPoolExecutor), _safe_calc_trailing_divs (ThreadPoolExecutor), _quarter_end_from_key
- **Constants**: QUARTER_UNIVERSE, DIV_THEME_SIZE

### is_dividend_universes_current
- **Defined**: rotations.py:1001-1005
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:1014 (load_or_build_dividend_universes)

### load_or_build_dividend_universes
- **Defined**: rotations.py:1008-1031
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:1035 (module-level, assigns HIGH_YIELD_UNIVERSE, DIV_GROWTH_UNIVERSE)
- **Calls**: is_dividend_universes_current, build_quarter_dividend_universes, WriteThroughPath.write_text
- **Reads**: DIVIDEND_CACHE_FILE (dividend_universes_500.json) — JSON
- **Writes**: DIVIDEND_CACHE_FILE (dividend_universes_500.json) — JSON

### _build_gics_mappings
- **Defined**: rotations.py:1058-1088
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:1192 (load_or_build_gics_mappings)
- **Calls**: norgatedata.classification_at_level
- **Constants**: QUARTER_UNIVERSE

### _build_sector_universes
- **Defined**: rotations.py:1091-1109
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:1193 (load_or_build_gics_mappings)
- **Constants**: QUARTER_UNIVERSE, SECTOR_LIST

### _build_industry_universes
- **Defined**: rotations.py:1112-1149
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:1194 (load_or_build_gics_mappings)
- **Calls**: (modifies global INDUSTRY_LIST)
- **Constants**: QUARTER_UNIVERSE, INDUSTRY_MIN_STOCKS

### _is_gics_current
- **Defined**: rotations.py:1152-1158
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:1183 (load_or_build_gics_mappings)

### _gics_to_json
- **Defined**: rotations.py:1161-1168
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:1196 (load_or_build_gics_mappings)

### _json_to_gics
- **Defined**: rotations.py:1171-1175
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:1182 (load_or_build_gics_mappings)

### load_or_build_gics_mappings
- **Defined**: rotations.py:1178-1198
- **Cell**: 4 — Universe Construction
- **Called by**:
  - rotations.py:1202 (module-level, assigns TICKER_SECTOR, TICKER_SUBINDUSTRY, SECTOR_UNIVERSES, INDUSTRY_UNIVERSES)
- **Calls**: _json_to_gics, _is_gics_current, _build_gics_mappings, _build_sector_universes, _build_industry_universes, WriteThroughPath.write_text, _gics_to_json
- **Reads**: GICS_CACHE_FILE (gics_mappings_500.json) — JSON
- **Writes**: GICS_CACHE_FILE (gics_mappings_500.json) — JSON

---

### calc_rolling_stats
- **Defined**: rotations.py:1221-1258
- **Cell**: 5 — Signal Cache
- **Called by**:
  - rotations.py:3582 (_append_trade_rows)
- **Calls**: np.mean, np.std

### RollingStatsAccumulator.__init__
- **Defined**: rotations.py:1270-1282
- **Cell**: 5 — Signal Cache
- **Called by**: instantiated in _numba_pass5_signal (rotations.py), signals_engine._build_signals_from_df
- **Parallel impl**: YES — signals_engine.py:237 has RollingStatsAccumulator (pure Python, no bars tracking)

### RollingStatsAccumulator.add
- **Defined**: rotations.py:1284-1300
- **Cell**: 5 — Signal Cache
- **Called by**:
  - rotations.py:1885 (_build_signals_from_df, indirectly via numba-replaced path)
- **Parallel impl**: YES — signals_engine.py:249 (no bars parameter)

### RollingStatsAccumulator.get_stats
- **Defined**: rotations.py:1302-1331
- **Cell**: 5 — Signal Cache
- **Called by**:
  - (called after trade close in pass5 hot loop)
- **Parallel impl**: YES — signals_engine.py:265

### _numba_passes_1_to_4
- **Defined**: rotations.py:1344-1507
- **Cell**: 5 — Signal Cache
- **Called by**:
  - rotations.py:1832 (_build_signals_from_df)
- **Calls**: numba.njit compiled — pure numpy array operations
- **Columns created**: trends, resistance, support, is_up_rot, is_down_rot, rotation_open, up_range, down_range, up_range_ema, down_range_ema, upper_target, lower_target, is_breakout, is_breakdown, is_btfd, is_stfr, btfd_entry_price, stfr_entry_price, rotation_ids, btfd_triggered, stfr_triggered, is_breakout_seq
- **Constants**: RV_MULT, EMA_MULT
- **Key logic (Pass 3 BTFD/STFR)**: BTFD requires `trends[i] == 0 AND trends[i-1] == 0` (both current and previous bar in downtrend). STFR requires `trends[i] == 1 AND trends[i-1] == 1` (both current and previous bar in uptrend). This prevents false signals on trend-change bars.
- **Parallel impl**: NO — signals_engine._build_signals_from_df implements passes 1-4 inline in pure Python (no numba)

### _numba_pass5_signal
- **Defined**: rotations.py:1511-1791
- **Cell**: 5 — Signal Cache
- **Called by**:
  - rotations.py:1893 (_build_signals_from_df, for each of 6 signal types)
- **Calls**: numba.njit compiled — pure numpy array operations, inline accumulator
- **Columns created**: entry_price_col, change_col, exit_idx_col, exit_price_col, final_change_col, mfe_col, mae_col, + 13 stats arrays per signal
- **Parallel impl**: NO — signals_engine._build_signals_from_df implements pass 5 inline in Python with RollingStatsAccumulator

### _build_signals_from_df
- **Defined**: rotations.py:1794-1930
- **Cell**: 5 — Signal Cache
- **Called by**:
  - rotations.py:2133 (build_signals_for_ticker)
  - rotations.py:3904 (process_basket_signals, for basket-level OHLC)
  - rotations.py:4390 (build_signals_for_ticker_live)
  - main.py:476 (get_basket_data, via signals_engine._build_signals_from_df)
- **Calls**: _numba_passes_1_to_4, _numba_pass5_signal, pd.concat
- **Reads**: df with columns: Open, High, Low, Close (passed as argument)
- **Writes**: none (returns DataFrame)
- **Columns created**: RV, RV_EMA, Trend, Resistance_Pivot, Support_Pivot, Is_Up_Rotation, Is_Down_Rotation, Rotation_Open, Up_Range, Down_Range, Up_Range_EMA, Down_Range_EMA, Upper_Target, Lower_Target, Is_Breakout, Is_Breakdown, Is_BTFD, Is_STFR, BTFD_Target_Entry, STFR_Target_Entry, Rotation_ID, BTFD_Triggered, STFR_Triggered, Is_Breakout_Sequence, {Sig}_Entry_Price, {Sig}_Change, {Sig}_Exit_Date, {Sig}_Exit_Price, {Sig}_Final_Change, {Sig}_MFE, {Sig}_MAE, {Sig}_Win_Rate, {Sig}_Avg_Winner, {Sig}_Avg_Loser, {Sig}_Avg_Winner_Bars, {Sig}_Avg_Loser_Bars, {Sig}_Avg_MFE, {Sig}_Avg_MAE, {Sig}_Historical_EV, {Sig}_Std_Dev, {Sig}_Risk_Adj_EV, {Sig}_EV_Last_3, {Sig}_Risk_Adj_EV_Last_3, {Sig}_Count, Ticker
- **Constants**: RV_MULT, EMA_MULT, RV_EMA_ALPHA, EQUITY_SIGNAL_LOGIC_VERSION (indirectly)
- **Parallel impl**: YES — signals_engine.py:298 has _build_signals_from_df (live 30m, pure Python, passes 1-5 inline, no numba, no bars stats)

### _build_signals_next_row
- **Defined**: rotations.py:1933-2121
- **Cell**: 5 — Signal Cache
- **Called by**:
  - rotations.py:2197 (_build_signals_append_ticker)
  - rotations.py:3874 (process_basket_signals, incremental basket append)
  - rotations.py:4529 (export_today_signals)
  - rotations.py:4661 (append_live_today_to_signals_parquet)
  - main.py:~530 (get_ticker_data, via signals_engine — but rotations version used internally)
- **Calls**: pd.to_datetime, pd.Series, np.nan arithmetic
- **Reads**: prev_row dict — all signal state columns (including Trend as `prev_trend`, BTFD_Triggered, STFR_Triggered)
- **Writes**: none (returns pd.Series new_row)
- **Columns created**: all pivot/signal state columns (same set as _build_signals_from_df, single row)
- **Constants**: RV_EMA_ALPHA, RV_MULT, EMA_MULT
- **Key logic (BTFD/STFR)**: BTFD requires `trend == False AND prev_trend == False`. STFR requires `trend == True AND prev_trend == True`. Mirrors the `trends[i-1]` guard in _numba_passes_1_to_4 Pass 3.
- **Parallel impl**: YES — signals_engine.py does NOT have _build_signals_next_row; main.py calls rotations version indirectly. For live intraday, signals_engine._build_signals_from_df handles row-by-row via full recompute.

### build_signals_for_ticker
- **Defined**: rotations.py:2124-2133
- **Cell**: 5 — Signal Cache
- **Called by**:
  - rotations.py:2314 (_incremental_update_signals, _safe_rebuild inner fn)
  - rotations.py:2531 (load_or_build_signals, _safe_build inner fn)
- **Calls**: norgatedata.price_timeseries, _build_signals_from_df

### _build_signals_append_ticker
- **Defined**: rotations.py:2136-2215
- **Cell**: 5 — Signal Cache
- **Called by**:
  - rotations.py:2254 (_incremental_update_signals, _safe_append inner fn)
- **Calls**: norgatedata.price_timeseries, _build_signals_next_row

### _incremental_update_signals
- **Defined**: rotations.py:2218-2395
- **Cell**: 5 — Signal Cache
- **Called by**:
  - rotations.py:2484 (load_or_build_signals)
- **Calls**: _build_signals_append_ticker (ThreadPoolExecutor), build_signals_for_ticker (ThreadPoolExecutor), WriteThroughPath.sync
- **Reads**: signals_500.parquet (cached_df input)
- **Writes**: SIGNALS_CACHE_FILE (signals_500.parquet) — parquet
- **Columns read**: Source, Date, Ticker, Close + all signal columns
- **Constants**: QUARTER_UNIVERSE

### _safe_append (inner)
- **Defined**: rotations.py:2253-2254
- **Cell**: 5 — Signal Cache (inner function of _incremental_update_signals)
- **Called by**: ThreadPoolExecutor inside _incremental_update_signals
- **Calls**: _build_signals_append_ticker

### _safe_rebuild (inner)
- **Defined**: rotations.py:2312-2316
- **Cell**: 5 — Signal Cache (inner function of _incremental_update_signals)
- **Called by**: ThreadPoolExecutor inside _incremental_update_signals
- **Calls**: build_signals_for_ticker

### _get_latest_norgate_date
- **Defined**: rotations.py:2403-2415
- **Cell**: 5 — Signal Cache
- **Called by**:
  - rotations.py:2434 (_signals_cache_is_current)
  - rotations.py:2471 (load_or_build_signals)
  - rotations_old_outputs.py (module-level, Daily Signal Exports cell)
- **Calls**: norgatedata.price_timeseries
- **Constants**: MARKET_SYMBOL

### _signals_cache_is_current
- **Defined**: rotations.py:2418-2449
- **Cell**: 5 — Signal Cache
- **Called by**:
  - rotations.py:2455 (load_or_build_signals)
- **Calls**: _get_latest_norgate_date
- **Columns read**: Date, Source, {Sig}_Win_Rate, {Sig}_Avg_Winner_Bars, {Sig}_Avg_Loser_Bars

### load_or_build_signals
- **Defined**: rotations.py:2452-2613
- **Cell**: 5 — Signal Cache
- **Called by**:
  - rotations.py:2616 (module-level, assigns all_signals_df)
- **Calls**: _signals_cache_is_current, _get_latest_norgate_date, _incremental_update_signals, build_signals_for_ticker (ThreadPoolExecutor, _safe_build), pd.concat
- **Reads**: SIGNALS_CACHE_FILE (signals_500.parquet) — parquet
- **Writes**: SIGNALS_CACHE_FILE (signals_500.parquet) — parquet
- **Constants**: QUARTER_UNIVERSE, SIGNALS, INCREMENTAL_MAX_DAYS, EQUITY_SIGNAL_LOGIC_VERSION

### _safe_build (inner)
- **Defined**: rotations.py:2529-2533
- **Cell**: 5 — Signal Cache (inner function of load_or_build_signals)
- **Called by**: ThreadPoolExecutor inside load_or_build_signals
- **Calls**: build_signals_for_ticker

---

### _cache_slugify_label
- **Defined**: rotations.py:2626-2627
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:2748 (_prebuild_equity_cache_from_signals)
  - rotations.py:3819 (process_basket_signals)

### _cache_build_quarter_lookup
- **Defined**: rotations.py:2630-2639
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:2663 (_compute_equity_close_for_cache)
- **Calls**: _quarter_start_from_key

### _cache_find_active_quarter
- **Defined**: rotations.py:2642-2647
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:2670 (_compute_equity_close_for_cache)
- **Calls**: bisect.bisect_right

### _compute_equity_close_for_cache
- **Defined**: rotations.py:2650-2692
- **Cell**: 6 — Basket Processing
- **Called by**: none directly in scope (used in earlier version; superseded by compute_equity_ohlc_cached)
- **Calls**: _cache_build_quarter_lookup, _cache_find_active_quarter
- **Columns read**: Date, Ticker, Close, Volume

### _get_data_signature
- **Defined**: rotations.py:2698-2720
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:2724 (_prebuild_equity_cache_from_signals)
  - rotations.py:3501 (compute_equity_ohlc_cached)
  - rotations.py:3824 (process_basket_signals)
  - rotations_old_outputs.py (_corr_cache_signature)
- **Calls**: hashlib.sha256, pd.util.hash_pandas_object
- **Columns read**: Date, Ticker, Open, High, Low, Close

### _prebuild_equity_cache_from_signals
- **Defined**: rotations.py:2723-2769
- **Cell**: 6 — Basket Processing
- **Called by**: none in scope (utility — called externally or from notebook)
- **Calls**: _get_data_signature, _cache_slugify_label, _load_equity_cache, _build_universe_signature, _is_equity_cache_valid, compute_equity_ohlc_cached
- **Constants**: BETA_UNIVERSE, LOW_BETA_UNIVERSE, MOMENTUM_UNIVERSE, MOMENTUM_LOSERS_UNIVERSE, HIGH_YIELD_UNIVERSE, DIV_GROWTH_UNIVERSE, RISK_ADJ_MOM_UNIVERSE, SECTOR_UNIVERSES, INDUSTRY_UNIVERSES

### compute_breadth_pivots
- **Defined**: rotations.py:2771-2886
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3719 (_finalize_basket_signals_output, for breadth trend)
  - rotations.py:3729 (_finalize_basket_signals_output, for BO breadth trend)
- **Calls**: np.asarray, np.full
- **Columns created**: B_Trend, B_Resistance, B_Support, B_Up_Rot, B_Down_Rot, B_Rot_High, B_Rot_Low, B_Bull_Div, B_Bear_Div
- **Constants**: RV_EMA_ALPHA, RV_MULT

### compute_signal_trades
- **Defined**: rotations.py:2889-2950
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations_old_outputs.py (export_basket_excel_reports)
- **Calls**: (pure Python loop over df)
- **Columns read**: date_col, entry_col, exit_col, price_col, high_col, low_col

### _build_quarter_lookup
- **Defined**: rotations.py:2956-2965
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:2986 (compute_breadth_from_trend)
  - rotations.py:3025 (compute_breadth_from_breakout)
  - rotations.py:3081 (compute_equity_ohlc)
  - rotations.py:3642 (_compute_within_basket_correlation)
- **Calls**: _quarter_start_from_key
- **Parallel impl**: audit_basket.py:52 has identical function

### _find_active_quarter
- **Defined**: rotations.py:2968-2973
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:2989 (compute_breadth_from_trend)
  - rotations.py:3027 (compute_breadth_from_breakout)
  - rotations.py:3145 (compute_equity_ohlc)
  - rotations.py:3650 (_compute_within_basket_correlation)
- **Calls**: bisect.bisect_right
- **Parallel impl**: audit_basket.py:60 has identical function

### compute_breadth_from_trend
- **Defined**: rotations.py:2976-3012
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3680 (_augment_basket_signals_with_breadth)
- **Calls**: _build_quarter_lookup, _find_active_quarter
- **Columns read**: Date, Ticker, Trend

### compute_breadth_from_breakout
- **Defined**: rotations.py:3015-3050
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3685 (_augment_basket_signals_with_breadth)
- **Calls**: _build_quarter_lookup, _find_active_quarter
- **Columns read**: Date, Ticker, Is_Breakout_Sequence

### compute_equity_ohlc
- **Defined**: rotations.py:3053-3221
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3509 (compute_equity_ohlc_cached, full rebuild)
  - rotations.py:3522 (compute_equity_ohlc_cached, incremental append)
  - rotations.py:3548 (compute_equity_curve)
- **Calls**: _build_quarter_lookup, _find_active_quarter, _quarter_end_from_key
- **Columns read**: Date, Ticker, Open, High, Low, Close, Volume
- **Columns created**: Ret, Open_Ret, High_Ret, Low_Ret, Dollar_Vol (intermediate)
- **Returns**: DataFrame with Date, Open, High, Low, Close (equity curve OHLC)

### _build_universe_signature
- **Defined**: rotations.py:3224-3232
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:2751 (_prebuild_equity_cache_from_signals)
  - rotations.py:3502 (compute_equity_ohlc_cached)
  - rotations.py:3825 (process_basket_signals)
  - rotations_old_outputs.py (_corr_cache_signature)
- **Calls**: hashlib.sha256

### _equity_cache_paths
- **Defined**: rotations.py:3235-3244
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3248 (_load_equity_cache)
  - rotations.py:3275 (_save_equity_cache)
  - rotations.py:3768 (_finalize_basket_signals_output)
- **Calls**: _basket_cache_folder, _cache_file_stem

### _load_equity_cache
- **Defined**: rotations.py:3247-3271
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:2750 (_prebuild_equity_cache_from_signals)
  - rotations.py:3503 (compute_equity_ohlc_cached)
- **Calls**: _equity_cache_paths
- **Reads**: {slug}_ohlc.parquet, {slug}_ohlc_meta.json — parquet + JSON

### _save_equity_cache
- **Defined**: rotations.py:3274-3284
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3513 (compute_equity_ohlc_cached, full rebuild)
  - rotations.py:3531 (compute_equity_ohlc_cached, no new rows)
  - rotations.py:3542 (compute_equity_ohlc_cached, append)
- **Calls**: _equity_cache_paths, pa.Table.from_pandas, pq.write_table
- **Writes**: {slug}_ohlc.parquet, {slug}_ohlc_meta.json — parquet + JSON
- **Constants**: CHART_SCHEMA_VERSION

### _build_equity_meta
- **Defined**: rotations.py:3287-3308
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3512 (compute_equity_ohlc_cached)
  - rotations.py:3530 (compute_equity_ohlc_cached)
  - rotations.py:3541 (compute_equity_ohlc_cached)
- **Constants**: EQUITY_CACHE_SCHEMA_VERSION, EQUITY_SIGNAL_LOGIC_VERSION, EQUITY_UNIVERSE_LOGIC_VERSION

### _is_equity_cache_valid
- **Defined**: rotations.py:3311-3330
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:2752 (_prebuild_equity_cache_from_signals)
  - rotations.py:3508 (compute_equity_ohlc_cached)
- **Constants**: EQUITY_CACHE_SCHEMA_VERSION, EQUITY_SIGNAL_LOGIC_VERSION, EQUITY_UNIVERSE_LOGIC_VERSION

### _basket_cache_folder
- **Defined**: rotations.py:3333-3340
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3236 (_equity_cache_paths)
  - rotations.py:3358 (_basket_cache_paths)
  - rotations.py:3756 (_finalize_basket_signals_output)

### _cache_file_stem
- **Defined**: rotations.py:3343-3353
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3238 (_equity_cache_paths)
  - rotations.py:3360 (_basket_cache_paths)
  - rotations.py:3756 (_finalize_basket_signals_output)
- **Calls**: _get_current_quarter_key
- **Constants**: SIZE

### _basket_cache_paths
- **Defined**: rotations.py:3356-3367
- **Cell**: 6 — Basket Processing
- **Called by**: (legacy, superseded by _find_basket_parquet / _find_basket_meta pattern)
- **Calls**: _basket_cache_folder, _cache_file_stem

### _find_basket_parquet
- **Defined**: rotations.py:3370-3388
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3828 (process_basket_signals)
  - rotations.py:4701 (_get_basket_ohlc_for_reports)
  - rotations.py:5020 (_build_group_daily_return_grid)
  - rotations_old_outputs.py (_find_basket_chart_path)
- **Constants**: SIZE
- **Parallel impl**: YES — main.py:91 has _find_basket_parquet (searches BASKET_CACHE_FOLDERS)

### _find_basket_meta
- **Defined**: rotations.py:3391-3409
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3829 (process_basket_signals)
- **Constants**: SIZE
- **Parallel impl**: YES — main.py:103 has _find_basket_meta (searches BASKET_CACHE_FOLDERS)

### _basket_signals_cache_paths
- **Defined**: rotations.py:3412-3417
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3424 (_get_chart_schema_version_from_parquet)
  - rotations.py:3435 (_load_basket_signals_cache)
  - rotations.py:3448 (_save_basket_signals_cache)

### _get_chart_schema_version_from_parquet
- **Defined**: rotations.py:3422-3431
- **Cell**: 6 — Basket Processing
- **Called by**: (utility, not called in scope — used from chart generation checks)
- **Calls**: _basket_signals_cache_paths, pq.read_metadata
- **Constants**: CHART_SCHEMA_VERSION

### _load_basket_signals_cache
- **Defined**: rotations.py:3434-3444
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3842 (process_basket_signals, fallback)
- **Calls**: _basket_signals_cache_paths
- **Reads**: {slug}_basket_signals.parquet, {slug}_basket_signals_meta.json — parquet + JSON (legacy)

### _save_basket_signals_cache
- **Defined**: rotations.py:3447-3457
- **Cell**: 6 — Basket Processing
- **Called by**: (superseded by _finalize_basket_signals_output)
- **Calls**: _basket_signals_cache_paths, pa.Table.from_pandas, pq.write_table
- **Writes**: {slug}_basket_signals.parquet, {slug}_basket_signals_meta.json — parquet + JSON
- **Constants**: CHART_SCHEMA_VERSION

### _build_basket_signals_meta
- **Defined**: rotations.py:3460-3477
- **Cell**: 6 — Basket Processing
- **Called by**: (superseded by combined_meta in _finalize_basket_signals_output)
- **Constants**: BASKET_SIGNALS_CACHE_SCHEMA_VERSION, EQUITY_SIGNAL_LOGIC_VERSION

### _is_basket_signals_cache_valid
- **Defined**: rotations.py:3480-3497
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3851 (process_basket_signals)
- **Constants**: BASKET_SIGNALS_CACHE_SCHEMA_VERSION, EQUITY_SIGNAL_LOGIC_VERSION

### compute_equity_ohlc_cached
- **Defined**: rotations.py:3500-3543
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:2759 (_prebuild_equity_cache_from_signals)
  - rotations.py:3859 (process_basket_signals)
- **Calls**: _get_data_signature, _build_universe_signature, _load_equity_cache, _is_equity_cache_valid, compute_equity_ohlc, _build_equity_meta, _save_equity_cache
- **Constants**: FORCE_REBUILD_EQUITY_CACHE

### compute_equity_curve
- **Defined**: rotations.py:3546-3553
- **Cell**: 6 — Basket Processing
- **Called by**: (compatibility helper for older notebook cells)
- **Calls**: compute_equity_ohlc
- **Columns created**: Equity (renamed from Close)

### _fmt_price
- **Defined**: rotations.py:3511-3512
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:4603 (export_today_signals)
  - rotations_old_outputs.py (Daily Signal Exports cell)
  - rotations_old_outputs.py (export_basket_excel_reports)

### _fmt_bars
- **Defined**: rotations.py:3515-3516
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:4606 (export_today_signals)
  - rotations_old_outputs.py (Daily Signal Exports cell)
  - rotations_old_outputs.py (export_basket_excel_reports)

### _fmt_pct
- **Defined**: rotations.py:3519-3520
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations_old_outputs.py (export_basket_excel_reports)

### _append_trade_rows
- **Defined**: rotations.py:3523-3563
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations_old_outputs.py (export_basket_excel_reports)
- **Calls**: calc_rolling_stats, pd.to_datetime

### _compute_within_basket_correlation
- **Defined**: rotations.py:3611-3676
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3740 (_finalize_basket_signals_output)
- **Calls**: _get_current_quarter_key, _build_quarter_lookup, _find_active_quarter
- **Columns read**: Date, Ticker, Close (from all_signals_df global)

### _augment_basket_signals_with_breadth
- **Defined**: rotations.py:3679-3715
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3891 (process_basket_signals, incremental append)
  - rotations.py:3907 (process_basket_signals, full rebuild)
- **Calls**: compute_breadth_from_trend, compute_breadth_from_breakout, pd.merge
- **Columns created**: Breadth_EMA, Uptrend_Pct, Downtrend_Pct, BO_Breadth_EMA, Breakout_Pct, Breakdown_Pct

### _finalize_basket_signals_output
- **Defined**: rotations.py:3718-3811
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3898 (process_basket_signals, incremental append path)
  - rotations.py:3908 (process_basket_signals, full rebuild path)
- **Calls**: compute_breadth_pivots, _compute_within_basket_correlation, _cache_file_stem, _basket_cache_folder, _equity_cache_paths, WriteThroughPath.sync, pa.Table.from_pandas, pq.write_table
- **Writes**: {slug}_signals.parquet, {slug}_signals_meta.json — parquet + JSON
- **Constants**: BASKET_SIGNALS_CACHE_SCHEMA_VERSION, EQUITY_SIGNAL_LOGIC_VERSION, EQUITY_UNIVERSE_LOGIC_VERSION, CHART_SCHEMA_VERSION

### process_basket_signals
- **Defined**: rotations.py:3814-3910
- **Cell**: 6 — Basket Processing
- **Called by**:
  - rotations.py:3931 (module-level basket processing loop)
- **Calls**: _cache_slugify_label, _get_data_signature, _build_universe_signature, _find_basket_parquet, _find_basket_meta, _load_basket_signals_cache, _is_basket_signals_cache_valid, compute_equity_ohlc_cached, _build_signals_next_row, _augment_basket_signals_with_breadth, _finalize_basket_signals_output, _build_signals_from_df
- **Constants**: FORCE_REBUILD_BASKET_SIGNALS

---

### _load_env_file
- **Defined**: rotations.py:4034-4064
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (module-level, start of cell 7)
- **Calls**: load_dotenv
- **Parallel impl**: YES — signals_engine.py:42 has _load_env_file (same logic)

### get_realtime_prices
- **Defined**: rotations.py:4066-4125
- **Cell**: 7 — Live Intraday Data
- **Called by**: none in scope (superseded by get_live_ohlc_bars)
- **Calls**: db.Live, db.SymbolMappingMsg
- **Constants**: DATABENTO_API_KEY, DATABENTO_DATASET, DATABENTO_SCHEMA, DATABENTO_STYPE_IN, DATABENTO_TIMEOUT_S

### get_realtime_ohlcv
- **Defined**: rotations.py:4128-4198
- **Cell**: 7 — Live Intraday Data
- **Called by**: none in scope (superseded by get_live_ohlc_bars)
- **Calls**: db.Live
- **Constants**: DATABENTO_API_KEY, DATABENTO_DATASET, DATABENTO_STYPE_IN, DATABENTO_TIMEOUT_S

### get_live_ohlc_bars
- **Defined**: rotations.py:4201-4248
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (export_today_signals)
  - rotations.py (_get_live_update_context)
- **Calls**: db.Historical, pd.Timestamp, pd.Timedelta, data.to_df, df.groupby
- **Returns**: Dict[symbol -> {Open, High, Low, Close}]
- **Constants**: DATABENTO_API_KEY, DATABENTO_DATASET

### _get_latest_norgate_date_fallback
- **Defined**: rotations.py:4251-4260
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py:4323 (_get_live_update_gate)
  - rotations_old_outputs.py:447 (_corr_asof_date)
- **Calls**: _get_latest_norgate_date

### _extract_spy_trade_date_from_df
- **Defined**: rotations.py:4263-4280
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py:4303 (_get_spy_last_trade_date_databento)

### _get_spy_last_trade_date_databento
- **Defined**: rotations.py:4283-4309
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py:4332 (_get_live_update_gate)
- **Calls**: db.Historical, _extract_spy_trade_date_from_df
- **Constants**: DATABENTO_API_KEY, DATABENTO_DATASET

### _get_live_update_gate
- **Defined**: rotations.py:4312-4350
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (_is_market_open_via_spy_volume)
  - rotations.py (export_today_signals)
  - rotations.py (append_live_today_to_signals_parquet)
  - rotations.py (_get_live_update_context)
- **Calls**: _get_latest_norgate_date_fallback, _get_spy_last_trade_date_databento
- **Constants**: _LIVE_GATE_CACHE (global cache)

### _is_market_open_via_spy_volume
- **Defined**: rotations.py:4353-4356
- **Cell**: 7 — Live Intraday Data
- **Called by**: (compatibility wrapper, not called in scope)
- **Calls**: _get_live_update_gate

### _append_live_row
- **Defined**: rotations.py:4359-4377
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (build_signals_for_ticker_live)
- **Columns created**: Open, High, Low, Close, Volume (live row appended to DatetimeIndex df)

### build_signals_for_ticker_live
- **Defined**: rotations.py:4380-4393
- **Cell**: 7 — Live Intraday Data
- **Called by**: none in scope (external API)
- **Calls**: _append_live_row, _build_signals_from_df

### _sort_signals_df
- **Defined**: rotations.py:4396-4413
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (export_today_signals)
  - rotations_old_outputs.py (Daily Signal Exports cell)
- **Calls**: df.sort_values
- **Columns read**: Signal_Type, Industry, Historical_EV

### _parse_ev (inner)
- **Defined**: rotations.py:4400-4404
- **Cell**: 7 — Live Intraday Data (inner of _sort_signals_df)

### export_today_signals
- **Defined**: rotations.py:4416-4627
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (module-level call at bottom of cell 7 — implicit)
- **Calls**: _get_live_update_gate, get_live_ohlc_bars, _get_latest_norgate_rows_by_ticker, _build_signals_next_row, _get_ticker_theme, _fmt_price, _fmt_bars, _sort_signals_df, WriteThroughPath.sync
- **Reads**: all_signals_df (global)
- **Writes**: LIVE_ROTATIONS_FOLDER/{date}_{time}_Live_Signals_for_top_500.xlsx — xlsx
- **Writes**: paths.data/live_signals_500.parquet — parquet
- **Columns created (xlsx output)**: Date, Ticker, Close, Signal_Type, Theme, Sector, Industry, Entry_Price, Win_Rate, Avg_Winner, Avg_Loser, Avg_Winner_Bars, Avg_Loser_Bars, Avg_MFE, Avg_MAE, Std_Dev, Historical_EV, EV_Last_3, Risk_Adj_EV, Risk_Adj_EV_Last_3, Count
- **Constants**: QUARTER_UNIVERSE, TICKER_SECTOR, TICKER_SUBINDUSTRY, _LIVE_UPDATE_CONTEXT_CACHE

### append_live_today_to_signals_parquet
- **Defined**: rotations.py:4630-4691
- **Cell**: 7 — Live Intraday Data
- **Called by**: (external — called from update scripts)
- **Calls**: _get_live_update_gate, _get_live_update_context, _build_signals_next_row
- **Reads**: SIGNALS_CACHE_FILE (signals_500.parquet) — parquet
- **Writes**: SIGNALS_CACHE_FILE (signals_500.parquet) — parquet (appends live rows with Source='live')

### _get_basket_ohlc_for_reports
- **Defined**: rotations.py:4694-4728
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (_compute_annual_returns_for_basket)
  - rotations.py (_compute_daily_returns_for_basket)
- **Calls**: _slugify_label (if present), _find_basket_parquet
- **Reads**: basket parquet files — parquet

### _compute_annual_returns_for_basket
- **Defined**: rotations.py:4731-4760
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (_build_group_annual_return_grid)
- **Calls**: _get_basket_ohlc_for_reports, _compute_live_basket_return
- **Columns read**: Date, Close

### _build_group_annual_return_grid
- **Defined**: rotations.py:4763-4783
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (_build_basket_annual_grid)
  - rotations.py (export_annual_returns)
- **Calls**: _compute_annual_returns_for_basket

### _compute_daily_returns_for_basket
- **Defined**: rotations.py:4786-4798
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (_build_group_daily_return_grid)
- **Calls**: _get_basket_ohlc_for_reports
- **Columns read**: Date, Close

### _get_latest_norgate_rows_by_ticker
- **Defined**: rotations.py:4801-4811
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (export_today_signals)
  - rotations.py (_get_live_update_context)
- **Reads**: all_signals_df (global)
- **Columns read**: Date, Ticker (indexed by Ticker)

### _compute_live_basket_return
- **Defined**: rotations.py:4814-4847
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (_compute_annual_returns_for_basket)
  - rotations.py (_build_group_daily_return_grid)
- **Columns read**: Close, Volume (from last_rows)

### _compute_live_basket_ohlc
- **Defined**: rotations.py:4850-4883
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (_build_group_daily_return_grid)
  - rotations.py (update_basket_parquets_with_live_ohlcv, indirectly)
- **Calls**: np.asarray, np.dot (wavg inner)
- **Columns read**: Close, Volume (from last_rows)

### wavg (inner)
- **Defined**: rotations.py:4876
- **Cell**: 7 — Live Intraday Data (inner of _compute_live_basket_ohlc)

### _compute_live_basket_ohlcv
- **Defined**: rotations.py:4886-4938
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (update_basket_parquets_with_live_ohlcv)
- **Columns read**: Close, Volume (from last_rows)

### _get_live_update_context
- **Defined**: rotations.py:4941-4987
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (append_live_today_to_signals_parquet)
  - rotations.py (_build_group_daily_return_grid)
  - rotations.py (_build_basket_daily_grid_last20)
- **Calls**: _get_live_update_gate, get_live_ohlc_bars, _get_latest_norgate_rows_by_ticker
- **Constants**: QUARTER_UNIVERSE, _LIVE_UPDATE_CONTEXT_CACHE

### _build_group_daily_return_grid
- **Defined**: rotations.py:4990-5050
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (_build_basket_daily_grid_last20)
  - rotations.py (export_last_20_days_returns)
- **Calls**: _get_live_update_context, _compute_daily_returns_for_basket, _compute_live_basket_return, _compute_live_basket_ohlc, _find_basket_parquet

### _render_return_table_pages
- **Defined**: rotations.py:5053-5183
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (export_annual_returns)
  - rotations.py (export_last_20_days_returns)

### _render_return_bar_charts
- **Defined**: rotations.py:5186-5297
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (export_annual_returns)
  - rotations.py (export_last_20_days_returns)

### _col_label (inner)
- **Defined**: rotations.py:5210
- **Cell**: 7 (inner of _render_return_bar_charts)

### _short_name (inner, in _render_return_bar_charts)
- **Defined**: rotations.py:5221
- **Cell**: 7 (inner of _render_return_bar_charts)

### _get_all_basket_specs_for_reports
- **Defined**: rotations.py:5299-5311
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (_build_basket_annual_grid)
  - rotations.py (_build_basket_daily_grid_last20)
- **Constants**: BETA_UNIVERSE, LOW_BETA_UNIVERSE, MOMENTUM_UNIVERSE, etc., SECTOR_UNIVERSES, INDUSTRY_UNIVERSES

### _build_basket_annual_grid
- **Defined**: rotations.py:5314-5327
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (export_annual_returns)
  - rotations.py (export_annual_returns_by_year)
- **Calls**: _get_all_basket_specs_for_reports, _build_group_annual_return_grid

### _build_basket_daily_grid_last20
- **Defined**: rotations.py:5330-5358
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (export_last_20_days_returns)
  - rotations.py (export_last_20_days_returns_by_day)
- **Calls**: _get_all_basket_specs_for_reports, _get_live_update_context, _build_group_daily_return_grid

### export_annual_returns
- **Defined**: rotations.py:5360-5400
- **Cell**: 7 — Live Intraday Data
- **Called by**: (module-level or manual call)
- **Calls**: _build_basket_annual_grid, _render_return_table_pages, _render_return_bar_charts, build_pdf
- **Writes**: SUMMARY_FOLDER/{date}_Annual_Returns*.pdf — PDF

### export_last_20_days_returns
- **Defined**: rotations.py:5402-5426
- **Cell**: 7 — Live Intraday Data
- **Calls**: _build_basket_daily_grid_last20, _render_return_table_pages, _render_return_bar_charts, build_pdf
- **Writes**: SUMMARY_FOLDER/{date}_Last_20_Days*.pdf — PDF

### _render_year_basket_bar_charts
- **Defined**: rotations.py:5428-5492
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (export_annual_returns_by_year)

### _short_name (inner, in _render_year_basket_bar_charts)
- **Defined**: rotations.py:5436
- **Cell**: 7 (inner of _render_year_basket_bar_charts)

### _render_day_basket_bar_charts
- **Defined**: rotations.py:5495-5559
- **Cell**: 7 — Live Intraday Data
- **Called by**:
  - rotations.py (export_last_20_days_returns_by_day)

### _short_name (inner, in _render_day_basket_bar_charts)
- **Defined**: rotations.py:5503
- **Cell**: 7 (inner of _render_day_basket_bar_charts)

### export_annual_returns_by_year
- **Defined**: rotations.py:5562-5600
- **Cell**: 7 — Live Intraday Data
- **Calls**: _build_basket_annual_grid, _render_year_basket_bar_charts, build_pdf
- **Writes**: SUMMARY_FOLDER/{date}_Annual_Returns_by_Year*.pdf — PDF

### export_last_20_days_returns_by_day
- **Defined**: rotations.py:5602-5623
- **Cell**: 7 — Live Intraday Data
- **Calls**: _build_basket_daily_grid_last20, _render_day_basket_bar_charts, build_pdf
- **Writes**: SUMMARY_FOLDER/{date}_Last_20_Days_by_Day*.pdf — PDF

### update_basket_parquets_with_live_ohlcv
- **Defined**: rotations.py:5625-5725
- **Cell**: 7 — Live Intraday Data
- **Called by**: (external, called after market close)
- **Calls**: _get_live_update_context, _compute_live_basket_ohlcv, _build_signals_next_row, _augment_basket_signals_with_breadth, _finalize_basket_signals_output
- **Reads**: basket parquets (via _find_basket_parquet)
- **Writes**: basket parquets (via _finalize_basket_signals_output)
- **Constants**: BASKET_RESULTS

---

### export_group_holdings
- **Defined**: rotations.py:5734-5788
- **Cell**: 8 — Holdings Exports (TradingView lists)
- **Called by**: (module-level call at end of cell 8)
- **Calls**: _get_current_quarter_key, WriteThroughPath.write_text
- **Writes**: HOLDINGS_FOLDER/Theme of Top {SIZE} {qtr}.txt, Sector of Top {SIZE} {qtr}.txt, Industry of Top {SIZE} {qtr}.txt — text

### export_current_quarter_universe
- **Defined**: rotations.py:5791-5804
- **Cell**: 8 — Holdings Exports (TradingView lists)
- **Called by**: (module-level call at end of cell 8)
- **Calls**: _get_current_quarter_key, WriteThroughPath.write_text
- **Writes**: HOLDINGS_FOLDER/Universe Top {SIZE} {qtr}.txt — text

---

## rotations_old_outputs.py

Extracted from rotations.py (Cells 9-15). Group B — Report Only outputs.
Imports all public names from rotations.py via `from rotations import *` plus explicit private helper imports.

### _get_ticker_theme
- **Defined**: rotations_old_outputs.py:122-124
- **Cell**: 9 — Signal Universe Filtering
- **Called by**:
  - rotations.py (export_today_signals, via globals check)
  - rotations_old_outputs.py (Daily Signal Exports cell, module-level)
- **Constants**: _thematic_universes (cell-level list)

### _basket_tsi
- **Defined**: rotations_old_outputs.py:190-419 (approximate — large function spanning most of cell 10)
- **Cell**: 10 — Daily Signal Exports (Excel)
- **Called by**: (module-level, exports basket TSI signals)
- **Writes**: PREVIOUS_DAY_ROTATIONS_FOLDER/{date}_Signals_for_top_{SIZE}.xlsx — xlsx

---

### _corr_asof_date
- **Defined**: rotations_old_outputs.py:446-452
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py:808 (build_correlation_reports)

### _corr_cache_signature
- **Defined**: rotations_old_outputs.py:455-472
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py:808 (build_correlation_reports)
- **Calls**: _build_universe_signature, hashlib.sha256
- **Constants**: BASKET_RESULTS, CORR_WINDOWS

### _load_corr_cache
- **Defined**: rotations_old_outputs.py:474-490
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py:808 (build_correlation_reports)
- **Reads**: CORR_CACHE_BASKET_OSC_FILE (basket_correlations_of_500.parquet), CORR_CACHE_SIG_FILE (correlation_meta_500.json) — parquet + JSON

### _save_corr_cache
- **Defined**: rotations_old_outputs.py:492-510
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py (build_correlation_reports)
- **Calls**: WriteThroughPath.write_text, WriteThroughPath.sync
- **Writes**: CORR_CACHE_SIG_FILE (correlation_meta_500.json) — JSON; CORR_CACHE_BASKET_OSC_FILE (basket_correlations_of_500.parquet) — parquet

### _quarter_key_from_date
- **Defined**: rotations_old_outputs.py:513-516
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py:604 (_build_stock_returns_matrix)

### _fallback_latest_quarter_key
- **Defined**: rotations_old_outputs.py:519-527
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py:604 (_build_stock_returns_matrix)

### _window_corr_matrix
- **Defined**: rotations_old_outputs.py:530-538
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py (build_correlation_reports, _pair_val inner)

### _corr_pairs
- **Defined**: rotations_old_outputs.py:541-559
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py (build_correlation_reports)

### _render_corr_heatmap
- **Defined**: rotations_old_outputs.py:561-575
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py (build_correlation_reports)
  - rotations_old_outputs.py (generate_basket_report_pdfs)

### _render_pairs_table
- **Defined**: rotations_old_outputs.py:577-602
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py (build_correlation_reports)

### _build_stock_returns_matrix
- **Defined**: rotations_old_outputs.py:604-614
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py:808 (build_correlation_reports)
- **Calls**: _quarter_key_from_date, _fallback_latest_quarter_key
- **Reads**: all_signals_df (global)
- **Columns read**: Date, Ticker, Close

### _build_basket_returns_matrix
- **Defined**: rotations_old_outputs.py:617-634
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py:808 (build_correlation_reports)
- **Reads**: basket parquets (via _get_basket_ohlc_for_reports or BASKET_RESULTS)

### _mean_offdiag
- **Defined**: rotations_old_outputs.py:636-646
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py (build_correlation_reports, _pair_val inner)
  - rotations_old_outputs.py (build_correlation_reports, basket osc series)

### _rolling_avg_pairwise_corr_series
- **Defined**: rotations_old_outputs.py:648-668
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py (build_correlation_reports, for basket osc series)

### _series_last_date
- **Defined**: rotations_old_outputs.py:670-677
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py:679 (_update_rolling_osc_incremental)

### _update_rolling_osc_incremental
- **Defined**: rotations_old_outputs.py:679-712
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py (build_correlation_reports)

### _update_within_osc_map_incremental
- **Defined**: rotations_old_outputs.py:715-741
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py (build_correlation_reports)

### _plot_corr_oscillator
- **Defined**: rotations_old_outputs.py:743-777
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py (build_correlation_reports)

### _plot_single_corr_oscillator
- **Defined**: rotations_old_outputs.py:779-806
- **Cell**: 11 — Correlation Cache
- **Called by**:
  - rotations_old_outputs.py (build_correlation_reports)

### build_correlation_reports
- **Defined**: rotations_old_outputs.py:808-1057 (approximate)
- **Cell**: 11 — Correlation Cache
- **Called by**: (module-level call at end of cell 11)
- **Calls**: _corr_asof_date, _corr_cache_signature, _load_corr_cache, _build_stock_returns_matrix, _build_basket_returns_matrix, _rolling_avg_pairwise_corr_series, _update_rolling_osc_incremental, _update_within_osc_map_incremental, _save_corr_cache, _corr_pairs, _window_corr_matrix, _pair_val, _render_corr_heatmap, _render_pairs_table, _plot_corr_oscillator, _plot_single_corr_oscillator, build_pdf
- **Reads**: basket parquets, correlation cache parquets
- **Writes**: CORR_FOLDER/{date}*_correlations.pdf — PDF; correlation cache files

### _pair_val (inner)
- **Defined**: rotations_old_outputs.py (inner of build_correlation_reports)
- **Cell**: 11 (inner of build_correlation_reports)

---

### export_basket_excel_reports
- **Defined**: rotations_old_outputs.py:1069-1164
- **Cell**: 12 — Per-Basket Excel Reports
- **Called by**:
  - rotations_old_outputs.py:1166 (module-level)
- **Calls**: compute_signal_trades, _append_trade_rows, _fmt_price, _fmt_pct, _fmt_bars, WriteThroughPath.sync
- **Reads**: BASKET_RESULTS global
- **Writes**: {hist_folder}/{slug}_Breadth_Signals.xlsx, {hist_folder}/{slug}_Equity_Signals.xlsx — xlsx

---

### plot_one_year_breadth_and_equity
- **Defined**: rotations_old_outputs.py:1176-1240 (approximate)
- **Cell**: 13 — PNG Chart Generation
- **Called by**: (compatibility helper, not called in current scope)
- **Calls**: plt.subplots, pd.merge

### format_date (inner)
- **Defined**: rotations_old_outputs.py:1226
- **Cell**: 13 (inner of plot_one_year_breadth_and_equity)

### _slugify_label
- **Defined**: rotations_old_outputs.py:1243
- **Cell**: 13 — PNG Chart Generation (note: distinct from _cache_slugify_label in cell 6)
- **Called by**:
  - rotations.py (_get_basket_ohlc_for_reports, via globals check)
  - rotations_old_outputs.py (_make_fmt)

### _make_fmt
- **Defined**: rotations_old_outputs.py:1247-1254
- **Cell**: 13 — PNG Chart Generation
- **Called by**:
  - rotations_old_outputs.py (plot_basket_charts)

### fmt (inner)
- **Defined**: rotations_old_outputs.py:1248
- **Cell**: 13 (inner of _make_fmt)

### plot_basket_charts
- **Defined**: rotations_old_outputs.py:1257-1586 (approximate)
- **Cell**: 13 — PNG Chart Generation
- **Called by**:
  - rotations_old_outputs.py (module-level, basket chart generation loop)
- **Calls**: _make_fmt, compute_breadth_pivots, plt.subplots, WriteThroughPath.sync
- **Reads**: merged_all DataFrame from BASKET_RESULTS
- **Writes**: {charts_folder}/{date_str}/{date_str}_Top_{n}_{slug}_of_{SIZE}.png — PNG

---

### _find_latest_file
- **Defined**: rotations_old_outputs.py:1607-1609
- **Cell**: 14 — Comprehensive Summary Report (PDF)
- **Called by**:
  - rotations_old_outputs.py (generate_summary_pdf)

### _date_label_from_file
- **Defined**: rotations_old_outputs.py:1612-1630
- **Cell**: 14 — Comprehensive Summary Report (PDF)
- **Called by**:
  - rotations_old_outputs.py (generate_summary_pdf)

### _find_basket_chart_path
- **Defined**: rotations_old_outputs.py:1632-1643
- **Cell**: 14 — Comprehensive Summary Report (PDF)
- **Called by**:
  - rotations_old_outputs.py (generate_summary_pdf)
  - rotations_old_outputs.py (generate_basket_report_pdfs)
- **Calls**: BASKET_RESULTS lookup

### _embed_image_page
- **Defined**: rotations_old_outputs.py:1645-1691
- **Cell**: 14 — Comprehensive Summary Report (PDF)
- **Called by**:
  - rotations_old_outputs.py (generate_summary_pdf)
  - rotations_old_outputs.py (generate_basket_report_pdfs)
- **Calls**: mpimg.imread, plt.subplots

### _render_df_table_pages
- **Defined**: rotations_old_outputs.py:1693-1785
- **Cell**: 14 — Comprehensive Summary Report (PDF)
- **Called by**:
  - rotations_old_outputs.py (generate_summary_pdf)
  - rotations_old_outputs.py (generate_basket_report_pdfs)

### _render_ytd_rebase_page
- **Defined**: rotations_old_outputs.py:1787-1835
- **Cell**: 14 — Comprehensive Summary Report (PDF)
- **Called by**:
  - rotations_old_outputs.py (generate_summary_pdf)

### generate_summary_pdf
- **Defined**: rotations_old_outputs.py:1837-1991 (approximate)
- **Cell**: 14 — Comprehensive Summary Report (PDF)
- **Called by**: (module-level call at end of cell 14)
- **Calls**: _find_latest_file, _date_label_from_file, _find_basket_chart_path, _embed_image_page, _render_df_table_pages, _render_ytd_rebase_page, _render_return_table_pages, build_pdf, WriteThroughPath.sync
- **Reads**: signals xlsx files, basket PNGs
- **Writes**: SUMMARY_FOLDER/{date}_Summary.pdf — PDF
- **Constants**: BASKET_RESULTS

---

### generate_basket_report_pdfs
- **Defined**: rotations_old_outputs.py:2003-2176
- **Cell**: 15 — Per-Basket Report (PDF)
- **Called by**: (module-level call at end of cell 15)
- **Calls**: _find_basket_chart_path, _embed_image_page, _render_df_table_pages, _render_corr_heatmap, _needs_write_and_mirror, WriteThroughPath.sync
- **Reads**: BASKET_RESULTS, OPEN_SIGNALS_DF, WITHIN_OSC_MAP, SNAPSHOT_CORR (globals)
- **Writes**: Baskets/Basket_Summary/{date}/{date}_{slug}_Report.pdf — PDF

---

## signals_engine.py

### _load_env_file
- **Defined**: signals_engine.py:42-51
- **Called by**:
  - signals_engine.py: (module-level call — implicit)
- **Calls**: load_dotenv
- **Parallel impl**: YES — rotations.py:3956 has identical function

### _refresh_runtime_config
- **Defined**: signals_engine.py:54-69
- **Called by**:
  - signals_engine.py:661 (main)
- **Calls**: os.getenv (re-reads all config globals)

### _universe_signature
- **Defined**: signals_engine.py:72-77
- **Called by**:
  - signals_engine.py:177 (load_or_build_intraday_30m)
- **Calls**: hashlib.sha256

### _quarter_key_for_date
- **Defined**: signals_engine.py:80-81
- **Called by**:
  - signals_engine.py:97 (load_universe_tickers)

### _latest_quarter_key
- **Defined**: signals_engine.py:84-85
- **Called by**:
  - signals_engine.py:98 (load_universe_tickers)

### load_universe_tickers
- **Defined**: signals_engine.py:88-102
- **Called by**:
  - signals_engine.py:661 (main)
- **Calls**: pickle.load, _quarter_key_for_date, _latest_quarter_key
- **Reads**: UNIVERSE_PICKLE_PATH (top500stocks.pkl) — pickle

### _fetch_1m_chunk
- **Defined**: signals_engine.py:105-143
- **Called by**:
  - signals_engine.py:206 (load_or_build_intraday_30m)
- **Calls**: db.Historical.timeseries.get_range
- **Constants**: DATABENTO_API_KEY, DATABENTO_DATASET, DATABENTO_STYPE_IN

### _resample_30m
- **Defined**: signals_engine.py:146-173
- **Called by**:
  - signals_engine.py:214 (load_or_build_intraday_30m)
- **Calls**: df.resample
- **Constants**: RTH_ONLY

### load_or_build_intraday_30m
- **Defined**: signals_engine.py:176-231
- **Called by**:
  - signals_engine.py:661 (main)
- **Calls**: _universe_signature, _fetch_1m_chunk, _resample_30m, pickle.load, pickle.dump
- **Reads**: INTRADAY_CACHE_FILE (intraday_30m_cache_top_500.pkl) — pickle
- **Writes**: INTRADAY_CACHE_FILE (intraday_30m_cache_top_500.pkl) — pickle
- **Constants**: FORCE_REBUILD_INTRADAY_CACHE, DATABENTO_LOOKBACK_DAYS, DATABENTO_SYMBOL_CHUNK

### RollingStatsAccumulator.__init__
- **Defined**: signals_engine.py:237-247
- **Called by**: instantiated in _build_signals_from_df
- **Parallel impl**: YES — rotations.py:1270 (extended: has bars tracking, deque)

### RollingStatsAccumulator.add
- **Defined**: signals_engine.py:249-263
- **Parallel impl**: YES — rotations.py:1284 (extended: has bars parameter)

### RollingStatsAccumulator.get_stats
- **Defined**: signals_engine.py:265-295
- **Parallel impl**: YES — rotations.py:1302 (extended: has avg_winner_bars, avg_loser_bars)

### _build_signals_from_df
- **Defined**: signals_engine.py:298-555
- **Called by**:
  - signals_engine.py:556 (build_all_signals_30m, per ticker)
  - main.py:476 (get_basket_data, for basket live signals)
  - main.py:~530 (get_ticker_data, for live ticker signals)
- **Calls**: RollingStatsAccumulator (inline passes 1-5)
- **Columns created**: RV, RV_EMA, Trend, Resistance_Pivot, Support_Pivot, Is_Up_Rotation, Is_Down_Rotation, Rotation_Open, Up_Range, Down_Range, Up_Range_EMA, Down_Range_EMA, Upper_Target, Lower_Target, Is_Breakout, Is_Breakdown, Is_BTFD, Is_STFR, BTFD_Target_Entry, STFR_Target_Entry, Is_Breakout_Sequence, Rotation_ID, BTFD_Triggered, STFR_Triggered, + signal stat columns per signal type, Ticker
- **Constants**: RV_MULT, EMA_MULT, RV_EMA_ALPHA, SIGNALS
- **Key logic (BTFD/STFR)**: BTFD requires `trend == False AND prev_trend == False`. STFR requires `trend == True AND prev_trend == True`. Mirrors the `trends[i-1]` guard in rotations.py _numba_passes_1_to_4 Pass 3.
- **Parallel impl**: YES — rotations.py:1794 (uses numba JIT for passes 1-4+5, has bars tracking in accumulator)

### build_all_signals_30m
- **Defined**: signals_engine.py:556-571
- **Called by**:
  - signals_engine.py:573 (load_or_build_signals_30m)
- **Calls**: _build_signals_from_df (per ticker)

### load_or_build_signals_30m
- **Defined**: signals_engine.py:573-600
- **Called by**:
  - signals_engine.py:661 (main)
- **Calls**: build_all_signals_30m, pickle.load, pickle.dump
- **Reads**: INTRADAY_SIGNALS_CACHE_FILE (intraday_30m_signals_top_500.pkl) — pickle
- **Writes**: INTRADAY_SIGNALS_CACHE_FILE (intraday_30m_signals_top_500.pkl) — pickle

### _make_signal_export
- **Defined**: signals_engine.py:602-638
- **Called by**:
  - signals_engine.py:640 (export_today_yesterday)
- **Columns read**: Date, Ticker, Is_Up_Rotation, Is_Down_Rotation, Is_Breakout, Is_Breakdown, Is_BTFD, Is_STFR, + signal stat columns
- **Columns created**: Ticker, Date, Close, Signal_Type, Entry_Price, Win_Rate, Avg_Winner, Avg_Loser, Avg_MFE, Avg_MAE, Historical_EV, EV_Last_3, Risk_Adj_EV, Risk_Adj_EV_Last_3, Count, Std_Dev

### export_today_yesterday
- **Defined**: signals_engine.py:640-659
- **Called by**:
  - signals_engine.py:661 (main)
- **Calls**: _make_signal_export
- **Writes**: SIGNAL_EXPORT_FOLDER/{date}_30m_Signals.xlsx, {prev_date}_30m_Signals.xlsx — xlsx

### main
- **Defined**: signals_engine.py:661-680
- **Called by**: (entry point when run as script)
- **Calls**: _load_env_file, _refresh_runtime_config, load_universe_tickers, load_or_build_intraday_30m, load_or_build_signals_30m, export_today_yesterday

---

## main.py

### _read_live_parquet
- **Defined**: main.py:79-89
- **Called by**:
  - main.py:325 (_compute_live_breadth)
  - main.py:457 (get_basket_data)
  - main.py:528 (get_ticker_data)

### _find_basket_parquet
- **Defined**: main.py:90-100
- **Called by**:
  - main.py:441 (get_basket_data)
- **Calls**: Path.glob (`{slug}_*_of_*_signals.parquet`, `{slug}_of_*_signals.parquet`)
- **Constants**: BASKET_CACHE_FOLDERS
- **Parallel impl**: YES — rotations.py:3370 has _find_basket_parquet

### _find_basket_meta
- **Defined**: main.py:102-112
- **Called by**:
  - main.py:143 (get_meta_file_tickers)
- **Calls**: Path.glob (`{slug}_*_of_*_signals_meta.json`, `{slug}_of_*_signals_meta.json`)
- **Constants**: BASKET_CACHE_FOLDERS
- **Parallel impl**: YES — rotations.py:3391 has _find_basket_meta

### get_dv_data
- **Defined**: main.py:117-130
- **Called by**:
  - main.py:641 (get_basket_summary, via dollar-volume weighting)
- **Reads**: INDIVIDUAL_SIGNALS_FILE (signals_500.parquet) — parquet (columns: Ticker, Date, Close, Volume)
- **Columns read**: Ticker, Date, Close, Volume

### clean_data_for_json
- **Defined**: main.py:132-133
- **Called by**:
  - main.py:502 (get_basket_data)
  - main.py:553 (get_ticker_data)
  - main.py:915+ (get_basket_summary, get_basket_correlation)

### get_latest_universe_tickers
- **Defined**: main.py:135-156
- **Called by**:
  - main.py:319 (_compute_live_breadth)
  - main.py:494 (get_basket_data)
- **Reads**: GICS_MAPPINGS_FILE (gics_mappings_500.json) — JSON; thematic JSON files — JSON

### get_meta_file_tickers
- **Defined**: main.py:159-168
- **Called by**: none in scope (utility)
- **Calls**: _find_basket_meta

### get_meta_file_weights
- **Defined**: main.py:172-188
- **Called by**:
  - main.py:641 (get_basket_summary)
- **Calls**: _find_basket_meta

### _get_universe_history
- **Defined**: main.py:191-208
- **Called by**:
  - main.py:220 (_get_ticker_join_dates)
  - main.py:235 (_get_tickers_for_date)
- **Reads**: GICS_MAPPINGS_FILE (gics_mappings_500.json) — JSON; thematic JSON files — JSON

### _quarter_str_to_date
- **Defined**: main.py:211-217
- **Called by**:
  - main.py:231 (_get_ticker_join_dates)
  - main.py:245 (_get_tickers_for_date)

### _get_ticker_join_dates
- **Defined**: main.py:220-232
- **Called by**:
  - main.py:641 (get_basket_summary)
- **Calls**: _get_universe_history, _quarter_str_to_date

### _get_tickers_for_date
- **Defined**: main.py:235-253
- **Called by**:
  - main.py:641 (get_basket_summary)
- **Calls**: _get_universe_history, _quarter_str_to_date

### _quarter_start
- **Defined**: main.py:256-258
- **Called by**:
  - main.py:279 (compute_current_basket_weights)

### compute_current_basket_weights
- **Defined**: main.py:261-317
- **Called by**:
  - main.py:496 (get_basket_data)
  - main.py:641 (get_basket_summary)
- **Calls**: _quarter_start
- **Reads**: INDIVIDUAL_SIGNALS_FILE (signals_500.parquet) — parquet (columns: Ticker, Date, Close, Volume)
- **Columns read**: Ticker, Date, Close, Volume, Dollar_Vol (computed)

### _compute_live_breadth
- **Defined**: main.py:319-418
- **Called by**:
  - main.py:483 (get_basket_data)
- **Calls**: get_latest_universe_tickers, _read_live_parquet
- **Reads**: LIVE_SIGNALS_FILE (live_signals_500.parquet) — parquet; INDIVIDUAL_SIGNALS_FILE (signals_500.parquet) — parquet
- **Columns read**: Ticker, Close, Date, Trend, Resistance_Pivot, Support_Pivot, Upper_Target, Lower_Target, Is_Breakout_Sequence

### read_root
- **Defined**: main.py:422
- **Called by**: GET / (FastAPI route)

### list_baskets
- **Defined**: main.py:365-380
- **Called by**: GET /api/baskets (FastAPI route)
- **Calls**: Path.glob (`*_of_*_signals.parquet`), re.sub
- **Reads**: basket parquets (glob via BASKET_CACHE_FOLDERS)
- **Glob pattern**: `*_of_*_signals.parquet` — stem parsed via `rsplit("_signals", 1)[0]` then `re.sub(r'(_\d+)?_of_\d+$', '', name)`
- **Constants**: BASKET_CACHE_FOLDERS, THEMATIC_CONFIG, DATA_STORAGE

### get_basket_breadth
- **Defined**: main.py:409-433
- **Called by**: GET /api/baskets/breadth (FastAPI route)
- **Calls**: Path.glob (`*_of_*_signals.parquet`), re.sub, pd.read_parquet
- **Reads**: basket parquets (glob via BASKET_CACHE_FOLDERS)
- **Columns read**: Date, Uptrend_Pct, Breakout_Pct
- **Glob pattern**: `*_of_*_signals.parquet` — slug extracted via `re.sub(r'(_\d+)?_of_\d+_signals$', '', f.stem)`
- **Constants**: BASKET_CACHE_FOLDERS

### get_basket_data
- **Defined**: main.py:448-503
- **Called by**: GET /api/baskets/{basket_name} (FastAPI route)
- **Calls**: _find_basket_parquet, _read_live_parquet, signals_engine._build_signals_from_df, _compute_live_breadth, get_latest_universe_tickers, compute_current_basket_weights, clean_data_for_json
- **Reads**: basket parquet (via _find_basket_parquet) — parquet; LIVE_BASKET_SIGNALS_FILE (live_basket_signals_500.parquet) — parquet

### list_tickers
- **Defined**: main.py:506-518
- **Called by**: GET /api/tickers (FastAPI route)
- **Reads**: TOP_500_FILE (top500stocks.json) — JSON; INDIVIDUAL_SIGNALS_FILE (signals_500.parquet) — parquet

### get_ticker_data
- **Defined**: main.py:521-554 (approximate)
- **Called by**: GET /api/tickers/{ticker} (FastAPI route)
- **Calls**: _read_live_parquet, signals_engine._build_signals_from_df, clean_data_for_json
- **Reads**: INDIVIDUAL_SIGNALS_FILE (signals_500.parquet) — parquet; LIVE_SIGNALS_FILE (live_signals_500.parquet) — parquet

### get_intraday_data
- **Defined**: main.py:555-612
- **Called by**: GET /api/tickers/{ticker}/intraday (FastAPI route)
- **Calls**: db.Historical.timeseries.get_range
- **Constants**: DB_API_KEY, DB_DATASET, DB_STYPE_IN

### get_ticker_signals
- **Defined**: main.py:613-707
- **Called by**: GET /api/ticker-signals (FastAPI route)
- **Reads**: INDIVIDUAL_SIGNALS_FILE (signals_500.parquet) — parquet; LIVE_SIGNALS_FILE (live_signals_500.parquet) — parquet
- **Calls**: _read_live_parquet
- **Key logic**: Returns per-ticker signal summary dict keyed by ticker symbol. Each entry contains `lt_trend` (BO/BD from Is_Breakout_Sequence), `st_trend` (Up/Dn from Trend), `mean_rev` (BTFD/STFR from open trade state), `pct_change` (daily % change from last 2 closes), `dollar_vol` (Close * Volume). Overrides `pct_change` with live data from LIVE_SIGNALS_FILE when available.
- **Numpy dtype casting**: `Close` and `Volume` columns arrive as float32 from parquet. The `pct_change` and `dollar_vol` values must be cast to native Python `float()`/`int()` before returning, otherwise FastAPI's JSON encoder raises `ValueError: numpy.float32 object is not iterable`. Casts applied at: line 676 (`float()` around pct calculation), lines 687-688 (`float(pct)` and `int(dv)` in result dict), line 702 (`float()` around live pct override).

### safe_float
- **Defined**: main.py:629-633
- **Called by**:
  - main.py:641 (get_basket_summary)

### safe_int
- **Defined**: main.py:635-639
- **Called by**:
  - main.py:641 (get_basket_summary)

### get_basket_summary
- **Defined**: main.py:641-914 (approximate — large endpoint)
- **Called by**: GET /api/baskets/{basket_name}/summary (FastAPI route)
- **Calls**: _find_basket_parquet, get_dv_data, get_meta_file_weights, _get_ticker_join_dates, _get_tickers_for_date, compute_current_basket_weights, safe_float, safe_int, clean_data_for_json
- **Reads**: basket parquet — parquet; INDIVIDUAL_SIGNALS_FILE (signals_500.parquet) — parquet (cols_needed includes BTFD_Triggered, STFR_Triggered); GICS_MAPPINGS_FILE (gics_mappings_500.json) — JSON
- **Key logic (live BTFD/STFR detection)**: BTFD fires when `hist_trend == 0.0 AND live_trend == 0.0` (previous-bar and live-bar both downtrend) plus `close <= prev_lower` and `not hist_btfd_triggered`. STFR fires when `hist_trend == 1.0 AND live_trend == 1.0` plus `close >= prev_upper` and `not hist_stfr_triggered`. Depends on `Trend` from last historical row (`hist_trend`), live-computed `Trend` (`live_trend`), and `BTFD_Triggered`/`STFR_Triggered` columns from signals parquet.

### get_basket_correlation
- **Defined**: main.py:915-~1029
- **Called by**: GET /api/baskets/{basket_name}/correlation (FastAPI route)
- **Calls**: clean_data_for_json
- **Reads**: INDIVIDUAL_SIGNALS_FILE (signals_500.parquet) — parquet; basket parquet

### handle_record (inner)
- **Defined**: main.py:983
- **Called by**: WebSocket handler loop in get_basket_correlation

---

## audit_basket.py

### _quarter_end_from_key
- **Defined**: audit_basket.py:29-35
- **Parallel impl**: YES — rotations.py:409 identical function

### _quarter_start_from_key
- **Defined**: audit_basket.py:36-42
- **Parallel impl**: YES — rotations.py:417 identical function

### _prev_quarter_key
- **Defined**: audit_basket.py:43-51
- **Called by**:
  - audit_basket.py:132 (main)

### _build_quarter_lookup
- **Defined**: audit_basket.py:52-59
- **Called by**:
  - audit_basket.py:132 (main)
- **Parallel impl**: YES — rotations.py:2956 identical function

### _find_active_quarter
- **Defined**: audit_basket.py:60-66
- **Called by**:
  - audit_basket.py:67 (walk_equity)
- **Parallel impl**: YES — rotations.py:2968 identical function

### walk_equity
- **Defined**: audit_basket.py:67-131
- **Called by**:
  - audit_basket.py:132 (main)
- **Calls**: _find_active_quarter

### main (audit_basket)
- **Defined**: audit_basket.py:132-327 (approximate)
- **Called by**: (script entry point)
- **Calls**: _prev_quarter_key, _build_quarter_lookup, walk_equity, cagr (inner)
- **Reads**: signals_500.parquet (INDIVIDUAL_SIGNALS_FILE equivalent), top500stocks.json — parquet + JSON

### cagr (inner)
- **Defined**: audit_basket.py:283
- **Called by**: audit_basket.py:main

---

## databento_test.py

### _load_env
- **Defined**: databento_test.py:39-91 (approximate)
- **Called by**:
  - databento_test.py:592 (main)
- **Calls**: load_dotenv

### _result_line
- **Defined**: databento_test.py:92-95
- **Called by**: various test functions

### _decode
- **Defined**: databento_test.py:97-102
- **Called by**: test functions

### _instrument_id
- **Defined**: databento_test.py:104-111
- **Called by**: test functions

### _ts_event_ns
- **Defined**: databento_test.py:113-121
- **Called by**: test functions

### _ts_et
- **Defined**: databento_test.py:122-131
- **Called by**: test functions

### _ohlcv_prices
- **Defined**: databento_test.py:133-153
- **Called by**: test functions

### _scale_df_prices
- **Defined**: databento_test.py:155-166
- **Called by**: test functions

### _live_run
- **Defined**: databento_test.py:168-209
- **Called by**: test3, test4, test5
- **Calls**: db.Live (threaded)

### _run (inner, in _live_run)
- **Defined**: databento_test.py:176
- **Called by**: threading.Thread inside _live_run

### print_available_schemas
- **Defined**: databento_test.py:211-239
- **Called by**: (manual call)
- **Calls**: db.Historical.metadata

### test1_historical_ohlcv_1d
- **Defined**: databento_test.py:241-283
- **Called by**:
  - databento_test.py:592 (main)
- **Calls**: db.Historical.timeseries.get_range

### test2_historical_ohlcv_1m_aggregate
- **Defined**: databento_test.py:284-350
- **Called by**:
  - databento_test.py:592 (main)

### test3_live_ohlcv_1d
- **Defined**: databento_test.py:351-438
- **Called by**:
  - databento_test.py:592 (main)
- **Calls**: _live_run

### on_msg (inner, in test3)
- **Defined**: databento_test.py:359
- **Called by**: _live_run callback

### _run (inner, in test3)
- **Defined**: databento_test.py:371
- **Called by**: threading.Thread inside test3

### test4_live_mbp1_replay
- **Defined**: databento_test.py:440-524
- **Called by**:
  - databento_test.py:592 (main)
- **Calls**: _live_run

### on_msg (inner, in test4)
- **Defined**: databento_test.py:463
- **Called by**: _live_run callback

### test5_live_mbp1_snapshot
- **Defined**: databento_test.py:526-591
- **Called by**:
  - databento_test.py:592 (main)
- **Calls**: _live_run

### on_msg (inner, in test5)
- **Defined**: databento_test.py:535
- **Called by**: _live_run callback

### main (databento_test)
- **Defined**: databento_test.py:592-624
- **Called by**: (script entry point)
- **Calls**: _load_env, test1_historical_ohlcv_1d, test2_historical_ohlcv_1m_aggregate, test3_live_ohlcv_1d, test4_live_mbp1_replay, test5_live_mbp1_snapshot

---

## check_data.py / check_pivots.py / debug_pickles.py / debug_pickles_v2.py

These are minimal diagnostic/utility scripts (5-25 lines each) with no function definitions. They contain only module-level code that reads and prints data from the shared parquet/pickle files.

---

## Cross-Repo Parallel Implementations Summary

| Function | rotations.py | signals_engine.py / main.py | Notes |
|---|---|---|---|
| _build_signals_from_df | rotations.py:1794 (numba-accelerated, full history) | signals_engine.py:298 (pure Python, 30m intraday) | Different inputs: EOD OHLC vs 30m bars; signals_engine has no bars tracking in accumulator |
| _build_signals_next_row | rotations.py:1933 (incremental single row) | main.py calls rotations version via signals_engine import | signals_engine has no equivalent; live row computation done via full recompute |
| RollingStatsAccumulator | rotations.py:1264 (has bars tracking, deque) | signals_engine.py:234 (no bars, list for last_3) | Interface difference: rotations.add() has optional bars param |
| _find_basket_parquet | rotations.py:3370 | main.py:90 | Different search folder lists; main.py no longer has legacy `*_basket.parquet` fallback |
| _find_basket_meta | rotations.py:3391 | main.py:102 | Different search folder lists; main.py no longer has legacy `*_basket_meta.json` fallback |
| _quarter_end_from_key | rotations.py:409 | audit_basket.py:29 | Identical |
| _quarter_start_from_key | rotations.py:417 | audit_basket.py:36 | Identical |
| _build_quarter_lookup | rotations.py:2956 | audit_basket.py:52 | Identical |
| _find_active_quarter | rotations.py:2968 | audit_basket.py:60 | Identical |
| _load_env_file | rotations.py:4034 | signals_engine.py:42 | Identical logic, different search paths |

---

## Key Constants Reference

| Constant | File | Line | Value / Description |
|---|---|---|---|
| EQUITY_SIGNAL_LOGIC_VERSION | rotations.py | 98 | '2026-02-10-codex-1' — bump to invalidate all basket signal caches |
| EQUITY_UNIVERSE_LOGIC_VERSION | rotations.py | 99 | '2026-02-10-codex-1' — bump to invalidate universe-dependent caches |
| EQUITY_CACHE_SCHEMA_VERSION | rotations.py | 97 | 1 — bump to force rebuild of equity OHLC caches |
| BASKET_SIGNALS_CACHE_SCHEMA_VERSION | rotations.py | 101 | 1 — bump to force rebuild of basket signal caches |
| CHART_SCHEMA_VERSION | rotations.py | 103 | 2 — bump to force rebuild of basket chart PNGs |
| FORCE_REBUILD_EQUITY_CACHE | rotations.py | 100 | False — set True to force rebuild |
| FORCE_REBUILD_BASKET_SIGNALS | rotations.py | 102 | False — set True to force rebuild |
| SIZE | rotations.py | 32 | 500 — universe size |
| START_YEAR | rotations.py | 31 | 2000 |
| MARKET_SYMBOL | rotations.py | 403 | 'SPY' |
| THEME_SIZE | rotations.py | 401 | 25 — tickers per thematic basket |
| LOOKBACK_DAYS | rotations.py | 404 | 252 — beta lookback |
| INCREMENTAL_MAX_DAYS | rotations.py | 2400 | 5 — calendar days stale before full rebuild |
| RV_MULT | rotations.py | 1216 | sqrt(252)/sqrt(21) |
| EMA_MULT | rotations.py | 1217 | 2.0/11.0 (span=10 range EMA) |
| RV_EMA_ALPHA | rotations.py | 1218 | 2.0/11.0 (span=10 RV EMA) |
| SIGNALS | rotations.py | 1215 | ['Up_Rot', 'Down_Rot', 'Breakout', 'Breakdown', 'BTFD', 'STFR'] |
