# Ideas & Goals

## Active Goals

### ~~1. Repo Consolidation~~ — COMPLETED (2026-03-16)
~~Create a single "rotations" repo combining rotations_app and rotations_signals with organized CLAUDE.md and agent files for efficient project setup and editing.~~

### ~~2. Backtest Accuracy~~ — COMPLETED (2026-03-18)
~~Test backtest feature for accuracy.~~ Completed sub-goals:
- Quarterly universe membership filtering: backtests now use shifting quarterly universes instead of only the current quarter. Trades are filtered to only include tickers that were in the basket at the entry date.
- Constituents overlay expanded to 6 sortable columns (Ticker, Entry, Ent.Wt, Return, Cur.Wt, Contrib) across all 3 chart types (TVChart, BacktestPanel, MultiBacktestPanel). Multi-leg adds a Leg column.
- Entry date shows current stint start (gap > 5 trading days = new stint), not first-ever appearance.
- Overlay sizing: constrained to chart area bounds (top: 4px, bottom: just above x-axis), scrollable.
- BENCHMARK_TIMING enabled for live loop per-step timing.
- `_was_taken` trade tracking: trades skipped due to leverage/cash constraints now properly counted as skipped, not executed. Stats computed only from taken trades.
- Live data staleness guard: `_live_is_current()` prevents stale intraday data from overriding Norgate end-of-day data across 8 endpoints.
- Analog rank direction fix: returns/uptrend/breakout now rank 1=highest (descending); correlation/volatility rank 1=lowest (ascending).

### ~~3. Equity Tab Improvements~~ — COMPLETED (2026-03-17)
~~Add constituent feature, scrolling/zooming, date range selector, chart colors, benchmark equity curves.~~

### ~~4. Distribution Tab Fixes~~ — COMPLETED (2026-03-16)
~~Show mean AND/OR median for winners and for losers separately (current shows single mean — not valuable); Fix bold borders on curves~~ Curve borders match fill colors, winner/loser median stats by trade outcome, KDE boundary clamping for MAE/MFE at zero.

### ~~5. Chart Tab — Signal Filtering~~ — COMPLETED (2026-03-16)
~~Currently shows ALL signals for stocks in the basket. Should filter to only entry signals that occurred while the ticker was in the basket (eliminate look-ahead bias).~~ Backend now filters constituent-ticker backtest trades to quarterly basket membership periods. Chart tab nav moved to top row.

### ~~6. Chart Tab — Feature Toggle~~ — COMPLETED (2026-03-16)
~~Move the feature toggle onto the Chart tab itself instead of using the one at the top of the page.~~ Feature toggle moved to Chart tab in backtest panel; main chart keeps its own toggles in header.

### ~~7. Path — Sorting~~ — COMPLETED (2026-03-16)
~~Add ability to sort by date, performance, ticker.~~ Sortable columns added to Path tab legend (BacktestPanel) and Returns tab legend (BasketSummary). Both support click-to-sort headers with ascending/descending toggle. Color ranking remains independent of sort order.

### ~~8. Backtest Pane Title~~ — COMPLETED (2026-03-16)
~~Fix visual asymmetry; include backtest variables in title.~~ Header height aligned to 41px matching sidebar accordion rows. Leverage preset buttons added. Position size and leverage preset rows with labels for symmetry.

### ~~9. Multi-Basket Backtesting~~ — COMPLETED (2026-03-23)
~~Multi-leg backtest overhaul.~~ Completed sub-goals:
- Searchable target picker (baskets + tickers) for single-leg and multi-leg
- Size/lev buttons uniform width, equity/size/lev inputs equal spacing
- Single-leg half-width card; multi-leg 2-column grid (up to 6 legs)
- Buy & Hold as entry signal option (basket or ticker)
- Fixed constituents overlay (single-leg: added daily_positions; multi-leg: fixed keying + position detail)
- Per-leg independent visibility toggle
- Per-leg curves show standalone full-portfolio returns (not allocation-weighted)
- Equity curves indexed to 0% return, rebased on timeframe change
- Strategy toggle buttons + timeframe buttons match cross-basket aesthetic
- All header rows 42px (matching accordion)
- Path tab added to multi-leg with Leg/Ticker/Date/Chg columns
- Path tab columns properly spaced (240px legend)
- Leverage multiplies position sizes (entry = equity × pos_size × max_lev)
- Removed Equity $ input (charts show % returns)
- All relevant regime filters (cross-basket source filtering)
- Trades tab: position size at entry, contribution to port, weight at exit
- Long-term portfolio stats (CAGR, Sharpe, Sortino, max DD duration)
- Multi-leg stats: correlation/volatility impact of adding a basket to baseline
- Multi-leg buy hold defaults to off (clean chart on first load)
- Constituents overlay expanded + sortable (see Goal 2)
- Strategy scanner script (`strategy_scanner.py`): sweeps backtest combinations (baskets x signals x filters x pos sizes x leverage) and collects results to CSV/JSON
- `_was_taken` trade tracking fix: skipped trades due to leverage/cash constraints properly counted

### ~~10. Backtest Performance & Stats UI~~ � COMPLETED (2026-03-23)
~~Backtest benchmark performance improvements and stats UI rewrite.~~ Completed sub-goals:
- Batch endpoint (`POST /api/backtest/benchmarks`) loads parquet once and computes equity curves + stats for all 6 signals in one request (eliminates GIL contention, ~3x faster than 7 parallel requests)
- Fixed missing `incrementProgress()` definition in BacktestPanel.tsx causing ReferenceError on all benchmark requests
- Fixed mtm_equity return statement incorrectly placed inside for loop (only marked first position to market)
- Aligned rounding between main backtest and benchmark equity curves
- Stats tab rewritten: replaced individual strategy cards with unified sortable table (strategies as rows, stats as columns) with portfolio/trade section dividers
- Buy & Hold stats added to stats table, computed from equity curve
- UI polish: signal column text centering, progress bar spacing/color, stats table gridlines and header wrapping

### 11. Variable Position Sizing
Add variable position sizing feature to backtest.

### ~~13. Theme List Dropdown Behavior~~ — RE-FIXED (2026-03-16)
~~Theme lists drop down automatically — should only drop down when the arrow is selected.~~ Regression occurred but has been fixed again by separating basket selection (row click) from expansion toggle (chevron click).

### ~~12. Basket Returns~~ — COMPLETED (2026-03-17)
~~Add a general basket analysis tab to analyze all basket returns together.~~ Implemented cross-basket period returns and single-basket daily returns chart with live data overlay, date presets (1D–ALL), group filters (All/T/S/I), searchable basket picker, hover overlay, and PNG export. Backend endpoint `GET /api/baskets/returns` with input validation. Future sub-goals (regime similarity, backtest integration) can be tracked as new goals.

### ~~13. Live Signal Auto-Refresh (Loops/Hooks)~~ — COMPLETED (2026-03-17)
~~Learn what Claude Code loops and hooks are and how to build time-based updates. Implement time-based auto-refresh for live signals during market hours.~~ 15-minute loop via live_loop.py + PM2. Fixed PM2 interpreter path for Windows, restored .env after repo consolidation, added cycle banners to stderr.

### ~~14. Time-Based Update for Signals & Basket Equity/Signals~~ — COMPLETED (2026-03-17)
~~Add scheduled updates for signals and basket equity/signals data during market hours.~~ Covered by Goal 13's live_loop.py — runs rotations.py every 15 minutes via PM2, which rebuilds signals and basket equity/signals through cache guards.

### ~~15. Optimize Basket Signals Build Time~~ — COMPLETED (2026-03-17)
~~Basket signals build and amend process is too slow — adding new themes or industries takes way too long. Needs performance optimization.~~ Full rebuild dropped from ~30 min to 7 min 47s (467s), averaging 16.9s per basket. Six optimizations: pre-computed returns matrices, correlation vectorization (numpy z-score variance decomposition), breadth vectorization (searchsorted + groupby), equity OHLC cumprod path, contributions merged into equity OHLC, extracted shared `_build_quarter_weights`.

### 17. Incremental Basket Rebuild Optimization — PENDING MERGE (2026-03-25)
Optimize the incremental basket rebuild so 1-2 new OHLC days don't trigger full-history recomputation of correlation, contributions, and breadth pivots across all 27 baskets.
- Branch: `optimize-incremental-rebuild`
- Implementation complete: incremental correlation (171x speedup), incremental contributions (6.6x speedup)
- All 30 baskets validated against production (exact match)
- Test scripts: `prep_test_data.py`, `test_incremental_run.py`, `test_incremental_benchmark.py`
- Plan doc: `.claude/optimize_basket_rebuild.md`
- Awaiting user verification before merge to main
- Phase 2 (breadth pivot state caching) and Phase 3 (parallelism) deferred

### 16. Export Button — IN PROGRESS (2026-03-24)
Export button properly functions with all features on the frontend, with titles / good aesthetic for charts.
Completed sub-goals:
- Cross-basket returns: PNG with descriptive labels (top-left: group/metric/preset/chart type, top-right: date range aligned to y-axis), y-axis moved to right on all bar/line charts, symmetrical padding
- Single-basket returns: PNG with labels, y-axis moved to right
- Analogs Summary: CSV with date range, values + ranks per basket, all timeframes
- Analogs Forward: PNG with 3-panel composite (left: horizon buttons + match dates, center: chart with condition/date labels, right: basket/chg legend)
- Analogs Aggregate: PNG with 3-panel composite (left: basket picker with avg, center: chart with labels, right: date/chg match list)
- Backtest Equity: PNG with full strategy description (target, tickers flag, entry/exit/stop, pos size, allocation, leverage, date range)
- Backtest Stats: CSV with portfolio + trade stats per leg
- Backtest Distribution: PNG with strategy labels
- Backtest Path: PNG with right-side legend (leg/ticker/date/chg) + strategy labels
- Backtest Trades: CSV with all trade details (ticker, dates, prices, return, bars, MFE, MAE)
- Backtest Multi-Strat Returns: PNG with right-side strategy/chg legend + labels
- Backtest Single-Strat Returns: PNG with strategy + timeframe labels, y-axis on right
- Signals: CSV with full filter description (universe, period, signal types, status, count)
- Remaining: Intrabasket tab exports (signals tables, correlation heatmap, returns chart, contribution chart) need implementation

### ~~19. Analogs Tab (Cross-Basket Analysis)~~ — COMPLETED (2026-03-18)
~~Add an "Analogs" tab to the cross-basket analysis panel.~~
- **19a.** ~~Add all relevant factors (same list as backtesting filters)~~ — Multi-timeframe return fingerprints (1D/1W/1M/1Q/1Y/3Y/5Y) with cross-basket rolling correlation
- **19b.** ~~Add historical ranking system~~ — Value-to-rank fingerprint process with Summary tab showing ranking table
- **19c.** ~~Cross-asset future returns tab (similar to intrabasket returns and backtest path tabs)~~ — Forward tab (cumulative line chart, 252 days per analog) + Aggregate tab (mean/median/min/max/std at 1M/3M/6M)
- **19d.** ~~Condition-based query mode~~ — Replaced fingerprint-matching with condition builder (basket + metric + operator + value). Backend `mode=query` evaluates conditions across all dates, returns forward returns at 1W/1M/3M/6M. Frontend tabs reduced to 4 (Summary/Matches/Forward/Aggregate).

## Completed

1. **BTFD/STFR trend guard — verify fix** (2026-03-13): Signals rebuilt (`signals_500.parquet`), zero co-fires confirmed. ~191K spurious BTFD/STFR signals eliminated. Also fixed a latent numpy float32 serialization bug in the ticker-signals endpoint exposed by the rebuild.
2. **Frontend sidebar empty columns** (2026-03-13): Resolved. Was a frontend parsing/column-mapping issue. Fixed alongside the BTFD/STFR consistency audit and sig-live styling work.
3. **Repo Consolidation** (2026-03-16): Consolidated rotations_signals and rotations_app into a single monorepo "rotations" at rallysquirrel20-hue/rotations. 44 files, 23,736 lines. Structure: signals/, app/backend/, app/frontend/. All agent definitions updated with relative paths.
4. **Basket Returns** (2026-03-17): Cross-basket period returns and single-basket daily returns chart with live data overlay, date presets, group filters, basket picker, hover overlay, and PNG export. Backend `GET /api/baskets/returns` endpoint with regex input validation and anchor-row logic.
5. **Optimize Basket Signals Build Time** (2026-03-17): Full rebuild dropped from ~30 min to 7 min 47s (467s). Six optimizations in `signals/rotations.py`: pre-computed returns matrices, correlation vectorization, breadth vectorization, equity OHLC cumprod, contributions merged into equity OHLC, extracted `_build_quarter_weights`.
6. **Analogs Tab** (2026-03-18): Elevated Analogs from sub-mode in BasketSummary to top-level AnalogsPanel component with 5 tabs (Summary, Analogs, Comparison, Forward, Aggregate). Backend expanded with multi-timeframe fingerprints, cross-basket correlation, forward_series, and aggregate stats. Cleaned up all analog code from BasketSummary.
7. **Backtest Quarterly Universe Filtering** (2026-03-18): Fixed survivorship bias in `basket_tickers` backtests. Both `run_backtest()` and `_build_leg_trades()` now use `_get_universe_history()` to load quarterly membership and filter trades to only tickers in the basket at entry date. Union of tickers across date range loaded for signal data.
8. **Constituents Overlay Overhaul** (2026-03-18): Expanded all 3 constituents overlays (TVChart, BacktestPanel, MultiBacktestPanel) from 4 to 6-7 sortable columns. Added entry_weight to backend daily_positions. TVChart candle-detail endpoint now returns stint-based entry date and EOD drifted weight. Overlay constrained to chart bounds with scrollable content.
9. **Trade Tracking Fix** (2026-03-18): Added `_was_taken` flag so trades skipped due to leverage/cash constraints are properly excluded from stats. `compute_stats()` now reports met_criteria vs taken vs skipped.
10. **Analogs Query Mode** (2026-03-18): Replaced fingerprint-matching analogs with condition-based historical query engine. Backend `mode=query` in `get_basket_returns()`. Frontend condition builder UI with basket/metric/operator/value. Tabs reduced from 5 to 4.
11. **Live Data Staleness Guard** (2026-03-18): Added `_live_is_current()` helper across 8 endpoints to prevent stale intraday data from overriding Norgate end-of-day data after market close.
12. **Analog Rank Direction Fix** (2026-03-18): Fixed ranking so returns/uptrend/breakout rank 1=highest (descending), correlation/volatility rank 1=lowest (ascending). Expanded multi-timeframe fingerprints from 4 to 7 windows.
13. **Strategy Scanner** (2026-03-18): New `strategy_scanner.py` script sweeps backtest parameter combinations and collects results to CSV/JSON.
