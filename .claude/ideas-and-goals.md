# Ideas & Goals

## Active Goals

### ~~1. Repo Consolidation~~ — COMPLETED (2026-03-16)
~~Create a single "rotations" repo combining rotations_app and rotations_signals with organized CLAUDE.md and agent files for efficient project setup and editing.~~

### 2. Backtest Accuracy
Test backtest feature for accuracy.

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

### 9. Multi-Basket Backtesting
Add ability to test using multiple baskets or the constituent tickers of multiple baskets.

### 10. Variable Position Sizing
Add variable position sizing feature to backtest.

### ~~11. Theme List Dropdown Behavior~~ — RE-FIXED (2026-03-16)
~~Theme lists drop down automatically — should only drop down when the arrow is selected.~~ Regression occurred but has been fixed again by separating basket selection (row click) from expansion toggle (chevron click).

### ~~12. Basket Returns~~ — COMPLETED (2026-03-17)
~~Add a general basket analysis tab to analyze all basket returns together.~~ Implemented cross-basket period returns and single-basket daily returns chart with live data overlay, date presets (1D–ALL), group filters (All/T/S/I), searchable basket picker, hover overlay, and PNG export. Backend endpoint `GET /api/baskets/returns` with input validation. Future sub-goals (regime similarity, backtest integration) can be tracked as new goals.

### ~~13. Live Signal Auto-Refresh (Loops/Hooks)~~ — COMPLETED (2026-03-17)
~~Learn what Claude Code loops and hooks are and how to build time-based updates. Implement time-based auto-refresh for live signals during market hours.~~ 15-minute loop via live_loop.py + PM2. Fixed PM2 interpreter path for Windows, restored .env after repo consolidation, added cycle banners to stderr.

### ~~14. Time-Based Update for Signals & Basket Equity/Signals~~ — COMPLETED (2026-03-17)
~~Add scheduled updates for signals and basket equity/signals data during market hours.~~ Covered by Goal 13's live_loop.py — runs rotations.py every 15 minutes via PM2, which rebuilds signals and basket equity/signals through cache guards.

### ~~15. Optimize Basket Signals Build Time~~ — COMPLETED (2026-03-17)
~~Basket signals build and amend process is too slow — adding new themes or industries takes way too long. Needs performance optimization.~~ Full rebuild dropped from ~30 min to 7 min 47s (467s), averaging 16.9s per basket. Six optimizations: pre-computed returns matrices, correlation vectorization (numpy z-score variance decomposition), breadth vectorization (searchsorted + groupby), equity OHLC cumprod path, contributions merged into equity OHLC, extracted shared `_build_quarter_weights`.

## Completed

1. **BTFD/STFR trend guard — verify fix** (2026-03-13): Signals rebuilt (`signals_500.parquet`), zero co-fires confirmed. ~191K spurious BTFD/STFR signals eliminated. Also fixed a latent numpy float32 serialization bug in the ticker-signals endpoint exposed by the rebuild.
2. **Frontend sidebar empty columns** (2026-03-13): Resolved. Was a frontend parsing/column-mapping issue. Fixed alongside the BTFD/STFR consistency audit and sig-live styling work.
3. **Repo Consolidation** (2026-03-16): Consolidated rotations_signals and rotations_app into a single monorepo "rotations" at rallysquirrel20-hue/rotations. 44 files, 23,736 lines. Structure: signals/, app/backend/, app/frontend/. All agent definitions updated with relative paths.
4. **Basket Returns** (2026-03-17): Cross-basket period returns and single-basket daily returns chart with live data overlay, date presets, group filters, basket picker, hover overlay, and PNG export. Backend `GET /api/baskets/returns` endpoint with regex input validation and anchor-row logic.
5. **Optimize Basket Signals Build Time** (2026-03-17): Full rebuild dropped from ~30 min to 7 min 47s (467s). Six optimizations in `signals/rotations.py`: pre-computed returns matrices, correlation vectorization, breadth vectorization, equity OHLC cumprod, contributions merged into equity OHLC, extracted `_build_quarter_weights`.
