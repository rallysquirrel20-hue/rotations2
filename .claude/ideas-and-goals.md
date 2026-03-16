# Ideas & Goals

## Active Goals

### ~~1. Repo Consolidation~~ — COMPLETED (2026-03-16)
~~Create a single "rotations" repo combining rotations_app and rotations_signals with organized CLAUDE.md and agent files for efficient project setup and editing.~~

### 2. Backtest Accuracy
Test backtest feature for accuracy.

### 3. Equity Tab Improvements
- Add constituent feature
- ~~Scrolling and zooming on equity curve chart~~ — COMPLETED (2026-03-16)
- ~~Date range selector~~ — COMPLETED (2026-03-16)
- ~~Fix ugly chart colors~~ — COMPLETED (2026-03-16) — Benchmark curves use coordinated blue (longs) / pink (shorts) palette; filtered = lime green; buy & hold = gray; all solid lines
- ~~Benchmark equity curves~~ — COMPLETED (2026-03-16) — 6 signal strategy benchmarks displayed alongside filtered backtest and buy & hold

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

### 12. General Basket Analysis Tab
Add a general basket analysis tab (or nest above current basket analysis features) to analyze all basket returns together.
- Akin to adding daily/annual reports to web app, with variable date ranges tied to quarterly universe tool
- Use historical returns of baskets as a feature in backtest to ID similar regimes historically
- Build tool to estimate which time periods had most similar features based on all basket returns and signal performance

### 13. Live Signal Auto-Refresh (Loops/Hooks) — IN PROGRESS (2026-03-16)
- ~~Learn what Claude Code loops and hooks are and how to build time-based updates~~ — COMPLETED
- Implement time-based auto-refresh for live signals during market hours — IMPLEMENTED (15-minute loop via live_loop.py + PM2)

### 14. Time-Based Update for Signals & Basket Equity/Signals
Add scheduled updates for signals and basket equity/signals data during market hours.

### 15. Optimize Basket Signals Build Time
Basket signals build and amend process is too slow — adding new themes or industries takes way too long. Needs performance optimization.

## Completed

1. **BTFD/STFR trend guard — verify fix** (2026-03-13): Signals rebuilt (`signals_500.parquet`), zero co-fires confirmed. ~191K spurious BTFD/STFR signals eliminated. Also fixed a latent numpy float32 serialization bug in the ticker-signals endpoint exposed by the rebuild.
2. **Frontend sidebar empty columns** (2026-03-13): Resolved. Was a frontend parsing/column-mapping issue. Fixed alongside the BTFD/STFR consistency audit and sig-live styling work.
3. **Repo Consolidation** (2026-03-16): Consolidated rotations_signals and rotations_app into a single monorepo "rotations" at rallysquirrel20-hue/rotations. 44 files, 23,736 lines. Structure: signals/, app/backend/, app/frontend/. All agent definitions updated with relative paths.
