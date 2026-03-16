# Ideas & Goals

## Active Goals

### 1. Repo Consolidation
Create a single "rotations" repo combining rotations_app and rotations_signals with organized CLAUDE.md and agent files for efficient project setup and editing.

### 2. Backtest Accuracy
Test backtest feature for accuracy.

### 3. Equity Tab Improvements
- Add constituent feature
- Scrolling and zooming on equity curve chart
- Date range selector
- Fix ugly chart colors

### 4. Distribution Tab Fixes
- Show mean AND/OR median for winners and for losers separately (current shows single mean — not valuable)
- Fix bold borders on curves

### 5. Chart Tab — Signal Filtering
Currently shows ALL signals for stocks in the basket. Should filter to only entry signals that occurred while the ticker was in the basket (eliminate look-ahead bias).

### 6. Chart Tab — Feature Toggle
Move the feature toggle onto the Chart tab itself instead of using the one at the top of the page.

### 7. Path — Sorting
Add ability to sort by date, performance, ticker.

### 8. Backtest Pane Title
- Fix visual asymmetry
- Include backtest variables in title

### 9. Multi-Basket Backtesting
Add ability to test using multiple baskets or the constituent tickers of multiple baskets.

### 10. Variable Position Sizing
Add variable position sizing feature to backtest.

### 11. Theme List Dropdown Behavior
Theme lists drop down automatically — should only drop down when the arrow is selected.

### 12. General Basket Analysis Tab
Add a general basket analysis tab (or nest above current basket analysis features) to analyze all basket returns together.
- Akin to adding daily/annual reports to web app, with variable date ranges tied to quarterly universe tool
- Use historical returns of baskets as a feature in backtest to ID similar regimes historically
- Build tool to estimate which time periods had most similar features based on all basket returns and signal performance

## Completed

1. **BTFD/STFR trend guard — verify fix** (2026-03-13): Signals rebuilt (`signals_500.parquet`), zero co-fires confirmed. ~191K spurious BTFD/STFR signals eliminated. Also fixed a latent numpy float32 serialization bug in the ticker-signals endpoint exposed by the rebuild.
2. **Frontend sidebar empty columns** (2026-03-13): Resolved. Was a frontend parsing/column-mapping issue. Fixed alongside the BTFD/STFR consistency audit and sig-live styling work.
