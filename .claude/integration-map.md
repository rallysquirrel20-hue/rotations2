# Cross-Repo Integration Map

> Auto-generated 2026-03-16. Tracks all communication points between rotations_signals and rotations_app.

## Shared Data Directory

`~/Documents/Python_Outputs/Data_Storage/`

- **Producer**: `rotations_signals/rotations.py`
- **Consumer**: `rotations_app/backend/main.py`, `rotations_app/backend/signals_engine.py`

---

## Integration Points

### `signals_500.parquet`
- **Written by**: `load_or_build_signals()` / `_incremental_update_signals()` — `rotations.py:2452` / `rotations.py:2218`
- **Read by**: `_compute_live_breadth()`, `get_basket_summary()`, `get_basket_correlation()`, `get_ticker_data()`, `get_ticker_signals()` — `main.py:259`, `766`, `1211`, `710`
  - `get_basket_summary()` reads `BTFD_Triggered` and `STFR_Triggered` columns (added to `cols_needed`)
  - `get_ticker_signals()` reads `Close` and `Volume` columns
- **Dtype note**: `Close`, `Volume`, and most numeric columns are stored as `float32`. FastAPI's JSON encoder cannot serialize `numpy.float32` values — consumers must cast to native Python `float`/`int` before returning JSON responses.
- **Columns written**: `Date`, `Ticker`, `Open`, `High`, `Low`, `Close`, `Volume`, `RV`, `RV_EMA`, `Trend` (float32: 1.0/0.0/NaN), `Resistance_Pivot`, `Support_Pivot`, `Rotation_Open`, `Up_Range`, `Down_Range`, `Up_Range_EMA`, `Down_Range_EMA`, `Upper_Target`, `Lower_Target`, `Is_Up_Rotation`, `Is_Down_Rotation`, `Is_Breakout`, `Is_Breakdown`, `Is_BTFD`, `Is_STFR`, `Is_Breakout_Sequence`, `Rotation_ID` (int32), `BTFD_Triggered`, `STFR_Triggered`, and for each of 6 signals (`Up_Rot`, `Down_Rot`, `Breakout`, `Breakdown`, `BTFD`, `STFR`): `{sig}_Entry_Price`, `{sig}_Exit_Date`, `{sig}_Exit_Price`, `{sig}_Final_Change`, `{sig}_MFE`, `{sig}_MAE`, `{sig}_Win_Rate`, `{sig}_Avg_Winner`, `{sig}_Avg_Loser`, `{sig}_Avg_Winner_Bars`, `{sig}_Avg_Loser_Bars`, `{sig}_Avg_MFE`, `{sig}_Avg_MAE`, `{sig}_Historical_EV`, `{sig}_Std_Dev`, `{sig}_Risk_Adj_EV`, `{sig}_EV_Last_3`, `{sig}_Risk_Adj_EV_Last_3`, `{sig}_Count`. Plus `Source='norgate'` on disk.
- **Status**: OK

### `top500stocks.json`
- **Written by**: `load_or_build_universe()` — `rotations.py:370`
- **Read by**: `list_tickers()` — `main.py:497`
- **Schema**: `{"YYYY Qn": ["TICKER", ...], ...}`
- **Status**: OK

### `top500stocks.pkl`
- **Written by**: NOBODY — `rotations.py` does NOT write this file
- **Read by**: NOBODY — pkl reference removed from `signals_engine.py`
- **Status**: RESOLVED — No longer needed. `signals_engine.py` now uses `top500stocks.json`.

### `gics_mappings_500.json`
- **Written by**: `load_or_build_gics_mappings()` — `rotations.py:1178`
- **Read by**: `get_latest_universe_tickers()`, `_get_universe_history()` — `main.py:118`, `157`
- **Schema**: `{"ticker_sector": {...}, "ticker_subindustry": {...}, "sector_u": {"Sector": {"YYYY Qn": [...]}}, "industry_u": {...}}`
- **Keys read by consumer**: `sector_u`, `industry_u` sub-dicts
- **Status**: OK

### Basket Signals Parquets (`*_basket_cache/`)
- **Written by**: `_finalize_basket_signals_output()` — `rotations.py:3718`
  - Naming: `{slug}_{n}_of_500_signals.parquet` (thematic), `{slug}_of_500_signals.parquet` (sector/industry)
- **Read by**: `get_basket_data()` — `main.py:440`
  - Naming glob: `{slug}_*_of_*_signals.parquet` (with `{slug}_of_*_signals.parquet` fallback)
- **Columns**: Full signal schema (same as `signals_500.parquet`) plus `Uptrend_Pct`, `Downtrend_Pct`, `Breadth_EMA`, `Breakout_Pct`, `Breakdown_Pct`, `BO_Breadth_EMA`, `B_Trend`, `B_Resistance`, `B_Support`, `B_Up_Rot`, `B_Down_Rot`, `B_Rot_High`, `B_Rot_Low`, `B_Bull_Div`, `B_Bear_Div`, `BO_B_*` variants, `Correlation_Pct`, `Source='norgate'`
- **Status**: OK

### Basket Meta JSONs (`*_basket_cache/`)
- **Written by**: `_finalize_basket_signals_output()` — `rotations.py:3776`
  - Naming: `*_signals_meta.json`
- **Read by**: `get_meta_file_tickers()` — `main.py:142`
  - Naming glob: `{slug}_*_of_*_signals_meta.json` (with `{slug}_of_*_signals_meta.json` fallback)
- **Schema**: `{schema_version, signal_logic_version, universe_logic_version, data_fingerprint, latest_source_date, last_cached_date, universe_signature, basket_type, state: {current_quarter, equity_prev_close, weights: {TICKER: float}}}`
- **Key read by consumer**: `state.weights`
- **Status**: OK

### Thematic Universe JSONs
- `beta_universes_500.json` — Written: `rotations.py:545`. Read: `main.py:131`, `168`. Schema: `{high: {quarter: [...]}, low: {quarter: [...]}}`. Status: OK
- `momentum_universes_500.json` — Written: `rotations.py:662`. Schema: `{winners: ..., losers: ...}`. Status: OK
- `dividend_universes_500.json` — Written: `rotations.py:1008`. Schema: `{high_yield: ..., div_growth: ...}`. Status: OK
- `risk_adj_momentum_500.json` — Written: `rotations.py:785`. Schema: `{"YYYY Qn": [...]}` (flat, no sub-key). Consumer reads with `key=None`. Status: OK

### `live_signals_500.parquet`
- **Written by**: `export_today_signals()` — `rotations.py:4446`
- **Read by**: `_compute_live_breadth()`, `get_basket_summary()`, `get_ticker_data()` — `main.py:265`, `766`, `710`
- **Columns written**: `Date`, `Ticker`, `Open`, `High`, `Low`, `Close` (pure OHLC, no signal columns, no `Source`)
- **OHLC source**: `get_live_ohlc_bars()` — all four fields (Open, High, Low, Close) derived from Databento Historical `ohlcv-1m` aggregation since 9:30 ET. Close is the last 1-minute bar's close price. (Previously Close was overridden by an `mbp-1` live-feed mid-price; that feed was removed 2026-03-16.)
- **Status**: OK

### `live_basket_signals_500.parquet`
- **Written by**: `export_today_signals()` — `rotations.py:5034`
- **Read by**: `get_basket_data()` — `main.py:449`
- **Columns written**: `Date`, `BasketName`, `Open`, `High`, `Low`, `Close`
- **Consumer expects**: `BasketName` or `Basket` (tries both)
- **Status**: OK

### `correlation_cache/`
- `basket_correlations_of_500.parquet` + `correlation_meta_500.json`
- **Written by**: `_save_corr_cache()` — `rotations.py:6235`
- **Read by**: Nothing in `main.py` or `signals_engine.py`
- **Status**: STALE — PDF-report artifacts only, not consumed by the app

---

## Signal Logic Parity (`rotations.py` vs `signals_engine.py`)

Both implement `_build_signals_from_df`. Known differences:

| Aspect | `rotations.py` | `signals_engine.py` |
|---|---|---|
| Acceleration | Numba JIT | Pure Python |
| `Avg_Winner_Bars`/`Avg_Loser_Bars` | Present | Present |
| BTFD entry price | `open_ if open_ <= lower_target else lower_target` | Matches (gap fill + prev-day target) |
| STFR entry price | `open_ if open_ >= upper_target else upper_target` | Matches (gap fill + prev-day target) |
| `{sig}_Change` column | Written (live running P&L) | Not written |

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
