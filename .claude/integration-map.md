# Monorepo Integration Map

Updated: 2026-04-08 (slug transform sync, /api/baskets/returns universe filtering, Q2 correlation fix)

## Data Flow

```
signals/build_universes.py  →  JSON caches (universes, GICS, thematic)
signals/build_signals.py    →  signals_500.parquet, signals_etf_50.parquet
signals/build_baskets.py    →  *_signals.parquet, *_ohlc.parquet, *_contributions.parquet
signals/live_updates.py     →  live_signals_500.parquet, live_basket_signals_500.parquet
                                    ↓
                            ~/Documents/Python_Outputs/Data_Storage/
                                    ↓
                            app/backend/main.py (FastAPI REST API)
                                    ↓
                            app/frontend/src/ (React + lightweight-charts)
```

---

## Shared Data Files

All files in `~/Documents/Python_Outputs/Data_Storage/` (configurable via `PYTHON_OUTPUTS_DIR` env var).

### Universe & Mapping Files

| File | Producer | Consumer Endpoints | Schema | Updated |
|------|----------|-------------------|--------|---------|
| `top500stocks.json` | `build_universes.py` | `/api/baskets`, `/api/tickers`, `/api/baskets/compositions` | `{quarter_key: [tickers]}` | Quarterly |
| `etf_universes_50.json` | `build_universes.py` | `/api/etfs` | `{quarter_key: [tickers]}` | Quarterly |
| `gics_mappings_500.json` | `build_universes.py` | `/api/baskets`, `/api/baskets/breadth`, `/api/baskets/returns`, `/api/baskets/compositions` | `{ticker_sector, ticker_subindustry, sector_u, industry_u}` | Quarterly |
| `ticker_names.json` | `build_universes.py` | `/api/ticker-names` | `{ticker: name}` | Quarterly |

### Thematic Universe Files (in `thematic_basket_cache/`)

| File | Producer | Consumer Endpoints | Schema |
|------|----------|-------------------|--------|
| `beta_universes_500.json` | `build_universes.py` | `/api/baskets/compositions`, `live_updates.py` | `{high: {q: tickers}, low: {q: tickers}}` |
| `momentum_universes_500.json` | `build_universes.py` | same | `{winners: {q: tickers}, losers: {q: tickers}}` |
| `dividend_universes_500.json` | `build_universes.py` | same | `{high_yield: ..., growth: ..., with_growth: ...}` |
| `risk_adj_momentum_500.json` | `build_universes.py` | same | `{winners: {q: tickers}, losers: {q: tickers}}` |
| `size_universes_500.json` | `build_universes.py` | same | `{q: tickers}` |
| `volume_universes_500.json` | `build_universes.py` | same | `{leaders: {q: tickers}, losers: {q: tickers}}` (YoY dollar volume) |

**THEMATIC_CONFIG key mapping** (in `main.py`):
- `High_Beta` → `beta_universes_500.json` key `"high"`
- `Low_Beta` → `beta_universes_500.json` key `"low"`
- `Momentum_Leaders` → `momentum_universes_500.json` key `"winners"`
- `Momentum_Losers` → `momentum_universes_500.json` key `"losers"`
- `High_Dividend_Yield` → `dividend_universes_500.json` key `"high_yield"`
- `Dividend_Growth` → `dividend_universes_500.json` key `"growth"`
- `Dividend_with_Growth` → `dividend_universes_500.json` key `"with_growth"`
- `Risk_Adjusted_Momentum` → `risk_adj_momentum_500.json` key `"winners"`
- `Risk_Adjusted_Momentum_Losers` → `risk_adj_momentum_500.json` key `"losers"`
- `Size` → `size_universes_500.json` key `None`
- `Volume_Leaders` → `volume_universes_500.json` key `"leaders"`
- `Volume_Losers` → `volume_universes_500.json` key `"losers"`

### Signal Files

| File | Producer | Consumer Endpoints | Key Columns | Updated |
|------|----------|-------------------|-------------|---------|
| `signals_500.parquet` | `build_signals.py` | `/api/tickers/{ticker}`, `/api/baskets/{name}/summary`, `/api/baskets/breadth`, `/api/signals/log` | Date, Ticker, OHLCV, Trend, Is_*, *_Entry_Price, *_Exit_Date, *_MFE, *_MAE, RV_EMA, Source | Daily 5pm |
| `signals_etf_50.parquet` | `build_signals.py` | `/api/tickers/{ticker}` (fallback), `/api/etfs` | Same as above | Daily 5pm |
| `live_signals_500.parquet` | `live_updates.py` | `/api/baskets/breadth`, `/api/tickers/{ticker}`, `/api/baskets/{name}/summary`, `/api/signals/log` | **Full signal rows** (same schema as signals_500) with Source='live' | Every 5 min intraday |
| `live_signals_etf_50.parquet` | `live_updates.py` | `/api/etfs` | OHLC only | Every 5 min intraday |

**Important**: `live_signals_500.parquet` now contains full signal columns (Trend, Is_Up_Rotation, Resistance_Pivot, etc.), not just OHLC. This was changed 2026-04-02.

### Basket Files (in `thematic_basket_cache/`, `sector_basket_cache/`, `industry_basket_cache/`)

| Pattern | Producer | Consumer Endpoints | Key Columns |
|---------|----------|-------------------|-------------|
| `{slug}_*_signals.parquet` | `build_baskets.py` | `/api/baskets/{name}`, `/api/baskets/returns`, `/api/baskets/breadth` | Date, OHLCV, Trend, Is_*, Uptrend_Pct, Breakout_Pct, Correlation_Pct, RV_EMA |
| `{slug}_*_ohlc.parquet` | `build_baskets.py` | (internal equity cache) | Date, OHLCV |
| `{slug}_*_contributions.parquet` | `build_baskets.py` | `/api/baskets/{name}/contributions`, `/api/baskets/{name}/summary` (returns tab) | Date, Ticker, Daily_Return, Weight_BOD |
| `{slug}_*_meta.json` | `build_baskets.py` | `/api/baskets/{name}` (weights) | state.weights, schema_version, data_sig |
| `live_basket_signals_500.parquet` | `live_updates.py` | `/api/baskets/breadth` (live overlay), `/api/baskets/returns` | BasketName, Date, OHLCV |

**BasketName → slug transform contract** (commit 97eb02c):
`main.py` uses `_basket_name_to_slug()` to convert `BasketName` values (e.g. `"Industry: Aerospace & Defense"`) to cache file slugs (e.g. `Aerospace_and_Defense`). This helper mirrors `signals/build_baskets.py:_cache_slugify_label` exactly: strips the `"Industry: "` / `"Sector: "` / `"Theme: "` prefix, then applies `/→space`, `&→and`, `-→space`, `space→_`. **If either implementation changes its substitution rules, the other must be updated in lockstep** or the live overlay silently fails for any basket whose name contains `&`, `-`, or `/`. Three call sites in `main.py` use this helper: the `list_baskets` live overlay loop, the `get_basket_returns` `live_closes` loop, and the `get_basket_data` single-basket live row match.

### Other Files

| File | Producer | Consumer | Purpose |
|------|----------|----------|---------|
| `returns_matrix_500.parquet` | `build_baskets.py` | `build_baskets.py` (internal) | Date×Ticker close return pivot for vectorized basket computation |
| `returns_matrix_500.fingerprint` | `build_baskets.py` | `build_baskets.py` | MD5 guard for freshness |
| `correlation_cache/` | `build_baskets.py` | `/api/baskets/{name}/summary` | Pre-computed correlation data |

---

## API Endpoints → Data Sources

| Endpoint | Reads From |
|----------|------------|
| `GET /api/baskets` | Glob `*_signals.parquet` across cache folders, filtered by `_is_valid_basket()` |
| `GET /api/baskets/breadth` | `*_signals.parquet` (last row), `live_signals_500.parquet` (live overlay via `_compute_live_breadth_batch`), `live_basket_signals_500.parquet` (live basket overlay) |
| `GET /api/baskets/returns` | `*_signals.parquet`, `live_basket_signals_500.parquet`, filtered by `_is_valid_basket()` |
| `GET /api/baskets/{name}` | `{slug}_*_signals.parquet`, meta JSON |
| `GET /api/baskets/{name}/summary` | `signals_500.parquet` (Ticker/Date/Close + available signal cols), `live_signals_500.parquet`, `*_contributions.parquet` |
| `GET /api/baskets/{name}/contributions` | `*_contributions.parquet` |
| `GET /api/baskets/{name}/correlation` | `signals_500.parquet` (Ticker/Date/Close) |
| `GET /api/baskets/compositions` | `gics_mappings_500.json`, thematic JSON files, filtered by `_is_valid_basket()` |
| `GET /api/tickers` | `top500stocks.json` |
| `GET /api/tickers/{ticker}` | `signals_500.parquet`, `live_signals_500.parquet` (merges live row) |
| `GET /api/ticker-signals` | `signals_500.parquet`, `live_signals_500.parquet` |
| `GET /api/signals/log` | `signals_500.parquet` or `*_signals.parquet` (baskets), filtered by `_is_valid_basket()` |
| `GET /api/etfs` | `etf_universes_50.json`, `signals_etf_50.parquet` |
| `GET /api/ticker-names` | `ticker_names.json` |
| `GET /api/live-signals` | `live_signals_500.parquet` |
| `WS /ws/live/{ticker}` | Databento Live API (real-time) |

---

## Industry Filtering

Industries are filtered by **dollar volume** during universe build (`build_universes.py`), matching how stocks/ETFs are filtered. Per quarter: industries with < 3 tickers are excluded, then the top 25% by total constituent dollar volume are selected (`INDUSTRY_TOP_PCT = 0.25` in `config.py`). Different quarters can have different qualifying industries.

The GICS file (`gics_mappings_500.json`) only contains qualifying industries. This pre-filtering flows through to all consumers:

| Endpoint | Filter behavior |
|----------|----------------|
| `/api/baskets` (sidebar) | Current quarter only (`_is_valid_basket`) |
| `/api/baskets/breadth` (sidebar metrics) | Current quarter only (`_is_valid_basket`) |
| `/api/baskets/compositions` | Full quarterly history (no filter — GICS file is the gate) |
| `/api/baskets/returns` | **Default**: current-quarter active industries only (~17) via `_industries_for_quarter_range(None, None)`. **With `universe_start`/`universe_end` params** (quarter keys like `'2026 Q1'`): industries active in any quarter within the supplied range. Frontend `BasketReturnsChart` passes these params when `isQuarterMode` is active. **Behavior change (97eb02c)**: previously returned all ~50 historical industries regardless of params; now defaults to current quarter only. |
| `/api/baskets/returns` (analogs) | All industries in GICS file (any quarter) — unchanged |
| `/api/signals/log` | Current quarter only (`_is_valid_basket`) |

Helper: `_is_valid_basket(slug)` — True for themes, sectors, and industries in current quarter's GICS.
Helper: `_industries_for_quarter_range(start_qkey, end_qkey)` — reads `industry_u` and returns slugs active in any quarter within the range; falls back to `_get_valid_industry_slugs()` when both bounds are None.
Helper: `_slug_to_gics_name(slug)` — handles `&` vs `and` mismatch (GICS uses `&`, slugs use `and`).
Helper: `_basket_name_to_slug(bname)` — mirrors `signals/build_baskets.py:_cache_slugify_label`; see the "BasketName → slug transform contract" note above.

**`Correlation_Pct` data-quality note** (commit 97eb02c):
Before this commit, `{slug}_*_signals.parquet` contained `NaN` for `Correlation_Pct` during the first ~14 days of a new quarter because `_compute_within_basket_correlation` checked ticker sparsity against the current-quarter slice (needed ≥14 non-NA rows). The fix checks against `sub_ret` (warmup + current quarter) so Q2+ dates now populate correctly. No schema change — the column existed before. Existing parquets are back-filled on the next `build_baskets.py` incremental run, which re-runs `_finalize_basket_signals_output` on the full merged history.

### Build filtering
- **Daily incremental** (`build_baskets.py`): only processes current-quarter industries (~17) + themes + sectors = ~38 baskets
- **Force rebuild** (`--force`): processes all historically qualifying industries (~50)
- **Live updates** (`live_updates.py`): only computes live baskets for current-quarter industries

---

## PM2 Process Architecture

| Process | Script | Schedule | Data Produced |
|---------|--------|----------|---------------|
| `rot-universes` | `loop_universes.py` → `build_universes.py` | 5pm ET, last trading day of quarter | Universe JSONs |
| `rot-signals` | `loop_signals.py` → `build_signals.py` + `build_baskets.py` | 5pm ET, every trading day | signals + basket parquets |
| `rot-live` | `loop_live.py` → `live_updates.py` | Every 5 min, 9:30–4:00 ET | live_signals, live_basket_signals |
| `rotations-backend` | `app/backend/main.py` | Always on | REST API |
| `rotations-frontend` | Vite dev server | Always on | UI on :5173 |
