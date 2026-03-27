# Sector Summary & Directional Bias Feature

## Context

The Rotations app has rich per-basket data (6 signal types, breadth, correlation, volatility, contributions, analogs) but no layer that synthesizes it into actionable summaries. The user wants a system that, for every basket, produces: (1) a brief narrative of what's happening, backed by stats, (2) a directional bias (bullish/bearish/neutral), and (3) a confidence score (1-5) to guide position sizing.

All raw data already exists in parquet caches. The analog system already finds historically similar periods and computes forward returns. This feature is a **scoring + narrative layer** on top of existing infrastructure.

---

## Architecture

### New backend module: `app/backend/basket_analysis.py`

Keeps `main.py` (5085 lines) from growing further. Follows the `signals_engine.py` precedent.

### Two new endpoints in `main.py`

| Endpoint | Purpose |
|---|---|
| `GET /api/baskets/{name}/analysis` | Full single-basket analysis (scoring + narrative + analog context) |
| `GET /api/baskets/analysis/batch?group=all` | All baskets scored, no analog detail (fast) |

### New frontend component: `SectorSummaryPanel.tsx`

New header button (like Analogs/Backtest), not a tab in BasketSummary. Two views: cross-basket overview table + per-basket detail with narrative, factor chart, and analog context.

---

## Backend: Scoring Engine

### Response shape (per basket)

```python
{
    "basket": str,
    "as_of": str,
    "directional_bias": "bullish" | "bearish" | "neutral",
    "confidence": int,           # 1-5
    "composite_score": float,    # -1.0 to +1.0
    "narrative": str,
    "factors": {
        "signal_consensus":   { "score": float, "detail": str },
        "signal_quality":     { "score": float, "detail": str },
        "breadth_regime":     { "score": float, "detail": str },
        "correlation_regime": { "score": float, "detail": str },
        "volatility_regime":  { "score": float, "detail": str },
        "basket_trend":       { "score": float, "detail": str },
        "recent_outcomes":    { "score": float, "detail": str },
        "analog_outlook":     { "score": float, "detail": str },
    },
    "stats": { ... },           # raw metrics backing the narrative
    "analog_summary": {
        "self_history": { "match_count", "mean_forward_1q", "std_forward_1q", "pct_positive_1q" },
        "cross_basket": { "match_count", "mean_forward_1q", "std_forward_1q", "pct_positive_1q" },
        "agreement": bool,
    }
}
```

### 8 scoring factors (each -1.0 to +1.0)

| # | Factor | What it measures | Weight |
|---|--------|-----------------|--------|
| 1 | **Signal Consensus** | % of constituent stocks with long vs short open signals across all 6 types | 0.20 |
| 2 | **Signal Quality** | Weighted avg Risk-Adj EV of open signals (positive EV long signals = bullish) | 0.15 |
| 3 | **Breadth Regime** | Uptrend% level + B_Trend direction + 1M change in breadth | 0.15 |
| 4 | **Correlation Regime** | Correlation% level + 1M change (rising corr = bearish for longs) | 0.10 |
| 5 | **Volatility Regime** | RV_EMA percentile vs trailing 252d + 1M change (rising vol = caution) | 0.10 |
| 6 | **Basket Trend** | Basket equity LT trend (breakout/breakdown) + ST trend + failed breakout detection | 0.15 |
| 7 | **Recent Outcomes** | Hit rate of signals opened in last 63 trading days (failed breakouts drag this down) | 0.10 |
| 8 | **Analog Outlook** | Mean forward 1Q return from matched historical periods, adjusted by dispersion | 0.05 |

### Factor scoring details

#### Factor 1: Signal Consensus (weight: 0.20)
```
For each constituent stock, determine current signal direction:
  LT Trend: Breakout = +1, Breakdown = -1
  ST Trend: Up_Rot = +1, Down_Rot = -1
  MR: BTFD (open) = +1, STFR (open) = -1

net_direction = (sum of all +1 signals) / (total signal count)
score = (net_direction - 0.5) * 2  # maps [0,1] -> [-1,1]
```

#### Factor 2: Signal Quality (weight: 0.15)
```
For each open signal across all constituents:
  weighted_ev = risk_adj_ev * (1 if long_signal else -1)

mean_ev = weighted average of all open signal EVs
Sparse data fallback: win_rate - 0.5 scaled to [-1,1]
score = clamp(mean_ev / 0.05, -1, 1)  # ±5% EV -> ±1
```

#### Factor 3: Breadth Regime (weight: 0.15)
```
level_score = (uptrend_pct - 50) / 50        # [0,100] -> [-1,1]
trend_score = +1 if B_Trend == 1.0 else -1
momentum_score = clamp(breadth_1m_change / 30, -1, 1)

score = 0.4 * level_score + 0.3 * trend_score + 0.3 * momentum_score
```

#### Factor 4: Correlation Regime (weight: 0.10)
```
level_penalty = -(correlation_pct - 50) / 50  # high corr = negative
change_signal = -clamp(corr_1m_change / 20, -1, 1)

score = 0.5 * level_penalty + 0.5 * change_signal
```

#### Factor 5: Volatility Regime (weight: 0.10)
```
rv_percentile = percentile_rank(current rv_ema vs trailing 252 values)
level_signal = -(rv_percentile - 0.5) * 2
rv_momentum = -clamp(rv_1m_change / rv_ema_median, -1, 1)

score = 0.5 * level_signal + 0.5 * rv_momentum
```

#### Factor 6: Basket Trend (weight: 0.15)
```
lt_score = +1 if breakout sequence else -1
st_score = +1 if Trend == 1.0 else -1

# Failed breakout: was in breakout recently but now back in breakdown
if recent_failed_breakout: lt_score = -0.5

score = 0.6 * lt_score + 0.4 * st_score
```

#### Factor 7: Recent Outcomes (weight: 0.10)
```
# Signals opened in last 63 trading days
For each: long + positive perf = hit, long + negative = miss (mirror for short)

hit_rate = hits / (hits + misses)
score = (hit_rate - 0.5) * 2
```

#### Factor 8: Analog Outlook (weight: 0.05)
```
# Combined from both analog sources (self-history + cross-basket)
self_direction = clamp(self_mean_fwd / 0.10, -1, 1)
cross_direction = clamp(cross_mean_fwd / 0.10, -1, 1)
self_confidence = 1.0 - clamp(self_std / 0.20, 0, 1)
cross_confidence = 1.0 - clamp(cross_std / 0.20, 0, 1)

score = 0.6 * (self_direction * self_confidence) + 0.4 * (cross_direction * cross_confidence)
```

### Composite score -> Bias + Confidence

```
composite = weighted sum of 8 factors (-1.0 to +1.0)

Bias:
  composite >  0.10 -> "bullish"
  composite < -0.10 -> "bearish"
  else              -> "neutral"

Confidence (1-5):
  magnitude = abs(composite)
  agreement = fraction of factors aligned with composite sign
  analog_boost = +0.5 if both analog sources agree with bias, -0.5 if they disagree
  raw = magnitude * 2.5 + agreement * 2.5 + analog_boost
  confidence = clamp(round(raw), 1, 5)
```

---

## Narrative Generation: Claude API

The scoring engine computes all stats and factor scores deterministically. The structured data is then passed to Claude API to generate natural prose.

**Prompt structure:**
- System prompt: "You are a quantitative sector analyst. Write a brief, direct summary..."
- User message: JSON blob of all `stats`, `factors`, `analog_summary`, `directional_bias`, `confidence`
- Output: 3-6 sentence narrative referencing specific stats

**Implementation:**
- `async _generate_narrative(name, factors, stats, analog_summary, bias, confidence) -> str`
- Uses `anthropic` Python SDK
- Model: `claude-haiku-4-5-20251001` (fast, cheap, sufficient for structured-to-prose)
- Timeout: 5s with fallback to a simple template string if API fails
- **Caching**: narratives cached alongside scoring results with 15-min TTL (matches live_loop cycle)
- For batch endpoint: narratives generated in parallel via `asyncio.gather()` across all baskets

**Prompt will instruct Claude to:**
- Lead with what's happening now (basket trend, today's signals)
- Reference breadth, correlation, volatility changes with actual numbers
- Include analog historical context with forward return stats
- Close with the directional bias and confidence rationale
- Stay under 150 words, no bullet points, plain prose

---

## Analog Integration: Dual Approach

Two analog systems feed into the analysis:

### 1. Per-basket self-history (primary, for narrative)
- Compare this basket's current regime (Uptrend%, Breakout%, Correlation%, RV_EMA, recent return) against its own historical values
- Find dates where this specific basket had similar stats
- Compute forward 1Q returns from those matched dates
- Answers: "When THIS sector looked like this before, what happened next?"
- Implementation: new `_query_self_analogs(slug, lookback=63)` function in `basket_analysis.py`
- Method: for each historical date, compute euclidean distance in normalized factor space (z-score each metric across time), select top N closest matches excluding ±21 day neighbors

### 2. Cross-basket fingerprint (secondary, for additional context)
- Reuse existing `mode=analogs` logic that matches the current state of ALL 27 baskets simultaneously
- Answers: "When the OVERALL market configuration looked like this, what happened to this sector?"
- Implementation: call into existing analog computation (main.py:831-1324), extract this basket's forward returns from the cross-basket result

**Response includes both:**
```python
"analog_summary": {
    "self_history": {
        "match_count": int,
        "mean_forward_1q": float,
        "std_forward_1q": float,
        "pct_positive_1q": float,
    },
    "cross_basket": {
        "match_count": int,
        "mean_forward_1q": float,
        "std_forward_1q": float,
        "pct_positive_1q": float,
    },
    "agreement": bool,  # do both point the same direction?
}
```

When both analogs agree on direction, confidence gets a boost. When they disagree, the narrative notes the divergence.

---

## Backend: Data Access

All data already exists. Per-basket analysis reads:

| Source | What we extract |
|---|---|
| `basket_signals_cache/{slug}_signals.parquet` | Basket OHLC, Trend, breakout/breakdown states, Uptrend_Pct, Breakout_Pct, Correlation_Pct, RV_EMA, breadth pivots (B_Trend, B_Up_Rot, etc.) |
| `signals_cache_500.parquet` | Per-constituent current signal states, entry prices, performance, EV stats |
| `live_signals_500.parquet` + `live_basket_signals_500.parquet` | Intraday overlay for today's signals |
| Existing analog logic (main.py:831-1324) | Cross-basket fingerprint matches + forward returns |
| New self-history analog logic | Per-basket regime matching against its own past |

Shared data-access helpers (`_find_basket_parquet`, `get_latest_universe_tickers`, file path constants) will be imported from `main.py` or extracted to avoid circular imports.

---

## Frontend: SectorSummaryPanel

### Integration in App.tsx

- New `showSectorSummary` state, mutually exclusive with other panels
- New "Summary" header button following existing pattern (lines 1297-1340)
- Renders `<SectorSummaryPanel>` in the content stack

### View 1: Cross-Basket Overview (batch endpoint, no analogs)

Sortable table of all baskets:

| Basket | Bias | Conf | Trend | Breadth% | BO% | Corr% | RV% | Score |
|--------|------|------|-------|----------|-----|-------|-----|-------|
| Diversified Banks | BEAR | 4 | BD | 40 | 20 | 72 | 24 | -0.45 |
| Semiconductors | BULL | 3 | BO | 85 | 60 | 45 | 18 | +0.32 |

- Row tinting: green (bullish), red (bearish), neutral (white)
- Confidence as filled dots (1-5)
- Filter tabs: ALL / Themes / Sectors / Industries
- Click row -> detail view

### View 2: Single-Basket Detail (per-basket endpoint, with analogs)

Three stacked sections:

**A. Narrative card** - full-width, stat highlights in accent color
**B. Factor breakdown** - horizontal bar chart (-1 to +1), green/red, canvas-rendered
**C. Analog context** - mini forward path chart with +/-1 sigma band + stats box (self-history + cross-basket side by side)

---

## Implementation Phases

### Phase 1: Scoring engine (`basket_analysis.py`)
- Create module with 8 factor functions + composite scoring
- Wire up `GET /api/baskets/{name}/analysis` endpoint
- Wire up `GET /api/baskets/analysis/batch` endpoint
- Test with real data via curl

### Phase 2: Dual analog system
- Build `_query_self_analogs(slug)` — per-basket regime matching against own history
- Extract cross-basket analog helper from existing `mode=analogs` logic (main.py:831-1324)
- Wire both into factor 8 + `analog_summary` response field
- Agreement/disagreement detection between the two analog sources

### Phase 3: Claude API narrative generation
- Install/configure `anthropic` SDK in backend
- `async _generate_narrative()` with structured stats -> Claude Haiku -> prose
- Fallback to simple template string if API times out or fails
- Parallel generation for batch endpoint via `asyncio.gather()`
- 15-min TTL cache for generated narratives

### Phase 4: Frontend panel
- `SectorSummaryPanel.tsx` with cross-basket table view
- Per-basket detail view (narrative + factor chart + analog mini-view)
- App.tsx integration (button, state, render)

### Phase 5: Polish
- 60-second TTL cache on batch endpoint (data updates every 15 min via live_loop)
- Edge cases: baskets with <3 stocks, new baskets with short history
- Live overlay path (use live-adjusted breadth/trend when available)

---

## Critical Files

| File | Role |
|---|---|
| `app/backend/basket_analysis.py` | **NEW** - scoring engine + Claude API narrative generator |
| `app/backend/main.py` | Add 2 endpoints, import basket_analysis |
| `app/frontend/src/components/SectorSummaryPanel.tsx` | **NEW** - summary panel component |
| `app/frontend/src/App.tsx` | Add button, state, render branch |
| `app/frontend/src/index.css` | Styling for new panel |

## Environment Requirement

- `ANTHROPIC_API_KEY` env var in `app/backend/.env` (same pattern as existing `DATABENTO_API_KEY`)

---

## Verification

1. Start backend: `cd app/backend && python main.py`
2. Hit `GET /api/baskets/diversified_banks_of_500/analysis` — verify response has all fields, narrative reads naturally
3. Hit `GET /api/baskets/analysis/batch` — verify all ~27 baskets return with scores
4. Start frontend: `cd app/frontend && npm run dev`
5. Click "Summary" header button — cross-basket table renders, sortable, filterable
6. Click a basket row — detail view shows narrative, factor chart, analog context
7. Verify narrative matches real data (spot-check breadth%, correlation% against existing BasketSummary tabs)
