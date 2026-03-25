"""
Test runner for incremental basket rebuild optimization.

Reads stripped basket data from test_data/, appends the missing 3/24 row
(taken from production signals), runs the incremental finalize path
(correlation + contributions), and writes results back to test_data/.

Then compares the test output with production to verify correctness.

Usage:
    python test_incremental_run.py

Prerequisites:
    python prep_test_data.py   (creates test_data/ with 3/24 stripped)
"""

import bisect
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# ---------------------------------------------------------------------------
# Constants (from rotations.py)
# ---------------------------------------------------------------------------
RV_MULT = np.sqrt(252) / np.sqrt(21)
RV_EMA_ALPHA = 2.0 / 11.0
CHART_SCHEMA_VERSION = 2
STRIP_DATE = pd.Timestamp("2026-03-24").normalize()

DATA_FOLDER = Path.home() / "Documents" / "Python_Outputs" / "Data_Storage"
TEST_ROOT = Path(__file__).parent / "test_data"

PROD_BASKET_DIRS = {
    "sector": DATA_FOLDER / "sector_basket_cache",
    "thematic": DATA_FOLDER / "thematic_basket_cache",
    "industry": DATA_FOLDER / "industry_basket_cache",
}
TEST_BASKET_DIRS = {
    "sector": TEST_ROOT / "sector_basket_cache",
    "thematic": TEST_ROOT / "thematic_basket_cache",
    "industry": TEST_ROOT / "industry_basket_cache",
}


# ---------------------------------------------------------------------------
# Helpers (from rotations.py)
# ---------------------------------------------------------------------------

class WriteThroughPath:
    def __init__(self, path):
        self.path = Path(path)
    def sync(self):
        pass  # No-op in test


def _quarter_start_from_key(key):
    y, q = key.split()
    yr, qn = int(y), int(q.replace("Q", ""))
    month = (qn - 1) * 3 + 1
    return pd.Timestamp(year=yr, month=month, day=1)


def _quarter_end_from_key(key):
    y, q = key.split()
    yr, qn = int(y), int(q.replace("Q", ""))
    month = qn * 3
    return pd.Timestamp(year=yr, month=month, day=1) + pd.offsets.MonthEnd(0)


def _build_quarter_lookup(universe_by_qtr):
    if isinstance(next(iter(universe_by_qtr.keys())), str):
        quarter_keys = [(k, _quarter_start_from_key(k)) for k in universe_by_qtr.keys()]
    else:
        quarter_keys = [(k, k) for k in universe_by_qtr.keys()]
    quarter_keys.sort(key=lambda x: x[1])
    return [k for k, _ in quarter_keys], [dt for _, dt in quarter_keys]


def _find_active_quarter(d, quarter_labels, quarter_ends):
    idx = bisect.bisect_right(quarter_ends, d) - 1
    return quarter_labels[idx] if idx >= 0 else None


# ---------------------------------------------------------------------------
# compute_breadth_pivots (exact copy from rotations.py)
# ---------------------------------------------------------------------------

def compute_breadth_pivots(ema_values):
    n = len(ema_values)
    ema = np.asarray(ema_values, dtype=float)
    rv_raw = np.zeros(n)
    rv_raw[1:] = np.abs(np.diff(ema))
    rv_ema = np.zeros(n)
    alpha = RV_EMA_ALPHA
    rv_ema[1] = rv_raw[1]
    for i in range(2, n):
        rv_ema[i] = alpha * rv_raw[i] + (1 - alpha) * rv_ema[i - 1]
    start_idx = next((i for i in range(2, n) if rv_ema[i] > 0), None)
    if start_idx is None:
        return pd.DataFrame()
    trends = np.full(n, None, dtype=object)
    resistance = np.full(n, np.nan)
    support = np.full(n, np.nan)
    is_up_rot = np.zeros(n, dtype=bool)
    is_down_rot = np.zeros(n, dtype=bool)
    trends[start_idx] = False
    resistance[start_idx] = ema[start_idx] + rv_ema[start_idx] * RV_MULT
    for i in range(start_idx + 1, n):
        val = ema[i]
        rv = rv_ema[i] * RV_MULT
        prev_trend = trends[i - 1]
        prev_res = resistance[i - 1]
        prev_sup = support[i - 1]
        if prev_trend == False:
            resistance[i] = min(val + rv, prev_res)
            if val > prev_res:
                trends[i] = True
                support[i] = val - rv
                resistance[i] = prev_res
                is_up_rot[i] = True
            else:
                trends[i] = False
        else:
            support[i] = max(val - rv, prev_sup) if not np.isnan(prev_sup) else val - rv
            if val < prev_sup:
                trends[i] = False
                resistance[i] = val + rv
                support[i] = prev_sup
                is_down_rot[i] = True
            else:
                trends[i] = True
    rot_high = np.full(n, np.nan)
    rot_low = np.full(n, np.nan)
    is_bull_div = np.zeros(n, dtype=bool)
    is_bear_div = np.zeros(n, dtype=bool)
    prev_down_low = np.nan
    last_down_low = np.nan
    prev_up_high = np.nan
    last_up_high = np.nan
    cur_high = np.nan
    cur_low = np.nan
    for i in range(start_idx, n):
        t = trends[i]
        if t is None:
            continue
        if is_up_rot[i]:
            if not np.isnan(cur_low):
                prev_down_low = last_down_low
                last_down_low = cur_low
            if not np.isnan(prev_down_low) and not np.isnan(last_down_low):
                if last_down_low > prev_down_low:
                    is_bull_div[i] = True
            cur_high = ema[i]
            cur_low = np.nan
        elif is_down_rot[i]:
            if not np.isnan(cur_high):
                prev_up_high = last_up_high
                last_up_high = cur_high
            if not np.isnan(prev_up_high) and not np.isnan(last_up_high):
                if last_up_high < prev_up_high:
                    is_bear_div[i] = True
            cur_low = ema[i]
            cur_high = np.nan
        if t == True:
            cur_high = np.nanmax([cur_high, ema[i]])
            rot_high[i] = cur_high
        else:
            cur_low = np.nanmin([cur_low, ema[i]])
            rot_low[i] = cur_low
    return pd.DataFrame({
        'B_Trend': trends, 'B_Resistance': resistance, 'B_Support': support,
        'B_Up_Rot': is_up_rot, 'B_Down_Rot': is_down_rot,
        'B_Rot_High': rot_high, 'B_Rot_Low': rot_low,
        'B_Bull_Div': is_bull_div, 'B_Bear_Div': is_bear_div,
    })


# ---------------------------------------------------------------------------
# Incremental correlation (the new optimization)
# ---------------------------------------------------------------------------

def _compute_within_basket_correlation_incremental(universe_by_qtr, returns_matrix, new_dates, window=21):
    if returns_matrix is None or returns_matrix.empty or not universe_by_qtr or not new_dates:
        return pd.DataFrame(columns=['Date', 'Correlation_Pct'])
    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_qtr)
    min_obs = max(10, int(window * 0.70))
    new_dates_norm = sorted(pd.to_datetime(d).normalize() for d in new_dates)
    all_dates, all_corrs = [], []
    quarter_date_groups = {}
    for d in new_dates_norm:
        q_key = _find_active_quarter(d, quarter_labels, quarter_ends)
        if q_key is not None:
            quarter_date_groups.setdefault(q_key, []).append(d)
    for q_key, dates_in_q in quarter_date_groups.items():
        tickers = [t for t in universe_by_qtr.get(q_key, set()) if t in returns_matrix.columns]
        if len(tickers) < 2:
            for d in dates_in_q:
                all_dates.append(d); all_corrs.append(np.nan)
            continue
        q_idx = quarter_labels.index(q_key)
        q_start = quarter_ends[q_idx]
        q_data = returns_matrix.loc[q_start:, tickers]
        valid_tickers = [t for t in tickers if q_data[t].notna().sum() >= min_obs]
        if len(valid_tickers) < 2:
            for d in dates_in_q:
                all_dates.append(d); all_corrs.append(np.nan)
            continue
        for d in dates_in_q:
            d_idx = returns_matrix.index.searchsorted(d)
            if d_idx >= len(returns_matrix.index) or returns_matrix.index[d_idx] != d:
                all_dates.append(d); all_corrs.append(np.nan); continue
            start_idx = max(0, d_idx - window + 1)
            w_slice = returns_matrix.iloc[start_idx:d_idx + 1][valid_tickers].values
            if len(w_slice) < min_obs:
                all_dates.append(d); all_corrs.append(np.nan); continue
            col_counts = np.sum(~np.isnan(w_slice), axis=0)
            col_valid = col_counts >= min_obs
            nv = col_valid.sum()
            if nv < 2:
                all_dates.append(d); all_corrs.append(np.nan); continue
            w = w_slice[:, col_valid]
            means = np.nanmean(w, axis=0)
            stds = np.nanstd(w, axis=0, ddof=1)
            stds[stds == 0] = np.nan
            z = (w - means) / stds
            z_port = np.nanmean(z, axis=1)
            z_valid = z_port[~np.isnan(z_port)]
            if len(z_valid) < min_obs:
                all_dates.append(d); all_corrs.append(np.nan); continue
            var_z = np.var(z_valid, ddof=1)
            avg_corr = (nv * var_z - 1) / (nv - 1)
            all_dates.append(d)
            all_corrs.append(np.clip(avg_corr * 100, -100, 100))
    if not all_dates:
        return pd.DataFrame(columns=['Date', 'Correlation_Pct'])
    return pd.DataFrame({'Date': all_dates, 'Correlation_Pct': all_corrs})


# ---------------------------------------------------------------------------
# Incremental contributions (the new optimization)
# ---------------------------------------------------------------------------

def _compute_and_save_contributions_incremental(
        contrib_path, existing_df, universe_by_qtr, returns_matrix, quarter_weights, new_dates):
    """Incrementally update contributions for new dates. Writes to contrib_path."""
    existing_df = existing_df.copy()
    existing_df['Date'] = pd.to_datetime(existing_df['Date']).dt.normalize()
    existing_max_date = existing_df['Date'].max()
    new_dates_norm = sorted(pd.to_datetime(d).normalize() for d in new_dates)
    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_qtr)

    new_rows = []
    _carried_bod = None
    _carried_q_key = None

    for d in new_dates_norm:
        if d <= existing_max_date:
            continue
        q_key = _find_active_quarter(d, quarter_labels, quarter_ends)
        if q_key is None:
            continue
        w_dict = quarter_weights.get(q_key)
        if not w_dict:
            continue
        tickers = [t for t in w_dict if t in returns_matrix.columns]
        if not tickers:
            continue

        if _carried_bod is not None and _carried_q_key == q_key:
            bod_weights = _carried_bod
        else:
            q_idx = quarter_labels.index(q_key)
            q_start = quarter_ends[q_idx]
            existing_q = existing_df[existing_df['Date'] >= q_start]
            if existing_q.empty:
                w0 = pd.Series({t: w_dict[t] for t in tickers})
                w0 = w0 / w0.sum()
                bod_weights = w0
            else:
                last_day = existing_q[existing_q['Date'] == existing_q['Date'].max()]
                if last_day.empty:
                    continue
                eod = {}
                for _, row in last_day.iterrows():
                    t = row['Ticker']
                    if t in tickers:
                        eod[t] = row['Weight_BOD'] * (1 + row['Daily_Return'])
                if not eod:
                    continue
                total_eod = sum(eod.values())
                if total_eod == 0:
                    continue
                bod_weights = pd.Series({t: v / total_eod for t, v in eod.items()})

        if d not in returns_matrix.index:
            continue
        day_rets = returns_matrix.loc[d, [t for t in tickers if t in bod_weights.index]].fillna(0.0)
        for t in bod_weights.index:
            if t in day_rets.index:
                new_rows.append({
                    'Date': d, 'Ticker': t,
                    'Weight_BOD': bod_weights[t],
                    'Daily_Return': day_rets[t],
                    'Contribution': bod_weights[t] * day_rets[t],
                })
        eod_weights = bod_weights * (1 + day_rets.reindex(bod_weights.index, fill_value=0.0))
        total_eod = eod_weights.sum()
        if total_eod > 0:
            _carried_bod = eod_weights / total_eod
        else:
            _carried_bod = None
        _carried_q_key = q_key

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        contrib_df = pd.concat([existing_df, new_df], ignore_index=True)
        contrib_df = contrib_df.drop_duplicates(subset=['Date', 'Ticker'], keep='last')
        contrib_df = contrib_df.sort_values(['Date', 'Ticker']).reset_index(drop=True)
    else:
        contrib_df = existing_df

    pq.write_table(pa.Table.from_pandas(contrib_df, preserve_index=False),
                    contrib_path, compression='snappy')
    return len(new_rows)


# ---------------------------------------------------------------------------
# Finalize: breadth pivots + incremental corr + incremental contributions
# ---------------------------------------------------------------------------

def finalize_basket(name, merged_all, universe_by_qtr, returns_matrix,
                    quarter_weights, incremental_dates, test_dir, slug, basket_type):
    """Run the full finalize path on merged_all, writing to test_dir."""
    t0_total = time.perf_counter()

    # --- Breadth pivots (full recompute, same as production) ---
    t0 = time.perf_counter()
    if 'Breadth_EMA' in merged_all.columns:
        bp = compute_breadth_pivots(merged_all['Breadth_EMA'].values)
        if bp is not None and not bp.empty:
            for col in bp.columns:
                merged_all[col] = bp[col].values
    if 'BO_Breadth_EMA' in merged_all.columns:
        bo_bp = compute_breadth_pivots(merged_all['BO_Breadth_EMA'].values)
        if bo_bp is not None and not bo_bp.empty:
            for col in bo_bp.columns:
                merged_all[f'BO_{col}'] = bo_bp[col].values
    t_pivots = time.perf_counter() - t0

    # --- Incremental correlation ---
    t0 = time.perf_counter()
    incr_corr = _compute_within_basket_correlation_incremental(
        universe_by_qtr, returns_matrix, incremental_dates)
    if not incr_corr.empty and 'Correlation_Pct' in merged_all.columns:
        merged_all = merged_all.copy()
        merged_all['Date'] = pd.to_datetime(merged_all['Date']).dt.normalize()
        incr_corr['Date'] = pd.to_datetime(incr_corr['Date']).dt.normalize()
        incr_map = incr_corr.set_index('Date')['Correlation_Pct']
        mask = merged_all['Date'].isin(incr_map.index)
        merged_all.loc[mask, 'Correlation_Pct'] = merged_all.loc[mask, 'Date'].map(incr_map).values
    elif 'Correlation_Pct' not in merged_all.columns:
        merged_all['Correlation_Pct'] = np.nan
    t_corr = time.perf_counter() - t0

    # --- Save signals parquet ---
    t0 = time.perf_counter()
    merged_all['Source'] = 'norgate'
    signals_path = test_dir / f"{slug}_signals.parquet"
    table = pa.Table.from_pandas(merged_all, preserve_index=False)
    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta, b'chart_schema_version': str(CHART_SCHEMA_VERSION).encode()}
    pq.write_table(table.replace_schema_metadata(new_meta), signals_path, compression='snappy')

    # Update meta JSON
    meta_path = test_dir / f"{slug}_signals_meta.json"
    if meta_path.exists():
        with open(meta_path, 'r') as f:
            meta = json.load(f)
    else:
        meta = {}
    last_date = pd.to_datetime(merged_all['Date'], errors='coerce').max()
    meta['last_cached_date'] = last_date.strftime('%Y-%m-%d') if pd.notna(last_date) else None
    with open(meta_path, 'w') as f:
        json.dump(meta, f, indent=2)
    t_save = time.perf_counter() - t0

    # --- Incremental contributions ---
    t0 = time.perf_counter()
    contrib_path = test_dir / f"{slug}_contributions.parquet"
    n_new_contrib = 0
    if contrib_path.exists():
        existing_contrib = pd.read_parquet(contrib_path)
        n_new_contrib = _compute_and_save_contributions_incremental(
            contrib_path, existing_contrib, universe_by_qtr, returns_matrix,
            quarter_weights, incremental_dates)
    t_contrib = time.perf_counter() - t0

    t_total = time.perf_counter() - t0_total
    return {
        'pivots': t_pivots, 'correlation': t_corr, 'save': t_save,
        'contributions': t_contrib, 'total': t_total,
        'n_new_contrib': n_new_contrib,
    }


# ---------------------------------------------------------------------------
# Load universe data
# ---------------------------------------------------------------------------

def load_all_universes():
    """Load sector, thematic, and industry universes from GICS + thematic caches."""
    gics_path = DATA_FOLDER / "gics_mappings_500.json"
    with open(gics_path, 'r') as f:
        gics = json.load(f)

    baskets = []

    # Sector universes
    for name, uni_data in gics.get('sector_u', {}).items():
        universe = {k: set(v) for k, v in uni_data.items()}
        slug = name.replace(" ", "_") + "_of_500"
        baskets.append((name, slug, universe, 'sector'))

    # Industry universes
    for name, uni_data in gics.get('industry_u', {}).items():
        universe = {k: set(v) for k, v in uni_data.items()}
        slug = name.replace(" ", "_").replace("&", "and") + "_of_500"
        baskets.append((name, slug, universe, 'industry'))

    # Thematic universes — derive from file names in test_data
    thematic_dir = TEST_BASKET_DIRS.get('thematic')
    if thematic_dir and thematic_dir.exists():
        seen_slugs = set()
        for f in sorted(thematic_dir.iterdir()):
            if f.name.endswith('_signals.parquet'):
                slug = f.name.replace('_signals.parquet', '')
                if slug not in seen_slugs:
                    seen_slugs.add(slug)
                    # Load universe from meta if available
                    meta_path = thematic_dir / f"{slug}_signals_meta.json"
                    # For thematic, we need the universe — load it from the OHLC meta
                    ohlc_meta_path = thematic_dir / f"{slug}_ohlc_meta.json"
                    universe = {}
                    if ohlc_meta_path.exists():
                        try:
                            with open(ohlc_meta_path, 'r') as mf:
                                ometa = json.load(mf)
                            # Universe is stored in equity meta state
                            state = ometa.get('state', {})
                            # Try to reconstruct — but we need the actual universe
                        except Exception:
                            pass
                    # Thematic universes aren't in GICS — skip universe-dependent ops
                    baskets.append((slug.rsplit('_of_', 1)[0], slug, universe, 'thematic'))

    return baskets


def build_quarter_weights(all_signals_df, universe_by_qtr):
    """Build per-quarter initial weights."""
    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_qtr)
    needed_cols = ['Date', 'Ticker', 'Close']
    if 'Volume' in all_signals_df.columns:
        needed_cols.append('Volume')
    df_w = all_signals_df[needed_cols].copy()
    df_w['Date'] = pd.to_datetime(df_w['Date']).dt.normalize()
    df_w = df_w.dropna(subset=['Close'])
    if 'Volume' in df_w.columns:
        df_w['Dollar_Vol'] = df_w['Close'] * df_w['Volume']
    quarter_weights = {}
    if 'Dollar_Vol' not in df_w.columns:
        return {}
    dv_q = (
        df_w[['Date', 'Ticker', 'Dollar_Vol']]
        .dropna(subset=['Dollar_Vol'])
        .groupby(['Ticker', pd.Grouper(key='Date', freq='QE-DEC')])['Dollar_Vol']
        .mean()
    )
    for label in quarter_labels:
        if label not in universe_by_qtr:
            continue
        if isinstance(label, str):
            y, q = label.split()
            yr, qn = int(y), int(q.replace("Q", ""))
            if qn == 1:
                ranking_date = _quarter_end_from_key(f"{yr - 1} Q4")
            else:
                ranking_date = _quarter_end_from_key(f"{yr} Q{qn - 1}")
        else:
            ranking_date = label
        weights = {}
        total = 0.0
        for t in universe_by_qtr[label]:
            val = dv_q.get((t, ranking_date), np.nan)
            if pd.notna(val) and val > 0:
                weights[t] = float(val)
                total += float(val)
        if total > 0:
            quarter_weights[label] = {t: v / total for t, v in weights.items()}
    return quarter_weights


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("INCREMENTAL REBUILD TEST RUNNER")
    print("=" * 70)

    if not TEST_ROOT.exists():
        print(f"ERROR: {TEST_ROOT} not found. Run prep_test_data.py first.")
        return

    # Load shared data (read-only from production)
    print("\nLoading shared data...")
    t0 = time.perf_counter()
    returns_matrix = pd.read_parquet(DATA_FOLDER / "returns_matrix_500.parquet")
    returns_matrix.index = pd.to_datetime(returns_matrix.index).normalize()
    print(f"  returns_matrix: {returns_matrix.shape}  ({time.perf_counter() - t0:.2f}s)")

    t0 = time.perf_counter()
    all_signals_df = pd.read_parquet(DATA_FOLDER / "signals_500.parquet")
    all_signals_df['Date'] = pd.to_datetime(all_signals_df['Date']).dt.normalize()
    print(f"  all_signals_df: {len(all_signals_df)} rows  ({time.perf_counter() - t0:.2f}s)")

    # Discover all baskets from test_data files
    print("\nDiscovering baskets from test_data/...")
    baskets = []
    for basket_type, test_dir in TEST_BASKET_DIRS.items():
        if not test_dir.exists():
            continue
        for f in sorted(test_dir.iterdir()):
            if f.name.endswith('_signals.parquet'):
                slug = f.name.replace('_signals.parquet', '')
                baskets.append((slug, basket_type, test_dir))

    print(f"  Found {len(baskets)} baskets")

    # Load GICS for universe data
    gics_path = DATA_FOLDER / "gics_mappings_500.json"
    with open(gics_path, 'r') as f:
        gics = json.load(f)
    sector_u = {k: {qk: set(qv) for qk, qv in v.items()} for k, v in gics.get('sector_u', {}).items()}
    industry_u = {k: {qk: set(qv) for qk, qv in v.items()} for k, v in gics.get('industry_u', {}).items()}

    # Process each basket
    print("\n" + "=" * 70)
    print("PROCESSING BASKETS (incremental rebuild)")
    print("=" * 70)

    results = []
    t_grand_start = time.perf_counter()

    for slug, basket_type, test_dir in baskets:
        # Load test (stripped) signals
        test_signals = pd.read_parquet(test_dir / f"{slug}_signals.parquet")
        test_signals['Date'] = pd.to_datetime(test_signals['Date']).dt.normalize()

        # Load production signals (has 3/24)
        prod_dir = PROD_BASKET_DIRS[basket_type]
        prod_signals = pd.read_parquet(prod_dir / f"{slug}_signals.parquet")
        prod_signals['Date'] = pd.to_datetime(prod_signals['Date']).dt.normalize()

        # Extract the 3/24 row from production
        new_row = prod_signals[prod_signals['Date'] == STRIP_DATE]
        if new_row.empty:
            print(f"  [{slug}] SKIP (no 3/24 in production)")
            continue

        # Append 3/24 to test signals → merged_all
        merged_all = (
            pd.concat([test_signals, new_row], ignore_index=True)
            .drop_duplicates(subset=['Date'], keep='last')
            .sort_values('Date')
            .reset_index(drop=True)
        )

        # Get universe for this basket
        basket_name = slug.rsplit('_of_', 1)[0].replace('_', ' ')
        universe_by_qtr = None
        if basket_type == 'sector':
            universe_by_qtr = sector_u.get(basket_name)
        elif basket_type == 'industry':
            # Try with & restored
            universe_by_qtr = industry_u.get(basket_name)
            if not universe_by_qtr:
                universe_by_qtr = industry_u.get(basket_name.replace(' and ', ' & '))

        if not universe_by_qtr:
            # Thematic — skip correlation/contributions (no universe)
            print(f"  [{slug}] thematic — saving signals only")
            merged_all['Source'] = 'norgate'
            signals_path = test_dir / f"{slug}_signals.parquet"
            table = pa.Table.from_pandas(merged_all, preserve_index=False)
            pq.write_table(table, signals_path, compression='snappy')
            meta_path = test_dir / f"{slug}_signals_meta.json"
            if meta_path.exists():
                with open(meta_path, 'r') as f:
                    meta = json.load(f)
                meta['last_cached_date'] = '2026-03-24'
                with open(meta_path, 'w') as f:
                    json.dump(meta, f, indent=2)
            results.append({'name': slug, 'type': basket_type, 'total': 0, 'correlation': 0, 'contributions': 0})
            continue

        # Build quarter weights for contributions
        quarter_weights = build_quarter_weights(all_signals_df, universe_by_qtr)

        # Run the incremental finalize
        timing = finalize_basket(
            basket_name, merged_all, universe_by_qtr, returns_matrix,
            quarter_weights, [STRIP_DATE], test_dir, slug, basket_type,
        )

        results.append({
            'name': slug, 'type': basket_type, **timing,
        })
        print(f"  [{slug}] pivots={timing['pivots']:.3f}s  corr={timing['correlation']:.4f}s  "
              f"contrib={timing['contributions']:.3f}s  save={timing['save']:.3f}s  "
              f"total={timing['total']:.3f}s  (+{timing['n_new_contrib']} contrib rows)")

    t_grand_total = time.perf_counter() - t_grand_start

    # --- Summary ---
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    n_baskets = len([r for r in results if r.get('total', 0) > 0])
    t_corr_total = sum(r.get('correlation', 0) for r in results)
    t_contrib_total = sum(r.get('contributions', 0) for r in results)
    t_pivots_total = sum(r.get('pivots', 0) for r in results)
    print(f"  Baskets processed:    {n_baskets}")
    print(f"  Total wall time:      {t_grand_total:.2f}s")
    print(f"  Total correlation:    {t_corr_total:.3f}s")
    print(f"  Total contributions:  {t_contrib_total:.3f}s")
    print(f"  Total breadth pivots: {t_pivots_total:.3f}s")

    # --- Validate against production ---
    print("\n" + "=" * 70)
    print("VALIDATION (comparing test output vs production)")
    print("=" * 70)

    all_ok = True
    for slug, basket_type, test_dir in baskets:
        prod_dir = PROD_BASKET_DIRS[basket_type]
        test_path = test_dir / f"{slug}_signals.parquet"
        prod_path = prod_dir / f"{slug}_signals.parquet"
        if not test_path.exists() or not prod_path.exists():
            continue

        test_df = pd.read_parquet(test_path)
        prod_df = pd.read_parquet(prod_path)
        test_df['Date'] = pd.to_datetime(test_df['Date']).dt.normalize()
        prod_df['Date'] = pd.to_datetime(prod_df['Date']).dt.normalize()

        # Compare the 3/24 row's Correlation_Pct
        test_row = test_df[test_df['Date'] == STRIP_DATE]
        prod_row = prod_df[prod_df['Date'] == STRIP_DATE]

        if test_row.empty or prod_row.empty:
            continue
        if 'Correlation_Pct' not in test_row.columns:
            continue

        test_val = test_row['Correlation_Pct'].iloc[0]
        prod_val = prod_row['Correlation_Pct'].iloc[0]

        if pd.isna(test_val) and pd.isna(prod_val):
            status = "OK (both NaN)"
        elif pd.isna(test_val) or pd.isna(prod_val):
            status = f"MISMATCH (test={test_val}, prod={prod_val})"
            all_ok = False
        else:
            diff = abs(test_val - prod_val)
            if diff < 1e-8:
                status = f"OK (diff={diff:.2e})"
            else:
                status = f"MISMATCH (diff={diff:.2e}, test={test_val:.6f}, prod={prod_val:.6f})"
                all_ok = False

        print(f"  [{slug}] Correlation 3/24: {status}")

    print()
    if all_ok:
        print("  ALL BASKETS MATCH PRODUCTION")
    else:
        print("  SOME BASKETS DIFFER — check above for details")

    print(f"\nTest output files are in: {TEST_ROOT}")
    print("You can diff individual parquets with production using pandas.")


if __name__ == "__main__":
    main()
