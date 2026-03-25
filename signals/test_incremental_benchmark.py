"""
Benchmark: incremental vs full-recompute for basket correlation and contributions.

Loads real cached data, strips the last N days to simulate an incremental append,
then times both the old (full) and new (incremental) paths and validates correctness.

Usage:
    python test_incremental_benchmark.py [--days 1] [--basket Information_Technology]
"""

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_FOLDER = Path.home() / "Documents" / "Python_Outputs" / "Data_Storage"
RETURNS_MATRIX_PATH = DATA_FOLDER / "returns_matrix_500.parquet"

BASKET_FOLDERS = {
    "sector": DATA_FOLDER / "sector_basket_cache",
    "thematic": DATA_FOLDER / "thematic_basket_cache",
    "industry": DATA_FOLDER / "industry_basket_cache",
}

# ---------------------------------------------------------------------------
# Import functions from rotations.py via runpy (avoid full pipeline execution)
# We'll import just the functions we need by running a targeted extraction.
# ---------------------------------------------------------------------------

# Instead of importing the whole pipeline, we replicate the core math here
# to keep the test isolated and non-destructive.


def _build_quarter_lookup(universe_by_qtr):
    """Build sorted quarter-start arrays for O(log n) bisect lookup."""
    import bisect  # noqa: F811

    def _quarter_start_from_key(key):
        y, q = key.split()
        yr, qn = int(y), int(q.replace("Q", ""))
        month = (qn - 1) * 3 + 1
        return pd.Timestamp(year=yr, month=month, day=1)

    if isinstance(next(iter(universe_by_qtr.keys())), str):
        quarter_keys = [(k, _quarter_start_from_key(k)) for k in universe_by_qtr.keys()]
    else:
        quarter_keys = [(k, k) for k in universe_by_qtr.keys()]
    quarter_keys.sort(key=lambda x: x[1])
    quarter_labels = [k for k, _ in quarter_keys]
    quarter_ends = [dt for _, dt in quarter_keys]
    return quarter_labels, quarter_ends


def _find_active_quarter(d, quarter_labels, quarter_ends):
    import bisect
    idx = bisect.bisect_right(quarter_ends, d) - 1
    if idx < 0:
        return None
    return quarter_labels[idx]


def _quarter_end_from_key(key):
    y, q = key.split()
    yr, qn = int(y), int(q.replace("Q", ""))
    month = qn * 3
    return pd.Timestamp(year=yr, month=month, day=1) + pd.offsets.MonthEnd(0)


def compute_within_basket_correlation_full(universe_by_qtr, returns_matrix, window=21):
    """Full recompute — mirrors _compute_within_basket_correlation in rotations.py."""
    if returns_matrix is None or returns_matrix.empty or not universe_by_qtr:
        return pd.DataFrame(columns=['Date', 'Correlation_Pct'])

    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_qtr)
    min_obs = max(10, int(window * 0.70))
    all_dates, all_corrs = [], []

    for q_idx, q_key in enumerate(quarter_labels):
        tickers = [t for t in universe_by_qtr.get(q_key, set()) if t in returns_matrix.columns]
        if len(tickers) < 2:
            continue
        q_start = quarter_ends[q_idx]
        q_end = quarter_ends[q_idx + 1] if q_idx + 1 < len(quarter_ends) else returns_matrix.index[-1]
        warmup_start_idx = returns_matrix.index.searchsorted(q_start) - window
        warmup_start = returns_matrix.index[max(0, warmup_start_idx)]
        sub_ret = returns_matrix.loc[warmup_start:q_end, tickers]
        if len(sub_ret) < window:
            continue
        q_data = sub_ret.loc[q_start:]
        valid_tickers = [t for t in tickers if q_data[t].notna().sum() >= min_obs]
        if len(valid_tickers) < 2:
            continue
        sub_ret = sub_ret[valid_tickers]
        ret_arr = sub_ret.values
        dates_arr = sub_ret.index
        if q_idx + 1 < len(quarter_ends):
            q_output_end = q_end - pd.Timedelta(days=1)
        else:
            q_output_end = q_end

        for d_idx in range(window, len(dates_arr)):
            d = dates_arr[d_idx]
            if d < q_start or d > q_output_end:
                continue
            w_slice = ret_arr[d_idx - window + 1:d_idx + 1, :]
            col_counts = np.sum(~np.isnan(w_slice), axis=0)
            col_valid = col_counts >= min_obs
            nv = col_valid.sum()
            if nv < 2:
                all_dates.append(d)
                all_corrs.append(np.nan)
                continue
            w = w_slice[:, col_valid]
            means = np.nanmean(w, axis=0)
            stds = np.nanstd(w, axis=0, ddof=1)
            stds[stds == 0] = np.nan
            z = (w - means) / stds
            z_port = np.nanmean(z, axis=1)
            z_valid = z_port[~np.isnan(z_port)]
            if len(z_valid) < min_obs:
                all_dates.append(d)
                all_corrs.append(np.nan)
                continue
            var_z = np.var(z_valid, ddof=1)
            avg_corr = (nv * var_z - 1) / (nv - 1)
            all_dates.append(d)
            all_corrs.append(np.clip(avg_corr * 100, -100, 100))

    if not all_dates:
        return pd.DataFrame(columns=['Date', 'Correlation_Pct'])
    result = pd.DataFrame({'Date': all_dates, 'Correlation_Pct': all_corrs})
    return result.drop_duplicates(subset=['Date'], keep='last').sort_values('Date')


def compute_within_basket_correlation_incremental(universe_by_qtr, returns_matrix, new_dates, window=21):
    """Incremental — mirrors _compute_within_basket_correlation_incremental in rotations.py."""
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
                all_dates.append(d)
                all_corrs.append(np.nan)
            continue
        q_idx = quarter_labels.index(q_key)
        q_start = quarter_ends[q_idx]
        q_data = returns_matrix.loc[q_start:, tickers]
        valid_tickers = [t for t in tickers if q_data[t].notna().sum() >= min_obs]
        if len(valid_tickers) < 2:
            for d in dates_in_q:
                all_dates.append(d)
                all_corrs.append(np.nan)
            continue

        for d in dates_in_q:
            d_idx_in_matrix = returns_matrix.index.searchsorted(d)
            if d_idx_in_matrix >= len(returns_matrix.index) or returns_matrix.index[d_idx_in_matrix] != d:
                all_dates.append(d)
                all_corrs.append(np.nan)
                continue
            start_idx = max(0, d_idx_in_matrix - window + 1)
            w_slice = returns_matrix.iloc[start_idx:d_idx_in_matrix + 1][valid_tickers].values
            if len(w_slice) < min_obs:
                all_dates.append(d)
                all_corrs.append(np.nan)
                continue
            col_counts = np.sum(~np.isnan(w_slice), axis=0)
            col_valid = col_counts >= min_obs
            nv = col_valid.sum()
            if nv < 2:
                all_dates.append(d)
                all_corrs.append(np.nan)
                continue
            w = w_slice[:, col_valid]
            means = np.nanmean(w, axis=0)
            stds = np.nanstd(w, axis=0, ddof=1)
            stds[stds == 0] = np.nan
            z = (w - means) / stds
            z_port = np.nanmean(z, axis=1)
            z_valid = z_port[~np.isnan(z_port)]
            if len(z_valid) < min_obs:
                all_dates.append(d)
                all_corrs.append(np.nan)
                continue
            var_z = np.var(z_valid, ddof=1)
            avg_corr = (nv * var_z - 1) / (nv - 1)
            all_dates.append(d)
            all_corrs.append(np.clip(avg_corr * 100, -100, 100))

    if not all_dates:
        return pd.DataFrame(columns=['Date', 'Correlation_Pct'])
    return pd.DataFrame({'Date': all_dates, 'Correlation_Pct': all_corrs})


def compute_contributions_full(universe_by_qtr, returns_matrix, all_signals_df, quarter_weights):
    """Full recompute — mirrors _compute_and_save_contributions logic."""
    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_qtr)
    quarter_dfs = []

    for q_idx, q_key in enumerate(quarter_labels):
        w_dict = quarter_weights.get(q_key)
        if not w_dict:
            continue
        tickers = [t for t in w_dict if t in returns_matrix.columns]
        if not tickers:
            continue
        q_start = quarter_ends[q_idx]
        if q_idx + 1 < len(quarter_ends):
            q_end = quarter_ends[q_idx + 1] - pd.Timedelta(days=1)
        else:
            q_end = returns_matrix.index[-1]
        rets_q = returns_matrix.loc[q_start:q_end, tickers].copy().fillna(0.0)
        if rets_q.empty:
            continue
        w0 = pd.Series({t: w_dict[t] for t in tickers})
        w0 = w0 / w0.sum()
        cum_growth = (1 + rets_q).cumprod()
        weighted_cum = cum_growth.multiply(w0, axis=1)
        row_sums = weighted_cum.sum(axis=1).replace(0, np.nan)
        drifted_weights = weighted_cum.div(row_sums, axis=0)
        bod_weights = drifted_weights.shift(1)
        bod_weights.iloc[0] = w0
        contributions = bod_weights * rets_q
        bod_long = bod_weights.stack().rename('Weight_BOD')
        ret_long = rets_q.stack().rename('Daily_Return')
        contrib_long = contributions.stack().rename('Contribution')
        q_df = pd.concat([bod_long, ret_long, contrib_long], axis=1).reset_index()
        q_df.columns = ['Date', 'Ticker', 'Weight_BOD', 'Daily_Return', 'Contribution']
        quarter_dfs.append(q_df)

    if not quarter_dfs:
        return pd.DataFrame(columns=['Date', 'Ticker', 'Weight_BOD', 'Daily_Return', 'Contribution'])
    result = pd.concat(quarter_dfs, ignore_index=True)
    result = result.drop_duplicates(subset=['Date', 'Ticker'], keep='last')
    return result.sort_values(['Date', 'Ticker']).reset_index(drop=True)


def compute_contributions_incremental(existing_df, universe_by_qtr, returns_matrix, quarter_weights, new_dates):
    """Incremental — mirrors _compute_and_save_contributions_incremental logic."""
    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_qtr)
    existing_df = existing_df.copy()
    existing_df['Date'] = pd.to_datetime(existing_df['Date']).dt.normalize()
    existing_max_date = existing_df['Date'].max()
    new_dates_norm = sorted(pd.to_datetime(d).normalize() for d in new_dates)

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
                    'Date': d,
                    'Ticker': t,
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
        result = pd.concat([existing_df, new_df], ignore_index=True)
        result = result.drop_duplicates(subset=['Date', 'Ticker'], keep='last')
        return result.sort_values(['Date', 'Ticker']).reset_index(drop=True)
    return existing_df


def build_quarter_weights(all_signals_df, universe_by_qtr):
    """Build per-quarter initial weights (same logic as _compute_and_save_contributions)."""
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
    if 'Dollar_Vol' in df_w.columns:
        dv_q = (
            df_w[['Date', 'Ticker', 'Dollar_Vol']]
            .dropna(subset=['Dollar_Vol'])
            .groupby(['Ticker', pd.Grouper(key='Date', freq='QE-DEC')])['Dollar_Vol']
            .mean()
        )
    else:
        return {}
    for label in quarter_labels:
        if label not in universe_by_qtr or dv_q is None:
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
# Load universe from GICS mappings
# ---------------------------------------------------------------------------

def load_sector_universe(sector_name):
    """Load a sector's universe_by_qtr from the gics_mappings JSON."""
    gics_path = DATA_FOLDER / "gics_mappings_500.json"
    if not gics_path.exists():
        print(f"ERROR: {gics_path} not found")
        sys.exit(1)
    with open(gics_path, 'r') as f:
        data = json.load(f)
    sector_universes = data.get('sector_u', {})
    # Allow underscore-separated names (e.g. Information_Technology -> Information Technology)
    if sector_name not in sector_universes:
        sector_name = sector_name.replace("_", " ")
    if sector_name not in sector_universes:
        print(f"ERROR: Sector '{sector_name}' not found. Available: {list(sector_universes.keys())}")
        sys.exit(1)
    # Convert lists to sets
    return {k: set(v) for k, v in sector_universes[sector_name].items()}


# ---------------------------------------------------------------------------
# Main benchmark
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Benchmark incremental vs full basket recompute")
    parser.add_argument("--days", type=int, default=1, help="Number of days to strip for simulation")
    parser.add_argument("--basket", type=str, default="Information_Technology",
                        help="Basket name (sector) to test")
    args = parser.parse_args()

    print(f"=== Incremental Rebuild Benchmark ===")
    print(f"Basket: {args.basket}, Simulated new days: {args.days}")
    print()

    # 1. Load returns matrix
    print("Loading returns matrix...", end=" ", flush=True)
    t0 = time.perf_counter()
    returns_matrix = pd.read_parquet(RETURNS_MATRIX_PATH)
    returns_matrix.index = pd.to_datetime(returns_matrix.index).normalize()
    print(f"done ({time.perf_counter() - t0:.2f}s, shape={returns_matrix.shape})")

    # 2. Load universe
    print("Loading universe...", end=" ", flush=True)
    universe_by_qtr = load_sector_universe(args.basket)
    print(f"done ({len(universe_by_qtr)} quarters)")

    # 3. Load signals for quarter weights
    print("Loading all_signals_df...", end=" ", flush=True)
    t0 = time.perf_counter()
    all_signals_df = pd.read_parquet(DATA_FOLDER / "signals_500.parquet")
    all_signals_df['Date'] = pd.to_datetime(all_signals_df['Date']).dt.normalize()
    print(f"done ({time.perf_counter() - t0:.2f}s, {len(all_signals_df)} rows)")

    # 4. Build quarter weights
    print("Building quarter weights...", end=" ", flush=True)
    t0 = time.perf_counter()
    quarter_weights = build_quarter_weights(all_signals_df, universe_by_qtr)
    print(f"done ({time.perf_counter() - t0:.2f}s, {len(quarter_weights)} quarters)")

    # 5. Load existing basket signals + contributions
    slug = args.basket.replace(" ", "_")
    # Ensure slug has underscores regardless of input format
    if "_" not in slug and " " in args.basket:
        slug = args.basket.replace(" ", "_")
    signals_path = BASKET_FOLDERS["sector"] / f"{slug}_of_500_signals.parquet"
    contrib_path = BASKET_FOLDERS["sector"] / f"{slug}_of_500_contributions.parquet"

    if not signals_path.exists():
        print(f"ERROR: {signals_path} not found")
        sys.exit(1)

    print("Loading basket signals...", end=" ", flush=True)
    basket_signals = pd.read_parquet(signals_path)
    basket_signals['Date'] = pd.to_datetime(basket_signals['Date']).dt.normalize()
    basket_signals = basket_signals.sort_values('Date').reset_index(drop=True)
    print(f"done ({len(basket_signals)} rows)")

    if contrib_path.exists():
        print("Loading contributions...", end=" ", flush=True)
        existing_contrib = pd.read_parquet(contrib_path)
        existing_contrib['Date'] = pd.to_datetime(existing_contrib['Date']).dt.normalize()
        print(f"done ({len(existing_contrib)} rows)")
    else:
        existing_contrib = None

    # 6. Identify the last N dates to strip
    unique_dates = sorted(basket_signals['Date'].unique())
    if len(unique_dates) < args.days + 1:
        print(f"ERROR: Not enough dates to strip {args.days} days")
        sys.exit(1)

    new_dates = unique_dates[-args.days:]
    cutoff = unique_dates[-(args.days + 1)]
    print(f"\nSimulating: stripping last {args.days} day(s)")
    print(f"  New dates: {[str(d.date()) for d in new_dates]}")
    print(f"  Cutoff:    {cutoff.date()}")

    # 7. Also trim returns matrix to include the new dates (simulates data update)
    # but the "cached" state only knows up to cutoff

    print()
    print("=" * 60)
    print("CORRELATION BENCHMARK")
    print("=" * 60)

    # --- Full recompute ---
    print("\n[FULL] Computing correlation over all dates...", flush=True)
    t0 = time.perf_counter()
    corr_full = compute_within_basket_correlation_full(universe_by_qtr, returns_matrix)
    t_full_corr = time.perf_counter() - t0
    print(f"  Time: {t_full_corr:.4f}s  ({len(corr_full)} rows)")

    # --- Incremental ---
    print(f"\n[INCR] Computing correlation for {args.days} new date(s) only...", flush=True)
    t0 = time.perf_counter()
    corr_incr = compute_within_basket_correlation_incremental(
        universe_by_qtr, returns_matrix, new_dates)
    t_incr_corr = time.perf_counter() - t0
    print(f"  Time: {t_incr_corr:.4f}s  ({len(corr_incr)} rows)")

    # --- Validate ---
    if not corr_full.empty and not corr_incr.empty:
        corr_full_norm = corr_full.copy()
        corr_full_norm['Date'] = pd.to_datetime(corr_full_norm['Date']).dt.normalize()
        corr_incr_norm = corr_incr.copy()
        corr_incr_norm['Date'] = pd.to_datetime(corr_incr_norm['Date']).dt.normalize()

        # Compare only the new dates
        full_new = corr_full_norm[corr_full_norm['Date'].isin(new_dates)].set_index('Date').sort_index()
        incr_new = corr_incr_norm.set_index('Date').sort_index()

        if len(full_new) > 0 and len(incr_new) > 0:
            merged = full_new.join(incr_new, lsuffix='_full', rsuffix='_incr', how='inner')
            if len(merged) > 0:
                diff = (merged['Correlation_Pct_full'] - merged['Correlation_Pct_incr']).abs()
                max_diff = diff.max()
                match = np.allclose(
                    merged['Correlation_Pct_full'].values,
                    merged['Correlation_Pct_incr'].values,
                    atol=1e-10, equal_nan=True)
                print(f"\n  VALIDATION: max_diff={max_diff:.2e}, match={match}")
                for _, row in merged.iterrows():
                    print(f"    {row.name.date()}: full={row['Correlation_Pct_full']:.6f}  incr={row['Correlation_Pct_incr']:.6f}")
            else:
                print("\n  VALIDATION: No overlapping dates to compare")
        else:
            print(f"\n  VALIDATION: Skipped (full_new={len(full_new)}, incr_new={len(incr_new)})")
    else:
        print("\n  VALIDATION: Skipped (empty results)")

    speedup_corr = t_full_corr / t_incr_corr if t_incr_corr > 0 else float('inf')
    print(f"\n  SPEEDUP: {speedup_corr:.1f}x  ({t_full_corr:.4f}s -> {t_incr_corr:.4f}s)")

    print()
    print("=" * 60)
    print("CONTRIBUTIONS BENCHMARK")
    print("=" * 60)

    if existing_contrib is not None:
        # Strip new dates from contributions to simulate cached state
        cached_contrib = existing_contrib[~existing_contrib['Date'].isin(new_dates)].copy()

        # --- Full recompute ---
        print("\n[FULL] Computing contributions over all quarters...", flush=True)
        t0 = time.perf_counter()
        contrib_full = compute_contributions_full(
            universe_by_qtr, returns_matrix, all_signals_df, quarter_weights)
        t_full_contrib = time.perf_counter() - t0
        print(f"  Time: {t_full_contrib:.4f}s  ({len(contrib_full)} rows)")

        # --- Incremental ---
        print(f"\n[INCR] Computing contributions for {args.days} new date(s) only...", flush=True)
        t0 = time.perf_counter()
        contrib_incr = compute_contributions_incremental(
            cached_contrib, universe_by_qtr, returns_matrix, quarter_weights, new_dates)
        t_incr_contrib = time.perf_counter() - t0
        print(f"  Time: {t_incr_contrib:.4f}s  ({len(contrib_incr)} rows)")

        # --- Validate ---
        if not contrib_full.empty and not contrib_incr.empty:
            # Compare only the new dates
            full_new_c = contrib_full[contrib_full['Date'].isin(new_dates)].set_index(['Date', 'Ticker']).sort_index()
            incr_new_c = contrib_incr[contrib_incr['Date'].isin(new_dates)].set_index(['Date', 'Ticker']).sort_index()

            if len(full_new_c) > 0 and len(incr_new_c) > 0:
                merged_c = full_new_c.join(incr_new_c, lsuffix='_full', rsuffix='_incr', how='inner')
                if len(merged_c) > 0:
                    for col in ['Weight_BOD', 'Contribution']:
                        diff_c = (merged_c[f'{col}_full'] - merged_c[f'{col}_incr']).abs()
                        max_diff_c = diff_c.max()
                        match_c = np.allclose(
                            merged_c[f'{col}_full'].values,
                            merged_c[f'{col}_incr'].values,
                            atol=1e-8, equal_nan=True)
                        print(f"\n  VALIDATION ({col}): max_diff={max_diff_c:.2e}, match={match_c}")
                else:
                    print("\n  VALIDATION: No overlapping (Date, Ticker) pairs")
            else:
                print(f"\n  VALIDATION: Skipped (full_new={len(full_new_c)}, incr_new={len(incr_new_c)})")

        speedup_contrib = t_full_contrib / t_incr_contrib if t_incr_contrib > 0 else float('inf')
        print(f"\n  SPEEDUP: {speedup_contrib:.1f}x  ({t_full_contrib:.4f}s -> {t_incr_contrib:.4f}s)")
    else:
        print("\n  Skipped (no contributions file)")

    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Correlation:   {t_full_corr:.4f}s (full) vs {t_incr_corr:.4f}s (incr) = {speedup_corr:.1f}x speedup")
    if existing_contrib is not None:
        print(f"  Contributions: {t_full_contrib:.4f}s (full) vs {t_incr_contrib:.4f}s (incr) = {speedup_contrib:.1f}x speedup")
    print(f"\n  Projected 27-basket savings (correlation alone):")
    print(f"    Full:        {t_full_corr * 27:.1f}s")
    print(f"    Incremental: {t_incr_corr * 27:.1f}s")
    print(f"    Saved:       {(t_full_corr - t_incr_corr) * 27:.1f}s")


if __name__ == "__main__":
    main()
